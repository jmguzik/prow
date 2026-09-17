/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/prow/pkg/config"
	pkgio "sigs.k8s.io/prow/pkg/io"
	"sigs.k8s.io/prow/pkg/io/providers"
)

// The history bucket holds job results that were moved out of a formerly public
// bucket. Deck needs to read it so that job history and PR history stay
// continuous across the migration, but the contents of the artifacts must never
// be served: the whole reason the data moved is that the logs may contain
// credentials. Anyone who wants them has to authenticate to the storage
// provider directly, where the bucket's own IAM applies.
//
// Artifact *names* are a deliberate exception. Listing is what job history is
// built on, so Iterator is not guarded and anyone who can reach a history page
// can enumerate the object names under a run. Only the bytes are withheld.
//
// Deck's credentials can read the whole bucket, so "do not serve artifacts"
// cannot be a property of the UI. handleArtifactView accepts a caller-supplied
// source, which means a hand-written request would otherwise stream any object
// in the bucket straight back out. The rule is therefore enforced in the
// storage layer, by wrapping the single Opener that every Deck code path shares,
// and the UI changes are only there to keep the page coherent.

// The only objects Deck may read from the history bucket are the few that job
// history and PR history need to render a row. Matching them has to be done on
// the whole key rather than the file name: a job controls the names of the
// files it uploads, so a basename test would serve artifacts/started.json or
// artifacts/1.txt straight out of the bucket that was made private to stop
// exactly that.
var (
	// Run metadata sits directly in the numbered build directory. Build IDs are
	// always numeric -- listBuildIDs parses them as uint64, so a run in a
	// differently named directory could not be listed here anyway.
	//
	// prowjob.json is included because it supplies the PR refs column. It also
	// embeds the full PodSpec, so it is readable by explicit decision rather
	// than by oversight: any credential passed to a job as a literal env value
	// is reachable through it.
	historyRunMetadata = regexp.MustCompile(`^([^/]+/)*[0-9]+/(started|finished|prowjob)\.json$`)
	// The latest build marker sits in the job directory, one level above the
	// builds.
	historyLatestBuild = regexp.MustCompile(`^([^/]+/)*latest-build\.txt$`)
	// Presubmit builds are found through "<buildID>.txt" indirection files
	// under <prefix>/directory/<job>/, each holding a gs:// path to the real run
	// directory; resolveSymLink has to read them. Requiring the "directory"
	// segment keeps this from matching a numerically named file a job uploaded.
	historySymlinkFile = regexp.MustCompile(`(^|/)directory/[^/]+/[0-9]+\.txt$`)
)

// historyArtifactsDir is the directory a decorated job's own output is uploaded
// into. Nothing below it can be metadata, whatever it is called.
const historyArtifactsDir = "artifacts"

// historyBucket describes the read-only archive of pre-migration job results.
// It is configured entirely through Deck's flags so that a deployment can turn
// it on without touching the shared Prow config.
type historyBucket struct {
	// name is the bucket name with no scheme, e.g. "prow-history".
	name string
	// storageProvider is "gs" or "s3".
	storageProvider string
	// browserPrefix is prepended to a run path to build the "Artifacts" link.
	// It should point at an endpoint that enforces a login, because the bucket
	// is private and Deck will not serve its contents itself.
	browserPrefix string
	// banner is shown at the top of any Spyglass page served from this bucket.
	banner string
}

const defaultHistoryBanner = "The artifacts for this job were archived to a private bucket and are not served here. Use the Artifacts link to view them; you will be asked to sign in."

// enabled reports whether a history bucket was configured.
func (h *historyBucket) enabled() bool {
	return h != nil && h.name != ""
}

// parseHistoryBucket validates the --history-bucket flag value, which may be
// given as "gs://name", "s3://name" or a bare "name" (assumed to be GCS).
func parseHistoryBucket(value, browserPrefix, banner string) (*historyBucket, error) {
	if value == "" {
		return &historyBucket{}, nil
	}
	if !strings.Contains(value, "://") {
		value = providers.GS + "://" + value
	}
	storageProvider, name, relativePath, err := providers.ParseStoragePath(value)
	if err != nil {
		return nil, fmt.Errorf("invalid --history-bucket %q: %w", value, err)
	}
	if relativePath != "" {
		return nil, fmt.Errorf("invalid --history-bucket %q: expected a bucket, not a path within one", value)
	}
	if browserPrefix == "" {
		return nil, fmt.Errorf("--history-bucket-browser-prefix is required when --history-bucket is set: without it the archived artifacts would be unreachable")
	}
	if banner == "" {
		banner = defaultHistoryBanner
	}
	return &historyBucket{
		name:            name,
		storageProvider: storageProvider,
		browserPrefix:   browserPrefix,
		banner:          banner,
	}, nil
}

// matches reports whether the given bucket name is the history bucket.
func (h *historyBucket) matches(bucketName string) bool {
	return h.enabled() && bucketName == h.name
}

// artifactsLink builds the login-gated link to a run directory in the archive.
func (h *historyBucket) artifactsLink(runPath string) string {
	link := h.browserPrefix + strings.TrimPrefix(runPath, "/")
	if !strings.HasSuffix(link, "/") {
		link += "/"
	}
	return link
}

// viewSource rewrites a Spyglass source key of the form
// "<provider>/<bucket>/<path>" to address the history bucket instead. It
// reports false when there is nothing to rewrite, including when the key
// already points at the archive.
func (h *historyBucket) viewSource(src string) (string, bool) {
	if !h.enabled() {
		return "", false
	}
	parts := strings.SplitN(strings.TrimPrefix(src, "/"), "/", 3)
	if len(parts) < 3 {
		// Not a storage key; "prowjob/<job>/<id>" and friends are resolved
		// against the live cluster and never live in the archive.
		return "", false
	}
	if parts[0] != providers.GS && parts[0] != providers.S3 {
		return "", false
	}
	if parts[1] == h.name {
		return "", false
	}
	return path.Join(h.storageProvider, h.name, parts[2]), true
}

// keepInArchive returns the source to render, given the one that was requested
// and the one symlink resolution produced.
//
// ResolveSymlink takes the bucket from the contents of the "<buildID>.txt"
// indirection file, not from the key that was requested. Those files were
// written before the migration and still name the bucket they were written to,
// so resolving a presubmit link into the archive otherwise lands back outside
// it: the page would then render lenses and link to the public browser prefix
// for a run that only exists in the private bucket.
func (h *historyBucket) keepInArchive(requested, resolved string) string {
	if !h.sourceIsArchived(requested) {
		return resolved
	}
	if target, ok := h.viewSource(resolved); ok {
		return target
	}
	return resolved
}

// sourceIsArchived reports whether a Spyglass source key addresses the archive.
func (h *historyBucket) sourceIsArchived(src string) bool {
	if !h.enabled() {
		return false
	}
	parts := strings.SplitN(strings.TrimPrefix(src, "/"), "/", 3)
	return len(parts) >= 2 && h.matches(parts[1])
}

// spyglassRedirect asks the /view handler to bounce the browser to an equivalent
// page in the history bucket. renderSpyglass discovers this only after it has
// established that the live bucket has nothing, so it cannot simply return the
// other page's HTML: the URL has to change for the artifacts link, the lens
// suppression and any later reload to agree about which bucket is in play.
type spyglassRedirect struct {
	target string
}

func (e spyglassRedirect) Error() string {
	return fmt.Sprintf("redirect to %s", e.target)
}

// readAllowed reports whether Deck may read the object at key, which is a path
// within the history bucket.
func (h *historyBucket) readAllowed(key string) bool {
	key = strings.TrimPrefix(path.Clean(key), "/")
	for _, segment := range strings.Split(key, "/") {
		if segment == historyArtifactsDir {
			return false
		}
	}
	return historyRunMetadata.MatchString(key) ||
		historyLatestBuild.MatchString(key) ||
		historySymlinkFile.MatchString(key)
}

// guardedOpener wraps an Opener and refuses to read anything from the history
// bucket beyond the metadata files history rendering needs. Blocked reads are
// reported as "not found" rather than "forbidden" so that existing callers,
// which already handle a missing artifact, degrade instead of erroring.
type guardedOpener struct {
	pkgio.Opener
	history *historyBucket
}

// newGuardedOpener returns opener unchanged when no history bucket is
// configured, so the wrapper cannot affect a default deployment.
func newGuardedOpener(opener pkgio.Opener, history *historyBucket) pkgio.Opener {
	if !history.enabled() {
		return opener
	}
	return &guardedOpener{Opener: opener, history: history}
}

// blocked reports whether the given storage path addresses an object in the
// history bucket that Deck is not allowed to read. An unparseable path is left
// to the underlying Opener to reject.
func (g *guardedOpener) blocked(storagePath string) bool {
	_, bucket, relativePath, err := providers.ParseStoragePath(storagePath)
	if err != nil {
		return false
	}
	if !g.history.matches(bucket) {
		return false
	}
	return !g.history.readAllowed(relativePath)
}

func (g *guardedOpener) notFound(storagePath string) error {
	return fmt.Errorf("%s is archived in the history bucket and is not served by deck: %w", storagePath, os.ErrNotExist)
}

func (g *guardedOpener) Reader(ctx context.Context, storagePath string) (pkgio.ReadCloser, error) {
	if g.blocked(storagePath) {
		return nil, g.notFound(storagePath)
	}
	return g.Opener.Reader(ctx, storagePath)
}

func (g *guardedOpener) RangeReader(ctx context.Context, storagePath string, offset, length int64) (io.ReadCloser, error) {
	if g.blocked(storagePath) {
		return nil, g.notFound(storagePath)
	}
	return g.Opener.RangeReader(ctx, storagePath, offset, length)
}

func (g *guardedOpener) Attributes(ctx context.Context, storagePath string) (pkgio.Attributes, error) {
	if g.blocked(storagePath) {
		return pkgio.Attributes{}, g.notFound(storagePath)
	}
	return g.Opener.Attributes(ctx, storagePath)
}

// SignedURL would hand the caller a URL that reads the object without passing
// through Deck again, so it is refused for the whole bucket rather than for the
// unreadable objects only. Nothing should be linking into the archive except
// through browserPrefix.
func (g *guardedOpener) SignedURL(ctx context.Context, storagePath string, opts pkgio.SignedURLOptions) (string, error) {
	if _, bucket, _, err := providers.ParseStoragePath(storagePath); err == nil && g.history.matches(bucket) {
		return "", g.notFound(storagePath)
	}
	return g.Opener.SignedURL(ctx, storagePath, opts)
}

// The archive is read-only. Deck never writes to job storage, so these exist to
// keep that true if a future code path tries.
func (g *guardedOpener) Writer(ctx context.Context, storagePath string, opts ...pkgio.WriterOptions) (pkgio.WriteCloser, error) {
	if _, bucket, _, err := providers.ParseStoragePath(storagePath); err == nil && g.history.matches(bucket) {
		return nil, fmt.Errorf("refusing to write to the read-only history bucket %q", bucket)
	}
	return g.Opener.Writer(ctx, storagePath, opts...)
}

func (g *guardedOpener) UpdateAttributes(ctx context.Context, storagePath string, attrs pkgio.ObjectAttrsToUpdate) (*pkgio.Attributes, error) {
	if _, bucket, _, err := providers.ParseStoragePath(storagePath); err == nil && g.history.matches(bucket) {
		return nil, fmt.Errorf("refusing to modify the read-only history bucket %q", bucket)
	}
	return g.Opener.UpdateAttributes(ctx, storagePath, attrs)
}

// historyAwareConfigGetter returns a config.Getter that reports the history
// bucket as an allowed storage bucket.
//
// ValidateStorageBucket rejects any bucket that is not named by a job's
// GCSConfiguration or by deck.additional_allowed_buckets, both of which live in
// the shared Prow config. Since the history bucket is configured by flag, the
// allowance has to be added here instead. The config is copied rather than
// mutated because the config agent replaces it on reload and readers hold it
// without locking; the copy is recomputed only when the agent swaps in a new
// one.
func historyAwareConfigGetter(inner config.Getter, history *historyBucket) config.Getter {
	if !history.enabled() {
		return inner
	}
	var (
		mu     sync.Mutex
		source *config.Config
		result *config.Config
	)
	return func() *config.Config {
		current := inner()
		mu.Lock()
		defer mu.Unlock()
		if current == source {
			return result
		}
		augmented := *current
		augmented.Deck.AllKnownStorageBuckets = current.Deck.AllKnownStorageBuckets.Union(sets.New[string](history.name))
		source = current
		result = &augmented
		return result
	}
}
