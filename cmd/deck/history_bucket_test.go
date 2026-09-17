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
	stdio "io"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"k8s.io/apimachinery/pkg/util/sets"

	prowapi "sigs.k8s.io/prow/pkg/apis/prowjobs/v1"

	"sigs.k8s.io/prow/pkg/config"
	pkgio "sigs.k8s.io/prow/pkg/io"
)

func TestParseHistoryBucket(t *testing.T) {
	cases := []struct {
		name          string
		value         string
		browserPrefix string
		banner        string
		want          historyBucket
		wantErr       bool
	}{
		{
			name: "unset flag disables the feature",
			want: historyBucket{},
		},
		{
			name:          "a bare bucket name is assumed to be GCS",
			value:         "prow-history",
			browserPrefix: "https://storage.cloud.google.com/prow-history/",
			want: historyBucket{
				name:            "prow-history",
				storageProvider: "gs",
				browserPrefix:   "https://storage.cloud.google.com/prow-history/",
				banner:          defaultHistoryBanner,
			},
		},
		{
			name:          "an explicit gs:// scheme is accepted",
			value:         "gs://prow-history",
			browserPrefix: "https://storage.cloud.google.com/prow-history/",
			want: historyBucket{
				name:            "prow-history",
				storageProvider: "gs",
				browserPrefix:   "https://storage.cloud.google.com/prow-history/",
				banner:          defaultHistoryBanner,
			},
		},
		{
			name:          "s3 is accepted",
			value:         "s3://prow-history",
			browserPrefix: "https://example.com/prow-history/",
			banner:        "gone fishing",
			want: historyBucket{
				name:            "prow-history",
				storageProvider: "s3",
				browserPrefix:   "https://example.com/prow-history/",
				banner:          "gone fishing",
			},
		},
		{
			name:          "a path within a bucket is rejected",
			value:         "gs://prow-history/logs",
			browserPrefix: "https://storage.cloud.google.com/prow-history/",
			wantErr:       true,
		},
		{
			// Without it the archived artifacts would have no reachable link at
			// all, which is worse than not enabling the feature.
			name:    "a browser prefix is required",
			value:   "gs://prow-history",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseHistoryBucket(tc.value, tc.browserPrefix, tc.banner)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseHistoryBucket() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if *got != tc.want {
				t.Errorf("parseHistoryBucket() = %+v, want %+v", *got, tc.want)
			}
			if want := tc.value != ""; got.enabled() != want {
				t.Errorf("enabled() = %v, want %v", got.enabled(), want)
			}
		})
	}
}

func TestHistoryBucketArtifactsLink(t *testing.T) {
	h := &historyBucket{name: "prow-history", storageProvider: "gs", browserPrefix: "https://storage.cloud.google.com/prow-history/"}
	cases := []struct {
		runPath string
		want    string
	}{
		{runPath: "logs/job/123", want: "https://storage.cloud.google.com/prow-history/logs/job/123/"},
		// RunPath is inconsistent about a leading slash, and doubling it would
		// address a different object.
		{runPath: "/logs/job/123", want: "https://storage.cloud.google.com/prow-history/logs/job/123/"},
		{runPath: "logs/job/123/", want: "https://storage.cloud.google.com/prow-history/logs/job/123/"},
	}
	for _, tc := range cases {
		if got := h.artifactsLink(tc.runPath); got != tc.want {
			t.Errorf("artifactsLink(%q) = %q, want %q", tc.runPath, got, tc.want)
		}
	}
}

func TestHistoryBucketViewSource(t *testing.T) {
	h := &historyBucket{name: "prow-history", storageProvider: "gs", browserPrefix: "https://example.com/"}
	cases := []struct {
		name         string
		history      *historyBucket
		src          string
		want         string
		wantOK       bool
		wantArchived bool
	}{
		{
			name:    "a live bucket key is rewritten to the archive",
			history: h,
			src:     "gs/kubernetes-jenkins/logs/job/123",
			want:    "gs/prow-history/logs/job/123",
			wantOK:  true,
		},
		{
			name:    "a leading slash is tolerated",
			history: h,
			src:     "/gs/kubernetes-jenkins/logs/job/123",
			want:    "gs/prow-history/logs/job/123",
			wantOK:  true,
		},
		{
			name:         "a key already in the archive is not rewritten again",
			history:      h,
			src:          "gs/prow-history/logs/job/123",
			wantArchived: true,
		},
		{
			// These are resolved against the cluster, not storage, so there is
			// nothing in the archive to fall back to.
			name:    "a prowjob key is left alone",
			history: h,
			src:     "prowjob/some-job/123",
		},
		{
			name:    "a key too short to name an object is left alone",
			history: h,
			src:     "gs/kubernetes-jenkins",
		},
		{
			name:    "a disabled history bucket rewrites nothing",
			history: &historyBucket{},
			src:     "gs/kubernetes-jenkins/logs/job/123",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := tc.history.viewSource(tc.src)
			if ok != tc.wantOK || got != tc.want {
				t.Errorf("viewSource(%q) = %q, %v; want %q, %v", tc.src, got, ok, tc.want, tc.wantOK)
			}
			if archived := tc.history.sourceIsArchived(tc.src); archived != tc.wantArchived {
				t.Errorf("sourceIsArchived(%q) = %v, want %v", tc.src, archived, tc.wantArchived)
			}
		})
	}
}

func TestHistoryBucketReadAllowed(t *testing.T) {
	h := &historyBucket{name: "prow-history", storageProvider: "gs", browserPrefix: "https://example.com/"}
	cases := map[string]bool{
		// What history rendering actually needs.
		"logs/job/latest-build.txt":                     true,
		"logs/job/123/started.json":                     true,
		"logs/job/123/finished.json":                    true,
		"logs/job/123/prowjob.json":                     true,
		"pr-logs/directory/job/latest-build.txt":        true,
		"pr-logs/directory/job/1254406011708510.txt":    true,
		"pr-logs/pull/org/repo/99/job/123/started.json": true,
		// The reason the bucket is private in the first place.
		"logs/job/123/build-log.txt":          false,
		"logs/job/123/artifacts/junit_01.xml": false,
		// A job names the files it uploads, so matching on the file name alone
		// would hand back job output that happens to be called the right thing.
		"logs/job/123/artifacts/started.json":      false,
		"logs/job/123/artifacts/prowjob.json":      false,
		"logs/job/123/artifacts/latest-build.txt":  false,
		"logs/job/123/artifacts/1.txt":             false,
		"logs/job/123/artifacts/e2e/finished.json": false,
		"logs/job/123/artifacts/456/started.json":  false,
		"logs/job/123/artifacts/directory/j/1.txt": false,
		// Run metadata is only metadata where a run directory can be: the
		// build ID is numeric everywhere history can display it.
		"logs/job/not-a-build-id/started.json": false,
		// Only the presubmit indirection files are numerically named.
		"logs/job/123/1.txt": false,
	}
	for key, want := range cases {
		if got := h.readAllowed(key); got != want {
			t.Errorf("readAllowed(%q) = %v, want %v", key, got, want)
		}
	}
}

func TestSearchOrderPutsArchiveLast(t *testing.T) {
	history := &historyBucket{name: "prow-history", storageProvider: "gs", browserPrefix: "https://example.com/"}
	toSearch := map[string]sets.Set[string]{
		"gs://prow-history":       nil,
		"gs://kubernetes-jenkins": nil,
		"gs://another-bucket":     nil,
	}
	want := []string{"gs://another-bucket", "gs://kubernetes-jenkins", "gs://prow-history"}
	// Map order is random, so run it enough times that an accidental dependency
	// on it would show up.
	for i := 0; i < 20; i++ {
		if got := searchOrder(toSearch, history); !reflect.DeepEqual(got, want) {
			t.Fatalf("searchOrder() = %v, want %v", got, want)
		}
	}
	if got := searchOrder(toSearch, &historyBucket{}); len(got) != 3 {
		t.Errorf("searchOrder() with no history bucket = %v, want all three buckets", got)
	}
}

// recordingOpener records what reached the underlying storage. The embedded
// interface is nil, so any method the guard is expected to intercept panics
// rather than silently passing through.
type recordingOpener struct {
	pkgio.Opener
	reads   []string
	signed  []string
	writes  []string
	updates []string
}

func (o *recordingOpener) Reader(_ context.Context, path string) (pkgio.ReadCloser, error) {
	o.reads = append(o.reads, path)
	return stdio.NopCloser(strings.NewReader("content")), nil
}

func (o *recordingOpener) RangeReader(_ context.Context, path string, _, _ int64) (stdio.ReadCloser, error) {
	o.reads = append(o.reads, path)
	return stdio.NopCloser(strings.NewReader("content")), nil
}

func (o *recordingOpener) Attributes(_ context.Context, path string) (pkgio.Attributes, error) {
	o.reads = append(o.reads, path)
	return pkgio.Attributes{}, nil
}

func (o *recordingOpener) SignedURL(_ context.Context, path string, _ pkgio.SignedURLOptions) (string, error) {
	o.signed = append(o.signed, path)
	return "https://signed/" + path, nil
}

func (o *recordingOpener) Writer(_ context.Context, path string, _ ...pkgio.WriterOptions) (pkgio.WriteCloser, error) {
	o.writes = append(o.writes, path)
	return nil, nil
}

func (o *recordingOpener) UpdateAttributes(_ context.Context, path string, _ pkgio.ObjectAttrsToUpdate) (*pkgio.Attributes, error) {
	o.updates = append(o.updates, path)
	return nil, nil
}

func TestGuardedOpenerLeavesDefaultDeploymentAlone(t *testing.T) {
	inner := &recordingOpener{}
	if got := newGuardedOpener(inner, &historyBucket{}); got != pkgio.Opener(inner) {
		t.Errorf("newGuardedOpener() wrapped the opener even though no history bucket is configured")
	}
}

func TestGuardedOpenerReader(t *testing.T) {
	history := &historyBucket{name: "prow-history", storageProvider: "gs", browserPrefix: "https://example.com/"}
	cases := []struct {
		name        string
		path        string
		wantBlocked bool
	}{
		{
			name: "metadata in the archive is readable",
			path: "gs://prow-history/logs/job/123/started.json",
		},
		{
			name: "the symlink indirection in the archive is readable",
			path: "gs://prow-history/pr-logs/directory/job/123.txt",
		},
		{
			name:        "a log in the archive is not",
			path:        "gs://prow-history/logs/job/123/build-log.txt",
			wantBlocked: true,
		},
		{
			name:        "nor is an artifact in the archive",
			path:        "gs://prow-history/logs/job/123/artifacts/junit_01.xml",
			wantBlocked: true,
		},
		{
			name: "a log in any other bucket is untouched",
			path: "gs://kubernetes-jenkins/logs/job/123/build-log.txt",
		},
		{
			// A bucket whose name merely starts with the archive's must not be
			// caught by it.
			name: "a bucket with a similar name is untouched",
			path: "gs://prow-history-2/logs/job/123/build-log.txt",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, read := range []struct {
				kind string
				call func(pkgio.Opener) error
			}{
				{"Reader", func(o pkgio.Opener) error { _, err := o.Reader(context.Background(), tc.path); return err }},
				{"RangeReader", func(o pkgio.Opener) error {
					_, err := o.RangeReader(context.Background(), tc.path, 0, 10)
					return err
				}},
				{"Attributes", func(o pkgio.Opener) error { _, err := o.Attributes(context.Background(), tc.path); return err }},
			} {
				inner := &recordingOpener{}
				err := read.call(newGuardedOpener(inner, history))
				if tc.wantBlocked {
					if err == nil {
						t.Errorf("%s(%q) succeeded, want it blocked", read.kind, tc.path)
					} else if !pkgio.IsNotExist(err) {
						// Callers already cope with a missing artifact, and
						// reporting it any other way turns pages into errors.
						t.Errorf("%s(%q) error = %v, want it to read as not-exist", read.kind, tc.path, err)
					}
					if len(inner.reads) != 0 {
						t.Errorf("%s(%q) reached storage: %v", read.kind, tc.path, inner.reads)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s(%q) unexpected error: %v", read.kind, tc.path, err)
				}
				if len(inner.reads) != 1 {
					t.Errorf("%s(%q) did not reach storage", read.kind, tc.path)
				}
			}
		})
	}
}

func TestGuardedOpenerRefusesWholeArchiveForNonReads(t *testing.T) {
	history := &historyBucket{name: "prow-history", storageProvider: "gs", browserPrefix: "https://example.com/"}
	// A signed URL reads the object without passing through deck again, so even
	// the metadata files that deck itself may read are refused.
	archived := "gs://prow-history/logs/job/123/started.json"
	live := "gs://kubernetes-jenkins/logs/job/123/started.json"

	inner := &recordingOpener{}
	guarded := newGuardedOpener(inner, history)
	if _, err := guarded.SignedURL(context.Background(), archived, pkgio.SignedURLOptions{}); err == nil {
		t.Error("SignedURL for the archive succeeded, want it refused")
	}
	if _, err := guarded.SignedURL(context.Background(), live, pkgio.SignedURLOptions{}); err != nil {
		t.Errorf("SignedURL for the live bucket failed: %v", err)
	}
	if len(inner.signed) != 1 || inner.signed[0] != live {
		t.Errorf("signed URLs reached storage for %v, want only %q", inner.signed, live)
	}

	if _, err := guarded.Writer(context.Background(), archived); err == nil {
		t.Error("Writer for the archive succeeded, want it refused")
	}
	if _, err := guarded.UpdateAttributes(context.Background(), archived, pkgio.ObjectAttrsToUpdate{}); err == nil {
		t.Error("UpdateAttributes for the archive succeeded, want it refused")
	}
	if len(inner.writes) != 0 || len(inner.updates) != 0 {
		t.Errorf("a mutation reached the read-only archive: writes=%v updates=%v", inner.writes, inner.updates)
	}
}

func TestHistoryAwareConfigGetter(t *testing.T) {
	history := &historyBucket{name: "prow-history", storageProvider: "gs", browserPrefix: "https://example.com/"}
	original := &config.Config{
		ProwConfig: config.ProwConfig{
			Deck: config.Deck{
				AllKnownStorageBuckets: sets.New[string]("kubernetes-jenkins"),
			},
		},
	}
	current := original
	getter := historyAwareConfigGetter(func() *config.Config { return current }, history)

	got := getter()
	if !got.Deck.AllKnownStorageBuckets.Has(history.name) {
		t.Errorf("history bucket %q is not allowed: %v", history.name, sets.List(got.Deck.AllKnownStorageBuckets))
	}
	if !got.Deck.AllKnownStorageBuckets.Has("kubernetes-jenkins") {
		t.Error("the configured buckets were dropped")
	}
	// The config agent hands the same Config to every reader without locking,
	// so augmenting it in place would be a data race.
	if original.Deck.AllKnownStorageBuckets.Has(history.name) {
		t.Error("the shared config was mutated")
	}
	if again := getter(); again != got {
		t.Error("the augmented config was rebuilt for an unchanged source config")
	}

	// A config reload must not leave the old copy in place.
	current = &config.Config{
		ProwConfig: config.ProwConfig{
			Deck: config.Deck{
				AllKnownStorageBuckets: sets.New[string]("some-other-bucket"),
			},
		},
	}
	reloaded := getter()
	if reloaded == got {
		t.Fatal("the augmented config was not recomputed after a reload")
	}
	if !reloaded.Deck.AllKnownStorageBuckets.HasAll("some-other-bucket", history.name) {
		t.Errorf("reloaded config has %v", sets.List(reloaded.Deck.AllKnownStorageBuckets))
	}

	// A disabled history bucket must not interpose at all.
	plain := historyAwareConfigGetter(func() *config.Config { return original }, &historyBucket{})
	if plain() != original {
		t.Error("a disabled history bucket still copied the config")
	}
}

func historyObject(bucket, name, content string) fakestorage.Object {
	return fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: bucket, Name: name},
		Content:     []byte(content),
	}
}

// Test_getJobHistoryMergesHistoryBucket covers the point of the feature: a job
// whose older runs were moved to the archive still shows one continuous page.
func Test_getJobHistoryMergesHistoryBucket(t *testing.T) {
	const (
		live    = "kubernetes-jenkins"
		archive = "prow-history"
		root    = "logs/some-job"
	)
	started := func(ts int) string { return fmt.Sprintf(`{"timestamp": %d}`, ts) }
	finished := func(ts int) string { return fmt.Sprintf(`{"timestamp": %d,"passed": true,"result": "SUCCESS"}`, ts) }

	gcsServer := fakestorage.NewServer([]fakestorage.Object{
		historyObject(live, root+"/latest-build.txt", "300"),
		historyObject(live, root+"/300/started.json", started(300)),
		historyObject(live, root+"/300/finished.json", finished(360)),
		// Also present in the archive: during the migration a run can be copied
		// before the original is removed.
		historyObject(archive, root+"/300/started.json", started(300)),
		historyObject(archive, root+"/300/finished.json", finished(360)),
		// latest-build.txt in the archive is stale by definition.
		historyObject(archive, root+"/latest-build.txt", "200"),
		historyObject(archive, root+"/200/started.json", started(200)),
		historyObject(archive, root+"/200/finished.json", finished(260)),
		historyObject(archive, root+"/100/started.json", started(100)),
		historyObject(archive, root+"/100/finished.json", finished(160)),
	})
	defer gcsServer.Stop()

	history := &historyBucket{name: archive, storageProvider: "gs", browserPrefix: "https://example.com/"}
	// Validation is left on and the archive is deliberately absent from the
	// allowed list: the deployment this is written for cannot change the shared
	// prow config, so the flag alone has to be enough.
	validate := false
	inner := func() *config.Config {
		return &config.Config{
			ProwConfig: config.ProwConfig{
				Deck: config.Deck{
					SkipStoragePathValidation: &validate,
					AllKnownStorageBuckets:    sets.New[string](live),
				},
			},
		}
	}

	jobURL, err := url.Parse("https://prow.k8s.io/job-history/gs/" + live + "/" + root)
	if err != nil {
		t.Fatalf("failed to parse url: %v", err)
	}

	t.Run("without the config getter the archive is rejected", func(t *testing.T) {
		_, err := getJobHistory(context.Background(), jobURL, inner, pkgio.NewGCSOpener(gcsServer.Client()), history)
		if err == nil {
			t.Fatal("expected the unaugmented config to reject the history bucket")
		}
	})

	got, err := getJobHistory(context.Background(), jobURL, historyAwareConfigGetter(inner, history), pkgio.NewGCSOpener(gcsServer.Client()), history)
	if err != nil {
		t.Fatalf("getJobHistory() error: %v", err)
	}
	want := jobHistoryTemplate{
		Name:         root,
		ResultsShown: 3,
		ResultsTotal: 3,
		Builds: []buildData{
			{
				index:        0,
				SpyglassLink: "/view/gs/" + live + "/" + root + "/300",
				ID:           "300",
				Started:      time.Unix(300, 0),
				Duration:     60 * time.Second,
				Result:       "SUCCESS",
				commitHash:   "Unknown",
			},
			{
				index:        1,
				SpyglassLink: "/view/gs/" + archive + "/" + root + "/200",
				ID:           "200",
				Started:      time.Unix(200, 0),
				Duration:     60 * time.Second,
				Result:       "SUCCESS",
				commitHash:   "Unknown",
			},
			{
				index:        2,
				SpyglassLink: "/view/gs/" + archive + "/" + root + "/100",
				ID:           "100",
				Started:      time.Unix(100, 0),
				Duration:     60 * time.Second,
				Result:       "SUCCESS",
				commitHash:   "Unknown",
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("getJobHistory() =\n%+v\nwant\n%+v", got, want)
	}
}

// Test_getPRHistoryDedupesAcrossBuckets covers the window in which a run has
// been copied to the archive but not yet deleted from the live bucket: it is
// listed by both, and must still occupy one cell.
func Test_getPRHistoryDedupesAcrossBuckets(t *testing.T) {
	const (
		live    = "kubernetes-jenkins"
		archive = "prow-history"
		prefix  = "pr-logs/pull/test-infra/17183/pull-test-infra-bazel"
	)
	started := func(ts int) string { return fmt.Sprintf(`{"timestamp": %d}`, ts) }
	finished := func(ts int) string { return fmt.Sprintf(`{"timestamp": %d,"passed": true,"result": "SUCCESS"}`, ts) }

	gcsServer := fakestorage.NewServer([]fakestorage.Object{
		historyObject(live, prefix+"/300/started.json", started(300)),
		historyObject(live, prefix+"/300/finished.json", finished(360)),
		// The same run, already copied across.
		historyObject(archive, prefix+"/300/started.json", started(300)),
		historyObject(archive, prefix+"/300/finished.json", finished(360)),
		// And one that only exists in the archive.
		historyObject(archive, prefix+"/100/started.json", started(100)),
		historyObject(archive, prefix+"/100/finished.json", finished(160)),
	})
	defer gcsServer.Stop()

	history := &historyBucket{name: archive, storageProvider: "gs", browserPrefix: "https://example.com/"}
	validate := false
	inner := func() *config.Config {
		return &config.Config{
			JobConfig: config.JobConfig{
				PresubmitsStatic: map[string][]config.Presubmit{
					"kubernetes/test-infra": {{JobBase: config.JobBase{
						Name: "pull-test-infra-bazel",
						UtilityConfig: config.UtilityConfig{
							DecorationConfig: &prowapi.DecorationConfig{
								GCSConfiguration: &prowapi.GCSConfiguration{
									Bucket:       live,
									PathStrategy: prowapi.PathStrategyLegacy,
									DefaultOrg:   "kubernetes",
								},
							},
						},
					}}},
				},
			},
			ProwConfig: config.ProwConfig{
				Deck: config.Deck{
					SkipStoragePathValidation: &validate,
					AllKnownStorageBuckets:    sets.New[string](live),
				},
			},
		}
	}

	prHistoryURL, err := url.Parse("https://prow.k8s.io/pr-history/?org=kubernetes&repo=test-infra&pr=17183")
	if err != nil {
		t.Fatalf("failed to parse url: %v", err)
	}
	cfg := historyAwareConfigGetter(inner, history)
	got, err := getPRHistory(context.Background(), prHistoryURL, cfg(), pkgio.NewGCSOpener(gcsServer.Client()), nil, nil, "github.com", history)
	if err != nil {
		t.Fatalf("getPRHistory() error: %v", err)
	}

	if len(got.Jobs) != 1 {
		t.Fatalf("got %d job rows, want 1: %+v", len(got.Jobs), got.Jobs)
	}
	job := got.Jobs[0]
	if want := "/job-history/gs/" + live + "/pr-logs/directory/pull-test-infra-bazel"; job.Link != want {
		t.Errorf("job link = %q, want the live bucket's %q", job.Link, want)
	}
	links := map[string]string{}
	for _, build := range job.Builds {
		if _, duplicate := links[build.ID]; duplicate {
			t.Errorf("build %s appears more than once", build.ID)
		}
		links[build.ID] = build.SpyglassLink
	}
	want := map[string]string{
		"300": "/view/gs/" + live + "/" + prefix + "/300",
		"100": "/view/gs/" + archive + "/" + prefix + "/100",
	}
	if !reflect.DeepEqual(links, want) {
		t.Errorf("builds =\n%v\nwant\n%v", links, want)
	}
	if len(got.Commits) != 1 || got.Commits[0].MaxWidth != 2 {
		t.Errorf("commits = %+v, want a single column two builds wide", got.Commits)
	}
}

func TestHistoryBucketKeepInArchive(t *testing.T) {
	h := &historyBucket{name: "prow-history", storageProvider: "gs", browserPrefix: "https://example.com/"}
	cases := []struct {
		name      string
		history   *historyBucket
		requested string
		resolved  string
		want      string
	}{
		{
			// The symlink files copied into the archive still say
			// "gs://kubernetes-jenkins/..." because that is where they were
			// written. Following that verbatim would leave the archive.
			name:      "a symlink resolved out of the archive is pulled back in",
			history:   h,
			requested: "gs/prow-history/pr-logs/directory/job/123",
			resolved:  "gs/kubernetes-jenkins/pr-logs/pull/org/repo/99/job/123",
			want:      "gs/prow-history/pr-logs/pull/org/repo/99/job/123",
		},
		{
			name:      "a resolution that stayed in the archive is untouched",
			history:   h,
			requested: "gs/prow-history/pr-logs/directory/job/123",
			resolved:  "gs/prow-history/pr-logs/pull/org/repo/99/job/123",
			want:      "gs/prow-history/pr-logs/pull/org/repo/99/job/123",
		},
		{
			name:      "a request against the live bucket is untouched",
			history:   h,
			requested: "gs/kubernetes-jenkins/pr-logs/directory/job/123",
			resolved:  "gs/kubernetes-jenkins/pr-logs/pull/org/repo/99/job/123",
			want:      "gs/kubernetes-jenkins/pr-logs/pull/org/repo/99/job/123",
		},
		{
			name:      "no history bucket changes nothing",
			history:   &historyBucket{},
			requested: "gs/prow-history/pr-logs/directory/job/123",
			resolved:  "gs/kubernetes-jenkins/pr-logs/pull/org/repo/99/job/123",
			want:      "gs/kubernetes-jenkins/pr-logs/pull/org/repo/99/job/123",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.history.keepInArchive(tc.requested, tc.resolved)
			if got != tc.want {
				t.Errorf("keepInArchive(%q, %q) = %q, want %q", tc.requested, tc.resolved, got, tc.want)
			}
			// Whatever it returns has to agree with the flag that drives lens
			// suppression and the artifacts link.
			if archived := tc.history.sourceIsArchived(got); archived != tc.history.sourceIsArchived(tc.want) {
				t.Errorf("sourceIsArchived(%q) = %v, inconsistent with the expected source", got, archived)
			}
		})
	}
}
