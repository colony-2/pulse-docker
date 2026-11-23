package provider

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/colony-2/pulse-docker/internal/launchlogs"
	api "github.com/colony-2/pulse-provider-api"
)

func TestLogsBoundedCaptureAndQueue(t *testing.T) {
	t.Run("queue_capacity", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			result := make(chan string, 1)
			writes := 0
			go func() {
				input := io.NopCloser(bytes.NewReader(bytes.Repeat(nativeFrame(2, []byte("x")), 512)))
				reason, _ := (&captureGroup{}).readWithAppend(t.Context(), "queue", input, false, func(string, api.LogSource, []byte, time.Time) error {
					writes++
					if writes == 1 {
						close(started)
						<-release
					}
					return nil
				})
				result <- reason
			}()
			<-started
			synctest.Wait()
			close(release)
			if reason := <-result; reason != "queue_capacity" || writes < 64 || writes > 65 {
				t.Fatal(reason, writes)
			}
		})
	})
	t.Run("capture_capacity", func(t *testing.T) {
		daemon := &daemon{containers: map[string]container{}}
		server := httptest.NewServer(http.HandlerFunc(daemon.serve))
		defer server.Close()
		native, err := open(t.Context(), Config{DependencyCache: CacheConfig{Disabled: true}}, &engine{Base: server.URL, Client: server.Client()}, false)
		if err != nil {
			t.Fatal(err)
		}
		provider := wrapNative(native)
		store := enableArchive(t, provider)
		defer provider.Close()
		for slot := range 128 {
			provider.capture.active[fmt.Sprintf("occupied-%d", slot)] = &captureState{}
		}
		const id = "over-capture-capacity"
		out, _, err := provider.Submit(t.Context(), []api.Launch{assignment(id)}, api.SubmitOptions{})
		obs, readErr := store.Observation(id)
		if err != nil || len(out) != 1 || out[0].Status != api.Accepted || readErr != nil || !slices.Contains(obs.History.GapReasons, "capture_capacity") || len(provider.capture.active) != 128 || provider.capture.busy(id) {
			t.Fatal(out, err, obs, readErr)
		}
		daemon.mu.Lock()
		defer daemon.mu.Unlock()
		if len(daemon.creates) != 1 || daemon.containers[id].State.Status != "running" {
			t.Fatal("capture capacity interfered with admission")
		}
	})
}

func TestLogsRetainWorkerUntilReplayRecovers(t *testing.T) {
	for _, reason := range []string{"archive_budget", "native_logs_unavailable"} {
		t.Run(reason, func(t *testing.T) {
			const id, blocker = "retry-native", "budget-blocker"
			const privateBody = "private daemon body must not be emitted"
			var warning bytes.Buffer
			prior := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&warning, nil)))
			defer slog.SetDefault(prior)
			daemon := &daemon{containers: map[string]container{}}
			var available atomic.Bool
			var reads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/attach") && r.URL.Query().Get("stdout") == "1":
					w.WriteHeader(http.StatusServiceUnavailable)
				case strings.HasSuffix(r.URL.Path, "/logs"):
					reads.Add(1)
					if reason == "native_logs_unavailable" && !available.Load() {
						http.Error(w, privateBody, http.StatusServiceUnavailable)
						return
					}
					w.Write(retainedFrame(2, bytes.Repeat([]byte("a"), 1000)))
					w.Write(retainedFrame(2, []byte("final stderr still in Docker\n")))
				default:
					daemon.serve(w, r)
					if strings.HasSuffix(r.URL.Path, "/start") {
						finishFixtureWorker(daemon, id, 7)
					}
				}
			}))
			defer server.Close()
			native, err := open(t.Context(), Config{DependencyCache: CacheConfig{Disabled: true}}, &engine{Base: server.URL, Client: server.Client()}, false)
			if err != nil {
				t.Fatal(err)
			}
			provider := wrapNative(native)
			store := enableTestArchive(t, provider, LoggingConfig{Directory: t.TempDir(), Owner: "test", MaxBytes: 4096})
			defer provider.Close()
			if reason == "archive_budget" {
				if err := store.Begin(launchlogs.History{LaunchID: blocker}); err != nil {
					t.Fatal(err)
				}
				if err := store.Append(blocker, api.LogSourceStdout, bytes.Repeat([]byte("b"), 2400)); err != nil {
					t.Fatal(err)
				}
			}
			out, _, err := provider.Submit(t.Context(), []api.Launch{assignment(id)}, api.SubmitOptions{})
			if err != nil || len(out) != 1 || out[0].Status != api.Accepted {
				t.Fatal(out, err)
			}
			provider.capture.wg.Wait()
			obs, err := store.DurableObservation(id)
			if err != nil || obs.Finalized || !slices.Contains(obs.History.GapReasons, reason) || obs.History.Process == nil || obs.History.Process.ExitCode == nil || *obs.History.Process.ExitCode != 7 {
				t.Fatal(obs, err)
			}
			daemon.mu.Lock()
			worker, exists := daemon.containers[id]
			daemon.mu.Unlock()
			if !exists || worker.State.Status != "exited" || reads.Load() != 1 {
				t.Fatal("worker not retained for replay", exists, worker.State, reads.Load())
			}
			logged := warning.String()
			if !strings.Contains(logged, "level=WARN") || !strings.Contains(logged, "launch="+id+" container="+id+" reason="+reason) || strings.Contains(logged, privateBody) {
				t.Fatal(logged)
			}
			if reason == "archive_budget" {
				page, err := store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: id})
				if err != nil || slices.ContainsFunc(page.Records, func(record api.LaunchLogRecord) bool {
					return bytes.Contains(record.Data, []byte("final stderr still in Docker"))
				}) {
					t.Fatal("fixture did not leave a native-only tail", page, err)
				}
				if err := provider.Maintain(t.Context()); err != nil {
					t.Fatal(err)
				}
				provider.capture.wg.Wait()
				if reads.Load() != 1 {
					t.Fatal("retried native log reads while archive remained over budget", reads.Load())
				}
				if err := store.Complete(blocker); err != nil {
					t.Fatal(err)
				}
			}
			available.Store(true)
			if err := provider.Maintain(t.Context()); err != nil {
				t.Fatal(err)
			}
			provider.capture.wg.Wait()
			page, err := store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: id})
			var workerStderr, diagnostics bytes.Buffer
			for _, record := range page.Records {
				if record.Source == api.LogSourceStderr {
					workerStderr.Write(record.Data)
				} else if record.Source == api.LogSourceLaunch {
					diagnostics.Write(record.Data)
				}
			}
			if err != nil || !page.Finalized || !page.Incomplete || workerStderr.String() != strings.Repeat("a", 1000)+"final stderr still in Docker\n" || !strings.Contains(diagnostics.String(), "code=7") {
				t.Fatal(page.Finalized, page.Incomplete, workerStderr.String(), diagnostics.String(), err)
			}
			obs, err = store.DurableObservation(id)
			daemon.mu.Lock()
			_, exists = daemon.containers[id]
			creates := len(daemon.creates)
			daemon.mu.Unlock()
			if err != nil || obs.Receipt == nil || obs.Receipt.Cleanup != "removed" || exists || creates != 1 || reads.Load() != 2 {
				t.Fatal(obs, err, exists, creates, reads.Load())
			}
		})
	}
}

func TestLogsStartupRecoversInterruptedFirstArchiveWrite(t *testing.T) {
	const id = "interrupted-archive"
	const archive = "1234567890abcdef1234567890abcdef"
	config := LoggingConfig{Directory: t.TempDir(), Owner: "test"}
	dir := filepath.Join(launchlogs.Scope(config.Directory, "daemon", config.Owner), fmt.Sprintf("%x", sha256.Sum256([]byte(id))))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".commit-123456"), []byte(`{"launch_id":`), 0600); err != nil {
		t.Fatal(err)
	}
	worker := container{ID: id}
	autoRemove := false
	worker.HostConfig.AutoRemove = &autoRemove
	worker.HostConfig.RestartPolicy.Name = "no"
	worker.Config.Labels = map[string]string{retentionLabel: retentionMode, retentionScopeLabel: retentionScope(config, "daemon"), retentionArchiveLabel: archive,
		"pulse_managed_by": "pulse", "pulse_launch_id": id, "pulse_process_fingerprint": strings.Repeat("a", 64), roleLabel: "worker", accountingLabel: `{"Slots":1}`}
	daemon := &daemon{containers: map[string]container{id: worker}}
	finishFixtureWorker(daemon, id, 7)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/logs") {
			w.Write(retainedFrame(2, []byte("startup stderr\n")))
			return
		}
		daemon.serve(w, r)
	}))
	defer server.Close()
	native, err := open(t.Context(), Config{DependencyCache: CacheConfig{Disabled: true}}, &engine{Base: server.URL, Client: server.Client()}, false)
	if err != nil {
		t.Fatal(err)
	}
	provider := wrapNative(native)
	store := enableTestArchive(t, provider, config)
	defer provider.Close()
	provider.capture.reconcile(t.Context(), native, true)
	page := awaitFixtureLog(t, store, id)
	provider.capture.wg.Wait()
	obs, err := store.DurableObservation(id)
	if err != nil || obs.Archive != archive || obs.Receipt == nil || obs.Receipt.Phase != "uncertain" || obs.Receipt.Cleanup != "removed" || obs.History.Process == nil || obs.History.Process.ExitCode == nil || *obs.History.Process.ExitCode != 7 {
		t.Fatal(obs, err)
	}
	if !slices.ContainsFunc(page.Records, func(record api.LaunchLogRecord) bool {
		return record.Source == api.LogSourceStderr && string(record.Data) == "startup stderr\n"
	}) {
		t.Fatal(page)
	}
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	if len(daemon.containers) != 0 || len(daemon.creates) != 0 {
		t.Fatal("startup recovery did not remove the original worker without resubmission")
	}
}

func TestLogsExpiredCleanupRequiresDurableIdentity(t *testing.T) {
	config := LoggingConfig{Directory: t.TempDir(), Owner: "owner"}
	store, err := launchlogs.Open(config, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	const id, containerID = "one", "exact-container"
	if err := store.Begin(launchlogs.History{LaunchID: id, ContainerID: containerID}); err != nil {
		t.Fatal(err)
	}
	obs, err := store.Observation(id)
	if err != nil {
		t.Fatal(err)
	}
	scope := retentionScope(config, "daemon")
	hash := strings.Repeat("a", 64)
	labels := map[string]string{retentionLabel: retentionMode, retentionScopeLabel: scope, retentionArchiveLabel: obs.Archive,
		"pulse_managed_by": "pulse", "pulse_launch_id": id, "pulse_process_fingerprint": hash, roleLabel: "worker"}
	status := "exited"
	deleted, deleteFailure := 0, true
	anchor := time.Now().Add(-25 * time.Hour).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/containers/"+containerID+"/json":
			json.NewEncoder(w).Encode(map[string]any{"Id": containerID, "HostConfig": map[string]any{"AutoRemove": false}, "Config": map[string]any{"Labels": labels}, "State": map[string]any{"Status": status, "ExitCode": 7, "StartedAt": anchor.Add(-time.Minute), "FinishedAt": anchor}})
		case r.Method == http.MethodDelete && r.URL.Path == "/containers/"+containerID && r.URL.RawQuery == "":
			deleted++
			if deleteFailure {
				w.WriteHeader(http.StatusConflict)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		default:
			t.Errorf("unexpected Docker effect %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	p := &engineProvider{e: &engine{Base: server.URL, Client: server.Client()}, gate: make(chan struct{}, 1)}
	group := &captureGroup{store: store, ctx: t.Context(), scope: scope}
	for range 2 {
		group.cleanup(p, id, containerID)
	}
	if deleted != 0 {
		t.Fatal("expired native evidence was deleted with no receipt")
	}
	group.store, err = launchlogs.Open(config, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	group.cleanup(p, id, containerID)
	if deleted != 0 {
		t.Fatal("reopen bypassed missing receipt")
	}
	receipt := launchlogs.Receipt{Container: containerID, Fingerprint: hash, Scope: scope, Phase: "accepted"}
	mismatch := receipt
	mismatch.Fingerprint = strings.Repeat("b", 64)
	if err := group.store.RecordReceipt(id, mismatch); err != nil {
		t.Fatal(err)
	}
	group.cleanup(p, id, containerID)
	if deleted != 0 {
		t.Fatal("expired native evidence was deleted with a mismatched receipt")
	}
	if err := group.store.RecordReceipt(id, receipt); err != nil {
		t.Fatal(err)
	}
	status = "running"
	group.cleanup(p, id, containerID)
	status = "exited"
	labels[retentionScopeLabel] = "foreign"
	group.cleanup(p, id, containerID)
	labels[retentionScopeLabel] = scope
	if deleted != 0 {
		t.Fatal("deleted a running or foreign worker")
	}
	if outcome := group.observe(p.e, id, containerID, false); !outcome.saved || !outcome.stopped {
		t.Fatal(outcome)
	}
	group.cleanup(p, id, containerID)
	obs, err = group.store.DurableObservation(id)
	if deleted != 1 || err != nil || obs.Receipt.Cleanup != "pending" {
		t.Fatal(deleted, obs, err)
	}
	deleteFailure = false
	group.cleanup(p, id, containerID)
	obs, err = group.store.DurableObservation(id)
	if deleted != 2 || err != nil || obs.Receipt.Cleanup != "expired" || !obs.Finalized || !slices.Contains(obs.History.GapReasons, "native_retention_expired") || *obs.History.Process.ExitCode != 7 {
		t.Fatal(deleted, obs, err)
	}
}

func TestLogsDamagedArchiveAtGraceExpiry(t *testing.T) {
	for _, scenario := range []struct {
		name           string
		reopen         bool
		readonly       bool
		receiptMissing bool
	}{
		{name: "in_memory_failure"},
		{name: "reopen", reopen: true},
		{name: "write_failure_verified_receipt", reopen: true, readonly: true},
		{name: "write_failure_missing_receipt", reopen: true, readonly: true, receiptMissing: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			const id, workerID = "damaged-capture", "exact-worker"
			config := LoggingConfig{Directory: t.TempDir(), Owner: "owner", Compress: true}
			store, err := launchlogs.Open(config, "daemon")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Begin(launchlogs.History{LaunchID: id, ContainerID: workerID}); err != nil {
				t.Fatal(err)
			}
			initial, err := store.Observation(id)
			if err != nil {
				t.Fatal(err)
			}
			anchor := time.Now().UTC().Add(-retentionGrace)
			scope := retentionScope(config, "daemon")
			receipt := launchlogs.Receipt{Container: workerID, Scope: scope, Fingerprint: strings.Repeat("a", 64), Phase: "accepted", Cleanup: "pending", StoppedAt: &anchor}
			if err := store.RecordReceipt(id, receipt); err != nil {
				t.Fatal(err)
			}
			if scenario.receiptMissing {
				if err := store.ClearReceipt(id); err != nil {
					t.Fatal(err)
				}
			}
			exit := 7
			if err := store.RecordProcess(id, launchlogs.Process{State: "exited", ExitCode: &exit, FinishedAt: &anchor, Source: "inspect", ObservedAt: anchor}); err != nil {
				t.Fatal(err)
			}
			if err := store.Append(id, api.LogSourceStderr, []byte("worker stderr")); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(launchlogs.Scope(config.Directory, "daemon", config.Owner), fmt.Sprintf("%x", sha256.Sum256([]byte(id))))
			segments, err := filepath.Glob(filepath.Join(dir, "*.records"))
			if err != nil || len(segments) != 1 {
				t.Fatal(segments, err)
			}
			segment := segments[0]
			damaged, err := os.ReadFile(segment)
			if err != nil {
				t.Fatal(err)
			}
			damaged[len(damaged)-1] ^= 1
			if err := os.WriteFile(segment, damaged, 0600); err != nil {
				t.Fatal(err)
			}
			if err := store.ResetAppend(id); err == nil {
				t.Fatal("fixture did not damage the committed frame")
			}
			if scenario.reopen {
				store, err = launchlogs.Open(config, "daemon")
				if err != nil {
					t.Fatal(err)
				}
			}
			worker := container{ID: workerID}
			autoRemove := false
			worker.HostConfig.AutoRemove = &autoRemove
			worker.HostConfig.RestartPolicy.Name = "no"
			worker.Config.Labels = map[string]string{
				"pulse_managed_by": "pulse", "pulse_launch_id": id, "pulse_process_fingerprint": receipt.Fingerprint,
				roleLabel: "worker", accountingLabel: `{"slots":1}`,
				retentionLabel: retentionMode, retentionScopeLabel: scope, retentionArchiveLabel: initial.Archive,
			}
			worker.State.Status, worker.State.ExitCode = "exited", exit
			worker.State.StartedAt = anchor.Add(-time.Second).Format(time.RFC3339Nano)
			worker.State.FinishedAt = anchor.Format(time.RFC3339Nano)
			daemon := &daemon{containers: map[string]container{workerID: worker}}
			var deletes atomic.Int32
			var archiveWritable atomic.Bool
			archiveWritable.Store(!scenario.readonly)
			assertCapture := func(obs launchlogs.Observation, err error) {
				t.Helper()
				if err != nil ||
					obs.History.Process == nil || obs.History.Process.ExitCode == nil || *obs.History.Process.ExitCode != exit ||
					obs.Archive != initial.Archive || obs.Receipt == nil || obs.Receipt.Container != workerID ||
					obs.Receipt.Scope != scope || obs.Receipt.Phase != "accepted" || obs.Receipt.Fingerprint != receipt.Fingerprint {
					t.Errorf("invalid durable identity or outcome: %+v (%v)", obs, err)
				}
				if archiveWritable.Load() {
					if !obs.Finalized || !obs.History.Incomplete || obs.History.Capture != "closed" ||
						!slices.Contains(obs.History.GapReasons, "native_retention_expired") || !slices.Contains(obs.History.GapReasons, "storage_unavailable") {
						t.Errorf("missing durable incomplete disposition: %+v", obs)
					}
				} else if obs.Finalized || obs.History.Capture == "closed" {
					t.Errorf("capture was reported closed despite failed writes: %+v", obs)
				}
				if actual, err := os.ReadFile(segment); err != nil || !bytes.Equal(actual, damaged) {
					t.Errorf("committed segment was modified: size=%d want=%d (%v)", len(actual), len(damaged), err)
				}
			}
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				switch {
				case request.Method == http.MethodDelete && request.URL.Path == "/containers/"+workerID && request.URL.RawQuery == "":
					deletes.Add(1)
					assertCapture(store.DurableObservation(id))
				case request.Method != http.MethodGet:
					t.Errorf("unexpected Docker effect: %s %s", request.Method, request.URL)
					response.WriteHeader(http.StatusInternalServerError)
					return
				case strings.HasSuffix(request.URL.Path, "/logs"):
					t.Error("expired native logs should not be re-read")
				}
				daemon.serve(response, request)
			}))
			defer server.Close()
			provider := &engineProvider{e: &engine{Base: server.URL, Client: server.Client()}, gate: make(chan struct{}, 1)}
			group := &captureGroup{store: store, ctx: t.Context(), scope: scope, active: map[string]*captureState{}, now: func() time.Time { return anchor.Add(retentionGrace) }}
			reconcile := func(startup bool) {
				group.reconcile(t.Context(), provider, startup)
				group.wg.Wait()
			}
			if scenario.readonly {
				if err := os.Chmod(dir, 0500); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { os.Chmod(dir, 0700) })
				if probe, err := os.CreateTemp(dir, "write-probe-"); err == nil {
					probe.Close()
					os.Remove(probe.Name())
					t.Skip("test user can bypass directory write permissions")
				} else if !os.IsPermission(err) {
					t.Fatal(err)
				}
				before, err := store.DurableObservation(id)
				if err != nil || (before.Receipt == nil) != scenario.receiptMissing {
					t.Fatal("unexpected receipt before failed write", before, err)
				}
				if scenario.receiptMissing {
					reconcile(false)
					reconcile(false)
					group.store, err = launchlogs.Open(config, "daemon")
					if err != nil {
						t.Fatal(err)
					}
					store = group.store
					reconcile(true)
					blocked, err := store.DurableObservation(id)
					if err != nil || blocked.Receipt != nil || blocked.Finalized || deletes.Load() != 0 {
						t.Fatal("cleanup did not stay blocked without a receipt", deletes.Load(), blocked, err)
					}
					if err := os.Chmod(dir, 0700); err != nil {
						t.Fatal(err)
					}
					if err := store.RecordReceipt(id, receipt); err != nil {
						t.Fatal(err)
					}
					archiveWritable.Store(true)
				} else {
					group.now = func() time.Time { return anchor.Add(retentionGrace - time.Nanosecond) }
					group.cleanup(provider, id, workerID)
					if deletes.Load() != 0 {
						t.Fatal("receipt bypassed the grace period")
					}
					group.now = func() time.Time { return anchor.Add(retentionGrace) }
				}
			}
			reconcile(scenario.reopen)
			reconcile(false)
			final, err := store.DurableObservation(id)
			assertCapture(final, err)
			wantCleanup := "expired"
			if !archiveWritable.Load() {
				wantCleanup = receipt.Cleanup
			}
			if deletes.Load() != 1 || final.Receipt == nil || final.Receipt.Cleanup != wantCleanup {
				t.Fatal("expected exactly one expiry deletion", deletes.Load(), final.Receipt)
			}
			if !archiveWritable.Load() {
				t.Run("storage_restored", func(t *testing.T) {
					if err := os.Chmod(dir, 0700); err != nil {
						t.Fatal(err)
					}
					reopened, err := launchlogs.Open(config, "daemon")
					if err != nil {
						t.Fatal(err)
					}
					group.store = reopened
					archiveWritable.Store(true)
					for _, startup := range []bool{true, false, true} {
						reconcile(startup)
						closed, err := reopened.DurableObservation(id)
						if err != nil || !closed.Finalized || !closed.History.Incomplete || closed.History.Capture != "closed" ||
							!slices.Contains(closed.History.GapReasons, "container_missing") || !slices.Contains(closed.History.GapReasons, "storage_unavailable") ||
							slices.Contains(closed.History.GapReasons, "native_retention_expired") {
							t.Fatalf("missing container must close incomplete without inferring expiry: %+v (%v)", closed, err)
						}
						if process := closed.History.Process; process == nil || process.State != "exited" || process.ExitCode == nil || *process.ExitCode != exit ||
							process.FinishedAt == nil || !process.FinishedAt.Equal(anchor) || process.Source != "inspect" {
							t.Fatalf("lost exit evidence: %+v", process)
						}
						if closed.Archive != initial.Archive || closed.Receipt == nil || closed.Receipt.Container != workerID ||
							closed.Receipt.Fingerprint != receipt.Fingerprint || closed.Receipt.Scope != scope || closed.Receipt.Phase != "accepted" {
							t.Fatalf("lost identity: %+v", closed)
						}
						entries, err := filepath.Glob(filepath.Join(dir, "*.records*"))
						if err != nil || !slices.Equal(entries, []string{segment}) {
							t.Fatalf("unexpected records or compression: %v (%v)", entries, err)
						}
						if actual, err := os.ReadFile(segment); err != nil || !bytes.Equal(actual, damaged) {
							t.Fatalf("damaged evidence changed: size=%d want=%d (%v)", len(actual), len(damaged), err)
						}
						if deletes.Load() != 1 {
							t.Fatalf("unexpected DELETE count: %d", deletes.Load())
						}
					}
				})
			}
			daemon.mu.Lock()
			defer daemon.mu.Unlock()
			if len(daemon.creates) != 0 || len(daemon.containers) != 0 {
				t.Fatal("worker was resubmitted or not removed")
			}
		})
	}
}
