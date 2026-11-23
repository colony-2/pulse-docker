package servercli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/pulse-docker/internal/launchlogs"
	api "github.com/colony-2/pulse-provider-api"
)

func archiveCLI(t *testing.T) (serverOptions, *launchlogs.Store) {
	return archiveCLIWithReadSpy(t, nil)
}

func archiveCLIWithReadSpy(t *testing.T, hits *atomic.Int32) (serverOptions, *launchlogs.Store) {
	t.Helper()
	dir, err := os.MkdirTemp("", "cli-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "docker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			hits.Add(1)
		}
		if r.Method != "GET" {
			t.Error("inspection mutated daemon")
		}
		if r.URL.Path == "/version" {
			json.NewEncoder(w).Encode(map[string]string{"ApiVersion": "1.49"})
		} else if r.URL.Path == "/v1.49/info" {
			json.NewEncoder(w).Encode(map[string]string{"ID": "daemon"})
		} else {
			w.WriteHeader(500)
		}
	}))
	server.Listener.Close()
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	o := serverOptions{owner: "test", poll: time.Millisecond, logging: launchlogs.Config{Directory: t.TempDir(), Owner: "test", Compress: true}, transport: transportOptions{startup: time.Second}, limit: 100}
	o.config.Socket = "unix://" + socket
	s, err := launchlogs.Open(o.logging, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	return o, s
}

func TestCLIHistoryLogStreamValidationAndEmptyStderr(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	var hits atomic.Int32
	o, store := archiveCLIWithReadSpy(t, &hits)
	const id = "nonzero-empty-stderr"
	if err := store.Begin(launchlogs.History{LaunchID: id}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(id, api.LogSourceStdout, []byte("only stdout\n")); err != nil {
		t.Fatal(err)
	}
	exit := 7
	if err := store.RecordProcess(id, launchlogs.Process{State: "exited", ExitCode: &exit, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(id); err != nil {
		t.Fatal(err)
	}
	run := func(stream string, extra ...string) (string, string, error) {
		cmd := NewCommand("test")
		var output, diagnostic bytes.Buffer
		cmd.SetOut(&output)
		cmd.SetErr(&diagnostic)
		cmd.SetArgs(append([]string{"logs", id, "--stream", stream, "--socket", o.config.Socket, "--owner", o.owner, "--log-directory", o.logging.Directory}, extra...))
		err := cmd.ExecuteContext(t.Context())
		return output.String(), diagnostic.String(), err
	}
	if output, diagnostic, err := run("invalid", "--config", filepath.Join(t.TempDir(), "missing.json")); err == nil || err.Error() != "--stream must be all, stdout, or stderr" || output != "" || diagnostic != "" || hits.Load() != 0 {
		t.Fatal(output, diagnostic, err, hits.Load())
	}
	if output, diagnostic, err := run("stderr"); err != nil || output != "" || !strings.Contains(diagnostic, "code=7") || hits.Load() == 0 {
		t.Fatal(output, diagnostic, err, hits.Load())
	}
}

type logFollowWriter func([]byte) (int, error)

func (writer logFollowWriter) Write(data []byte) (int, error) { return writer(data) }

func TestCLIHistoryFollowReportsEventualSummaryAndGaps(t *testing.T) {
	o, store := archiveCLI(t)
	const id = "follow-worker"
	const first = "before exit\n"
	const second = "after exit\n"
	if err := store.Begin(launchlogs.History{LaunchID: id}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(id, api.LogSourceStderr, []byte(first)); err != nil {
		t.Fatal(err)
	}
	var output, diagnostics bytes.Buffer
	o.out = logFollowWriter(func(data []byte) (int, error) {
		if output.Len() == 0 {
			before, err := store.Observation(id)
			if err != nil || before.Finalized || before.History.Process != nil {
				t.Fatal(before, err)
			}
			exit := 7
			if err := errors.Join(store.Append(id, api.LogSourceStderr, []byte(second)), store.Gap(id, "output_ended"),
				store.Gap(id, "native_logs_unavailable"), store.RecordProcess(id, launchlogs.Process{State: "exited", ExitCode: &exit, ObservedAt: time.Now()})); err != nil {
				t.Fatal(err)
			}
			if err := store.Complete(id); err != nil {
				t.Fatal(err)
			}
		}
		return output.Write(data)
	})
	o.log = &diagnostics
	o.follow = true
	o.stream = "stderr"
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := logs(ctx, o, []string{id})
	if err == nil || err.Error() != "retained capture is incomplete or interrupted" || output.String() != first+second || !strings.Contains(diagnostics.String(), "capture gaps: [output_ended native_logs_unavailable]") || strings.Count(diagnostics.String(), "Docker worker exited: code=7") != 1 {
		t.Fatal(output.String(), diagnostics.String(), err)
	}
}
func TestCLIHistoryFilteringAndBinaryLogOffsets(t *testing.T) {
	o, s := archiveCLI(t)
	for _, h := range []launchlogs.History{{LaunchID: "old", TenantID: "t", JobID: "old", SubmittedAt: time.Now().Add(-time.Hour)}, {LaunchID: "new", TenantID: "t", JobID: "job", SubmittedAt: time.Now()}, {LaunchID: "foreign", TenantID: "other", JobID: "job"}} {
		s.Begin(h)
	}
	s.Append("new", api.LogSourceLaunch, []byte("admission: accepted\n"))
	s.Append("new", api.LogSourceStdout, []byte{0, 255, 10})
	s.Append("new", api.LogSourceStderr, []byte("tail"))
	exit, oom := 7, false
	if err := s.RecordProcess("new", launchlogs.Process{State: "exited", ExitCode: &exit, OOMKilled: &oom, ObservedAt: time.Now(), Source: "inspect", Reason: "nonzero_exit"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete("new"); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	o.out = &stdout
	o.log = &stderr
	o.json = true
	o.tenant = "t"
	o.job = "job"
	if err := launches(t.Context(), o, nil); err != nil {
		t.Fatal(err)
	}
	var h []launchlogs.History
	if json.Unmarshal(stdout.Bytes(), &h) != nil || len(h) != 1 || h[0].LaunchID != "new" || h[0].Process == nil || h[0].Process.ExitCode == nil || *h[0].Process.ExitCode != 7 {
		t.Fatal(stdout.String())
	}
	stdout.Reset()
	o.json = false
	o.offset = 1
	if err := logs(t.Context(), o, []string{"new"}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stdout.Bytes(), append([]byte{255, 10}, []byte("tail")...)) || !bytes.Contains(stderr.Bytes(), []byte("admission: accepted")) {
		t.Fatal(stdout.Bytes(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	o.stream = "stderr"
	o.offset = 4
	if err := logs(t.Context(), o, []string{"new"}); err != nil || stdout.String() != "ail" || !bytes.Contains(stderr.Bytes(), []byte("code=7")) {
		t.Fatal(err, stdout.String(), stderr.String())
	}
	stdout.Reset()
	if err := launches(t.Context(), o, []string{"new"}); err != nil || !bytes.Contains(stdout.Bytes(), []byte("nonzero_exit")) || !bytes.Contains(stdout.Bytes(), []byte("Process observed at")) {
		t.Fatal(err, stdout.String())
	}
	stdout.Reset()
	o.offset = 100
	if err := logs(t.Context(), o, []string{"new"}); err == nil {
		t.Fatal("beyond retained offset accepted")
	}
	s.Finish("old", false, true)
	o.offset = 0
	if err := logs(context.Background(), o, []string{"old"}); err == nil {
		t.Fatal("interrupted empty logs reported success")
	}
}

func TestLaunchesSinceDurationAndTimestamp(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	o, s := archiveCLI(t)
	now := time.Now()
	for _, h := range []launchlogs.History{
		{LaunchID: "old", SubmittedAt: now.Add(-48 * time.Hour)},
		{LaunchID: "recent", SubmittedAt: now.Add(-time.Hour)},
		{LaunchID: "unknown"},
	} {
		if err := s.Begin(h); err != nil {
			t.Fatal(err)
		}
	}
	for _, since := range []string{"24h", "90m", now.Add(-24 * time.Hour).Format(time.RFC3339)} {
		t.Run(since, func(t *testing.T) {
			cmd := NewCommand("test")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{"launches", "--socket", o.config.Socket, "--log-directory", o.logging.Directory, "--owner", o.owner, "--since", since, "--json"})
			if err := cmd.Execute(); err != nil {
				t.Fatal(err)
			}
			var history []launchlogs.History
			if err := json.Unmarshal(out.Bytes(), &history); err != nil || len(history) != 1 || history[0].LaunchID != "recent" {
				t.Fatalf("since %q: history=%+v error=%v output=%s", since, history, err, out.String())
			}
		})
	}
}
