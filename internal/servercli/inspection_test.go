package servercli

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/colony-2/pulse-docker/internal/launchlogs"
	api "github.com/colony-2/pulse-provider-api"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func archiveCLI(t *testing.T) (serverOptions, *launchlogs.Store) {
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
func TestCLIHistoryFilteringAndBinaryLogOffsets(t *testing.T) {
	o, s := archiveCLI(t)
	for _, h := range []launchlogs.History{{LaunchID: "old", TenantID: "t", JobID: "old", SubmittedAt: time.Now().Add(-time.Hour)}, {LaunchID: "new", TenantID: "t", JobID: "job", SubmittedAt: time.Now()}, {LaunchID: "foreign", TenantID: "other", JobID: "job"}} {
		s.Begin(h)
	}
	s.Append("new", api.LogSourceLaunch, []byte("admission: accepted\n"))
	s.Append("new", api.LogSourceStdout, []byte{0, 255, 10})
	s.Append("new", api.LogSourceStderr, []byte("tail"))
	s.Finish("new", true, false)
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
	if json.Unmarshal(stdout.Bytes(), &h) != nil || len(h) != 1 || h[0].LaunchID != "new" {
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
