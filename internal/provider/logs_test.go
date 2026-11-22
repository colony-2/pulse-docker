package provider

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/colony-2/pulse-docker/internal/launchlogs"
	api "github.com/colony-2/pulse-provider-api"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func enableArchive(t *testing.T, p *Provider) *launchlogs.Store {
	t.Helper()
	store, err := launchlogs.Open(launchlogs.Config{Directory: t.TempDir(), Owner: "test", Compress: true}, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	p.capture = &captureGroup{store: store, ctx: ctx, cancel: cancel, active: map[string]*captureState{}}
	p.logs = launchlogs.New(store.GetLaunchLog)
	p.native.capture = p.capture
	t.Cleanup(p.Close)
	return store
}
func TestLogsDisabledAndValidationBeforeEffects(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	n, s := testProvider(t, d)
	defer s.Close()
	p := wrapNative(n)
	defer p.Close()
	ls := []api.Launch{assignment("one")}
	for _, options := range []api.SubmitOptions{{Listener: &api.ListenOptions{}}, {Listener: &api.ListenOptions{Context: t.Context(), Duration: -1, OnLog: func(context.Context, api.LaunchLog) error { return nil }}}} {
		if _, _, err := p.Submit(t.Context(), ls, options); err == nil {
			t.Fatal("invalid listener accepted")
		}
	}
	if len(d.creates) != 0 {
		t.Fatal("validation had effects")
	}
	if _, err := p.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one"}); err != api.ErrLaunchLogUnsupported {
		t.Fatal(err)
	}
	results, sub, err := p.Submit(t.Context(), ls, api.SubmitOptions{Listener: &api.ListenOptions{Context: t.Context(), OnLog: func(context.Context, api.LaunchLog) error { t.Error("disabled callback"); return nil }}})
	if err != nil || len(results) != 1 || results[0].Status != api.Accepted || sub == nil {
		t.Fatal(results, sub, err)
	}
	r, _ := sub.Wait(t.Context())
	if r.Reason != api.ListenerSourceUnavailable {
		t.Fatal(r)
	}
}
func nativeFrame(src byte, b []byte) []byte {
	header := make([]byte, 8)
	header[0] = src
	binary.BigEndian.PutUint32(header[4:], uint32(len(b)))
	return append(header, b...)
}
func TestLogsFastCaptureBeforeStartAndAfterRemoval(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	started := make(chan struct{})
	attached := make(chan struct{})
	payload := []byte{0, 255, 128, 10}
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/attach") && r.URL.Query().Get("stdout") == "1" {
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n")
			close(attached)
			rw.Flush()
			<-started
			rw.Write(nativeFrame(1, payload))
			rw.Write(nativeFrame(2, []byte("stderr")))
			rw.Flush()
			return
		}
		if strings.HasSuffix(r.URL.Path, "/start") {
			select {
			case <-attached:
			default:
				t.Error("start preceded output attachment")
			}
			once.Do(func() { close(started) })
		}
		d.serve(w, r)
	}))
	defer server.Close()
	n, err := open(t.Context(), Config{DependencyCache: CacheConfig{Disabled: true}}, &engine{Base: server.URL, Client: server.Client()}, false)
	if err != nil {
		t.Fatal(err)
	}
	p := wrapNative(n)
	store := enableArchive(t, p)
	l := assignment("fast")
	out, sub, err := p.Submit(t.Context(), []api.Launch{l}, api.SubmitOptions{})
	if err != nil || sub != nil || len(out) != 1 || out[0].Status != api.Accepted {
		t.Fatal(out, sub, err)
	}
	deadline := time.Now().Add(time.Second)
	var page api.LaunchLogResponse
	for {
		page, err = store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "fast"})
		if err != nil {
			t.Fatal(err)
		}
		if page.Finalized {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("capture did not close")
		}
		time.Sleep(time.Millisecond)
	}
	d.mu.Lock()
	delete(d.containers, "fast")
	d.mu.Unlock()
	var stdout, stderr []byte
	for _, r := range page.Records {
		if bytes.Contains(r.Data, []byte(l.Lease)) {
			t.Fatal("lease archived")
		}
		switch r.Source {
		case api.LogSourceStdout:
			stdout = append(stdout, r.Data...)
		case api.LogSourceStderr:
			stderr = append(stderr, r.Data...)
		}
	}
	if page.Incomplete || !bytes.Equal(stdout, payload) || string(stderr) != "stderr" {
		t.Fatal(page, stdout, stderr)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stdin["fast"] != string(l.Lease) {
		t.Fatal("lease delivery changed")
	}
}
func TestLogsCaptureFailurePreservesLeaseAdmission(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/attach") && r.URL.Query().Get("stdout") == "1" {
			w.WriteHeader(500)
			io.WriteString(w, "private daemon body")
			return
		}
		d.serve(w, r)
	}))
	defer server.Close()
	n, err := open(t.Context(), Config{DependencyCache: CacheConfig{Disabled: true}}, &engine{Base: server.URL, Client: server.Client()}, false)
	if err != nil {
		t.Fatal(err)
	}
	p := wrapNative(n)
	store := enableArchive(t, p)
	l := assignment("failed-capture")
	out, _, err := p.Submit(t.Context(), []api.Launch{l}, api.SubmitOptions{})
	if err != nil || out[0].Status != api.Accepted {
		t.Fatal(out, err)
	}
	page, err := store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: l.LaunchID})
	if err != nil || !page.Incomplete || !page.Finalized {
		t.Fatal(page, err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stdin[l.LaunchID] != string(l.Lease) || len(d.creates) != 1 {
		t.Fatal("capture failure affected execution")
	}
}
func TestLogsRecoveryVerifiesPrefixWithoutReplay(t *testing.T) {
	for _, prefix := range []string{"prefix", "rotated"} {
		t.Run(prefix, func(t *testing.T) {
			store, err := launchlogs.Open(launchlogs.Config{Directory: t.TempDir(), Owner: "test"}, "daemon")
			if err != nil {
				t.Fatal(err)
			}
			store.Begin(launchlogs.History{LaunchID: "one"})
			store.Append("one", api.LogSourceStdout, []byte("prefix"))
			store.Finish("one", false, true)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || !strings.HasSuffix(r.URL.Path, "/logs") {
					t.Error("recovery had worker effects", r.Method, r.URL)
				}
				w.Write(nativeFrame(1, []byte(time.Now().UTC().Format(time.RFC3339Nano)+" "+prefix+"suffix")))
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			c := &captureGroup{store: store, ctx: ctx, cancel: cancel, active: map[string]*captureState{}}
			c.start(&engine{Base: server.URL, Client: server.Client()}, "one", "container", true)
			c.wg.Wait()
			page, err := store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one"})
			if err != nil || !page.Finalized || !page.Incomplete {
				t.Fatal(page, err)
			}
			var raw []byte
			for _, r := range page.Records {
				raw = append(raw, r.Data...)
			}
			want := "prefix"
			if prefix == "prefix" {
				want += "suffix"
			}
			if string(raw) != want {
				t.Fatal("duplicated or unverified replay", string(raw))
			}
		})
	}
}
