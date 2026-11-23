package provider

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/pulse-docker/internal/launchlogs"
	api "github.com/colony-2/pulse-provider-api"
)

func enableArchive(t *testing.T, p *Provider) *launchlogs.Store {
	t.Helper()
	return enableTestArchive(t, p, launchlogs.Config{Directory: t.TempDir(), Owner: "test", Compress: true})
}

func enableTestArchive(t *testing.T, p *Provider, logging launchlogs.Config) *launchlogs.Store {
	t.Helper()
	store, err := launchlogs.Open(logging, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	p.capture = &captureGroup{store: store, ctx: ctx, cancel: cancel, active: map[string]*captureState{}}
	p.capture.scope = retentionScope(logging, "daemon")
	p.logs = launchlogs.New(store.GetLaunchLog)
	p.native.capture = p.capture
	t.Cleanup(p.Close)
	return store
}

func finishFixtureWorker(d *daemon, id string, exit int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	c := d.containers[id]
	c.State.Status = "exited"
	c.State.StartedAt = time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)
	c.State.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	c.State.ExitCode = exit
	d.containers[id] = c
}

func awaitFixtureLog(t *testing.T, store *launchlogs.Store, id string) api.LaunchLogResponse {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		page, err := store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: id})
		if err != nil {
			t.Fatal(err)
		}
		if page.Finalized {
			return page
		}
		if time.Now().After(deadline) {
			t.Fatal("capture did not close")
		}
		time.Sleep(time.Millisecond)
	}
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

func retainedFrame(source byte, data []byte) []byte {
	return nativeFrame(source, append([]byte("2026-11-23T00:00:00Z "), data...))
}

func TestLogsFastCaptureBeforeStartAndAfterRemoval(t *testing.T) {
	payload := []byte{0, 255, 128, 10}
	stdout, stderr := nativeFrame(1, payload), nativeFrame(2, []byte("stderr"))
	normalized := nativeFrame(1, []byte("\x00\uFFFD\uFFFD\n"))
	for _, tc := range []struct {
		name       string
		frames     [][]byte
		retained   [][]byte
		incomplete bool
	}{
		{name: "stdout first", frames: [][]byte{stdout, stderr}},
		{name: "stderr first", frames: [][]byte{stderr, stdout}},
		{name: "interleaved fragments", frames: [][]byte{nativeFrame(2, []byte("std")), nativeFrame(1, payload[:2]), nativeFrame(2, []byte("err")), nativeFrame(1, payload[2:])}},
		{name: "JSON-file normalization and reordering", frames: [][]byte{stdout, stderr}, retained: [][]byte{stderr, normalized}},
		{name: "JSON-file mismatch with extra tail", frames: [][]byte{stdout, stderr}, retained: [][]byte{stderr, normalized, nativeFrame(2, []byte("extra"))}, incomplete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &daemon{containers: map[string]container{}}
			started := make(chan struct{})
			attached := make(chan struct{})
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
					for _, frame := range tc.frames {
						rw.Write(frame)
					}
					rw.Flush()
					return
				}
				if strings.HasSuffix(r.URL.Path, "/start") {
					select {
					case <-attached:
					default:
						t.Error("start preceded output attachment")
					}
					d.serve(w, r)
					finishFixtureWorker(d, "fast", 0)
					once.Do(func() { close(started) })
					return
				}
				if strings.HasSuffix(r.URL.Path, "/logs") {
					retained := tc.retained
					if retained == nil {
						retained = tc.frames
					}
					for _, frame := range retained {
						w.Write(retainedFrame(frame[0], frame[8:]))
					}
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
			l := assignment("fast")
			out, sub, err := p.Submit(t.Context(), []api.Launch{l}, api.SubmitOptions{})
			if err != nil || sub != nil || len(out) != 1 || out[0].Status != api.Accepted {
				t.Fatal(out, sub, err)
			}
			page := awaitFixtureLog(t, store, "fast")
			obs, err := store.Observation("fast")
			if err != nil || obs.History.Process == nil || obs.History.Process.ExitCode == nil || *obs.History.Process.ExitCode != 0 {
				t.Fatal(obs, err)
			}
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
			if page.Incomplete != tc.incomplete || !bytes.Equal(stdout, payload) || string(stderr) != "stderr" {
				t.Fatal(page, stdout, stderr)
			}
			p.capture.wg.Wait()
			results, _, err := p.Submit(t.Context(), []api.Launch{l}, api.SubmitOptions{})
			if err != nil || results[0].Status != api.Accepted {
				t.Fatal("accepted receipt was not honored", results, err)
			}
			conflict := l
			conflict.Job.JobID = "different"
			results, _, err = p.Submit(t.Context(), []api.Launch{conflict}, api.SubmitOptions{})
			if err != nil || results[0].Status != api.Rejected {
				t.Fatal("receipt did not reject conflict", results, err)
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			if d.stdin["fast"] != string(l.Lease) || len(d.creates) != 1 || len(d.containers) != 0 {
				t.Fatal("lease delivery changed")
			}
		})
	}
}

func TestLogsObservationOutlivesSubmitAndListener(t *testing.T) {
	const id = "independent-observation"
	daemon := &daemon{containers: map[string]container{}}
	finished := make(chan struct{})
	waiting := make(chan struct{})
	var once sync.Once
	var waitCanceled atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/attach") && r.URL.Query().Get("stdout") == "1":
			w.WriteHeader(http.StatusServiceUnavailable)
		case strings.HasSuffix(r.URL.Path, "/wait"):
			once.Do(func() { close(waiting) })
			select {
			case <-finished:
				json.NewEncoder(w).Encode(map[string]int{"StatusCode": 7})
			case <-r.Context().Done():
				waitCanceled.Store(true)
			}
		case strings.HasSuffix(r.URL.Path, "/logs"):
			w.Write(retainedFrame(2, []byte("after listener stop\n")))
		default:
			daemon.serve(w, r)
		}
	}))
	defer server.Close()
	native, err := open(t.Context(), Config{DependencyCache: CacheConfig{Disabled: true}}, &engine{Base: server.URL, Client: server.Client()}, false)
	if err != nil {
		t.Fatal(err)
	}
	provider := wrapNative(native)
	store := enableArchive(t, provider)
	defer provider.Close()
	submitCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	out, listener, err := provider.Submit(submitCtx, []api.Launch{assignment(id)}, api.SubmitOptions{Listener: &api.ListenOptions{
		Context: t.Context(), OnLog: func(context.Context, api.LaunchLog) error { return nil },
	}})
	if err != nil || len(out) != 1 || out[0].Status != api.Accepted || listener == nil {
		t.Fatal(out, listener, err)
	}
	select {
	case <-waiting:
	case <-submitCtx.Done():
		t.Fatal("exit observation did not start before Submit deadline")
	}
	listener.Stop()
	if result, err := listener.Wait(t.Context()); err != nil || result.Reason != api.ListenerStopped {
		t.Fatal(result, err)
	}
	<-submitCtx.Done()
	if submitCtx.Err() != context.DeadlineExceeded || waitCanceled.Load() {
		t.Fatal("Submit deadline canceled Docker wait", submitCtx.Err())
	}
	obs, err := store.Observation(id)
	if err != nil || obs.Finalized || obs.History.Process == nil || obs.History.Process.State != "running" {
		t.Fatal(obs, err)
	}
	finishFixtureWorker(daemon, id, 7)
	close(finished)
	page := awaitFixtureLog(t, store, id)
	provider.capture.wg.Wait()
	obs, err = store.DurableObservation(id)
	if err != nil || waitCanceled.Load() || obs.History.Process == nil || obs.History.Process.ExitCode == nil || *obs.History.Process.ExitCode != 7 || obs.Receipt == nil || obs.Receipt.Cleanup != "removed" {
		t.Fatal(obs, err, waitCanceled.Load())
	}
	if !slices.ContainsFunc(page.Records, func(record api.LaunchLogRecord) bool {
		return record.Source == api.LogSourceStderr && string(record.Data) == "after listener stop\n"
	}) {
		t.Fatal(page)
	}
}

func TestLogsTerminalCaptureRegressions(t *testing.T) {
	for _, scenario := range []string{"eof_before_exit", "retry_create_rejection", "stalled_wait"} {
		t.Run(scenario, func(t *testing.T) {
			const id = "capture-regression"
			daemon := &daemon{containers: map[string]container{}}
			finished := make(chan struct{})
			started := make(chan struct{})
			attachEnded := make(chan struct{})
			var createAttempts, waitAttempts, logReads atomic.Int32
			var waitCanceled atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				path := request.URL.Path
				switch {
				case strings.HasSuffix(path, "/containers/create") && scenario == "retry_create_rejection" && createAttempts.Add(1) == 1:
					http.Error(response, "invalid create", http.StatusBadRequest)
				case strings.HasSuffix(path, "/"+id+"/attach") && request.URL.Query().Get("stdout") == "1":
					conn, stream, err := response.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					stream.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n")
					stream.Flush()
					releaseOutput := finished
					if scenario == "eof_before_exit" {
						releaseOutput = started
					}
					select {
					case <-releaseOutput:
					case <-time.After(5 * time.Second):
						return
					}
					stream.Write(nativeFrame(1, []byte("stdout\n")))
					if scenario != "eof_before_exit" {
						stream.Write(nativeFrame(2, []byte("stderr tail\n")))
					}
					stream.Flush()
					if scenario == "eof_before_exit" {
						conn.Close()
						close(attachEnded)
					}
				case strings.HasSuffix(path, "/"+id+"/start"):
					daemon.serve(response, request)
					if scenario == "eof_before_exit" {
						close(started)
						select {
						case <-attachEnded:
						case <-request.Context().Done():
							return
						}
					}
					if scenario != "stalled_wait" {
						finishFixtureWorker(daemon, id, 7)
						close(finished)
					}
				case strings.HasSuffix(path, "/"+id+"/wait"):
					if scenario != "stalled_wait" {
						<-finished
					} else if waitAttempts.Add(1) == 1 {
						response.WriteHeader(http.StatusOK)
						response.(http.Flusher).Flush()
						<-request.Context().Done()
						waitCanceled.Store(true)
						return
					} else {
						finishFixtureWorker(daemon, id, 7)
						close(finished)
					}
					io.WriteString(response, `{"StatusCode":7}`)
				case strings.HasSuffix(path, "/"+id+"/logs"):
					logReads.Add(1)
					response.Write(retainedFrame(1, []byte("stdout\n")))
					response.Write(retainedFrame(2, []byte("stderr tail\n")))
				default:
					daemon.serve(response, request)
				}
			}))
			defer server.Close()
			native, err := open(t.Context(), Config{DependencyCache: CacheConfig{Disabled: true}}, &engine{Base: server.URL, Client: server.Client()}, false)
			if err != nil {
				t.Fatal(err)
			}
			provider := wrapNative(native)
			config := launchlogs.Config{Directory: t.TempDir(), Owner: "test", Compress: true}
			store := enableTestArchive(t, provider, config)
			defer provider.Close()
			provider.capture.waitTimeout = 100 * time.Millisecond
			launch := assignment(id)
			submit := func(want api.Status) {
				t.Helper()
				out, _, err := provider.Submit(t.Context(), []api.Launch{launch}, api.SubmitOptions{})
				if err != nil || len(out) != 1 || out[0].Status != want {
					t.Fatalf("wanted %s: %v, %v", want, out, err)
				}
			}
			var priorPage api.LaunchLogResponse
			priorRequest := api.LaunchLogRequest{LaunchID: id, PageSize: 1}
			if scenario == "retry_create_rejection" {
				submit(api.Rejected)
				first, err := store.GetLaunchLog(t.Context(), priorRequest)
				if err != nil || !first.Finalized || !first.HasMore {
					t.Fatal(first, err)
				}
				priorRequest.PageToken = first.NextPageToken
				priorPage, err = store.GetLaunchLog(t.Context(), priorRequest)
				if err != nil || !priorPage.Finalized || len(priorPage.Records) != 1 || string(priorPage.Records[0].Data) != "admission: rejected\n" {
					t.Fatal(priorPage, err)
				}
			}
			submit(api.Accepted)
			page := awaitFixtureLog(t, store, id)
			var stdout, stderr, diagnostic []byte
			for _, record := range page.Records {
				switch record.Source {
				case api.LogSourceStdout:
					stdout = append(stdout, record.Data...)
				case api.LogSourceStderr:
					stderr = append(stderr, record.Data...)
				case api.LogSourceLaunch:
					diagnostic = append(diagnostic, record.Data...)
				}
			}
			observation, err := store.Observation(id)
			if err != nil || page.Incomplete != (scenario == "eof_before_exit") || observation.History.Admission != "accepted" ||
				string(stdout) != "stdout\n" || string(stderr) != "stderr tail\n" || !bytes.Contains(diagnostic, []byte("Docker worker exited: code=7,")) || logReads.Load() != 1 {
				t.Fatalf("unexpected terminal log: %+v; stdout=%q stderr=%q launch=%q reads=%d err=%v", observation.History, stdout, stderr, diagnostic, logReads.Load(), err)
			}
			if scenario == "stalled_wait" && (!waitCanceled.Load() || waitAttempts.Load() != 2) {
				t.Fatal("stalled wait was not canceled and retried", waitCanceled.Load(), waitAttempts.Load())
			}
			provider.capture.wg.Wait()
			if scenario == "retry_create_rejection" {
				reopened, err := launchlogs.Open(config, "daemon")
				if err != nil {
					t.Fatal(err)
				}
				old, err := reopened.GetLaunchLog(t.Context(), priorRequest)
				if err != nil || !reflect.DeepEqual(old, priorPage) || createAttempts.Load() != 2 {
					t.Fatal("rejected capture changed after retry and reopen", old, err)
				}
			}
			daemon.mu.Lock()
			defer daemon.mu.Unlock()
			if len(daemon.containers) != 0 || len(daemon.creates) != 1 {
				t.Fatal("expected exactly one worker, removed by the provider")
			}
		})
	}
}

func TestLogsCaptureFailurePreservesLeaseAdmission(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	leaseReceived := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/attach") && r.URL.Query().Get("stdout") == "1" {
			w.WriteHeader(500)
			io.WriteString(w, "private daemon body")
			return
		}
		if strings.HasSuffix(r.URL.Path, "/logs") {
			w.Write(nativeFrame(2, []byte("2026-11-23T00:00:00Z native stderr\n")))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/attach") && r.URL.Query().Get("stdin") == "1" {
			defer close(leaseReceived)
		}
		d.serve(w, r)
		if strings.HasSuffix(r.URL.Path, "/start") {
			finishFixtureWorker(d, "failed-capture", 7)
		}
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
	page := awaitFixtureLog(t, store, l.LaunchID)
	observed, err := store.Observation(l.LaunchID)
	if err != nil || !page.Incomplete || observed.History.Process == nil || observed.History.Process.ExitCode == nil || *observed.History.Process.ExitCode != 7 {
		t.Fatal(page, observed, err)
	}
	// Submit finishes writing before the daemon handler necessarily observes EOF.
	// Wait for the fixture to record the complete lease before asserting its bytes.
	select {
	case <-leaseReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("daemon fixture did not finish receiving lease")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stdin[l.LaunchID] != string(l.Lease) || len(d.creates) != 1 {
		t.Fatal("capture failure affected execution")
	}
}
func TestLogsRecoveryVerifiesPrefixWithoutReplay(t *testing.T) {
	for _, prefix := range []string{"prefix", "rotated", "pre", ""} {
		t.Run(prefix, func(t *testing.T) {
			store, err := launchlogs.Open(launchlogs.Config{Directory: t.TempDir(), Owner: "test"}, "daemon")
			if err != nil {
				t.Fatal(err)
			}
			store.Begin(launchlogs.History{LaunchID: "one"})
			store.Append("one", api.LogSourceStdout, []byte("prefix"))
			store.Finish("one", false, true)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/json"):
					json.NewEncoder(w).Encode(map[string]any{"Id": "container", "State": map[string]any{"Status": "exited", "ExitCode": 7, "StartedAt": "2026-11-23T00:00:00Z"}})
				case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/logs"):
					text := prefix
					if prefix == "prefix" || prefix == "rotated" {
						text += "suffix"
					}
					if text != "" {
						w.Write(nativeFrame(1, []byte(time.Now().UTC().Format(time.RFC3339Nano)+" "+text)))
					}
				default:
					t.Error("recovery had worker effects", r.Method, r.URL)
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			c := &captureGroup{store: store, ctx: ctx, cancel: cancel, active: map[string]*captureState{}}
			p := &engineProvider{e: &engine{Base: server.URL, Client: server.Client()}, gate: make(chan struct{}, 1)}
			c.start(p, "one", "container", true, true)
			c.wg.Wait()
			page, err := store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one"})
			if err != nil || !page.Finalized || !page.Incomplete {
				t.Fatal(page, err)
			}
			var raw []byte
			for _, r := range page.Records {
				if r.Source == api.LogSourceStdout {
					raw = append(raw, r.Data...)
				}
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
