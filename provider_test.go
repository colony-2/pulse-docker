package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	provider "github.com/colony-2/pulse-provider-api"
)

func assignment(id string) provider.Launch {
	return provider.Launch{LaunchID: id, Allocation: provider.Allocation{Base: "ubuntu-26*", CPUMillis: 101, MemoryBytes: 8 << 20, ScratchBytes: 4097}, Job: provider.JobRef{JobDB: "http://jobdb/tenant", InstanceID: "db", TenantID: "tenant", JobID: id}, Lease: " synthetic opaque lease\n "}
}
func structuredProvider(t *testing.T, d *daemon) *Provider {
	t.Helper()
	n, s := testProvider(t, d)
	n.cfg.DependencyCache.Disabled = true
	p := wrapNative(n)
	t.Cleanup(s.Close)
	t.Cleanup(p.Close)
	return p
}
func TestConfigCopiesAndValidation(t *testing.T) {
	cfg := Config{ExecutorEnv: map[string]string{"CUSTOM": "value"}, LegacyEndpoints: []LegacyEndpoint{{"db", "tenant", "http://jobdb/tenant"}, {"db", "tenant", "http://jobdb/tenant"}}}
	copied, err := normalizeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ExecutorEnv["CUSTOM"] = "changed"
	cfg.LegacyEndpoints[0].Endpoint = "http://bad/tenant"
	if copied.ExecutorEnv["CUSTOM"] != "value" || copied.LegacyEndpoints[0].Endpoint != "http://jobdb/tenant" {
		t.Fatal("retained mutable configuration")
	}
	if copied.ScratchPath != "/scratch" || copied.DependencyCache.Namespace != "default" || copied.DependencyCache.Generation != "1" || copied.DependencyCache.MaxAge != 30*24*time.Hour {
		t.Fatal(copied)
	}
	for _, cfg := range []Config{
		{CPUMillis: -1}, {MemoryBytes: -1}, {Overhead: -1}, {MaxContainers: -1},
		{ScratchPath: "/"}, {ScratchPath: "/scratch/..", DependencyCache: CacheConfig{Disabled: true}}, {ScratchPath: "relative"}, {ScratchPath: "/nix"}, {ScratchPath: "/var/cache/pulse/sub"},
		{ExecutorEnv: map[string]string{"": "value"}}, {ExecutorEnv: map[string]string{"a=b": "value"}}, {ExecutorEnv: map[string]string{"a": "\x00"}},
		{ExecutorEnv: map[string]string{"C2J_EXECUTION_CPU": "10m"}}, {ExecutorEnv: map[string]string{"C2J_JOBDB": "bad"}}, {ExecutorEnv: map[string]string{"PULSE_FOO": "bad"}},
		{LegacyEndpoints: []LegacyEndpoint{{"db", "tenant", "http://user:pass@jobdb/tenant"}}},
		{LegacyEndpoints: []LegacyEndpoint{{"db", "tenant", "http://jobdb/other"}}},
		{LegacyEndpoints: []LegacyEndpoint{{"db", "tenant", "http://jobdb/tenant"}, {"db", "tenant", "http://other/tenant"}}},
	} {
		if _, err := normalizeConfig(cfg); err == nil {
			t.Fatalf("static validation accepted invalid configuration: %#v", cfg)
		}
		if _, err := New(t.Context(), cfg); err == nil {
			t.Fatalf("accepted invalid configuration: %#v", cfg)
		}
	}
}
func TestLaunchBatchValidationBeforeEffects(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	p := structuredProvider(t, d)
	bad := []provider.Launch{assignment("x")}
	bad[0].Lease = ""
	cases := [][]provider.Launch{nil, bad, {assignment("x"), assignment("x")}}
	for _, mutate := range []func(*provider.Launch){
		func(l *provider.Launch) { l.Lease = provider.Lease(strings.Repeat("x", (1<<20)+1)) },
		func(l *provider.Launch) { l.Lease = "invalid\xff" }, func(l *provider.Launch) { l.Lease = "invalid\x00" },
		func(l *provider.Launch) { l.Base = "invalid base" }, func(l *provider.Launch) { l.CPUMillis = -1 },
		func(l *provider.Launch) { l.Capabilities = []string{"Docker"} }, func(l *provider.Launch) { l.Job.JobDB = "http://credentials@jobdb/tenant" },
		func(l *provider.Launch) { l.TimeoutSeconds = 31536001 },
	} {
		l := assignment("invalid")
		mutate(&l)
		cases = append(cases, []provider.Launch{assignment("valid"), l})
	}
	huge := make([]provider.Launch, 101)
	for i := range huge {
		huge[i] = assignment(fmt.Sprint(i))
	}
	cases = append(cases, huge)
	for _, ls := range cases {
		if out, _, err := p.Submit(t.Context(), ls, provider.SubmitOptions{}); err == nil || len(out) != 0 {
			t.Fatal(out, err)
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.creates) != 0 {
		t.Fatal("invalid batch had effects")
	}
}
func TestLaunchMaximumBatchAndIndependentDeclines(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	p := structuredProvider(t, d)
	ls := make([]provider.Launch, 100)
	for i := range ls {
		ls[i] = assignment(fmt.Sprint(i))
		ls[i].Capabilities = []string{"docker"}
	}
	out, _, err := p.Submit(t.Context(), ls, provider.SubmitOptions{})
	if err != nil || len(out) != 100 {
		t.Fatal(out, err)
	}
	for i, r := range out {
		if r.LaunchID != ls[i].LaunchID || r.Status != provider.Unsupported || r.Reason == "" {
			t.Fatal(r)
		}
	}
	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Hour)
	ls = []provider.Launch{assignment("valid"), assignment("capability"), assignment("unmatched"), assignment("future"), assignment("past"), assignment("timeout"), assignment("oversized")}
	ls[1].Capabilities = []string{"docker"}
	ls[2].Base = "ubuntu-99*"
	ls[3].StartBefore = &future
	ls[4].StartBefore = &past
	ls[5].TimeoutSeconds = 1
	ls[6].CPUMillis = 999999
	before := copyLaunches(ls)
	out, _, err = p.Submit(t.Context(), ls, provider.SubmitOptions{})
	if err != nil || len(out) != len(ls) {
		t.Fatal(out, err)
	}
	want := map[string]provider.Status{"valid": provider.Accepted, "capability": provider.Unsupported, "unmatched": provider.Unsupported, "future": provider.Unsupported, "past": provider.Rejected, "timeout": provider.Unsupported, "oversized": provider.Unsupported}
	for _, r := range out {
		if r.Status != want[r.LaunchID] || (r.Status == provider.Accepted) != (r.Reason == "") {
			t.Fatal(r)
		}
	}
	if !reflect.DeepEqual(ls, before) {
		t.Fatal("arguments changed")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.creates) != 1 {
		t.Fatal("declines created workers", len(d.creates))
	}
}
func TestLeaseExactDeliveryRedactionAndRequestedFacts(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	p := structuredProvider(t, d)
	p.env = map[string]string{"CUSTOM": "literal $HOME"}
	l := assignment("lease")
	out, _, err := p.Submit(t.Context(), []provider.Launch{l}, provider.SubmitOptions{})
	if err != nil || out[0].Status != provider.Accepted {
		t.Fatal(out, err)
	}
	// Synchronize on the daemon's completed attachment, which observes EOF.
	deadline := time.After(3 * time.Second)
	for {
		d.mu.Lock()
		delivered, ok := d.stdin[l.LaunchID]
		d.mu.Unlock()
		if ok {
			if delivered != string(l.Lease) {
				t.Fatal("lease bytes changed")
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("stdin did not reach EOF")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	d.mu.Lock()
	body := d.creates[0]
	data, _ := json.Marshal(body)
	d.mu.Unlock()
	if strings.Contains(string(data), string(l.Lease)) || strings.Contains(fmt.Sprint(out, err), string(l.Lease)) {
		t.Fatal("lease leaked")
	}
	env := envMap(anyStrings(body["Env"]))
	for k, want := range map[string]string{"CUSTOM": "literal $HOME", "C2J_EXECUTION_CPU": "101m", "C2J_EXECUTION_EPHEMERAL_STORAGE": "4.097k", "C2J_EXECUTION_MEMORY": "8192Ki", "C2J_EXECUTION_BASE": "ubuntu-26.04-arm64", "C2J_EXECUTION_CAPABILITIES": "[]"} {
		if env[k] != want {
			t.Fatal(k, env[k], want)
		}
	}
	page, err := p.List(t.Context(), provider.ListRequest{})
	if err != nil || len(page.Items) != 1 || page.Items[0].Job != l.Job {
		t.Fatal(page, err)
	}
	b, _ := json.Marshal(page)
	if strings.Contains(string(b), string(l.Lease)) {
		t.Fatal("lease in listing")
	}
}
func anyStrings(v any) []string {
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		out := []string{}
		for _, v := range s {
			out = append(out, v.(string))
		}
		return out
	}
	return nil
}
func TestCapabilitiesNativeFirstIndependentAndObservational(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	p := structuredProvider(t, d)
	p.platforms = []string{"linux/arm64", "linux/amd64"}
	p.native.uncertain["reserved"] = charge{CPU: 2000, Memory: 4 << 30, Slots: 2}
	a, err := p.GetCapabilities(t.Context())
	if err != nil || a.BasesComplete || len(a.Bases) != 2 || a.Bases[0].Name != "ubuntu-26.04-arm64" || len(a.Bases[0].Capabilities) != 0 {
		t.Fatal(a, err)
	}
	a.Bases[0].Name = "changed"
	a.Bases[0].Capabilities = append(a.Bases[0].Capabilities, "docker")
	b, _ := p.GetCapabilities(t.Context())
	if b.Bases[0].Name == "changed" || len(b.Bases[0].Capabilities) != 0 || len(d.creates) != 0 || len(p.native.uncertain) != 1 {
		t.Fatal("discovery mutated state", b)
	}
}
func TestListLegacyMappingsAndCursor(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	p := structuredProvider(t, d)
	for _, id := range []string{"a", "b", "c"} {
		var c container
		c.ID = id
		c.State.Status = "running"
		c.Config.Labels = metadata(assignment(id))
		delete(c.Config.Labels, "pulse_jobdb")
		d.containers[id] = c
	}
	if _, err := p.List(t.Context(), provider.ListRequest{}); err == nil {
		t.Fatal("missing mapping accepted")
	}
	p.targets["db\x00other"] = "http://wrong/other"
	if _, err := p.List(t.Context(), provider.ListRequest{}); err == nil {
		t.Fatal("cross-tenant mapping accepted")
	}
	p.targets["db\x00tenant"] = "http://legacy/tenant"
	first, err := p.List(t.Context(), provider.ListRequest{PageSize: 1})
	if err != nil || len(first.Items) != 1 || first.Items[0].Job.JobDB != "http://legacy/tenant" || first.NextPageToken == "" {
		t.Fatal(first, err)
	}
	first.Items[0].Refs[0] = "changed"
	last, err := p.List(t.Context(), provider.ListRequest{PageSize: 100, PageToken: first.NextPageToken})
	if err != nil || len(last.Items) != 2 || last.Items[0].ID != "b" {
		t.Fatal(last, err)
	}
	if _, err := p.List(t.Context(), provider.ListRequest{PageToken: first.NextPageToken, LaunchID: "a"}); err == nil {
		t.Fatal("mismatched filter accepted")
	}
	c := d.containers["a"]
	c.Config.Labels["pulse_jobdb"] = "http://explicit/tenant"
	d.containers["a"] = c
	again, err := p.List(t.Context(), provider.ListRequest{PageSize: 1})
	if err != nil || again.Items[0].Refs[0] != "a" || again.Items[0].Job.JobDB != "http://explicit/tenant" {
		t.Fatal(again, err)
	}
}
func TestCloseWaitsForCallsAndRetainsOwnership(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	created := make(chan struct{})
	release := make(chan struct{})
	once := sync.Once{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/containers/create") {
			once.Do(func() { close(created) })
			<-release
		}
		d.serve(w, r)
	}))
	defer s.Close()
	cfg := Config{LockDir: t.TempDir(), DependencyCache: CacheConfig{Disabled: true}}
	n, err := open(t.Context(), cfg, &engine{base: s.URL, client: s.Client()}, true)
	if err != nil {
		t.Fatal(err)
	}
	p := wrapNative(n)
	result := make(chan error, 1)
	go func() {
		out, _, err := p.Submit(t.Context(), []provider.Launch{assignment("entered")}, provider.SubmitOptions{})
		if err == nil && (len(out) != 1 || out[0].Status != provider.Accepted) {
			err = fmt.Errorf("unexpected result: %v", out)
		}
		result <- err
	}()
	<-created
	// Enter another operation and queue at admission before closing begins.
	if err := p.enter(t.Context()); err != nil {
		t.Fatal(err)
	}
	queued := make(chan error, 1)
	go func() {
		defer p.leave()
		_, err := n.Submit(t.Context(), launches(requests("queued")))
		queued <- err
	}()
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	// The lifecycle mutex is the barrier; no timing assumption about Close.
	for !p.isClosing() {
		time.Sleep(time.Millisecond)
	}
	other, err := open(t.Context(), cfg, &engine{base: s.URL, client: s.Client()}, true)
	if err == nil {
		other.Close()
		t.Fatal("ownership released before entered call")
	}
	select {
	case <-closed:
		t.Fatal("Close returned while call active")
	default:
	}
	out, _, err := p.Submit(t.Context(), []provider.Launch{assignment("late")}, provider.SubmitOptions{})
	if !errors.Is(err, errClosed) || len(out) != 1 || out[0].Status != provider.Unavailable {
		t.Fatal(out, err)
	}
	var closes sync.WaitGroup
	for i := 0; i < 10; i++ {
		closes.Add(1)
		go func() { defer closes.Done(); p.Close() }()
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := <-queued; !errors.Is(err, errClosed) {
		t.Fatal("queued mutation escaped closure", err)
	}
	<-closed
	closes.Wait()
	reopened, err := open(t.Context(), cfg, &engine{base: s.URL, client: s.Client()}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if d.containers["entered"].State.Status != "running" {
		t.Fatal("Close stopped worker")
	}
	if _, err := p.GetCapabilities(t.Context()); !errors.Is(err, errClosed) {
		t.Fatal(err)
	}
	if _, err := p.List(t.Context(), provider.ListRequest{}); !errors.Is(err, errClosed) {
		t.Fatal(err)
	}
	if err := p.Maintain(t.Context()); !errors.Is(err, errClosed) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := p.GetCapabilities(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestCanceledAdmissionGateDoesNotDispatch(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	p := structuredProvider(t, d)
	if err := p.native.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	out, _, err := p.Submit(ctx, []provider.Launch{assignment("canceled")}, provider.SubmitOptions{})
	p.native.release()
	if !errors.Is(err, context.Canceled) || len(out) != 0 || len(d.creates) != 0 {
		t.Fatal(out, err)
	}
}
func TestUnknownStartSurvivesClosure(t *testing.T) {
	d := &daemon{containers: map[string]container{}, startError: true}
	p := structuredProvider(t, d)
	out, _, err := p.Submit(t.Context(), []provider.Launch{assignment("uncertain")}, provider.SubmitOptions{})
	if err != nil || len(out) != 1 || out[0].Status != provider.Unknown {
		t.Fatal(out, err)
	}
	p.Close()
	if out[0].Status != provider.Unknown {
		t.Fatal("closure rewrote uncertainty")
	}
}
func TestConstructionFailureReleasesOwnership(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	fail := true
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail && strings.HasSuffix(r.URL.Path, "/containers/json") {
			http.Error(w, "synthetic-secret", 500)
			return
		}
		d.serve(w, r)
	}))
	defer s.Close()
	cfg := Config{LockDir: t.TempDir()}
	n, err := open(t.Context(), cfg, &engine{base: s.URL, client: s.Client()}, true)
	if err == nil {
		n.Close()
		t.Fatal("construction should fail")
	}
	if strings.Contains(err.Error(), "synthetic-secret") {
		t.Fatal("daemon error leaked")
	}
	fail = false
	n, err = open(t.Context(), cfg, &engine{base: s.URL, client: s.Client()}, true)
	if err != nil {
		t.Fatal(err)
	}
	n.Close()
}
