// Package docker executes caller-assigned c2j leases using a local Docker daemon.
// It implements the shared Provider contract without importing Pulse.
package docker

import (
	"context"
	"errors"
	"fmt"
	"github.com/colony-2/pulse-docker/internal/bases"
	compute "github.com/colony-2/pulse-docker/internal/native"
	"github.com/colony-2/pulse-docker/internal/providerutil"
	provider "github.com/colony-2/pulse-provider-api"
	"maps"
	"sync"
)

var errClosed = errors.New("Docker provider is closed")

// Provider owns admission to one daemon. Construct it with New; do not copy it.
// Close waits for entered calls without stopping accepted workers.
type Provider struct {
	native    *engineProvider
	platforms []string
	env       map[string]string
	targets   map[string]string
	mu        sync.Mutex
	closing   bool
	active    sync.WaitGroup
	done      chan struct{}
}

var _ provider.Provider = (*Provider)(nil)

// New validates and copies configuration, discovers the daemon, acquires local
// admission ownership and probes its support. The context bounds initialization.
func New(ctx context.Context, cfg Config) (*Provider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	native, err := newNative(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return wrapNative(native), nil
}
func wrapNative(n *engineProvider) *Provider {
	p := &Provider{native: n, env: maps.Clone(n.cfg.ExecutorEnv), targets: map[string]string{}, done: make(chan struct{}), platforms: []string{n.platform}}
	for _, platform := range n.SupportedPlatforms() {
		if platform != n.platform {
			p.platforms = append(p.platforms, platform)
		}
	}
	for _, e := range n.cfg.LegacyEndpoints {
		p.targets[e.InstanceID+"\x00"+e.TenantID] = e.Endpoint
	}
	n.closing = p.isClosing
	return p
}
func (p *Provider) isClosing() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.closing }
func (p *Provider) enter(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing || p.native == nil {
		return errClosed
	}
	p.active.Add(1)
	return nil
}
func (p *Provider) leave() { p.active.Done() }

// Close rejects new calls, waits for entered calls, then releases local ownership
// and idle connections. Concurrent callers wait for the same cleanup.
func (p *Provider) Close() {
	p.mu.Lock()
	if p.closing {
		done := p.done
		p.mu.Unlock()
		<-done
		return
	}
	p.closing = true
	if p.done == nil {
		p.done = make(chan struct{})
	}
	done := p.done
	p.mu.Unlock()
	p.active.Wait()
	if p.native != nil {
		p.native.Close()
	}
	close(done)
}

// GetCapabilities returns a fresh native-first, partial base catalog. It does not
// query availability, pull images, reconcile accounting or perform maintenance.
func (p *Provider) GetCapabilities(ctx context.Context) (provider.Capabilities, error) {
	if err := p.enter(ctx); err != nil {
		return provider.Capabilities{}, err
	}
	defer p.leave()
	out := provider.Capabilities{Bases: []provider.BaseCapabilities{}}
	for _, b := range bases.Catalog(p.platforms...) {
		out.Bases = append(out.Bases, provider.BaseCapabilities{Name: b.Name, Capabilities: []string{}})
	}
	return out, nil
}
func metadata(l provider.Launch) map[string]string {
	return map[string]string{"pulse_metadata_version": "1", "pulse_managed_by": "pulse", "pulse_jobdb_instance_id": l.Job.InstanceID, "pulse_tenant_id": l.Job.TenantID, "pulse_job_id": l.Job.JobID, "pulse_launch_id": l.LaunchID, "pulse_jobdb": l.Job.JobDB}
}

func copyLaunches(in []provider.Launch) []provider.Launch {
	out := append([]provider.Launch(nil), in...)
	for i := range out {
		out[i].Capabilities = append([]string(nil), out[i].Capabilities...)
		if out[i].StartBefore != nil {
			t := *out[i].StartBefore
			out[i].StartBefore = &t
		}
	}
	return out
}
func nativeLaunch(l provider.Launch, platforms []string, env map[string]string) (compute.Launch, provider.Status, string) {
	if len(l.Capabilities) > 0 {
		return compute.Launch{}, provider.Unsupported, "native provider does not supply requested capabilities"
	}
	b, err := bases.Resolve(l.Base, platforms)
	if err != nil {
		return compute.Launch{}, provider.Unsupported, "no supported base matches selector"
	}
	a := compute.Allocation{CPUMillis: l.CPUMillis, MemoryBytes: l.MemoryBytes, ScratchBytes: l.ScratchBytes, Image: b.Image, Platform: b.Platform}
	m := metadata(l)
	process, err := executorProcess(l.Job.JobDB, l.Job.JobID, a, env, m)
	if err != nil {
		return compute.Launch{}, provider.Rejected, "invalid executor environment configuration"
	}
	process.Env["PULSE_EXECUTION_BASE"] = b.Name
	process.Env["C2J_EXECUTION_BASE"] = b.Name
	process.Env["C2J_EXECUTION_CAPABILITIES"] = "[]"
	process.Stdin = compute.SecretInput(l.Lease)
	return compute.Launch{Request: compute.Request{LaunchID: l.LaunchID, Allocation: a, TimeoutSeconds: l.TimeoutSeconds, StartBefore: l.StartBefore, Metadata: m}, Process: process}, "", ""
}

// Submit validates the entire batch before worker effects. Decisions describe
// admission; uncertain or missing results never authorize a retry or fallback.
func (p *Provider) Submit(ctx context.Context, launches []provider.Launch, _ provider.SubmitOptions) ([]provider.Submission, provider.LogSubscription, error) {
	launches = copyLaunches(launches)
	if err := providerutil.Launches(launches); err != nil {
		return nil, nil, err
	}
	if err := p.enter(ctx); err != nil {
		if !errors.Is(err, errClosed) {
			return nil, nil, err
		}
		out := make([]provider.Submission, 0, len(launches))
		for _, l := range launches {
			out = append(out, provider.Submission{LaunchID: l.LaunchID, Status: provider.Unavailable, Reason: errClosed.Error()})
		}
		return out, nil, err
	}
	defer p.leave()
	out := make([]provider.Submission, 0, len(launches))
	native := make([]compute.Launch, 0, len(launches))
	for _, l := range launches {
		n, status, reason := nativeLaunch(l, p.platforms, p.env)
		if status != "" {
			out = append(out, provider.Submission{LaunchID: l.LaunchID, Status: status, Reason: reason})
		} else {
			native = append(native, n)
		}
	}
	if len(native) == 0 {
		return out, nil, nil
	}
	results, err := p.native.Submit(ctx, native)
	if errors.Is(err, errClosed) {
		for _, l := range native {
			out = append(out, provider.Submission{LaunchID: l.LaunchID, Status: provider.Unavailable, Reason: errClosed.Error()})
		}
	}
	for _, r := range results {
		out = append(out, provider.Submission{LaunchID: r.LaunchID, Status: provider.Status(r.Status), Reason: r.Reason})
	}
	return out, nil, err
}
func (p *Provider) List(ctx context.Context, q provider.ListRequest) (provider.ListResponse, error) {
	if err := p.enter(ctx); err != nil {
		return provider.ListResponse{}, err
	}
	defer p.leave()
	q, err := providerutil.Normalize(q)
	if err != nil {
		return provider.ListResponse{}, err
	}
	page, err := p.native.List(ctx, compute.ListRequest{PageSize: q.PageSize, PageToken: q.PageToken, LaunchID: q.LaunchID})
	if err != nil {
		return provider.ListResponse{}, err
	}
	if page.Items == nil || len(page.Items) > q.PageSize || len(page.NextPageToken) > 16384 || (page.NextPageToken != "" && page.NextPageToken == q.PageToken) {
		return provider.ListResponse{}, fmt.Errorf("invalid native list page")
	}
	out := provider.ListResponse{Items: []provider.Instance{}, NextPageToken: page.NextPageToken}
	seen := map[string]bool{}
	for _, i := range page.Items {
		m := i.Metadata
		job := provider.JobRef{JobDB: m["pulse_jobdb"], InstanceID: m["pulse_jobdb_instance_id"], TenantID: m["pulse_tenant_id"], JobID: m["pulse_job_id"]}
		if job.JobDB == "" {
			job.JobDB = p.targets[job.InstanceID+"\x00"+job.TenantID]
		}
		item := provider.Instance{ID: i.ID, LaunchID: i.LaunchID, State: i.State, Job: job, Refs: i.Refs, CreatedAt: i.CreatedAt, StartedAt: i.StartedAt}
		if providerutil.Instance(item) != nil || seen[item.ID] || (q.LaunchID != "" && q.LaunchID != item.LaunchID) {
			return provider.ListResponse{}, fmt.Errorf("invalid native instance or incomplete job reference")
		}
		seen[item.ID] = true
		out.Items = append(out.Items, item)
	}
	return out, nil
}

// Maintain performs the existing throttled retirement of expired owned caches.
// It is serialized with submission and never stops workers.
func (p *Provider) Maintain(ctx context.Context) error {
	if err := p.enter(ctx); err != nil {
		return err
	}
	defer p.leave()
	return p.native.Maintain(ctx)
}
