package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/colony-2/pulse-docker/internal/launchlogs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	compute "github.com/colony-2/pulse-docker/internal/native"
)

const accountingLabel = "pulse_docker_accounting"
const limitsLabel = "pulse_docker_limits"

type charge struct {
	CPU    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
	Slots  int   `json:"slots"`
}

func (c charge) add(b charge) charge {
	return charge{c.CPU + b.CPU, c.Memory + b.Memory, c.Slots + b.Slots}
}

type engineProvider struct {
	capture         *captureGroup
	daemonID        string
	cfg             Config
	e               *engine
	platform        string
	platformProbes  map[string]platformProbe
	gate            chan struct{}
	uncertain       map[string]charge
	readyCaches     map[string]bool
	lock            *os.File
	closeOnce       sync.Once
	closing         func() bool
	unenforced      bool
	extraHosts      []string
	lastMaintenance time.Time
}
type nativePlan struct {
	Request    compute.Request
	ImageID    string
	Allocation compute.Allocation
	ImageUser  string
	ImageEnv   []string
}
type hostConfig struct {
	AutoRemove    *bool `json:"AutoRemove"`
	NanoCPUs      int64 `json:"NanoCpus"`
	Memory        int64 `json:"Memory"`
	MemorySwap    int64 `json:"MemorySwap"`
	RestartPolicy struct {
		Name string `json:"Name"`
	} `json:"RestartPolicy"`
	Tmpfs  map[string]string `json:"Tmpfs"`
	Mounts []volumeMount     `json:"Mounts"`
}
type container struct {
	HostConfig hostConfig `json:"HostConfig"`
	ID         string     `json:"Id"`
	Name       string     `json:"Name"`
	Mounts     []struct {
		Type, Name, Destination string
		RW                      bool
	} `json:"Mounts"`
	Config struct {
		Labels map[string]string `json:"Labels"`
		Image  string
		Env    []string
	} `json:"Config"`
	State struct {
		Status     string `json:"Status"`
		StartedAt  string `json:"StartedAt"`
		FinishedAt string `json:"FinishedAt"`
		ExitCode   int    `json:"ExitCode"`
	} `json:"State"`
}

func newNative(ctx context.Context, cfg Config) (*engineProvider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var err error
	cfg, err = normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	socket, err := ResolveSocket(cfg.Socket)
	if err != nil {
		return nil, err
	}
	cfg.Socket = socket
	e := newEngine(cfg.Socket)
	e.RegistryAuth = cfg.RegistryAuth
	success := false
	defer func() {
		if !success {
			e.Client.CloseIdleConnections()
		}
	}()
	slog.Info("initializing Docker provider", "socket", cfg.Socket)
	p, err := open(ctx, cfg, e, true)
	if err != nil {
		return nil, probeDiagnostic("initialize daemon", err)
	}
	if !p.unenforced {
		if err := p.checkLimits(ctx); err != nil {
			p.Close()
			return nil, err
		}
	}
	if err := p.discoverPlatforms(ctx); err != nil {
		p.Close()
		return nil, err
	}
	slog.Info("Docker provider ready", "native_platform", p.platform, "supported_platforms", p.SupportedPlatforms(), "resource_limits_enforced", !p.unenforced)
	if err := p.Maintain(ctx); err != nil {
		slog.Warn("Docker cache maintenance deferred", "error", err)
	}
	if err := ctx.Err(); err != nil {
		p.Close()
		return nil, err
	}
	success = true
	return p, nil
}
func open(ctx context.Context, cfg Config, e *engine, locking bool) (*engineProvider, error) {
	var err error
	cfg, err = normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	var version struct {
		APIVersion string `json:"ApiVersion"`
	}
	if err := e.Call(ctx, "GET", "/version", nil, &version); err != nil {
		return nil, err
	}
	if !regexp.MustCompile(`^1\.[0-9]+$`).MatchString(version.APIVersion) {
		return nil, fmt.Errorf("invalid Docker API version")
	}
	e.Version = "/v" + version.APIVersion
	var info struct {
		ID                                          string
		NCPU                                        int
		MemTotal                                    int64
		OSType, Architecture                        string
		OperatingSystem, Name                       string
		MemoryLimit, SwapLimit, CPUSet, CPUCfsQuota bool
	}
	if err := e.Call(ctx, "GET", "/info", nil, &info); err != nil {
		return nil, err
	}
	if info.ID == "" {
		return nil, fmt.Errorf("Docker daemon has no identity")
	}
	arch := info.Architecture
	switch arch {
	case "aarch64":
		arch = "arm64"
	case "x86_64":
		arch = "amd64"
	}
	if info.OSType != "linux" {
		return nil, fmt.Errorf("Docker daemon must run Linux")
	}
	if cfg.CPUMillis == 0 {
		cfg.CPUMillis = int64(info.NCPU) * 1000
	}
	if cfg.MemoryBytes == 0 {
		cfg.MemoryBytes = info.MemTotal
	}
	if cfg.CPUMillis > int64(info.NCPU)*1000 || cfg.MemoryBytes > info.MemTotal {
		return nil, fmt.Errorf("Docker budget exceeds daemon resources")
	}
	p := &engineProvider{cfg: cfg, daemonID: info.ID, e: e, platform: info.OSType + "/" + arch, gate: make(chan struct{}, 1), uncertain: map[string]charge{}, readyCaches: map[string]bool{}}
	p.extraHosts = hostGatewayMapping(runtime.GOOS, info.OperatingSystem, info.Name)
	if !info.MemoryLimit || !info.SwapLimit || !info.CPUCfsQuota {
		p.disableLimits("Docker reports CPU, memory, or swap limits unavailable")
	}
	if locking {
		if cfg.LockDir == "" {
			cfg.LockDir = filepath.Join(os.TempDir(), "pulse-docker-locks")
		}
		if err := os.MkdirAll(cfg.LockDir, 0700); err != nil {
			return nil, err
		}
		sum := sha256.Sum256([]byte(info.ID))
		f, err := os.OpenFile(filepath.Join(cfg.LockDir, hex.EncodeToString(sum[:])+".lock"), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			f.Close()
			return nil, fmt.Errorf("Docker daemon already has an admission owner")
		}
		p.lock = f
	}
	if _, _, err := p.usage(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func (p *engineProvider) disableLimits(reason string) {
	p.unenforced = true
	slog.Warn("Docker resource limits unavailable; jobs will run without CPU, memory, or swap enforcement; admission budgets still apply", "reason", reason)
}
func (p *engineProvider) Close() {
	p.closeOnce.Do(func() {
		if p.e != nil && p.e.Client != nil {
			p.e.Client.CloseIdleConnections()
		}
		if p.lock != nil {
			syscall.Flock(int(p.lock.Fd()), syscall.LOCK_UN)
			p.lock.Close()
		}
	})
}
func (p *engineProvider) acquire(ctx context.Context) error {
	select {
	case p.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			p.release()
			return err
		}
		if p.closing != nil && p.closing() {
			p.release()
			return errClosed
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *engineProvider) release() { <-p.gate }
func (p *engineProvider) fits(c charge) bool {
	return c.CPU <= p.cfg.CPUMillis && c.Memory <= p.cfg.MemoryBytes && (p.cfg.MaxContainers == 0 || c.Slots <= p.cfg.MaxContainers)
}

// A scratch-only request must not impose an application memory limit.
func containerLimits(a compute.Allocation) (cpu, memory int64) {
	cpu = a.CPUMillis * 1000000
	if a.MemoryBytes > 0 {
		memory = a.MemoryBytes + a.ScratchBytes
	}
	return
}
func (p *engineProvider) cost(a compute.Allocation) charge {
	return charge{a.CPUMillis, a.MemoryBytes + a.ScratchBytes + p.cfg.Overhead, 1}
}
func (p *engineProvider) usage(ctx context.Context) (charge, map[string]container, error) {
	filter, _ := json.Marshal(map[string][]string{"label": {"pulse_managed_by=pulse"}})
	q := url.Values{"all": {"1"}, "filters": {string(filter)}}
	var list []struct {
		ID string `json:"Id"`
	}
	if err := p.e.Call(ctx, "GET", "/containers/json?"+q.Encode(), nil, &list); err != nil {
		return charge{}, nil, err
	}
	total := charge{}
	found := map[string]container{}
	for _, item := range list {
		var c container
		if err := p.e.Call(ctx, "GET", "/containers/"+url.PathEscape(item.ID)+"/json", nil, &c); err != nil {
			var api *apiError
			if errors.As(err, &api) && api.Code == 404 {
				// An auto-removed container can disappear after the list response.
				continue
			}
			return charge{}, nil, err
		}
		id := c.Config.Labels["pulse_launch_id"]
		if id == "" {
			return charge{}, nil, fmt.Errorf("managed container lacks launch ID")
		}
		role := c.Config.Labels[roleLabel]
		if role != "" && role != "worker" && role != cacheInitRole {
			return charge{}, nil, fmt.Errorf("unknown managed container role")
		}
		if _, ok := found[id]; ok {
			return charge{}, nil, fmt.Errorf("duplicate managed launch ID")
		}
		if role != cacheInitRole {
			found[id] = c
		} else if !validInitializer(c) {
			return charge{}, nil, fmt.Errorf("invalid cache initializer identity")
		}
		// One Pulse owns this daemon. A helper without our reservation belongs
		// to an interrupted invocation; remove it before it consumes the only slot.
		// Its volume marker, not the container, records completed initialization.
		_, ourHelper := p.uncertain[id]
		if role == cacheInitRole && !ourHelper && c.Config.Labels[cacheOwnerLabel] == p.cacheOwner() {
			if err := p.e.Call(ctx, "DELETE", "/containers/"+c.ID+"?force=1", nil, nil); err != nil && !apiStatus(err, 404) {
				return charge{}, nil, err
			}
			continue
		}
		var cost charge
		if err := json.Unmarshal([]byte(c.Config.Labels[accountingLabel]), &cost); err != nil || cost.CPU < 0 || cost.Memory < 0 || cost.Slots != 1 {
			return charge{}, nil, fmt.Errorf("invalid managed capacity accounting")
		}
		h := c.HostConfig
		validLimits := h.NanoCPUs >= 0 && h.NanoCPUs%1000000 == 0 && h.NanoCPUs/1000000 == cost.CPU && h.Memory >= 0 && h.Memory <= cost.Memory && h.MemorySwap == h.Memory
		switch c.Config.Labels[limitsLabel] {
		case "disabled":
			validLimits = h.NanoCPUs == 0 && h.Memory == 0 && h.MemorySwap == 0
		case "", "enforced": // Containers from older versions have no limits label.
		default:
			validLimits = false
		}
		if !validLimits || h.RestartPolicy.Name != "no" {
			return charge{}, nil, fmt.Errorf("managed container limits disagree with accounting")
		}
		delete(p.uncertain, id)
		switch c.State.Status {
		case "exited", "dead":
		default:
			total = total.add(cost)
		}
	}
	for _, cost := range p.uncertain {
		total = total.add(cost)
	}
	return total, found, nil
}
func (p *engineProvider) resolve(ctx context.Context, r compute.Request, used charge) (nativePlan, compute.Status, string) {
	result := compute.Submission{}
	a := r.Allocation
	a.CPUMillis = (a.CPUMillis + 9) / 10 * 10
	a.MemoryBytes = (a.MemoryBytes + 4095) / 4096 * 4096
	a.ScratchBytes = (a.ScratchBytes + 4095) / 4096 * 4096
	if a.MemoryBytes > 0 && a.MemoryBytes+a.ScratchBytes < 6<<20 {
		a.MemoryBytes = (6 << 20) - a.ScratchBytes
	}
	cost := p.cost(a)
	switch {
	case r.StartBefore != nil && !r.StartBefore.After(time.Now()):
		result.Status = compute.Rejected
		result.Reason = "start deadline expired"
	case r.StartBefore != nil:
		result.Status = compute.Unsupported
		result.Reason = "Docker does not enforce an additional actual-start deadline; c2j validates the supplied lease"
	case r.TimeoutSeconds != 0:
		result.Status = compute.Unsupported
		result.Reason = "Docker runs the executor directly; configure execution timeouts in c2j instead of defaults.timeout"
	case !p.supportsPlatform(r.Platform):
		result.Status = compute.Unsupported
		result.Reason = p.platformReason(r.Platform)
	case !p.fits(cost):
		result.Status = compute.Unsupported
		result.Reason = "request exceeds Docker pool budget"
	case !p.fits(used.add(cost)):
		result.Status = compute.NoCapacity
		result.Reason = "Docker pool full"
	default:
		image, e := p.e.ResolveImage(ctx, r.Image, r.Platform)
		if e != nil {
			result.Status = compute.Unavailable
			result.Reason = "Docker image resolution failed"
			if errors.Is(e, errImagePlatform) {
				result.Status = compute.Unsupported
				result.Reason = "image platform differs"
			}
			return nativePlan{}, result.Status, result.Reason
		}
		a.ImageID = image.ID
		a.ImageDigest = ""
		if _, digest, pinned := strings.Cut(r.Image, "@"); pinned {
			for _, rd := range image.RepoDigests {
				if rd == r.Image {
					a.ImageDigest = digest
				}
			}
			if a.ImageDigest == "" {
				result.Status = compute.Unsupported
				result.Reason = "required manifest identity is unverified"
				return nativePlan{}, result.Status, result.Reason
			}
		}
		return nativePlan{Request: r, ImageID: image.ID, Allocation: a, ImageUser: image.Config.User, ImageEnv: image.Config.Env}, "", ""
	}
	return nativePlan{}, result.Status, result.Reason
}

func fingerprint(l compute.Launch) string {
	b, _ := json.Marshal(l)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (p *engineProvider) Submit(ctx context.Context, ls []compute.Launch) ([]compute.Submission, error) {
	if err := compute.ValidateLaunches(ls); err != nil {
		return nil, err
	}
	if err := p.acquire(ctx); err != nil {
		return nil, err
	}
	defer p.release()
	used, found, err := p.usage(ctx)
	if err != nil {
		out := []compute.Submission{}
		for _, l := range ls {
			out = append(out, compute.Submission{LaunchID: l.LaunchID, Status: compute.Unavailable, Reason: "Docker accounting unavailable before submission"})
		}
		return out, err
	}
	out := []compute.Submission{}
	for _, l := range ls {
		r := compute.Submission{LaunchID: l.LaunchID}
		hash := fingerprint(l)
		var observation launchlogs.Observation
		var observationErr error
		if p.capture != nil {
			observation, observationErr = p.capture.store.Observation(l.LaunchID)
		}
		if c, exists := found[l.LaunchID]; exists {
			if c.Config.Labels["pulse_process_fingerprint"] != hash {
				r.Status = compute.Rejected
				r.Reason = "launch ID conflict"
			} else if key := c.Config.Labels[cacheKeyLabel]; key != "" && !verifyCacheContainer(c, key) {
				r.Status, r.Reason = compute.Unknown, "existing cache attachment could not be verified"
			} else if c.State.Status == "created" {
				r.Status = compute.Unknown
				r.Reason = "previous launch was created but start is unconfirmed"
			} else {
				r.Status = compute.Accepted
			}
			out = append(out, r)
			continue
		}
		if p.capture != nil {
			prior := observation.Receipt
			switch {
			case observationErr != nil && !os.IsNotExist(observationErr):
				r.Status, r.Reason = compute.Unknown, "prior launch history could not be checked"
			case prior != nil && prior.Fingerprint != hash:
				r.Status, r.Reason = compute.Rejected, "launch ID conflict"
			case prior != nil && prior.Phase == "accepted" && prior.Scope == p.capture.scope:
				r.Status = compute.Accepted
			case prior != nil:
				r.Status, r.Reason = compute.Unknown, "previous Docker launch effect remains uncertain"
			}
			if r.Status != "" {
				out = append(out, r)
				continue
			}
		}
		if _, uncertain := p.uncertain[l.LaunchID]; uncertain {
			r.Status = compute.Unknown
			r.Reason = "previous Docker create remains uncertain"
			out = append(out, r)
			continue
		}

		plan, status, reason := p.resolve(ctx, l.Request, used)
		if status != "" {
			r.Status, r.Reason = status, reason
			out = append(out, r)
			continue
		}
		cost := p.cost(plan.Allocation)
		cache, cacheEnv, cacheErr := p.cachePlan(plan, l.Process.Env)
		if cacheErr != nil {
			r.Status, r.Reason = compute.Rejected, cacheErr.Error()
			out = append(out, r)
			continue
		}
		if cache != nil {
			if cacheErr = p.ensureCache(ctx, *cache, plan, cost); cacheErr != nil {
				r.Status, r.Reason = compute.Unavailable, "Docker dependency cache initialization failed: "+cacheErr.Error()
				out = append(out, r)
				// An uncertain initializer still reserves capacity, but no worker
				// has been created and provider fallback remains safe.
				used, found, err = p.usage(ctx)
				if err != nil {
					return out, err
				}
				continue
			}
		}

		used = used.add(cost)
		p.uncertain[l.LaunchID] = cost
		labels := map[string]string{}
		for k, v := range plan.Request.Metadata {
			labels[k] = v
		}
		labels["pulse_managed_by"] = "pulse"
		labels["pulse_launch_id"] = l.LaunchID
		labels[roleLabel] = "worker"
		delete(labels, cacheKeyLabel)
		delete(labels, cacheLayoutLabel)
		if cache != nil {
			labels[cacheKeyLabel] = cache.Key
			labels[cacheLayoutLabel] = cacheLayout
		}
		accounting, _ := json.Marshal(cost)
		labels[accountingLabel] = string(accounting)
		labels["pulse_process_fingerprint"] = hash
		for _, key := range []string{retentionLabel, retentionScopeLabel, retentionArchiveLabel} {
			delete(labels, key)
		}
		autoRemove := p.capture == nil
		receipt := launchlogs.Receipt{Fingerprint: hash, Phase: "uncertain", Cleanup: "pending"}
		if !autoRemove {
			receipt.Scope = p.capture.scope
			archive, archiveErr := p.capture.store.PrepareAttempt(launchlogs.History{
				LaunchID: l.LaunchID, InstanceID: labels["pulse_jobdb_instance_id"], TenantID: labels["pulse_tenant_id"], JobID: labels["pulse_job_id"], SubmittedAt: time.Now().UTC(),
			}, receipt)
			p.capture.report(l.LaunchID, "", "receipt", archiveErr)
			p.capture.report(l.LaunchID, "", "preparing", p.capture.store.Append(l.LaunchID, "launch", []byte("preparing Docker worker\n")))
			if archive == "" {
				archive = cacheHash([]string{receipt.Scope, l.LaunchID})[:32]
			}
			labels[retentionLabel], labels[retentionScopeLabel], labels[retentionArchiveLabel] = retentionMode, receipt.Scope, archive
		}
		labels[limitsLabel] = "enforced"
		if p.unenforced {
			labels[limitsLabel] = "disabled"
		}
		env := []string{}
		for k, v := range l.Process.Env {
			if _, managed := cacheEnv[k]; managed {
				continue
			}
			env = append(env, k+"="+v)
		}
		for k, v := range cacheEnv {
			env = append(env, k+"="+v)
		}
		if _, ok := l.Process.Env["TMPDIR"]; !ok && plan.Allocation.ScratchBytes > 0 {
			env = append(env, "TMPDIR="+p.cfg.ScratchPath)
		}
		if _, supplied := l.Process.Env["PULSE_SCRATCH_DIR"]; !supplied && plan.Allocation.ScratchBytes > 0 {
			env = append(env, "PULSE_SCRATCH_DIR="+p.cfg.ScratchPath)
		}
		a := plan.Allocation
		cpu, memory := containerLimits(a)
		if p.unenforced {
			cpu, memory = 0, 0
		}
		tmpfs := map[string]string{}
		if a.ScratchBytes > 0 {
			tmpfs[p.cfg.ScratchPath] = fmt.Sprintf("rw,size=%d,mode=1777", a.ScratchBytes)
		}
		body := map[string]any{"Image": plan.ImageID, "Entrypoint": l.Process.Command, "Cmd": l.Process.Args, "OpenStdin": l.Process.Stdin != "", "StdinOnce": true, "AttachStdin": l.Process.Stdin != "", "Env": env, "Labels": labels, "WorkingDir": l.Process.WorkingDir, "HostConfig": map[string]any{"NanoCpus": cpu, "Memory": memory, "MemorySwap": memory, "RestartPolicy": map[string]any{"Name": "no"}, "AutoRemove": autoRemove, "Init": true, "Tmpfs": tmpfs, "LogConfig": map[string]any{"Type": "json-file", "Config": map[string]string{"max-size": "10m", "max-file": "3"}}}}
		if cache != nil {
			body["HostConfig"].(map[string]any)["Mounts"] = cache.mounts()
		}
		if len(p.extraHosts) != 0 {
			body["HostConfig"].(map[string]any)["ExtraHosts"] = p.extraHosts
		}
		sum := sha256.Sum256([]byte(l.LaunchID))
		name := "pulse-" + hex.EncodeToString(sum[:16])
		var created struct {
			ID       string   `json:"Id"`
			Warnings []string `json:"Warnings"`
		}
		err = p.e.Call(ctx, "POST", containerCreatePath(name, plan.Request.Platform), body, &created)
		if err != nil {
			var api *apiError
			if errors.As(err, &api) && api.Code >= 400 && api.Code < 500 && api.Code != 409 {
				delete(p.uncertain, l.LaunchID)
				if p.capture != nil {
					p.capture.report(l.LaunchID, "", "receipt", p.capture.store.ClearReceipt(l.LaunchID))
					p.capture.report(l.LaunchID, "", "admission", p.capture.store.RecordAdmission(l.LaunchID, string(compute.Rejected), true, false))
				}
				used.CPU -= cost.CPU
				used.Memory -= cost.Memory
				used.Slots--
				r.Status = compute.Rejected
				r.Reason = err.Error()
			} else {
				r.Status = compute.Unknown
				r.Reason = "Docker create outcome uncertain"
			}
			out = append(out, r)
			continue
		}
		// Keep the reservation until delivery succeeds or accounting positively
		// observes this container. A returned ID alone does not verify creation,
		// configuration or startup; missing inventory must not permit replay.
		if created.ID == "" {
			r.Status = compute.Unknown
			r.Reason = "Docker create returned no ID"
			out = append(out, r)
			continue
		}
		receipt.Container = created.ID
		if p.capture != nil {
			p.capture.report(l.LaunchID, created.ID, "receipt", p.capture.store.RecordReceipt(l.LaunchID, receipt))
		}
		if len(created.Warnings) > 0 {
			r.Status = compute.Unknown
			r.Reason = "Docker create warnings require inspection; container not started"
			out = append(out, r)
			continue
		}
		var inspected container
		if err = p.e.Call(ctx, "GET", "/containers/"+url.PathEscape(created.ID)+"/json", nil, &inspected); err != nil || inspected.HostConfig.AutoRemove == nil || *inspected.HostConfig.AutoRemove != autoRemove || inspected.HostConfig.NanoCPUs != cpu || inspected.HostConfig.Memory != memory || inspected.HostConfig.MemorySwap != memory || inspected.HostConfig.RestartPolicy.Name != "no" || inspected.HostConfig.Tmpfs[p.cfg.ScratchPath] != tmpfs[p.cfg.ScratchPath] {
			r.Status = compute.Unknown
			r.Reason = "Docker configuration verification failed; container not started"
			out = append(out, r)
			continue
		}
		if p.capture != nil && (inspected.ID != created.ID || !p.capture.owned(inspected.Config.Labels) || inspected.Config.Labels[retentionArchiveLabel] != labels[retentionArchiveLabel] || inspected.Config.Labels["pulse_launch_id"] != l.LaunchID || inspected.Config.Labels["pulse_process_fingerprint"] != hash) {
			r.Status, r.Reason = compute.Unknown, "Docker retention verification failed; container not started"
			out = append(out, r)
			continue
		}
		if cache != nil && (!verifyCacheContainer(inspected, cache.Key) || !environmentContains(inspected.Config.Env, cacheEnv)) {
			r.Status, r.Reason = compute.Unknown, "Docker cache attachment verification failed; container not started"
			out = append(out, r)
			continue
		}
		if p.capture != nil {
			p.capture.warn(p.capture.store.Observe(l.LaunchID, created.ID, "", plan.ImageID))
			p.capture.start(p, l.LaunchID, created.ID, false, false)
		}
		if err = p.e.Start(ctx, created.ID, l.Process.Stdin); err != nil {
			r.Status = compute.Unknown
			r.Reason = "Docker start or stdin delivery outcome uncertain"
		} else {
			delete(p.uncertain, l.LaunchID)
			r.Status = compute.Accepted
		}
		if p.capture != nil {
			if err == nil {
				receipt.Phase = "accepted"
				p.capture.report(l.LaunchID, created.ID, "receipt", p.capture.store.RecordReceipt(l.LaunchID, receipt))
			}
			p.capture.started(l.LaunchID, err == nil)
		}
		out = append(out, r)
	}
	return out, nil
}
