package provider

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"regexp"
	"slices"
	"time"

	protocol "github.com/colony-2/pulse-docker/internal/engine"
	"github.com/colony-2/pulse-docker/internal/launchlogs"
)

const (
	retentionLabel        = "pulse_log_retention"
	retentionMode         = "owned-v1"
	retentionScopeLabel   = "pulse_log_scope"
	retentionArchiveLabel = "pulse_log_archive"
	retentionGrace        = 24 * time.Hour
)

var receiptFingerprint = regexp.MustCompile(`^[0-9a-f]{64}$`)
var archiveIdentity = regexp.MustCompile(`^[0-9a-f]{32}$`)

func retentionScope(config LoggingConfig, daemon string) string {
	return cacheHash(launchlogs.Scope(config.Directory, daemon, config.Owner))
}

func (c *captureGroup) report(id, container, reason string, err error) {
	if err != nil {
		slog.Warn("Docker launch diagnostics unavailable", "launch", id, "container", container, "reason", reason)
	}
}

func (c *captureGroup) mark(id, container, reason string) {
	c.report(id, container, reason, c.store.Gap(id, reason))
}

func (group *captureGroup) recoveryFailed(id, container, reason string) {
	slog.Warn("Docker native log recovery failed", "launch", id, "container", container, "reason", reason)
	group.mark(id, container, reason)
}

type termination struct{ stopped, missing, confirmed, saved bool }

func inspectWorker(ctx context.Context, engine *engine, id string) (protocol.WorkerInspect, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return engine.InspectWorker(ctx, id)
}

func exited(state protocol.State, code int, source string) launchlogs.Process {
	process := launchlogs.Process{State: "exited", ExitCode: &code, StartedAt: protocol.DockerTime(state.StartedAt), Source: source, ObservedAt: time.Now().UTC()}
	if state.Stopped() {
		process.OOMKilled = state.OOMKilled
		process.FinishedAt = protocol.DockerTime(state.FinishedAt)
		if process.StartedAt != nil && process.FinishedAt != nil && process.FinishedAt.Before(*process.StartedAt) {
			process.FinishedAt = nil
		}
	}
	if process.OOMKilled != nil && *process.OOMKilled {
		process.Reason = "oom_killed"
	} else if code != 0 {
		process.Reason = "nonzero_exit"
	}
	return process
}

func (c *captureGroup) observe(engine *engine, id, container string, started bool) termination {
	var saved bool
	commit := func(process launchlogs.Process) {
		err := c.store.RecordProcess(id, process)
		saved = err == nil
		c.report(id, container, "process_observation", err)
	}
	unknown := func(reason string) {
		commit(launchlogs.Process{State: "unknown", Reason: reason, ObservedAt: time.Now().UTC()})
	}
	current, err := inspectWorker(c.ctx, engine, container)
	var status int
	var waited bool
	waitTimeout := c.waitTimeout
	if waitTimeout <= 0 {
		waitTimeout = 30 * time.Second
	}
	backoff := 100 * time.Millisecond
	for running := false; c.ctx.Err() == nil && !apiStatus(err, 404) && !current.State.Stopped(); {
		if !running && (current.State.Status == "running" || current.State.Status == "paused") {
			running = true
			commit(launchlogs.Process{State: "running", Source: "inspect", StartedAt: protocol.DockerTime(current.State.StartedAt), ObservedAt: time.Now().UTC()})
		}
		if !started && !running {
			break
		}
		waitCtx, cancel := context.WithTimeout(c.ctx, waitTimeout)
		var waitErr error
		status, waitErr = engine.WaitWorker(waitCtx, container)
		cancel()
		waited = waitErr == nil
		current, err = inspectWorker(c.ctx, engine, container)
		if current.State.Stopped() || apiStatus(err, 404) || (waited && err != nil) {
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-c.ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		backoff = min(2*backoff, 2*time.Second)
	}
	if c.ctx.Err() != nil {
		return termination{}
	}
	state := current.State
	if err == nil && state.Terminal() {
		prior, _ := c.store.Observation(id)
		previous := prior.History.Process
		if waited && status != *state.ExitCode || previous != nil && previous.ExitCode != nil && *previous.ExitCode != *state.ExitCode {
			if previous == nil || previous.ExitCode == nil {
				facts := exited(protocol.State{}, status, "wait")
				previous = &facts
			}
			previous.State, previous.Reason = "unknown", "observation_conflict"
			commit(*previous)
			c.mark(id, container, "observation_conflict")
		} else {
			commit(exited(state, *state.ExitCode, "inspect"))
		}
		return termination{stopped: true, confirmed: true, saved: saved}
	}
	if err == nil && state.Error != "" && (state.Stopped() || state.Status == "created") {
		unknown("start_failed")
		c.mark(id, container, "start_failed")
	} else if waited && (err != nil || state.Stopped()) {
		waitState := state
		waitState.OOMKilled = nil
		commit(exited(waitState, status, "wait"))
		return termination{stopped: state.Stopped(), missing: apiStatus(err, 404), confirmed: true, saved: saved}
	} else if apiStatus(err, 404) {
		unknown("container_missing")
	} else {
		unknown("wait_unavailable")
		if state.Stopped() {
			c.mark(id, container, "wait_unavailable")
		}
	}
	return termination{stopped: err == nil && state.Stopped(), missing: apiStatus(err, 404), saved: saved}
}

func (c *captureGroup) owned(labels map[string]string) bool {
	return c.scope != "" && labels[retentionScopeLabel] == c.scope && labels[retentionLabel] == retentionMode &&
		labels["pulse_managed_by"] == "pulse" && labels[roleLabel] == "worker" &&
		receiptFingerprint.MatchString(labels["pulse_process_fingerprint"]) && archiveIdentity.MatchString(labels[retentionArchiveLabel])
}

func (c *captureGroup) graceExpired(anchor *time.Time) bool {
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	return anchor != nil && now().Sub(*anchor) >= retentionGrace
}

func (c *captureGroup) cleanup(provider *engineProvider, id, container string) {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	if err := provider.acquire(ctx); err != nil {
		return
	}
	defer provider.release()
	actual, err := provider.e.InspectWorker(ctx, container)
	if apiStatus(err, 404) {
		if obs, err := c.store.Observation(id); err == nil && obs.Receipt != nil && obs.Receipt.Container == container && obs.Receipt.Cleanup != "expired" {
			obs.Receipt.Cleanup = "removed"
			c.report(id, container, "cleanup_metadata", c.store.RecordReceipt(id, *obs.Receipt))
		}
		return
	}
	if err != nil {
		c.report(id, container, "cleanup_inspect", err)
		return
	}
	labels := actual.Config.Labels
	if !actual.State.Stopped() || actual.HostConfig.AutoRemove == nil || *actual.HostConfig.AutoRemove || !c.owned(labels) || labels["pulse_launch_id"] != id {
		return
	}
	verified := func(observation launchlogs.Observation) bool {
		receipt := observation.Receipt
		return receipt != nil && receipt.Container == container && receipt.Fingerprint == labels["pulse_process_fingerprint"] &&
			receipt.Scope == c.scope && observation.Archive == labels[retentionArchiveLabel] && (receipt.Phase == "accepted" || receipt.Phase == "uncertain")
	}
	obs, err := c.store.DurableObservation(id)
	if err != nil || !verified(obs) {
		slog.Warn("Stopped Docker worker retained to preserve launch identity", "launch", id, "container", container, "reason", "cleanup_blocked")
		return
	}
	receipt := obs.Receipt
	anchor := protocol.DockerTime(actual.State.FinishedAt)
	if started := protocol.DockerTime(actual.State.StartedAt); started != nil && anchor != nil && anchor.Before(*started) {
		anchor = nil
	}
	if anchor == nil {
		anchor = receipt.StoppedAt
	}
	if anchor == nil {
		now := time.Now().UTC()
		receipt.StoppedAt = &now
		receipt.Cleanup = "pending"
		c.report(id, container, "cleanup_anchor", c.store.RecordReceipt(id, *receipt))
	}
	expired := c.graceExpired(anchor)
	if !obs.Finalized && !expired {
		return
	}
	expiringCapture := expired && !obs.Finalized
	if expiringCapture {
		slog.Warn("Docker native log recovery grace expired", "launch", id, "container", container, "reason", "native_retention_expired")
		c.report(id, container, "terminal_commit", c.store.CompleteExpired(id))
		obs, err = c.store.DurableObservation(id)
		if err != nil || !verified(obs) {
			slog.Warn("Stopped Docker worker retained to preserve launch identity", "launch", id, "container", container, "reason", "cleanup_blocked")
			return
		}
		receipt = obs.Receipt
	}
	err = provider.e.Call(ctx, "DELETE", "/containers/"+url.PathEscape(container), nil, nil)
	if err != nil && !apiStatus(err, 404) {
		receipt.Cleanup = "pending"
		c.report(id, container, "cleanup_delete", err)
	} else if expired && (expiringCapture || slices.Contains(obs.History.GapReasons, "native_retention_expired")) {
		receipt.Cleanup = "expired"
	} else {
		receipt.Cleanup = "removed"
	}
	c.report(id, container, "cleanup_metadata", c.store.RecordReceipt(id, *receipt))
}

func (c *captureGroup) busy(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active[id] != nil
}

func (c *captureGroup) reconcile(ctx context.Context, provider *engineProvider, startup bool) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := provider.acquire(ctx); err != nil {
		return
	}
	_, inventory, err := provider.usage(ctx)
	provider.release()
	if err != nil {
		c.warn(err)
		return
	}
	for id, worker := range inventory {
		if ctx.Err() != nil {
			return
		}
		if c.busy(id) {
			continue
		}
		labels := worker.Config.Labels
		owned := c.owned(labels)
		obs, err := c.store.Observation(id)
		if owned && os.IsNotExist(err) {
			err = c.store.BeginRecovered(launchlogs.History{LaunchID: id, ContainerID: worker.ID, ImageID: worker.Config.Image}, labels[retentionArchiveLabel])
			if err == nil {
				obs, err = c.store.Observation(id)
			}
		}
		if err != nil {
			if owned {
				c.report(id, worker.ID, "storage_unavailable", err)
			}
			continue
		}
		if owned && obs.Archive == labels[retentionArchiveLabel] {
			fingerprint := labels["pulse_process_fingerprint"]
			if obs.Receipt == nil || obs.Receipt.Container == "" {
				phase := "uncertain"
				if obs.Receipt != nil && obs.Receipt.Fingerprint == fingerprint && obs.Receipt.Phase == "accepted" {
					phase = "accepted"
				}
				c.report(id, worker.ID, "receipt", c.store.RecordReceipt(id, launchlogs.Receipt{Container: worker.ID, Fingerprint: fingerprint, Scope: c.scope, Phase: phase, Cleanup: "pending"}))
			} else if obs.Receipt.Container != worker.ID || obs.Receipt.Fingerprint != fingerprint || obs.Receipt.Scope != c.scope {
				continue
			}
		} else if labels[retentionLabel] != "" || obs.History.ContainerID != worker.ID {
			continue
		}
		stopped := worker.State.Status == "exited" || worker.State.Status == "dead"
		if obs.Finalized {
			if owned && stopped {
				c.cleanup(provider, id, worker.ID)
			}
		} else if stopped || worker.State.Status == "running" || worker.State.Status == "paused" {
			c.start(provider, id, worker.ID, true, stopped)
		}
	}
	history, err := c.store.Histories()
	if err != nil {
		c.warn(err)
		return
	}
	for _, entry := range history {
		id := entry.LaunchID
		if ctx.Err() != nil {
			return
		}
		if _, found := inventory[id]; found || c.busy(id) {
			continue
		}
		obs, err := c.store.Observation(id)
		if err != nil {
			c.warn(err)
			continue
		}
		if obs.Finalized {
			if entry.ContainerID != "" && obs.Receipt != nil && (obs.Receipt.Cleanup == "pending" || obs.Receipt.Cleanup == "blocked") {
				c.cleanup(provider, id, entry.ContainerID)
			}
			continue
		}
		if !startup && entry.ContainerID == "" {
			continue
		}
		if entry.ContainerID != "" {
			_, err := inspectWorker(ctx, provider.e, entry.ContainerID)
			if !apiStatus(err, 404) {
				continue
			}
		}
		if err := c.store.RecordProcess(id, launchlogs.Process{State: "unknown", Reason: "container_missing", ObservedAt: time.Now().UTC()}); err != nil {
			c.report(id, entry.ContainerID, "process_observation", err)
			continue
		}
		if entry.ContainerID != "" {
			c.report(id, entry.ContainerID, "terminal_commit", c.store.CompleteMissingContainer(id))
			continue
		}
		c.mark(id, entry.ContainerID, "container_missing")
		c.report(id, entry.ContainerID, "terminal_commit", c.store.Complete(id))
	}
}
