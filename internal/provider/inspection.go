package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/colony-2/pulse-docker/internal/bases"
	protocol "github.com/colony-2/pulse-docker/internal/engine"
	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	"net/url"
	"sort"
	"strings"
)

type ContainerView struct {
	ID, LaunchID, TenantID, JobID, State, ImageID, C2JVersion string
	CPUMillis, MemoryBytes                                    int64
	Limits                                                    string
}
type StatusView struct {
	DaemonID, Platform string
	Configured         ConfigBudgets
	Observed           ConfigBudgets
	Containers         []ContainerView
	Note               string
}
type ConfigBudgets struct {
	CPUMillis, MemoryBytes, Overhead int64
	MaxContainers                    int
}
type ImageView struct {
	ID, OS, Architecture, Variant, Created string
	RepoTags, RepoDigests                  []string
}

func Inspect(ctx context.Context, cfg Config) (StatusView, []ImageView, error) {
	var out StatusView
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return out, nil, err
	}
	e, info, err := protocol.Connect(ctx, cfg.Socket)
	if err != nil {
		return out, nil, err
	}
	defer e.Client.CloseIdleConnections()
	out.DaemonID = info.ID
	out.Platform = info.OSType + "/" + info.Architecture
	out.Configured = ConfigBudgets{cfg.CPUMillis, cfg.MemoryBytes, cfg.Overhead, cfg.MaxContainers}
	out.Note = "Snapshot only; configured zero CPU/memory uses daemon capacity. Observed charges exclude uncertain in-memory reservations and do not establish free capacity or job completion. c2j version is unknown."
	out.Containers = []ContainerView{}
	filter, _ := json.Marshal(map[string][]string{"label": {"pulse_managed_by=pulse"}})
	var listed []struct {
		ID string `json:"Id"`
	}
	if err = e.Call(ctx, "GET", "/containers/json?"+url.Values{"all": {"1"}, "filters": {string(filter)}}.Encode(), nil, &listed); err != nil {
		return out, nil, err
	}
	refs := map[string]bool{}
	for _, c := range listed {
		var detail container
		if err = e.Call(ctx, "GET", "/containers/"+url.PathEscape(c.ID)+"/json", nil, &detail); apiStatus(err, 404) {
			continue
		} else if err != nil {
			return out, nil, err
		}
		m := detail.Config.Labels
		if m["pulse_managed_by"] != "pulse" {
			return out, nil, fmt.Errorf("invalid managed inventory")
		}
		if m[roleLabel] == cacheInitRole {
			continue
		}
		var cost charge
		if json.Unmarshal([]byte(m[accountingLabel]), &cost) != nil || cost.CPU < 0 || cost.Memory < 0 || cost.Slots != 1 {
			return out, nil, fmt.Errorf("invalid capacity accounting")
		}
		out.Containers = append(out.Containers, ContainerView{ID: detail.ID, LaunchID: m["pulse_launch_id"], TenantID: m["pulse_tenant_id"], JobID: m["pulse_job_id"], State: detail.State.Status, ImageID: detail.Config.Image, C2JVersion: "unknown", CPUMillis: cost.CPU, MemoryBytes: cost.Memory, Limits: m[limitsLabel]})
		refs[detail.Config.Image] = true
		if detail.State.Status != "exited" && detail.State.Status != "dead" {
			out.Observed.CPUMillis += cost.CPU
			out.Observed.MemoryBytes += cost.Memory
			out.Observed.MaxContainers++
		}
	}
	for _, b := range bases.Catalog("linux/amd64", "linux/arm64") {
		refs[b.Image] = true
	}
	var inventory []struct {
		ID                    string
		RepoTags, RepoDigests []string
	}
	if err = e.Call(ctx, "GET", "/images/json?all=1", nil, &inventory); err != nil {
		return out, nil, err
	}
	images := []ImageView{}
	for _, i := range inventory {
		include := refs[i.ID]
		for _, tag := range i.RepoTags {
			include = include || refs[tag]
		}
		for _, d := range i.RepoDigests {
			include = include || refs[d]
		}
		if !include {
			continue
		}
		var detail ImageView
		if err = e.Call(ctx, "GET", "/images/"+url.PathEscape(i.ID)+"/json", nil, &detail); err != nil {
			return out, nil, err
		}
		images = append(images, detail)
	}
	sort.Slice(out.Containers, func(i, j int) bool { return out.Containers[i].ID < out.Containers[j].ID })
	return out, images, nil
}

type ImageUpdate struct{ Image, Platform, PreviousID, ID string }

func (p *Provider) RefreshImages(ctx context.Context, report func(ImageUpdate)) error {
	if err := p.enter(ctx); err != nil {
		return err
	}
	defer p.leave()
	if err := p.native.acquire(ctx); err != nil {
		return err
	}
	defer p.native.release()
	seen := map[string]bool{}
	for _, b := range bases.Catalog(p.platforms...) {
		key := b.Image + "\x00" + b.Platform
		if seen[key] {
			continue
		}
		seen[key] = true
		var previous imageInfo
		err := p.native.e.Call(ctx, "GET", "/images/"+url.PathEscape(b.Image)+"/json", nil, &previous)
		if err != nil && !apiStatus(err, 404) {
			return err
		}
		update := ImageUpdate{Image: b.Image, Platform: b.Platform, PreviousID: previous.ID}
		manifest, err := p.native.e.PullDigest(ctx, b.Image, b.Platform)
		if err != nil {
			return err
		}
		image, err := p.native.e.InspectImage(ctx, b.Image, b.Platform)
		if err != nil {
			return err
		}
		var usable imageInfo
		if err = p.native.e.Call(ctx, "GET", "/images/"+url.PathEscape(image.ID)+"/json", nil, &usable); err != nil {
			return err
		}
		if usable.ID != image.ID || image.ID == "" || !image.Matches(b.Platform) {
			return fmt.Errorf("refreshed image identity is unverified")
		}
		if digest.Digest(manifest).Validate() != nil {
			return fmt.Errorf("refreshed image manifest is unverified")
		}
		ref, err := reference.ParseNormalizedNamed(b.Image)
		if err != nil {
			return err
		}
		verified := false
		for _, rd := range image.RepoDigests {
			verified = verified || rd == ref.Name()+"@"+manifest
		}
		if !verified || !strings.HasPrefix(image.ID, "sha256:") {
			return fmt.Errorf("refreshed image manifest is unverified")
		}
		update.ID = image.ID
		if report != nil {
			report(update)
		}
	}
	return nil
}
