package servercli

import (
	"context"
	"fmt"
	docker "github.com/colony-2/pulse-docker/internal/provider"
)

func manage(ctx context.Context, o serverOptions, name string) error {
	ctx, cancel := context.WithTimeout(ctx, o.managementTimeout)
	defer cancel()
	fmt.Fprintln(o.log, "Checking Linux daemon, ownership, limits and executable platforms; initialization may pull probe images and maintain owned caches...")
	p, err := docker.New(ctx, o.config)
	if err != nil {
		return err
	}
	defer p.Close()
	if name == "upgrade" {
		if err = p.RefreshImages(ctx, func(u docker.ImageUpdate) {
			fmt.Fprintf(o.log, "%s %s %s -> %s verified\n", u.Image, u.Platform, u.PreviousID, u.ID)
		}); err != nil {
			return fmt.Errorf("image refresh failed; previously reported updates remain applied: %w", err)
		}
		fmt.Fprintln(o.log, "Built-in worker images refreshed for future launches")
	} else {
		cap, err := p.GetCapabilities(ctx)
		if err != nil {
			return err
		}
		fmt.Fprintf(o.log, "Docker ready: %d bases; CPU budget %dm; memory budget %d bytes; containers %d (zero uses daemon capacity/unlimited)\n", len(cap.Bases), o.config.CPUMillis, o.config.MemoryBytes, o.config.MaxContainers)
	}
	return ctx.Err()
}
