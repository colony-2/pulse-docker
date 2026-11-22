package servercli

import (
	"context"
	"errors"
	"fmt"
	"github.com/colony-2/pulse-docker/internal/engine"
	"github.com/colony-2/pulse-docker/internal/launchlogs"
	docker "github.com/colony-2/pulse-docker/internal/provider"
	api "github.com/colony-2/pulse-provider-api"
	"text/tabwriter"
	"time"
)

func inspect(ctx context.Context, o serverOptions, name string) error {
	ctx, cancel := context.WithTimeout(ctx, o.transport.startup)
	defer cancel()
	status, images, err := docker.Inspect(ctx, o.config)
	if err != nil {
		return err
	}
	if name == "images" {
		if o.json {
			return outputJSON(o.out, images)
		}
		table := tabwriter.NewWriter(o.out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(table, "IMAGE\tPLATFORM\tDIGESTS")
		for _, i := range images {
			fmt.Fprintf(table, "%s\t%s/%s\t%v\n", i.ID, i.OS, i.Architecture, i.RepoDigests)
		}
		return table.Flush()
	}
	if o.json {
		return outputJSON(o.out, status)
	}
	table := tabwriter.NewWriter(o.out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "CONTAINER\tSTATE\tLAUNCH\tTENANT\tJOB\tCPU\tMEMORY")
	for _, c := range status.Containers {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%dm\t%d\n", c.ID, c.State, c.LaunchID, c.TenantID, c.JobID, c.CPUMillis, c.MemoryBytes)
	}
	if err := table.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(o.out, "Configured: CPU=%dm memory=%d containers=%d; observed: CPU=%dm memory=%d containers=%d\n%s\n", status.Configured.CPUMillis, status.Configured.MemoryBytes, status.Configured.MaxContainers, status.Observed.CPUMillis, status.Observed.MemoryBytes, status.Observed.MaxContainers, status.Note)
	return nil
}
func archiveRoot(ctx context.Context, o serverOptions) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, o.transport.startup)
	defer cancel()
	e, info, err := engine.Connect(ctx, o.config.Socket)
	if err != nil {
		return "", err
	}
	e.Client.CloseIdleConnections()
	return launchlogs.Scope(o.logging.Directory, info.ID, o.owner), nil
}
func launches(ctx context.Context, o serverOptions, args []string) error {
	root, err := archiveRoot(ctx, o)
	if err != nil {
		return err
	}
	history, err := launchlogs.Histories(root)
	if err != nil {
		return err
	}
	out := []launchlogs.History{}
	for _, h := range history {
		if len(args) > 0 && h.LaunchID != args[0] {
			continue
		}
		if o.tenant != "" && h.TenantID != o.tenant || o.job != "" && h.JobID != o.job || !o.sinceTime.IsZero() && (h.SubmittedAt.IsZero() || h.SubmittedAt.Before(o.sinceTime)) {
			continue
		}
		out = append(out, h)
		if o.limit > 0 && len(out) == o.limit {
			break
		}
	}
	if len(args) > 0 && len(out) == 0 {
		return api.ErrLaunchLogNotFound
	}
	if o.json {
		if len(args) > 0 {
			return outputJSON(o.out, out[0])
		}
		return outputJSON(o.out, out)
	}
	table := tabwriter.NewWriter(o.out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "SUBMITTED\tTENANT\tJOB\tADMISSION\tCAPTURE\tLAUNCH")
	for _, h := range out {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n", h.SubmittedAt.Format(time.RFC3339), h.TenantID, h.JobID, h.Admission, h.Capture, h.LaunchID)
	}
	return table.Flush()
}
func logs(ctx context.Context, o serverOptions, args []string) error {
	if len(args) > 0 {
		if o.launchID != "" {
			return fmt.Errorf("use a positional launch ID or --launch-id")
		}
		o.launchID = args[0]
	}
	if o.launchID == "" {
		return fmt.Errorf("launch ID is required")
	}
	root, err := archiveRoot(ctx, o)
	if err != nil {
		return err
	}
	var token string
	var consumed uint64
	warned := false
	for {
		page, err := launchlogs.Read(ctx, root, api.LaunchLogRequest{LaunchID: o.launchID, PageToken: token})
		if err != nil {
			return err
		}
		if page.Incomplete && !warned {
			fmt.Fprintln(o.log, "Warning: retained capture is incomplete or interrupted")
			warned = true
		}
		for _, r := range page.Records {
			if r.Source == api.LogSourceLaunch {
				fmt.Fprint(o.log, string(r.Data))
				continue
			}
			end := consumed + uint64(len(r.Data))
			if end > o.offset {
				start := uint64(0)
				if o.offset > consumed {
					start = o.offset - consumed
				}
				if _, err = o.out.Write(r.Data[start:]); err != nil {
					return err
				}
			}
			consumed = end
		}
		token = page.NextPageToken
		if page.HasMore {
			continue
		}
		if consumed < o.offset {
			return fmt.Errorf("offset is beyond retained output")
		}
		if !o.follow || page.Finalized {
			if page.Incomplete {
				return fmt.Errorf("retained capture is incomplete or interrupted")
			}
			return nil
		}
		timer := time.NewTimer(o.poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}
