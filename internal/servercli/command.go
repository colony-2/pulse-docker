// Package servercli implements local configuration and SDK hosting for Docker.
package servercli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/colony-2/pulse-docker/internal/config"
	docker "github.com/colony-2/pulse-docker/internal/provider"
	"github.com/spf13/cobra"
)

type serverOptions struct {
	mode, owner, configPath, version, progressMode string
	envFile, registryFile, endpointsFile           string
	config                                         docker.Config
	logging                                        docker.LoggingConfig
	transport                                      transportOptions
	poll, managementTimeout                        time.Duration
	log, out                                       io.Writer
	json                                           bool
	tenant, job, since, launchID                   string
	sinceTime                                      time.Time
	limit                                          int
	follow                                         bool
	offset                                         uint64
}

func NewCommand(version string) *cobra.Command {
	cmd := &cobra.Command{Use: "pulse-docker", Short: "Run and inspect a Pulse Docker provider", Version: version, SilenceErrors: true, SilenceUsage: true}
	cmd.AddCommand(NewServerCommand(), NewSetupCommand(), NewUpgradeCommand(), NewStatusCommand(), NewImagesCommand(), NewLaunchesCommand(), NewLogsCommand())
	return cmd
}
func NewServerCommand() *cobra.Command {
	return command("server", "Serve a Docker provider through the outbound or inbound plugin", nil)
}
func NewSetupCommand() *cobra.Command {
	return command("setup", "Validate Docker access and save local settings", nil)
}
func NewUpgradeCommand() *cobra.Command {
	return command("upgrade", "Refresh built-in worker images for future launches", nil)
}
func NewStatusCommand() *cobra.Command {
	return command("status", "Inspect managed workers and recorded charges", nil)
}
func NewImagesCommand() *cobra.Command {
	return command("images", "Inspect local built-in and worker images", nil)
}
func NewLaunchesCommand() *cobra.Command {
	cmd := command("launches", "Read retained launch observations", nil)
	cmd.AddCommand(command("show LAUNCH_ID", "Read one retained launch observation", nil))
	return cmd
}
func NewLogsCommand() *cobra.Command {
	return command("logs [LAUNCH_ID]", "Read retained worker output", nil)
}
func command(use, short string, run func(context.Context, serverOptions, []string) error) *cobra.Command {
	var o serverOptions
	cmd := &cobra.Command{Use: use, Short: short, SilenceErrors: true, SilenceUsage: true}
	name := cmd.Name()
	cmd.Args = cobra.NoArgs
	if name == "show" {
		cmd.Args = cobra.ExactArgs(1)
	}
	if name == "logs" {
		cmd.Args = cobra.MaximumNArgs(1)
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if err := loadConfiguration(cmd, &o); err != nil {
			return err
		}
		o.version = cmd.Root().Version
		o.log = cmd.ErrOrStderr()
		o.out = cmd.OutOrStdout()
		if err := o.resolve(name); err != nil {
			return err
		}
		if err := cmd.Flags().Set("log-directory", o.logging.Directory); err != nil {
			return err
		}
		for _, flag := range []string{"log-directory", "lock-dir", "scratch-path", "executor-env-file", "registry-auth-file", "legacy-endpoints-file", "tls-ca", "tls-cert", "tls-key", "shared-key-file"} {
			f := cmd.LocalNonPersistentFlags().Lookup(flag)
			if f != nil && f.Value.String() != "" {
				v, err := absolutePath(f.Value.String())
				if err != nil {
					return err
				}
				if err = f.Value.Set(v); err != nil {
					return err
				}
			}
		}
		if run != nil {
			return run(cmd.Context(), o, args)
		}
		var err error
		switch name {
		case "server":
			return runServer(cmd.Context(), o)
		case "setup", "upgrade":
			err = manage(cmd.Context(), o, name)
			if err == nil {
				err = saveConfiguration(cmd, o.configPath)
			}
			return err
		case "status", "images":
			return inspect(cmd.Context(), o, name)
		case "launches", "show":
			return launches(cmd.Context(), o, args)
		case "logs":
			return logs(cmd.Context(), o, args)
		}
		return fmt.Errorf("unknown command")
	}
	f := cmd.Flags()
	f.StringVarP(&o.configPath, "config", "f", "", "Configuration file (default ~/.pulse/docker/provider.json)")
	f.StringVarP(&o.owner, "owner", "o", "server", "Stable archive identity (daemon admission remains shared)")
	f.StringVar(&o.config.Socket, "socket", "", "Local Docker Unix endpoint (default: Docker context discovery)")
	f.StringVar(&o.config.LockDir, "lock-dir", "", "Shared daemon ownership directory")
	f.StringVar(&o.config.ScratchPath, "scratch-path", "/scratch", "Worker scratch path")
	f.Int64Var(&o.config.CPUMillis, "cpu-millis", 0, "Committed CPU budget (0 uses daemon capacity)")
	f.Int64Var(&o.config.MemoryBytes, "memory-bytes", 0, "Committed memory budget (0 uses daemon capacity)")
	f.Int64Var(&o.config.Overhead, "overhead-bytes", 0, "Memory overhead charged per worker")
	f.IntVar(&o.config.MaxContainers, "max-containers", 0, "Concurrent workers (0 unlimited)")
	f.BoolVar(&o.config.DependencyCache.Disabled, "cache-disabled", false, "Disable dependency caches")
	f.StringVar(&o.config.DependencyCache.Namespace, "cache-namespace", "default", "Dependency cache namespace")
	f.StringVar(&o.config.DependencyCache.Generation, "cache-generation", "1", "Dependency cache generation")
	f.DurationVar(&o.config.DependencyCache.MaxAge, "cache-max-age", 720*time.Hour, "Idle dependency cache retirement age")
	f.StringVar(&o.envFile, "executor-env-file", "", "Local JSON executor environment map")
	f.StringVar(&o.registryFile, "registry-auth-file", "", "Private encoded registry auth file")
	f.StringVar(&o.endpointsFile, "legacy-endpoints-file", "", "JSON legacy endpoint mappings")
	f.StringVar(&o.logging.Directory, "log-directory", "", "Private archive directory (default ~/.pulse/docker/logs)")
	f.DurationVar(&o.logging.MaxAge, "log-max-age", 168*time.Hour, "Closed output retention")
	f.Int64Var(&o.logging.MaxBytes, "log-max-bytes", 1<<30, "Archive byte budget per daemon/owner")
	f.BoolVar(&o.logging.Compress, "log-compress", true, "Compress sealed archive segments")
	f.DurationVar(&o.poll, "poll-interval", 2*time.Second, "Maintenance/follow interval")
	f.StringVarP(&o.mode, "mode", "m", "outbound", "Plugin mode: outbound or inbound")
	o.transport.flags(f)
	if name == "setup" || name == "upgrade" {
		f.DurationVar(&o.managementTimeout, "timeout", 2*time.Hour, "Maximum initialization/refresh time")
		f.StringVar(&o.progressMode, "progress", "auto", "Progress: auto, interactive or linear")
	}
	if name == "status" || name == "images" || name == "launches" || name == "show" {
		f.BoolVarP(&o.json, "json", "j", false, "Structured JSON output")
	}
	if name == "launches" || name == "show" {
		f.StringVar(&o.tenant, "tenant", "", "Exact tenant filter")
		f.StringVar(&o.job, "job", "", "Exact job filter")
		f.StringVar(&o.since, "since", "", "Earliest submission (RFC3339 timestamp or duration ago, e.g. 24h)")
		f.IntVar(&o.limit, "limit", 100, "Maximum observations (0 unlimited)")
	}
	if name == "logs" {
		f.BoolVarP(&o.follow, "follow", "F", false, "Follow retained output")
		f.Uint64Var(&o.offset, "offset", 0, "Cumulative retained raw byte offset")
		f.StringVarP(&o.launchID, "launch-id", "i", "", "Launch identity (alternative to positional argument)")
	}
	return cmd
}
func absolutePath(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	if s == "~" || len(s) > 1 && s[:2] == "~/" {
		h, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if s == "~" {
			s = h
		} else {
			s = filepath.Join(h, s[2:])
		}
	}
	return filepath.Abs(s)
}
func (o *serverOptions) resolve(name string) error {
	if o.mode != "inbound" && o.mode != "outbound" {
		return fmt.Errorf("--mode must be outbound or inbound")
	}
	if o.poll <= 0 || ((name == "setup" || name == "upgrade") && o.managementTimeout <= 0) {
		return fmt.Errorf("timeouts and poll interval must be positive")
	}
	if o.progressMode != "" && o.progressMode != "auto" && o.progressMode != "interactive" && o.progressMode != "linear" {
		return fmt.Errorf("invalid --progress")
	}
	if o.logging.Directory == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		o.logging.Directory = filepath.Join(home, ".pulse", "docker", "logs")
	}
	var err error
	for _, p := range []*string{&o.logging.Directory, &o.config.LockDir, &o.config.ScratchPath, &o.envFile, &o.registryFile, &o.endpointsFile, &o.transport.ca, &o.transport.cert, &o.transport.key, &o.transport.sharedKeyFile} {
		*p, err = absolutePath(*p)
		if err != nil {
			return err
		}
	}
	o.logging.Owner = o.owner
	if o.logging, err = o.logging.Normalize(); err != nil {
		return err
	}
	if o.envFile != "" {
		b, e := readFile(o.envFile, 1<<20)
		if e != nil {
			return fmt.Errorf("cannot read executor environment file")
		}
		if err = stringMapJSON(b, &o.config.ExecutorEnv); err != nil {
			return fmt.Errorf("invalid executor environment file")
		}
	}
	if o.registryFile != "" {
		b, e := readPrivateFile(o.registryFile, 1<<20)
		if e != nil {
			return fmt.Errorf("cannot read private registry auth file")
		}
		if err = validateRegistryAuth(b); err != nil {
			return err
		}
		o.config.RegistryAuth = string(bytesTrimLine(b))
	}
	if o.endpointsFile != "" {
		b, e := readFile(o.endpointsFile, 1<<20)
		if e != nil {
			return fmt.Errorf("cannot read endpoint mappings")
		}
		if strictJSON(b, &o.config.LegacyEndpoints) != nil {
			return fmt.Errorf("invalid endpoint mappings")
		}
	}
	o.config, err = config.Normalize(o.config)
	if err != nil {
		return err
	}
	if o.config.Socket != "" {
		o.config.Socket, err = docker.ResolveSocket(o.config.Socket)
		if err != nil {
			return err
		}
	}
	if name == "server" {
		if err = o.transport.validate(o.mode, o.owner); err != nil {
			return err
		}
	}
	if name == "setup" || name == "upgrade" {
		if err = o.transport.validateSettings(o.mode, o.owner); err != nil {
			return err
		}
		if err = o.transport.validateLocalCredentials(); err != nil {
			return err
		}
	}
	if o.limit < 0 {
		return fmt.Errorf("--limit must be nonnegative")
	}
	if o.since != "" {
		o.sinceTime, err = time.Parse(time.RFC3339, o.since)
		if err != nil {
			duration, parseErr := time.ParseDuration(o.since)
			if parseErr != nil || duration < 0 {
				return fmt.Errorf("--since requires an RFC3339 timestamp or nonnegative duration (e.g. 24h)")
			}
			o.sinceTime = time.Now().Add(-duration)
		}
	}
	return nil
}
func outputJSON(w io.Writer, v any) error {
	e := json.NewEncoder(w)
	e.SetIndent("", "  ")
	return e.Encode(v)
}

// Testable command factory; production and embedded commands use the same path.
func newCommand(run func(context.Context, serverOptions) error) *cobra.Command {
	cmd := NewCommand("test")
	for _, child := range cmd.Commands() {
		if child.Name() == "server" {
			cmd.RemoveCommand(child)
		}
	}
	cmd.AddCommand(command("server", "Serve a Docker provider", func(ctx context.Context, o serverOptions, _ []string) error { return run(ctx, o) }))
	return cmd
}
