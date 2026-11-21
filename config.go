package docker

import (
	"fmt"
	"github.com/colony-2/pulse-docker/internal/providerutil"
	provider "github.com/colony-2/pulse-provider-api"
	"maps"
	"path/filepath"
	"strings"
)

// LegacyEndpoint restores job endpoints on workers created before endpoint labels.
// Both instance and tenant identities participate in lookup.
type LegacyEndpoint struct{ InstanceID, TenantID, Endpoint string }

func normalizeConfig(cfg Config) (Config, error) {
	if err := cfg.DependencyCache.normalize(); err != nil {
		return Config{}, err
	}
	if cfg.CPUMillis < 0 || cfg.MemoryBytes < 0 || cfg.MaxContainers < 0 || cfg.Overhead < 0 {
		return Config{}, fmt.Errorf("invalid Docker capacity budget")
	}
	if cfg.ScratchPath == "" {
		cfg.ScratchPath = "/scratch"
	}
	if !filepath.IsAbs(cfg.ScratchPath) || filepath.Clean(cfg.ScratchPath) == "/" || strings.Contains(cfg.ScratchPath, ":") {
		return Config{}, fmt.Errorf("invalid scratch path")
	}
	if !cfg.DependencyCache.Disabled && (pathsOverlap(cfg.ScratchPath, "/nix") || pathsOverlap(cfg.ScratchPath, cachePath)) {
		return Config{}, fmt.Errorf("scratch path overlaps dependency cache")
	}

	cfg.ExecutorEnv = maps.Clone(cfg.ExecutorEnv)
	for k, v := range cfg.ExecutorEnv {
		if k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(v, 0) {
			return Config{}, fmt.Errorf("invalid executor environment entry")
		}
		if strings.HasPrefix(k, "C2J_EXECUTION_") || k == "C2J_JOBDB" || strings.HasPrefix(k, "PULSE_") {
			return Config{}, fmt.Errorf("reserved executor environment key")
		}
	}
	cfg.LegacyEndpoints = append([]LegacyEndpoint(nil), cfg.LegacyEndpoints...)
	seen := map[string]string{}
	for _, e := range cfg.LegacyEndpoints {
		if providerutil.Job(provider.JobRef{InstanceID: e.InstanceID, TenantID: e.TenantID, JobDB: e.Endpoint, JobID: "validation"}) != nil {
			return Config{}, fmt.Errorf("invalid legacy endpoint")
		}
		key := e.InstanceID + "\x00" + e.TenantID
		if old, ok := seen[key]; ok && old != e.Endpoint {
			return Config{}, fmt.Errorf("conflicting legacy endpoints")
		}
		seen[key] = e.Endpoint
	}
	return cfg, nil
}
