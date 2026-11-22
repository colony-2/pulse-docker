package config

import (
	"fmt"
	"github.com/colony-2/pulse-docker/internal/providerutil"
	provider "github.com/colony-2/pulse-provider-api"
	"maps"
	"path/filepath"
	"strings"
	"time"
)

// LegacyEndpoint restores job endpoints on workers created before endpoint labels.
// Both instance and tenant identities participate in lookup.
type LegacyEndpoint struct{ InstanceID, TenantID, Endpoint string }

func Normalize(cfg Config) (Config, error) {
	if err := cfg.DependencyCache.Normalize(); err != nil {
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

// Config supplies local Docker connectivity, committed-capacity budgets and
// copied host defaults. See docs/integration.md for zero values and validation.
type Config struct {
	DependencyCache                            CacheConfig
	Socket, LockDir, ScratchPath, RegistryAuth string
	CPUMillis, MemoryBytes, Overhead           int64
	MaxContainers                              int
	ExecutorEnv                                map[string]string
	LegacyEndpoints                            []LegacyEndpoint
}

// CacheConfig controls cache identity and retention.
// Its zero value enables automatic, tenant-isolated caching of the Colony base.
type CacheConfig struct {
	Disabled              bool
	Namespace, Generation string
	MaxAge                time.Duration
}

func (c *CacheConfig) Normalize() error {
	if c.Namespace == "" {
		c.Namespace = "default"
	}
	if c.Generation == "" {
		c.Generation = "1"
	}
	if c.MaxAge == 0 {
		c.MaxAge = 30 * 24 * time.Hour
	}
	if c.MaxAge < 0 || c.MaxAge > 365*24*time.Hour || len(c.Namespace) > 128 || len(c.Generation) > 128 || strings.ContainsAny(c.Namespace+c.Generation, "\x00\r\n") {
		return fmt.Errorf("invalid Docker dependency cache configuration")
	}
	return nil
}

const cachePath = "/var/cache/pulse"

func pathsOverlap(a, b string) bool {
	a, b = filepath.Clean(a), filepath.Clean(b)
	return a == "/" || b == "/" || a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
