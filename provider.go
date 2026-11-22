// Package docker executes caller-assigned c2j leases using a local Docker daemon.
package docker

import (
	"context"
	"github.com/colony-2/pulse-docker/internal/config"
	"github.com/colony-2/pulse-docker/internal/engine"
	implementation "github.com/colony-2/pulse-docker/internal/provider"
	api "github.com/colony-2/pulse-provider-api"
)

type Config = config.Config
type CacheConfig = config.CacheConfig
type LegacyEndpoint = config.LegacyEndpoint

// Provider owns admission to one daemon. Construct with New and do not copy.
type Provider struct{ inner *implementation.Provider }

func New(ctx context.Context, cfg Config) (*Provider, error) {
	p, err := implementation.New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Provider{inner: p}, nil
}
func ResolveSocket(socket string) (string, error)      { return engine.ResolveSocket(socket) }
func (p *Provider) Close()                             { p.inner.Close() }
func (p *Provider) Maintain(ctx context.Context) error { return p.inner.Maintain(ctx) }
func (p *Provider) GetCapabilities(ctx context.Context) (api.Capabilities, error) {
	return p.inner.GetCapabilities(ctx)
}
func (p *Provider) Submit(ctx context.Context, l []api.Launch, o api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
	return p.inner.Submit(ctx, l, o)
}
func (p *Provider) List(ctx context.Context, q api.ListRequest) (api.ListResponse, error) {
	return p.inner.List(ctx, q)
}

// LoggingConfig selects private retained output and independent live listeners.
type LoggingConfig = implementation.LoggingConfig
type Options = implementation.Options

// NewWithOptions enables optional host capture. New remains archive-free.
func NewWithOptions(ctx context.Context, cfg Config, o Options) (*Provider, error) {
	p, err := implementation.NewWithOptions(ctx, cfg, o)
	if err != nil {
		return nil, err
	}
	return &Provider{inner: p}, nil
}
func (p *Provider) GetLaunchLog(ctx context.Context, q api.LaunchLogRequest) (api.LaunchLogResponse, error) {
	return p.inner.GetLaunchLog(ctx, q)
}
