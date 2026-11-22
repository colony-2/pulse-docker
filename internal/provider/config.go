package provider

import "github.com/colony-2/pulse-docker/internal/config"

type Config = config.Config
type CacheConfig = config.CacheConfig
type LegacyEndpoint = config.LegacyEndpoint

var normalizeConfig = config.Normalize
