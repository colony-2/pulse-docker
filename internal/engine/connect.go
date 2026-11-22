package engine

import (
	"context"
	"fmt"
	"regexp"
)

type Info struct {
	ID                                          string
	NCPU                                        int
	MemTotal                                    int64
	OSType, Architecture, OperatingSystem, Name string
	MemoryLimit, SwapLimit, CPUCfsQuota         bool
}

func Connect(ctx context.Context, socket string) (*Engine, Info, error) {
	var info Info
	socket, err := ResolveSocket(socket)
	if err != nil {
		return nil, info, err
	}
	e := New(socket)
	var version struct {
		APIVersion string `json:"ApiVersion"`
	}
	if err = e.Call(ctx, "GET", "/version", nil, &version); err == nil {
		if !regexp.MustCompile(`^1\.[0-9]+$`).MatchString(version.APIVersion) {
			err = fmt.Errorf("invalid Docker API version")
		} else {
			e.Version = "/v" + version.APIVersion
			err = e.Call(ctx, "GET", "/info", nil, &info)
		}
	}
	if err != nil {
		e.Client.CloseIdleConnections()
		return nil, info, err
	}
	if info.ID == "" {
		e.Client.CloseIdleConnections()
		return nil, info, fmt.Errorf("Docker daemon has no identity")
	}
	return e, info, nil
}
