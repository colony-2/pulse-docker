package docker_test

import (
	"context"
	"fmt"
	"time"

	docker "github.com/colony-2/pulse-docker"
	provider "github.com/colony-2/pulse-provider-api"
)

// This compiles without a daemon. A host obtains the launch and its exported
// lease from its own control plane before calling Submit.
func ExampleNew() {
	run := func(ctx context.Context, launch provider.Launch) error {
		p, err := docker.New(ctx, docker.Config{ExecutorEnv: map[string]string{"APP_MODE": "worker"}})
		if err != nil {
			return err
		}
		defer p.Close()
		var backend provider.Provider = p
		results, _, err := backend.Submit(ctx, []provider.Launch{launch}, provider.SubmitOptions{})
		// Consume valid correlated results even when err is non-nil. Do not retry
		// unknown/missing results or treat them as permission for fallback.
		for _, result := range results {
			fmt.Println(result.LaunchID, result.Status)
		}
		if err != nil {
			return err
		}
		maintenance, stop := context.WithTimeout(ctx, time.Minute)
		defer stop()
		return p.Maintain(maintenance)
	}
	_ = run
}
