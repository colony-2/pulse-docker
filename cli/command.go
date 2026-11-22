// Package cli exposes fresh Cobra commands for standalone use and embedding.
package cli

import (
	"github.com/colony-2/pulse-docker/internal/servercli"
	"github.com/spf13/cobra"
)

func NewCommand(version string) *cobra.Command { return servercli.NewCommand(version) }
func NewServerCommand() *cobra.Command         { return servercli.NewServerCommand() }
func NewSetupCommand() *cobra.Command          { return servercli.NewSetupCommand() }
func NewUpgradeCommand() *cobra.Command        { return servercli.NewUpgradeCommand() }
func NewStatusCommand() *cobra.Command         { return servercli.NewStatusCommand() }
func NewImagesCommand() *cobra.Command         { return servercli.NewImagesCommand() }
func NewLaunchesCommand() *cobra.Command       { return servercli.NewLaunchesCommand() }
func NewLogsCommand() *cobra.Command           { return servercli.NewLogsCommand() }
