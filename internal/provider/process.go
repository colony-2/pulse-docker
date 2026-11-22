package provider

import (
	"fmt"
	compute "github.com/colony-2/pulse-docker/internal/native"
	"github.com/colony-2/pulse-docker/internal/quantity"
	"strings"
)

func executorProcess(jobdb, job string, a compute.Allocation, extra map[string]string, metadata map[string]string) (compute.Process, error) {
	env := map[string]string{}
	for k, v := range extra {
		if strings.HasPrefix(k, "C2J_EXECUTION_") || k == "C2J_JOBDB" || strings.HasPrefix(k, "PULSE_") {
			return compute.Process{}, fmt.Errorf("reserved executor environment key %s", k)
		}
		env[k] = v
	}
	if a.CPUMillis > 0 {
		env["C2J_EXECUTION_CPU"] = quantity.CPU(a.CPUMillis)
	}
	if a.MemoryBytes > 0 {
		env["C2J_EXECUTION_MEMORY"] = quantity.Bytes(a.MemoryBytes)
	}
	if a.ScratchBytes > 0 {
		env["C2J_EXECUTION_EPHEMERAL_STORAGE"] = quantity.Bytes(a.ScratchBytes)
	}
	env["C2J_EXECUTION_PLATFORM"] = a.Platform
	env["C2J_EXECUTION_IMAGE"] = a.Image
	// The provider must enforce this image constraint before starting the process,
	// just as it must guarantee the requested resources advertised above.
	if _, pinnedDigest, pinned := strings.Cut(a.Image, "@"); pinned {
		env["C2J_EXECUTION_IMAGE_DIGEST"] = pinnedDigest
	}
	for k, v := range metadata {
		env[strings.ToUpper(k)] = v
	}
	return compute.Process{Command: []string{"c2j"}, Args: []string{"run", "with-lease", "--jobdb", jobdb, "--job-id", job, "--lease-file", "-", "--ci", "--input-mode", "fail"}, Env: env}, nil
}
