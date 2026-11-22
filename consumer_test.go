package docker_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// This checks early isolation against this workspace. A resulting managed Git
// reference must separately pass scripts/verify-consumer.sh without a replacement.
func TestIndependentConsumerIsolation(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	mod := fmt.Sprintf("module independent.example/host\n\ngo 1.26.0\n\nrequire github.com/colony-2/pulse-docker v0.0.0\nreplace github.com/colony-2/pulse-docker => %q\n", root)
	if err = os.WriteFile(filepath.Join(dir, "go.mod"), []byte(mod), 0600); err != nil {
		t.Fatal(err)
	}
	consumer := `package host
import("context";"time";docker "github.com/colony-2/pulse-docker";"github.com/colony-2/pulse-docker/cli";provider "github.com/colony-2/pulse-provider-api")
var _ provider.Provider=(*docker.Provider)(nil)
var _ provider.ProviderWithLaunchLogs=(*docker.Provider)(nil)
var _ = docker.Config{docker.CacheConfig{},"","","","",0,0,0,0,nil,nil}
var _ = docker.NewWithOptions
var _ = docker.Options{Logging:&docker.LoggingConfig{}}
var _ = cli.NewCommand("consumer")
func Run(ctx context.Context, l provider.Launch) error {
 cfg:=docker.Config{Socket:"",LockDir:"",ScratchPath:"/scratch",RegistryAuth:"",CPUMillis:1000,MemoryBytes:1<<30,Overhead:0,MaxContainers:2,ExecutorEnv:map[string]string{"CUSTOM":"value"},LegacyEndpoints:[]docker.LegacyEndpoint{{InstanceID:"db",TenantID:"tenant",Endpoint:"http://jobdb/tenant"}},DependencyCache:docker.CacheConfig{Disabled:false,Namespace:"test",Generation:"1",MaxAge:30*24*time.Hour}}
 p,err:=docker.New(ctx,cfg);if err!=nil{return err};defer p.Close()
 var api provider.Provider=p
 _,err=api.GetCapabilities(ctx);if err!=nil{return err}
 _,_,err=api.Submit(ctx,[]provider.Launch{l},provider.SubmitOptions{});if err!=nil{return err}
 _,err=api.List(ctx,provider.ListRequest{});if err!=nil{return err}
 return p.Maintain(ctx)
}
`
	if err = os.WriteFile(filepath.Join(dir, "host.go"), []byte(consumer), 0600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "go", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("go %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	run("mod", "tidy")
	run("test", "-mod=readonly", "./...")
	graph := run("list", "-mod=readonly", "-m", "all")
	for _, line := range strings.Split(graph, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "github.com/colony-2/pulse" {
			t.Fatal("Pulse dependency", line)
		}
	}
	api := run("doc", "-all", "github.com/colony-2/pulse-docker")
	for _, forbidden := range []string{"type Process ", "type Launch ", "NativeProvider(", "type Engine ", "type SecretInput "} {
		if strings.Contains(api, forbidden) {
			t.Fatal("native API escaped", forbidden)
		}
	}
}
