//go:build ignore

// Local verification tooling only; this is not a provider executable.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

const modulePath = "github.com/colony-2/pulse-docker"

func main() {
	if len(os.Args) != 4 {
		fail(fmt.Errorf("usage: verifycheck live|consumer MODE_OR_HASH JSON_FILE"))
	}
	f, err := os.Open(os.Args[3])
	if err != nil {
		fail(err)
	}
	defer f.Close()
	switch os.Args[1] {
	case "live":
		err = live(f, os.Args[2])
	case "consumer":
		err = consumer(f, os.Args[2])
	default:
		err = fmt.Errorf("unknown check")
	}
	if err != nil {
		fail(err)
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(2) }

func live(r io.Reader, mode string) error {
	parents := []string{"TestDockerLifecycleResourcesIntegration", "TestDockerLeaseIntegration", "TestDockerDependencyCacheIntegration", "TestDockerNixFetchCacheIntegration", "TestDockerMultiPlatformIntegration", "TestDockerUpgradeIntegration"}
	switch mode {
	case "cli":
		parents = []string{"TestDockerCLIIntegration"}
	case "linux":
	case "remote":
		parents = []string{"TestDockerRemoteInboundLeaseIntegration", "TestDockerRemoteOutboundLeaseIntegration", "TestDockerFastCaptureIntegration", "TestDockerCaptureRestartIntegration", "TestDockerManagementIntegration"}
	case "desktop":
		parents = []string{"TestDockerDesktopIntegration"}
	case "lease-completion":
		parents = []string{"TestDockerLeaseIntegration"}
	default:
		return fmt.Errorf("unknown live mode")
	}
	allowed := map[string]bool{}
	for _, p := range parents {
		allowed[p] = true
	}
	started, ended := map[string]bool{}, map[string]bool{}
	packageStarted, packageDone := false, false
	branch, native, alternative := "", "", ""
	platformLine := regexp.MustCompile(`D28 native=(linux/(?:arm64|amd64)) alternative=(linux/(?:arm64|amd64))`)
	decoder := json.NewDecoder(r)
	for {
		var e struct{ Action, Package, Test, Output string }
		err := decoder.Decode(&e)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("invalid live event stream: %w", err)
		}
		if packageDone || e.Package != modulePath+"/internal/provider" {
			return fmt.Errorf("unexpected live event after completion or from another package")
		}
		if e.Action == "fail" || e.Action == "skip" {
			return fmt.Errorf("required live coverage %s: %s", e.Action, e.Test)
		}
		if e.Test == "" {
			switch e.Action {
			case "start":
				if packageStarted {
					return fmt.Errorf("duplicate package start")
				}
				packageStarted = true
			case "output":
				if !packageStarted {
					return fmt.Errorf("output before package start")
				}
			case "pass":
				if !packageStarted {
					return fmt.Errorf("package passed without start")
				}
				packageDone = true
			default:
				return fmt.Errorf("unexpected package action %q", e.Action)
			}
			continue
		}
		parent := strings.SplitN(e.Test, "/", 2)[0]
		if !packageStarted || !allowed[parent] {
			return fmt.Errorf("unexpected test %q", e.Test)
		}
		if mode == "lease-completion" && e.Test != parent && e.Test != parent+"/completed_lease" {
			return fmt.Errorf("unexpected lease subtest")
		}
		switch e.Action {
		case "run":
			if started[e.Test] {
				return fmt.Errorf("duplicate test run")
			}
			started[e.Test] = true
		case "pass":
			if !started[e.Test] || ended[e.Test] {
				return fmt.Errorf("test passed without a unique run: %s", e.Test)
			}
			ended[e.Test] = true
			if strings.HasPrefix(e.Test, "TestDockerMultiPlatformIntegration/") {
				switch e.Test {
				case "TestDockerMultiPlatformIntegration/unavailable_emulation_declines", "TestDockerMultiPlatformIntegration/verified_emulation":
					if branch != "" {
						return fmt.Errorf("conflicting D28 branches")
					}
					branch = e.Test
				default:
					return fmt.Errorf("unexpected D28 branch")
				}
			}
		case "output", "pause", "cont":
			if !started[e.Test] || ended[e.Test] {
				return fmt.Errorf("event outside running test")
			}
			if strings.HasPrefix(e.Test, "TestDockerMultiPlatformIntegration/") {
				if m := platformLine.FindStringSubmatch(e.Output); m != nil {
					if native != "" {
						return fmt.Errorf("duplicate D28 platform record")
					}
					native, alternative = m[1], m[2]
				}
			}
		default:
			return fmt.Errorf("unexpected test action %q", e.Action)
		}
	}
	if !packageDone {
		return fmt.Errorf("incomplete package result")
	}
	for name := range started {
		if !ended[name] {
			return fmt.Errorf("incomplete test result: %s", name)
		}
	}
	for _, p := range parents {
		if !ended[p] {
			return fmt.Errorf("required suite did not pass: %s", p)
		}
	}
	if mode == "lease-completion" && !ended[parents[0]+"/completed_lease"] {
		return fmt.Errorf("required completed-lease subtest did not pass")
	}
	if mode == "cli" {
		for _, scenario := range []string{"outbound_restart", "inbound_parent_loss", "invalid_leases", "invalid_leases/expired", "invalid_leases/completed"} {
			if !ended[parents[0]+"/"+scenario] {
				return fmt.Errorf("required CLI scenario did not pass: %s", scenario)
			}
		}
	}
	if mode == "linux" {
		if branch == "" || native == "" || native == alternative {
			return fmt.Errorf("missing or invalid D28 branch/platform result")
		}
		if strings.HasSuffix(branch, "/unavailable_emulation_declines") {
			fmt.Printf("D28 native=%s alternative=%s: safe decline proved; alternative execution unexercised\n", native, alternative)
		} else {
			fmt.Printf("D28 native=%s alternative=%s: verified execution passed\n", native, alternative)
		}
	}
	fmt.Printf("Live verification passed (%s scope).\n", mode)
	return nil
}

func consumer(r io.Reader, ref string) error {
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(ref) {
		return fmt.Errorf("reference must be a full Git hash")
	}
	var m struct {
		Path, Version, Sum, Error string
		Replace                   *json.RawMessage
		Origin                    *struct{ VCS, URL, Hash string }
	}
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&m); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("unexpected module data")
	}
	if m.Path != modulePath || m.Replace != nil || m.Error != "" || !regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+incompatible)?$`).MatchString(m.Version) || !regexp.MustCompile(`^h1:[A-Za-z0-9+/]{43}=$`).MatchString(m.Sum) {
		return fmt.Errorf("invalid resolved module identity or checksum")
	}
	pseudo := regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+-(?:[0-9A-Za-z.-]+\.)?([0-9]{14})-([0-9a-f]{12})(?:\+incompatible)?$`).FindStringSubmatch(m.Version)
	if pseudo != nil && pseudo[2] != ref[:12] {
		return fmt.Errorf("pseudo-version hash does not match requested commit")
	}
	method := "Go module origin"
	if m.Origin != nil {
		if m.Origin.VCS != "git" || strings.TrimSuffix(m.Origin.URL, ".git") != "https://"+modulePath || m.Origin.Hash != ref {
			return fmt.Errorf("resolved source identity does not match requested Git hash")
		}
	} else {
		// Cached module metadata may lack Origin. Verify the actual Git source and
		// canonical pseudo-version timestamp/hash (or the resolved tag's commit).
		dir, err := os.MkdirTemp("", "docker-source-identity-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		git := func(args ...string) (string, error) {
			cmd := exec.Command("git", args...)
			cmd.Dir = dir
			out, err := cmd.CombinedOutput()
			if err != nil {
				return "", fmt.Errorf("Git identity verification failed: %w", err)
			}
			return strings.TrimSpace(string(out)), nil
		}
		if _, err := git("init", "--quiet"); err != nil {
			return err
		}
		source := "https://" + modulePath + ".git"
		if _, err := git("fetch", "--quiet", "--depth=1", source, ref); err != nil {
			return err
		}
		hash, err := git("rev-parse", "FETCH_HEAD^{commit}")
		if err != nil || hash != ref {
			return fmt.Errorf("unverifiable Git source identity")
		}
		if pseudo != nil {
			stamp, err := git("show", "-s", "--format=%cI", "FETCH_HEAD")
			if err != nil {
				return err
			}
			when, err := time.Parse(time.RFC3339, stamp)
			if err != nil || pseudo[2] != ref[:12] || pseudo[1] != when.UTC().Format("20060102150405") {
				return fmt.Errorf("pseudo-version does not match source commit")
			}
		} else {
			if !regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$`).MatchString(m.Version) {
				return fmt.Errorf("invalid module version")
			}
			if _, err := git("fetch", "--quiet", "--depth=1", source, "refs/tags/"+m.Version); err != nil {
				return err
			}
			tag, err := git("rev-parse", "FETCH_HEAD^{commit}")
			if err != nil || tag != ref {
				return fmt.Errorf("resolved tag does not match source commit")
			}
		}
		method = "Git source commit/version mapping"
	}
	fmt.Printf("requested_git_hash=%s\nresolved_module=%s@%s\nmodule_checksum=%s\nsource_git_hash=%s\nidentity_method=%s\n", ref, m.Path, m.Version, m.Sum, ref, method)
	return nil
}
