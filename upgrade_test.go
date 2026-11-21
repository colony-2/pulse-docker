package docker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	compute "github.com/colony-2/pulse-docker/internal/native"
	provider "github.com/colony-2/pulse-provider-api"
)

type originalFixture struct {
	Launch                                                                  provider.Launch
	Defaults                                                                map[string]string
	NativeJSON, Fingerprint, Name, Accounting, CacheKey, CacheOwner, Cursor string
	CacheLabels                                                             map[string]string
}

func originalFixtures(t *testing.T) []originalFixture {
	t.Helper()
	b, err := os.ReadFile("testdata/upgrade/original.json")
	if err != nil {
		t.Fatal(err)
	}
	var f []originalFixture
	if err = json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	return f
}
func TestUpgradeOriginalSerializedLaunches(t *testing.T) {
	for _, f := range originalFixtures(t) {
		t.Run(f.Launch.LaunchID, func(t *testing.T) {
			n, status, reason := nativeLaunch(f.Launch, []string{"linux/arm64", "linux/amd64"}, f.Defaults)
			if status != "" {
				t.Fatal(status, reason)
			}
			b, err := json.Marshal(n)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != f.NativeJSON || fingerprint(n) != f.Fingerprint {
				t.Fatalf("original fingerprint changed\n got %s\nwant %s", b, f.NativeJSON)
			}
			reversed := map[string]string{}
			reversed["A_FIRST"] = f.Defaults["A_FIRST"]
			reversed["Z_LAST"] = f.Defaults["Z_LAST"]
			reordered, _, _ := nativeLaunch(f.Launch, []string{"linux/arm64", "linux/amd64"}, reversed)
			if fingerprint(reordered) != f.Fingerprint {
				t.Fatal("map insertion order changed identity")
			}
			sum := sha256.Sum256([]byte(n.LaunchID))
			if "pulse-"+hex.EncodeToString(sum[:16]) != f.Name {
				t.Fatal("worker name changed")
			}
			p := &engineProvider{cfg: Config{DependencyCache: CacheConfig{Namespace: "default", Generation: "1"}}}
			accounting, _ := json.Marshal(p.cost(n.Allocation))
			if string(accounting) != f.Accounting {
				t.Fatal("accounting changed", string(accounting), f.Accounting)
			}
			cache, _, err := p.cachePlan(nativePlan{Request: n.Request, ImageID: "sha256:synthetic"}, n.Process.Env)
			if err != nil {
				t.Fatal(err)
			}
			if f.CacheKey != "" && (cache == nil || cache.Key != f.CacheKey || cache.Owner != f.CacheOwner || !reflect.DeepEqual(cache.labels("tools"), f.CacheLabels)) {
				t.Fatal("cache identity changed", cache)
			}
			page, err := compute.Page([]compute.Instance{{ID: "a", LaunchID: n.LaunchID, State: "running"}, {ID: "b", LaunchID: n.LaunchID, State: "running"}}, compute.ListRequest{PageSize: 1, LaunchID: n.LaunchID})
			if err != nil || page.NextPageToken != f.Cursor {
				t.Fatal("cursor changed", page, err)
			}
		})
	}
}
func TestUpgradeOriginalWorkerIsRecognizedWithoutReplay(t *testing.T) {
	f := originalFixtures(t)[0]
	var n compute.Launch
	if err := json.Unmarshal([]byte(f.NativeJSON), &n); err != nil {
		t.Fatal(err)
	}
	d := &daemon{containers: map[string]container{}}
	p := structuredProvider(t, d)
	var c container
	c.ID = "prior-worker"
	c.HostConfig.RestartPolicy.Name = "no"
	c.State.Status = "running"
	c.Config.Labels = metadata(f.Launch)
	c.Config.Labels["pulse_process_fingerprint"] = f.Fingerprint
	c.Config.Labels[accountingLabel] = f.Accounting
	c.Config.Labels[limitsLabel] = "disabled"
	d.containers[c.ID] = c
	p.env = f.Defaults
	out, err := p.Submit(t.Context(), []provider.Launch{f.Launch})
	if err != nil || len(out) != 1 || out[0].Status != provider.Accepted || len(d.creates) != 0 {
		t.Fatal(out, err)
	}
	changed := f.Launch
	changed.Lease = "different-synthetic-lease"
	out, err = p.Submit(t.Context(), []provider.Launch{changed})
	if err != nil || out[0].Status != provider.Rejected || len(d.creates) != 0 {
		t.Fatal("changed immutable payload accepted", out, err)
	}
	changed = f.Launch
	changed.Job.JobID = "changed"
	out, err = p.Submit(t.Context(), []provider.Launch{changed})
	if err != nil || out[0].Status != provider.Rejected || len(d.creates) != 0 {
		t.Fatal(out, err)
	}
}
