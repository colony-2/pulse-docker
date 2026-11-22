# pulse-docker

`github.com/colony-2/pulse-docker` implements
[`pulse-provider-api.Provider`](https://github.com/colony-2/pulse-provider-api)
for a local Linux Docker daemon. Import its root package as `docker`.
It executes a caller-assigned c2j lease, with tenant-isolated dependency caches,
committed-capacity admission, and active-instance listing. It has no direct or
transitive Pulse dependency.

```go
p, err := docker.New(ctx, docker.Config{
    ExecutorEnv: map[string]string{"APP_MODE": "worker"},
})
if err != nil { return err }
defer p.Close()

var backend provider.Provider = p
results, _, err := backend.Submit(ctx, []provider.Launch{launch}, provider.SubmitOptions{})
// Inspect each valid result by LaunchID even when err != nil.
// Unknown or missing results never authorize retry or immediate fallback.
```

The host supplies `launch`, including its JobRef, resource/base constraints and
opaque exported lease. The library runs `c2j run with-lease` and sends the exact
lease on stdin. c2j validates authority and owns execution and renewal. The host
owns work discovery, lease acquisition, policy, endpoint rewriting, configuration
files and reuse of equivalent socket aliases. Neither a successful submission nor
a missing listing establishes job completion.

Read the [public integration contract](docs/integration.md) for configuration,
closure and result handling, and [compatibility notes](docs/compatibility.md) for
existing workers, caches, serialized identities and uncertainty. The compiling
[example](example_test.go) shows interface assignment, submission and maintenance.

Go 1.26 is required. The shared API is pinned to
`v0.0.0-20261120061856-2919f1ed5e42`. Submit with empty options for Docker execution. Production code imports only that API, private
helpers, distribution/reference and go-digest. c2j and JobDB dependencies support
the real executor and cache fixtures. Native process types are confined to the
module's `internal/` boundary; no root API accepts arbitrary commands.

Run `./scripts/verify.sh` for focused/full deterministic tests, race, vet, build,
fixture compilation, module verification and tidy-diff. This is also the
verification hook's entry point. [Live testing](docs/testing.md) explains the
separate required Docker suites and prerequisites. Deterministic success does
not certify live acceptance.

Once the managed candidate has an ordinary accessible Git reference,
`./scripts/verify-consumer.sh REF` resolves and compiles it in a fresh module
without replacements. This uses ordinary repository availability; no release,
tag, package or extra publication milestone is needed.

Extracted from Pulse `6c20e087383ac7c333f562aa373c873bd5423db6` under Apache-2.0.
See [LICENSE](LICENSE), [NOTICE](NOTICE), and the original-source
[upgrade fixtures](testdata/upgrade/README.md).
