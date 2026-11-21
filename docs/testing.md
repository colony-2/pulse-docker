# Docker library verification

`./scripts/verify.sh` is the deterministic verification hook entry point. It
runs a nonempty focused selection, the full deterministic suite and race suite,
vet, read-only production and fixture builds, tidy-diff, module verification and
live-suite compilation and the deterministic persisted-failure and completed-lease
fixture checks.
The full suite also compiles a fresh external consumer,
checks its module graph and inspects the exported API. This early consumer uses a
scratch replacement; it is not resulting-reference evidence.

The failed-command harness substitutes a controlled Go command and proves that a
required failure propagates unchanged through the hook entry point, without a
success report. `TestVerifyHookInputContract` also parses the actual hook and
checks its typed build-runner inputs, artifact bindings and standalone defaults.
With c2j available, `c2j test run --file .c2/tests/verify-hook.test.json` exercises
the recipe interface using a controlled command operation. It checks input
validation, candidate forwarding as environment data and command-error
propagation; the real deterministic suite still runs through `scripts/verify.sh`.
The tagged integration check requires the persisted-failure
regression and the completed-lease fixture check to be present and runs both
without Docker while compiling the live suites. The latter uses real SQLite and
the remote JobDB adapter to verify final-chapter completion and rejection of the
exported completed lease. It does not execute live scenarios.

Run `scripts/verify-consumer.sh REF` on the resulting ordinary managed Git
reference for the separate no-replacement consumer check. Record the resolved Go
module version, Git hash and checksum. Supply the full immutable Git hash; the
script checks Go origin metadata, or verifies Git commit/version mapping when
origin data is absent. It rejects replacements and Pulse
anywhere in the module graph. There is no release/tag requirement.
Go requires the commit to be reachable from the repository's ordinary advertised
history. A successful direct Git fetch of an unadvertised snapshot does not prove
Go module resolvability. If the candidate is not yet reachable, preserve the failed
consumer result and run this check after normal managed delivery supplies that
history; do not seed Go's VCS cache or create a separate publication ref.

## Required Linux execution

The live suites and their fixture binaries support `CGO_ENABLED=0`; a C compiler
is not a prerequisite for these behavior checks. Keep CGO enabled for the separate
deterministic race suite, which does require a working compiler.

Run `scripts/verify-live.sh linux` on a provisioned Linux Docker environment. It
selects exactly these six suites:

* `TestDockerLifecycleResourcesIntegration`: concurrent unbounded workers,
  committed budgets after close/reopen, process/stdin behavior, limits, scratch,
  active listing and expired-deadline decline.
* `TestDockerLeaseIntegration`: installed c2j and real JobDB; exact assigned work,
  heartbeat, peer/host-gateway connectivity, recipe errors/timeouts, invalid
  expired/completed leases, import/renewal transport failures, and queued unrelated
  work left untouched. Fixture gates synchronize observation before execution.
* `TestDockerDependencyCacheIntegration`: real cold/warm tools, image/tenant cache
  sharing/isolation and provider reopening with a test-only instrumented worker.
* `TestDockerNixFetchCacheIntegration`: real Nix GitHub fetcher against a controlled
  HTTPS fixture, freshness, shared stores and restart.
* `TestDockerUpgradeIntegration`: original recorded labels, accounting and
  fingerprints on a real holding worker and cache pair, no replay across closure,
  cache markers preserved and an unrelated sentinel volume untouched.
* `TestDockerMultiPlatformIntegration`: independent executable-platform checks,
  immutable image selection and shared accounting after reopening.

For a focused rerun after completed-lease fixture repairs, use
`scripts/verify-live.sh lease-completion`. It requires an explicit passing event
for `TestDockerLeaseIntegration/completed_lease`; an empty or skipped selection
fails. This mode verifies only that scenario and does not establish the rest of
other Linux coverage. D29 Desktop acceptance was removed by human direction.

The Linux suites need a reachable local Unix Docker daemon, Docker CLI, enough
resources for the existing 6 GiB cache budget, registry access to
`ghcr.io/colony-2/base2:latest` and Alpine, and Go/Git to build fixture binaries.
The runner must reach the fixture's published port, and containers must reach its
peer address and the host gateway. The fixture handles runner/daemon filesystem
separation with Docker copy and handles published-port reachability explicitly.
No production JobDB credentials are needed.

Optional: use `scripts/verify-live.sh desktop` on an actual macOS Docker Desktop host.
D29 acceptance is removed; this retained fixture is not required for delivery. It
checks user-socket discovery with no system socket dependency and requires the
daemon to identify itself as Docker Desktop, then runs the real supplied-lease/
connectivity scenarios. A simulated Darwin resolver test does not
certify Desktop execution.

D28 is conditional. The platform suite emits exactly one passing subtest:
`unavailable_emulation_declines` proves unsupported status and no worker creation
when verified emulation is unavailable; `verified_emulation` retains requested
architecture, immutable image and reopened accounting checks when available.
Both report the native and alternative platforms. Safe decline leaves alternative
execution explicitly unexercised. No emulation installation is required here.

The live runner requires all six Linux suites, a complete valid JSON event stream,
a passing package and exactly one passing D28 branch. A skip, missing branch,
contradictory result, unrelated suite or truncated stream fails. Messages alone
cannot certify assertions. The local `verifycheck.go` tool validates these results;
it is verification tooling, not a provider executable.

Record candidate identity, host architecture, Go/CLI/daemon/API versions, image
IDs/digests, installed c2j version, command exits and asserted outcomes. Keep large
logs in diagnostics. Fixtures delete only their own named resources. Cache tests
use isolated namespaces; do not prune unrelated daemon resources.

The immutable original-source fixtures and deterministic failure cases cover
upgrade labels, layout-1/layout-2 caches, accounting, fingerprints, no replay and
compatible pagination. A live extraction-upgrade check also needs running
original-metadata workers/caches across close/reopen; deterministic fixtures alone
do not certify that scenario.
