# Public Docker integration contract

Construct `*docker.Provider` with `New(context.Context, Config)`. It directly
implements `github.com/colony-2/pulse-provider-api.Provider`. Do not copy an
initialized provider. Its additional operations are `Maintain(context.Context)
error` and `Close()`. `ResolveSocket(string) (string, error)` is also public.
There is no native-provider accessor or public process/engine protocol. Error
text is diagnostic rather than a stable error-code API.

## Configuration

| Field | Meaning and default |
| --- | --- |
| `Socket string` | Empty discovers a local Unix socket. Explicit configuration takes precedence. Remote endpoints are rejected. |
| `LockDir string` | Empty uses `os.TempDir()/pulse-docker-locks`. Every local owner of the same daemon must share this directory. |
| `CPUMillis, MemoryBytes int64` | Committed pool budgets. Zero uses daemon capacity; negative or larger-than-daemon budgets reject construction. |
| `Overhead int64` | Additional bytes charged per worker, zero by default. Pulse's separate 256 MiB default remains a host choice. |
| `MaxContainers int` | Slot budget; zero is unlimited. Negative values reject construction. |
| `ScratchPath string` | Default `/scratch`; require an absolute non-root path without `:`. Enabled caches prohibit overlap with `/nix` and `/var/cache/pulse`. |
| `RegistryAuth string` | Host-supplied encoded registry authorization, sent only on image pulls. The library reads no host credential variable. |
| `ExecutorEnv map[string]string` | Copied defaults. Invalid entries and `C2J_EXECUTION_*`, `C2J_JOBDB`, `PULSE_*` overrides reject construction. Cache-specific conflicts decline the affected launch. |
| `DependencyCache CacheConfig` | Enabled for Colony base images by default. Fields: `Disabled bool`, `Namespace, Generation string`, `MaxAge time.Duration`. Defaults: `default`, `1`, 30 days; retention must be positive and at most one year after defaults. |
| `LegacyEndpoints []LegacyEndpoint` | Copied records with `InstanceID, TenantID, Endpoint string`. Both identities are lookup keys. Credential-free tenant endpoints are required; conflicting duplicates reject construction. |

Socket precedence preserves Pulse: explicit configuration; a named non-default
`DOCKER_CONTEXT`; `DOCKER_HOST`; saved named context; platform defaults. Explicit
`default` suppresses the saved context. With no selected context, macOS checks the
user's `.docker/run/docker.sock` before the system default. Selected malformed or
nonlocal contexts fail without fallback. Paths and symlink aliases normalize.

Construction checks static settings before daemon requests, negotiates the API,
requires a Linux daemon, locks its daemon identity, reconstructs accounting,
probes resource/platform support and performs startup cache maintenance. Failed
construction releases local resources. Startup maintenance failures are deferred
as in Pulse, while cancellation fails construction. Use bounded contexts.

## Calls and authority

`GetCapabilities` returns a fresh, partial catalog (`BasesComplete=false`), native
platform first, then verified alternatives. It advertises concrete Ubuntu 26.04
bases and empty optional capability sets. Saturation does not remove support.
Discovery and listing are observational: they never pull, create, reconcile
accounting or retire caches.

`Submit` validates the entire batch before worker creation: 1–100 unique launch
IDs, bounded quantities/timeouts, selector and capability syntax, complete JobRefs,
and nonempty UTF-8 leases up to 1 MiB without NUL. Valid lease bytes remain opaque
and unchanged. The caller must not mutate arguments concurrently with a call.
Retained mutable settings and launch data are copied.

Exact/wildcard selectors resolve without broadening the request. Canonical exact
`oci:<platform>:<image>` remains available; Ubuntu bases map to
`ghcr.io/colony-2/base2:latest`. Unsupported optional capabilities, positive runtime
caps and future actual-start deadlines decline before worker creation. Expired
start deadlines reject. Requested quantities remain executor facts even when
Docker enforcement/admission rounds them upward.

The private command is exactly:

```text
c2j run with-lease --jobdb ENDPOINT --job-id JOB --lease-file - --ci --input-mode fail
```

Attachment completes its upgrade before start. The exact lease is written to
stdin, then the connection closes for EOF. Cancellation closes blocked attachment
or writes. Leases never enter argv, environment, labels or public listings.
Worker errors suppress raw daemon response bodies.

Correlate all results by LaunchID. Batches are not atomic. Valid explicit results
remain authoritative alongside errors. `accepted` has an empty reason; other
statuses explain the decision. `unknown`, missing/invalid results, and canceled
calls alone do not establish non-acceptance. Never retry or immediately fall back
on that evidence. There is no durable terminal/replay ledger.

## Maintenance, listings and closure

Call `Maintain` periodically to run the existing hourly-throttled cache retirement.
It shares admission serialization with submission, protects active/uncertain
references, and removes only expired owned layout-2 resources. It never stops
workers or prunes unrelated resources.

`List` returns current active compute, with sorted native IDs and compatible
last-seen cursors. Zero page size means 100; accepted sizes are 1–100. Keep the
LaunchID filter fixed across pages. Current endpoint labels override legacy
mappings. Incomplete/invalid JobRefs, cursors and daemon failures return errors;
discard failed pages. Empty item/ref arrays remain non-nil, and timestamps are
reported only when available. Absence does not prove completion or free capacity.

`Close` atomically rejects new calls, waits for entered calls and releases local
ownership/idle connections. Calls queued for admission recheck closure before
effects. Repeated/concurrent Close calls wait for the same cleanup. New valid
submissions receive correlated `unavailable` results plus an error; other new
calls fail. Already uncertain outcomes remain uncertain. Closing never stops
accepted workers or deletes their caches. Since Close has no context, hosts must
bound in-flight call contexts before retiring a provider.

Ownership is local to a shared lock directory, not distributed coordination among
independent hosts. All owners of a daemon must use the same directory. Retaining
that owner until calls finish prevents worker creation after relinquishing its
budget.
