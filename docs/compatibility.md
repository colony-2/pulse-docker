# Existing worker and cache compatibility

The extraction retains Pulse's engine from
`6c20e087383ac7c333f562aa373c873bd5423db6`, including socket, image/platform,
limits, accounting, stdin, cache and listing behavior. Docker-specific structured
translation comes from its adapter, executor construction, base selection and
validation. Controller, scheduler, YAML, other backends and placement diagnostics
remain in Pulse.

The private native structs preserve JSON field order, tags, omissions, embedded
allocation/request fields and process representation. Fingerprints remain the
lowercase SHA-256 of that complete JSON before rounding/resolution additions.
They are not hashes of the new API launch. Recorded original-source
[fixtures](../testdata/upgrade/README.md) independently check bytes and identities.

Worker names stay `pulse-` plus the original LaunchID hash. Existing managed,
correlation, role, `pulse_process_fingerprint`, `pulse_docker_accounting` and
`pulse_docker_limits` labels keep their names and metadata version `1`. Existing
matching active launches are recognized without another create/start; changed
immutable payloads sharing their ID reject. A created but unconfirmed worker, or
unverifiable cache attachment, remains unknown.

Only new archive-enabled workers use provider-owned removal. Archive-free calls
and pre-existing workers retain their lifecycle mode. The archive and SDK log
formats remain compatible; exit details and reasons are optional history metadata,
and the terminal summary uses the existing launch-record source. Older providers
do not manage new retention-marked workers. Retain the archive metadata and run
the current writable provider to finish cleanup after an upgrade or rollback.

CPU rounds to 10 millicores; memory/scratch round to 4096-byte pages, including
Docker's minimum-memory adjustment. Charges include scratch, per-worker overhead,
slots, cache helpers and uncertain creation. Scratch-only requests do not add an
application memory limit. Missing daemon limit enforcement retains warnings and
admission charges, without reporting enforcement. Container creation uses the
verified locally addressable immutable image root ID and explicit platform,
retaining digest constraints and older-API native support.

One uncertainty repair is intentional: the old engine discarded its reservation
as soon as create returned an ID. A warning or failed inspection followed by
missing inventory could then permit replay. This library keeps the reservation
until successful stdin/start delivery or positive accounting observation. The
failure-injection tests prove warnings, failed inspection, conflicts, missing IDs
and lost replies cannot become permission to retry. Daemon errors also redact
under debug formatting, and retained probe detail removes echoed registry
authorization. Worker paths continue suppressing raw daemon response bodies.

Cache layout `2`, hash inputs and their order remain unchanged: layout, namespace,
generation, JobDB instance, tenant, platform, runtime user `0:0`. Owner identity
uses layout and namespace. Names remain `pulse-cache-v2-KEY-{tools,nix}` and
`pulse-cache-init-KEY`, mounts `/var/cache/pulse` and `/nix`, readiness
`/nix/.pulse-ready`. Tool environments remain image-specific; Nix sharing and
managed environment conflicts retain their previous rules. Older layouts and
workers predating fetch-cache sharing remain recognizable.

Listing preserves active-state mappings, endpoint restoration and raw URL-safe
base64 JSON cursors with fields `After` then `Launch`. Listing never reconciles
uncertain reservations or removes interrupted helpers. There is no cache migration,
terminal-history store or new replay guarantee.
