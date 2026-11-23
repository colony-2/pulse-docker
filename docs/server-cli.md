# Docker provider command

Build with `go build -o pulse-docker ./cmd/pulse-docker`, or install with
`go install github.com/colony-2/pulse-docker/cmd/pulse-docker@latest` after publication.
`cli.NewCommand(version)` and the individual `cli.New*Command` constructors support
embedding with inherited context, streams and parent hooks. Always construct fresh
commands. The root library imports neither Cobra nor the plugin SDK.

```sh
pulse-docker setup --cpu-millis 4000 --memory-bytes 8589934592 --max-containers 4
pulse-docker server --collector pulse.example.com:8443 \
  --tls-ca /etc/pulse/ca.pem --tls-cert /etc/pulse/client.pem --tls-key /etc/pulse/client.key
pulse-docker status --json
pulse-docker images
pulse-docker launches --tenant team --limit 20
pulse-docker launches show LAUNCH_ID
pulse-docker logs LAUNCH_ID --follow
pulse-docker logs LAUNCH_ID --stream stderr
pulse-docker upgrade --progress linear
```

Use a local Unix Docker socket, selected with the existing Docker context rules or
`--socket unix:///path/to/docker.sock`. Docker must already be installed. Containers
must reach the JobDB endpoints supplied in launches. No endpoint is rewritten.
Zero CPU/memory means daemon capacity; zero containers means unlimited. No VM,
backend-selection, software-installation or deprecated reconnect-attempt flags exist.

Setup runs bounded normal initialization, including probes and cache maintenance,
then saves local configuration. Setup and upgrade validate transport bounds,
mode/authentication combinations and any supplied credential files before daemon
effects or configuration saves. Collector addresses and credentials may be omitted
until server startup. Upgrade requires exclusive admission ownership and
explicitly pulls/verifies supported built-in image/platform pairs. It reports each
verified old/new ID; a later failure leaves prior updates applied and configuration
unchanged. Running workers, unrelated images and cache generations remain intact.
Stop embedded/standalone admission owners before setup, upgrade or a replacement
server. All owners must use the same `--lock-dir`, including different archive owners.

Status and images make read-only Docker requests alongside an active owner. Status
separates configured budgets from label-derived charges; uncertain reservations and
c2j versions can be unknown. It never promises free capacity or job completion.
Images includes built-ins and managed workers' references. Inspection errors fail.

Configuration is strict version-1 JSON at `~/.pulse/docker/provider.json`:

```json
{"version":1,"flags":{"owner":"server","cpu-millis":"4000","executor-env-file":"env.json"}}
```

Explicit flags override saved values. File paths are relative to the configuration;
command-line paths are relative to the current directory. Setup and successful
upgrade save private files through a synced temporary file and atomic rename.
A missing default file is allowed; an explicit missing file is accepted only by
setup. Duplicate keys, unknown options/versions, malformed types and oversized or
nonregular files fail. There is no macvz configuration fallback. Common flags and
defaults are listed by `pulse-docker server --help`.

Only credential selectors and paths are saved: `--executor-env-file` is a JSON string
map with reserved-key validation; `--registry-auth-file` is private encoded Docker
auth; `--legacy-endpoints-file` holds existing endpoint mappings. Never put secret
values in raw command flags. Shared-key environment lookup does not populate worker
environment. Inbound mode (`server --mode inbound`) is the SDK child-process protocol;
stdout belongs to that protocol and diagnostics go to stderr.

Outbound defaults to mTLS over TLS. Explicit `--reverse-auth mutual-key` accepts
exactly one of `--shared-key-file` or `--shared-key-env`; plaintext also requires
`--reverse-transport http`. Sessions negotiate SDK baseline 3 before constructing a
provider. Remote settings/default overrides and identity mismatches fail before
opening Docker. One supervisor retries indefinitely with capped jitter/backoff;
cleanup must finish before replacement. Signals stop retries and release ownership
without stopping accepted containers. No launch is queued or replayed on reconnect.

The standalone server captures private output under `~/.pulse/docker/logs`, scoped
by daemon identity and `--owner`. Default retention is 168 hours after capture closes,
with a 1 GiB byte budget and compressed sealed segments. `--log-max-age`,
`--log-max-bytes` and `--log-compress` configure it. History is retained separately
until its private metadata is explicitly removed. Logs/history begin with this
provider version; deleted historical output cannot be recovered.

Launches supports exact tenant/job filters, `--since` as an RFC3339 timestamp or
nonnegative duration ago (for example `24h`), newest-first ordering,
`--limit 100` (zero unlimited), JSON and individual lookup. History shows the last
observed process state, exit, OOM and reason separately from capture. An older
entry without exit information is unknown. `launches show` gives the observation
time and any capture gaps.

`logs --stream all|stdout|stderr` writes binary worker bytes to command stdout.
The default is `all`. Launch diagnostics and gap warnings go to command stderr
in every mode. The offset counts all retained worker bytes, including filtered
streams. Complete retrieval of a failed worker still succeeds; incomplete capture
prints the available bytes and returns failure. Empty worker stderr does not
establish that a worker succeeded. Inspection selects the daemon's archive through
a read-only identity query, so Docker must remain reachable.

See [logging and recovery limits](integration.md#optional-logs) and
[required acceptance checks](testing.md). Updating the host binary does not update
c2j in existing workers; image tags do not prove an installed c2j version.
