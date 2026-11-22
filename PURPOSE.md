This cell owns `github.com/colony-2/pulse-docker`, an independently importable Go library implementing `github.com/colony-2/pulse-provider-api.Provider` for executing c2j workers in Docker containers. Pulse consumes this library; the library does not import Pulse implementation or configuration packages.

Own Docker connectivity and local socket discovery, image/platform and digest verification, base resolution and capability discovery, capacity admission and per-daemon ownership, resource/scratch configuration, worker construction, supplied-lease delivery, dependency cache volumes and maintenance, active instance listing, cleanup, and provider-specific diagnostics. Expose typed configuration and explicit lifecycle operations suitable for Pulse and independent hosts.

Preserve existing Pulse Docker behavior during extraction, including configuration defaults, native-platform preference and verified emulation, legacy image bases, worker and cache labels/layouts, launch correlation and conflict handling, uncertain-start accounting, and recognition of existing resources. Hosts supply configuration, executor defaults, and legacy JobDB endpoint mappings. Keep discovery and listing observational. Release local resources on closure without stopping accepted workers.

Deliver the exact opaque lease to c2j through a protected channel such as stdin. Never log it or place it in arguments, environment, resource labels, or instance listings. Honor every supported constraint; decline unsupported requirements before dispatch. Do not retry ambiguous starts or claim replacement work. Preserve existing limitations on infrastructure runtime caps, start deadlines, and optional executor capabilities.

Own deterministic contract/engine tests, Docker integration fixtures and tests, independent-consumer checks, library documentation, and Go module maintenance. Preserve applicable upstream license and attribution notices for extracted code.

Pulse owns discovery of work, placement, lease acquisition, controller policy, configuration files, and controller-facing diagnostics. c2j owns recipe execution and lease renewal; JobDB owns durable execution state. This cell does not build another scheduler or provision Docker hosts.

The library and standalone cmd/pulse-docker program share this implementation.
The agreed remote-provider assignment includes SDK transport, local setup/image
refresh, observational inspection, launch history and retained logs. Remote
configuration cannot override local daemon access or capacity. Deployment,
distribution, host provisioning and scheduling remain outside this cell's program.
