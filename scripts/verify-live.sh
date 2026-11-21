#!/usr/bin/env bash
set -euo pipefail
export GOWORK=off GOTOOLCHAIN=local
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
required_pass=''
case "${1:-}" in
 linux) tests='^TestDocker(LifecycleResources|Lease|DependencyCache|NixFetchCache|MultiPlatform|Upgrade)Integration$'; expected=6;;
 desktop) tests='^TestDockerDesktopIntegration$'; expected=1;;
 lease-completion)
  tests='^TestDockerLeaseIntegration$/^completed_lease$'
  required_pass='TestDockerLeaseIntegration/completed_lease'
  expected=1;;
 *) echo 'Usage: scripts/verify-live.sh linux|desktop|lease-completion' >&2; exit 2;;
esac
selection_pattern=$tests
if [ -n "$required_pass" ]; then selection_pattern='^TestDockerLeaseIntegration$'; fi
if ! command -v docker >/dev/null; then
 echo 'Required prerequisite missing: Docker CLI (no live scenario ran).' >&2
 exit 2
fi
# Keep Docker's Go templates in this script, outside recipe template expansion.
printf 'candidate=%s\n' "${VERIFY_CANDIDATE:-$(git rev-parse HEAD)}"
uname -sm
go version
docker version
docker info --format 'daemon={{.Name}} os={{.OperatingSystem}} arch={{.Architecture}} memory={{.MemTotal}} cpus={{.NCPU}}'
selection=$(go test -mod=readonly -tags=integration -list "$selection_pattern" . | sed -n '/^Test/p')
test "$(printf '%s\n' "$selection" | wc -l | tr -d ' ')" = "$expected"
# Preserve native go-test failures; no missing host or daemon counts as a pass.
check_dir=$(mktemp -d)
log=$check_dir/events.json
trap 'rm -rf "$check_dir"' EXIT
go test -mod=readonly -tags=integration -count=1 -timeout=60m -json -run "$tests" . | tee "$log"
go build -o "$check_dir/check" "$script_dir/verifycheck.go"
"$check_dir/check" live "$1" "$log"
