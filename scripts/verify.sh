#!/usr/bin/env bash
set -euo pipefail
export GOWORK=off GOTOOLCHAIN=local
printf 'Verifying %s\n' "${VERIFY_CANDIDATE:-working-tree}"
go version
# A typo in a required selection must not silently pass with no tests.
focused='^Test(Config|ResolveSocket|Launch|Lease|Capabilities|Close|CanceledAdmission|UnknownStart|Upgrade|ConstructionFailure|Verify)'
selection=$(go test -mod=readonly -list "$focused" .)
test -n "$(printf '%s\n' "$selection" | sed -n '/^Test/p')"
go test -mod=readonly -count=1 -timeout=5m -run "$focused" ./...
go test -mod=readonly -count=1 -timeout=5m ./...
go test -mod=readonly -race -count=1 -timeout=5m ./...
go vet -mod=readonly ./...
go vet scripts/verifycheck.go
go build -mod=readonly ./...
go build -mod=readonly ./testdata/cache-worker ./testdata/jobdb-fixture ./testdata/nix-fetch-fixture
go mod tidy -diff
go mod verify
# Compile live suites and exercise their failure evidence and lease setup without Docker.
integration_tests='^TestDocker(LeaseFailureEvidence|CompletedLeaseFixture)$'
integration_selection=$(go test -mod=readonly -tags=integration -list "$integration_tests" .)
for required in TestDockerLeaseFailureEvidence TestDockerCompletedLeaseFixture; do
  printf '%s\n' "$integration_selection" | awk -v required="$required" '$0 == required { found=1 } END { exit !found }'
done
go test -mod=readonly -tags=integration -run "$integration_tests" ./...
# Verify the complete test/fixture graph, not just production imports.
graph=$(go list -mod=readonly -m all)
test -n "$graph"
printf '%s\n' "$graph" | awk '$1 == "github.com/colony-2/pulse" { print "Pulse found in module graph" > "/dev/stderr"; exit 1 }'
printf '%s\n' 'Deterministic verification passed. Required live Docker acceptance is separate.'
