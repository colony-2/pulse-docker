#!/usr/bin/env bash
set -euo pipefail
export GOWORK=off GOTOOLCHAIN=local
printf 'Verifying %s\n' "${VERIFY_CANDIDATE:-working-tree}"
go version
# A typo in a required selection must not silently pass with no tests.
focused='^Test(Config|ResolveSocket|Launch|Lease|Capabilities|Close|CanceledAdmission|UnknownStart|Upgrade|ConstructionFailure|Verify)'
# Required package/test pairs protect the move and the new remote surface.
while read -r package required; do
  selection=$(go test -mod=readonly -list "^${required}$" "$package")
  printf '%s\n' "$selection" | awk -v required="$required" '$0 == required {found=1} END {exit !found}'
done <<'REQUIRED'
./internal/provider TestConfigCopiesAndValidation
./internal/provider TestUpgradeOriginalSerializedLaunches
./internal/provider TestLeaseExactDeliveryRedactionAndRequestedFacts
./internal/provider TestLogsFastCaptureBeforeStartAndAfterRemoval
./internal/provider TestLogsRecoveryVerifiesPrefixWithoutReplay
./internal/engine TestResolveSocketSelection
./internal/engine TestOutputAttachmentRetainsBufferedBytes
./internal/launchlogs TestArchiveBinaryPagingReopen
./internal/launchlogs TestArchiveTornTailAndCommittedCorruption
./internal/launchlogs TestSubscriptionCallbackFailureAndBoundedResidence
./internal/servercli TestConfigurationPrecedencePathsAndPrivateSave
./internal/servercli TestManagementValidatesCredentialsBeforeEffects
./internal/servercli TestManagementValidatesTransportBeforeEffects
./internal/servercli TestManagementAllowsIncompleteConnectionSettings
./internal/servercli TestLaunchesSinceDurationAndTimestamp
./internal/servercli TestOutboundSDKReconnectAndCalls
./internal/servercli TestInboundSDKProcess
./internal/servercli TestCleanupTimeoutPreventsOverlappingReplacement
./cli TestEmbeddedCommandsInheritContextStreamsAndHooks
. TestVerifyHookInputContract
REQUIRED
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
integration_tests='^TestDocker(LeaseFailureEvidence|CompletedLeaseFixture|RemoteFixtureRemovalObservation)$'
integration_selection=$(go test -mod=readonly -tags=integration -list "$integration_tests" ./internal/provider)
for required in TestDockerLeaseFailureEvidence TestDockerCompletedLeaseFixture TestDockerRemoteFixtureRemovalObservation; do
  printf '%s\n' "$integration_selection" | awk -v required="$required" '$0 == required { found=1 } END { exit !found }'
done
go test -mod=readonly -tags=integration -run "$integration_tests" ./...
# Verify the complete test/fixture graph, not just production imports.
graph=$(go list -mod=readonly -m all)
test -n "$graph"
printf '%s\n' "$graph" | awk '$1 == "github.com/colony-2/pulse" { print "Pulse found in module graph" > "/dev/stderr"; exit 1 }'
printf '%s\n' 'Deterministic verification passed. Required live Docker acceptance is separate.'
