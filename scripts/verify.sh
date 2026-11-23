#!/usr/bin/env bash
set -euo pipefail
export GOWORK=off GOTOOLCHAIN=local
printf 'Verifying %s\n' "${VERIFY_CANDIDATE:-working-tree}"
go version
# A typo in a required selection must not silently pass with no tests.
focused='^Test(Config|ResolveSocket|Launch|Lease|Capabilities|Close|CanceledAdmission|UnknownStart|Upgrade|ConstructionFailure|Verify|DockerTermination|OutputPartial|Logs|ArchiveTerminal|CLIHistory)'
# Required package/test pairs protect the move and the new remote surface.
# Fast-capture cases verify binary bytes per stream for both frame orders and interleaving.
while read -r package required; do
  selection=$(go test -mod=readonly -list "^${required}$" "$package")
  printf '%s\n' "$selection" | awk -v required="$required" '$0 == required {found=1} END {exit !found}'
done <<'REQUIRED'
./internal/provider TestConfigCopiesAndValidation
./internal/provider TestUpgradeOriginalSerializedLaunches
./internal/provider TestLeaseExactDeliveryRedactionAndRequestedFacts
./internal/provider TestLogsFastCaptureBeforeStartAndAfterRemoval
./internal/provider TestLogsTerminalCaptureRegressions
./internal/provider TestLogsRetainWorkerUntilReplayRecovers
./internal/provider TestLogsStartupRecoversInterruptedFirstArchiveWrite
./internal/provider TestLogsBoundedCaptureAndQueue
./internal/provider TestLogsObservationOutlivesSubmitAndListener
./internal/provider TestLogsRecoveryVerifiesPrefixWithoutReplay
./internal/provider TestLogsExpiredCleanupRequiresDurableIdentity
./internal/provider TestLogsDamagedArchiveAtGraceExpiry
./internal/engine TestResolveSocketSelection
./internal/engine TestOutputAttachmentRetainsBufferedBytes
./internal/engine TestDockerTerminationEvidence
./internal/engine TestOutputPartialPayloadPreservesWorkerStderr
./internal/launchlogs TestArchiveBinaryPagingReopen
./internal/launchlogs TestArchiveTornTailAndCommittedCorruption
./internal/launchlogs TestArchiveTerminalCommitAndCompressionFailure
./internal/launchlogs TestArchiveTerminalReceiptSurvivesEviction
./internal/launchlogs TestArchiveTerminalRetryRetention
./internal/launchlogs TestArchiveTerminalResetValidatesCommittedTail
./internal/launchlogs TestArchiveTerminalResetValidatesSealedTail
./internal/launchlogs TestArchiveTerminalMissingMetadataPreservesDurableEvidence
./internal/launchlogs TestSubscriptionCallbackFailureAndBoundedResidence
./internal/servercli TestConfigurationPrecedencePathsAndPrivateSave
./internal/servercli TestManagementValidatesCredentialsBeforeEffects
./internal/servercli TestManagementValidatesTransportBeforeEffects
./internal/servercli TestManagementAllowsIncompleteConnectionSettings
./internal/servercli TestLaunchesSinceDurationAndTimestamp
./internal/servercli TestCLIHistoryFilteringAndBinaryLogOffsets
./internal/servercli TestCLIHistoryLogStreamValidationAndEmptyStderr
./internal/servercli TestCLIHistoryFollowReportsEventualSummaryAndGaps
./internal/servercli TestOutboundSDKReconnectAndCalls
./internal/servercli TestInboundSDKProcess
./internal/servercli TestInboundSDKRejectsInvalidBaseDirBeforeConstruction
./internal/servercli TestCleanupTimeoutPreventsOverlappingReplacement
./cli TestEmbeddedCommandsInheritContextStreamsAndHooks
. TestVerifyHookInputContract
. TestVerifyRemoteRequiredResults
REQUIRED
expiry_test='^TestLogsDamagedArchiveAtGraceExpiry$'
expiry_results=$(
  if [ "$(id -u)" = 0 ]; then
    expiry_dir=$(mktemp -d) || exit 1
    trap 'rm -rf -- "$expiry_dir"' EXIT
    go test -mod=readonly -race -c -o "$expiry_dir/provider.test" ./internal/provider &&
      chmod 0755 "$expiry_dir" && cd "$expiry_dir" &&
      setpriv --reuid=65534 --regid=65534 --clear-groups ./provider.test -test.v -test.count=1 -test.timeout=1m -test.run "$expiry_test"
  else
    go test -mod=readonly -race -v -count=1 -timeout=1m -run "$expiry_test" ./internal/provider
  fi
) || { printf '%s\n' "$expiry_results"; exit 1; }
printf '%s\n' "$expiry_results"
for scenario in in_memory_failure reopen write_failure_verified_receipt write_failure_verified_receipt/storage_restored write_failure_missing_receipt; do
  printf '%s\n' "$expiry_results" | awk -v name="TestLogsDamagedArchiveAtGraceExpiry/$scenario" '$1 == "---" && $2 == "PASS:" && $3 == name { found=1 } END { exit !found }'
done
go test -mod=readonly -count=1 -timeout=5m -run "$focused" ./...
go test -mod=readonly -count=1 -timeout=5m ./...
go test -mod=readonly -race -count=1 -timeout=5m ./...
go vet -mod=readonly ./...
go vet scripts/verifycheck.go
go build -mod=readonly ./...
go build -mod=readonly ./testdata/cache-worker ./testdata/jobdb-fixture ./testdata/nix-fetch-fixture
go mod tidy -diff
go mod verify
# Testdata packages are excluded by ./...; exercise the real fixture HTTP controls.
go test -mod=readonly -race -count=1 -timeout=1m ./testdata/jobdb-fixture
# Compile live suites and exercise their failure evidence and lease setup without Docker.
integration_tests='^TestDocker(LeaseFailureEvidence|CompletedLeaseFixture|RemoteFixtureRemovalObservation)$'
integration_selection=$(go test -mod=readonly -tags=integration -list "$integration_tests" ./internal/provider)
for required in TestDockerLeaseFailureEvidence TestDockerCompletedLeaseFixture TestDockerRemoteFixtureRemovalObservation; do
  printf '%s\n' "$integration_selection" | awk -v required="$required" '$0 == required { found=1 } END { exit !found }'
done
cli_selection=$(go test -mod=readonly -tags=integration -list '^TestDockerCLIIntegration$' ./internal/provider)
printf '%s\n' "$cli_selection" | awk '$0 == "TestDockerCLIIntegration" { found=1 } END { exit !found }'
go test -mod=readonly -tags=integration -run "$integration_tests" ./...
# Verify the complete test/fixture graph, not just production imports.
graph=$(go list -mod=readonly -m all)
test -n "$graph"
printf '%s\n' "$graph" | awk '$1 == "github.com/colony-2/pulse" { print "Pulse found in module graph" > "/dev/stderr"; exit 1 }'
printf '%s\n' 'Deterministic verification passed. Required live Docker acceptance is separate.'
