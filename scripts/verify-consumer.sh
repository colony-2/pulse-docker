#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -ne 1 ] || [ -z "$1" ]; then
  echo 'Usage: scripts/verify-consumer.sh ACCESSIBLE_MANAGED_REFERENCE' >&2
  exit 2
fi
ref=$1
if [[ ! "$ref" =~ ^[0-9a-f]{40}$ ]]; then echo "Require an accessible full Git hash" >&2; exit 2; fi
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
export GOWORK=off GOTOOLCHAIN=local GOFLAGS=
cd "$scratch"
go mod init independent.example/docker-host
go get "github.com/colony-2/pulse-docker@$ref"
cat > host_test.go <<'GO'
package host
import (
 "context"
 docker "github.com/colony-2/pulse-docker"
 provider "github.com/colony-2/pulse-provider-api"
)
var _ provider.Provider = (*docker.Provider)(nil)
func use(ctx context.Context, launch provider.Launch) error {
 p,err:=docker.New(ctx,docker.Config{ExecutorEnv:map[string]string{"CUSTOM":"value"}})
 if err!=nil{return err};defer p.Close()
 var backend provider.Provider=p
 _,err=backend.Submit(ctx,[]provider.Launch{launch});if err!=nil{return err}
 return p.Maintain(ctx)
}
GO
go mod tidy
replacements=$(go list -mod=readonly -m -f '{{if .Replace}}{{.Path}}{{end}}' all)
test -z "$replacements"
go list -mod=readonly -m all | awk '$1 == "github.com/colony-2/pulse" { found=1 } END { exit found }'
go mod download -json "github.com/colony-2/pulse-docker@$ref" > resolved-module.json
go build -o "$scratch/verifycheck" "$script_dir/verifycheck.go"
"$scratch/verifycheck" consumer "$ref" resolved-module.json > reference-evidence.txt
go test -mod=readonly ./...
cat reference-evidence.txt
printf 'Resulting reference consumer passed.\n'
