//go:build integration

package provider

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/c2j/pkg/jobdbschema"
	"github.com/colony-2/c2j/pkg/ops"
	"github.com/colony-2/c2j/pkg/recipe"
	"github.com/colony-2/c2j/pkg/starter"
	"github.com/colony-2/c2j/pkg/worker/commandop"
	"github.com/colony-2/c2j/pkg/workflowctl"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	jobworkflow "github.com/colony-2/jobdb/pkg/workflow"
)

// Completion must append the final outcome atomically with releasing the lease.
// Use this same setup in the local regression and the real Docker scenario.
func completeDockerFixtureLease(ctx context.Context, backend jobdb.WorkflowRuntime, lease jobdb.ExecutionLease) error {
	chapters, err := backend.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: lease.Job().JobKey})
	if err != nil {
		return fmt.Errorf("list chapters before fixture completion: %w", err)
	}
	var next int64
	for _, chapter := range chapters {
		next = max(next, chapter.Ordinal+1)
	}
	final := jobdb.Chapter{
		Ordinal: next, TaskType: lease.Route().JobType, CreatedAt: time.Now().UTC(),
		Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
			Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"result":"fixture-completed-before-launch"}`)},
		}},
	}
	return lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &final})
}

func TestDockerCompletedLeaseFixture(t *testing.T) {
	ops.Register(commandop.GetOp())
	ctx := celltest.Context(t.Context())
	backend, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	server := httptest.NewServer(celltest.Handler(remote.NewServer(backend)))
	defer server.Close()
	client, err := remote.New(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "docker-test", Name: "test", Repo: "example.test/pulse-fixture"}); err != nil {
		t.Fatal(err)
	}
	engine, err := jobworkflow.NewEngineBuilder().WithRuntime(client).BuildEngine()
	if err != nil {
		t.Fatal(err)
	}
	// Match the live fixture's recipe job and output shape without executing it.
	rec, err := recipe.LoadRecipeFromString([]byte("id: completed-lease-fixture\nop: command_execution\ninputs:\n  run: echo must-not-run\noutputs:\n  result: '${{ op.outputs.stdout }}'\n"))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := starter.StartRecipeJob(ctx, workflowctl.StartJob{
		Cell: "test", CellRepo: "example.test/pulse-fixture", TenantId: "docker-test", RecipeName: rec.GetMetadata().ID,
	}, jobdbschema.WorkflowEngine{Engine: engine, Registry: client}, *rec)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: ref.Key(), WorkerID: "docker-library-test", Routes: []jobdb.Route{{JobType: "recipe"}}, LeaseDuration: time.Minute})
	if err != nil || lease == nil {
		t.Fatal("acquire fixture lease", err)
	}
	capability, err := remote.ExportLease(lease)
	if err != nil {
		t.Fatal(err)
	}
	before, err := client.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: ref.Key()})
	if err != nil {
		t.Fatal(err)
	}
	if err := completeDockerFixtureLease(ctx, client, lease); err != nil {
		t.Fatal("complete fixture lease", err)
	}
	info, err := client.GetJob(ctx, ref.Key())
	if err != nil || info.Status != jobdb.JobStatusCompleted {
		t.Fatalf("fixture did not complete: status=%s error=%v", info.Status, err)
	}
	after, err := client.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: ref.Key()})
	if err != nil || len(after) != len(before)+1 {
		t.Fatalf("completion did not persist one final chapter: before=%d after=%d error=%v", len(before), len(after), err)
	}
	if _, err := client.ImportLease(ctx, capability); !errors.Is(err, jobdb.ErrExecutionLeaseLost) {
		t.Fatalf("completed lease remained importable: %v", err)
	}
	unchanged, err := client.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: ref.Key()})
	if err != nil || len(unchanged) != len(after) {
		t.Fatalf("failed import changed chapters: before=%d after=%d error=%v", len(after), len(unchanged), err)
	}
}
