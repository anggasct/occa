package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

const stageTestItem = "acme/widget#7"

func seedStageItem(t *testing.T, repo StageRepo) {
	t.Helper()
	if err := repo.UpsertWorkItem(context.Background(), WorkItem{ID: stageTestItem, Repo: "acme/widget", Number: 7}); err != nil {
		t.Fatalf("UpsertWorkItem: %v", err)
	}
}

func startStage(t *testing.T, repo StageRepo, stage Stage) int64 {
	t.Helper()
	id, err := repo.StartRun(context.Background(), StageRunStart{WorkItemID: stageTestItem, Stage: stage, TriggeredBy: "delivery:review/d1", WorkspacePath: "/tmp/w"})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	return id
}

func rawStageRun(t *testing.T, db *sql.DB, id int64) []any {
	t.Helper()
	row := make([]any, 19)
	ptrs := make([]any, len(row))
	for i := range row {
		ptrs[i] = &row[i]
	}
	if err := db.QueryRow(`SELECT `+stageRunColumns+` FROM stage_run WHERE id = ?`, id).Scan(ptrs...); err != nil {
		t.Fatalf("read stage run: %v", err)
	}
	return row
}

func TestStageGraphMigrationFreshAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	open := func() *SQLiteStore {
		s, err := OpenWithRetention(path, "", UsageRetention{Retention: time.Hour, MaxRows: 10}, time.Hour)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return s
	}
	s := open()
	assertVersion(t, s, SchemaVersion)
	for _, name := range []string{"work_item", "stage_run", "stage_input", "action_request",
		"idx_stage_run_item", "idx_stage_run_open", "idx_stage_input_open", "idx_action_request_item"} {
		var n int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name = ?`, name).Scan(&n); err != nil || n != 1 {
			t.Fatalf("schema object %s: count %d, %v", name, n, err)
		}
	}
	seedStageItem(t, s.StageRepo())
	runID := startStage(t, s.StageRepo(), StageReview)
	if _, err := s.db.Exec("PRAGMA user_version=25"); err != nil {
		t.Fatalf("rewind version: %v", err)
	}
	_ = s.Close()

	s = open()
	defer func() { _ = s.Close() }()
	assertVersion(t, s, SchemaVersion)
	runs, err := s.StageRepo().ListRuns(context.Background(), stageTestItem)
	if err != nil || len(runs) != 1 || runs[0].ID != runID {
		t.Fatalf("runs after re-migration = %+v, %v", runs, err)
	}
}

func TestStageRunAttemptsPerStage(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.StageRepo()
	seedStageItem(t, repo)

	first := startStage(t, repo, StageReview)
	if err := repo.EndRun(ctx, first, StageStateFailed, "model error"); err != nil {
		t.Fatalf("EndRun: %v", err)
	}
	startStage(t, repo, StageReview)
	startStage(t, repo, StageFix)

	runs, err := repo.ListRuns(ctx, stageTestItem)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	type key struct {
		stage   Stage
		attempt int
		state   StageState
	}
	var got []key
	for _, r := range runs {
		got = append(got, key{r.Stage, r.Attempt, r.State})
	}
	want := []key{{StageReview, 1, StageStateFailed}, {StageReview, 2, StageStateRunning}, {StageFix, 1, StageStateRunning}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runs = %+v, want %+v", got, want)
	}
	if runs[0].StartedAt == 0 || runs[0].EndedAt == 0 || runs[0].BlockerDetail != "model error" ||
		runs[0].TriggeredBy != "delivery:review/d1" || runs[0].WorkspacePath != "/tmp/w" {
		t.Fatalf("first run = %+v", runs[0])
	}

	if _, err := s.db.Exec(`INSERT INTO stage_run (work_item_id, stage, attempt, state, created_at) VALUES (?, 'review', 2, 'running', 1)`, stageTestItem); err == nil {
		t.Fatal("a second (work_item, stage, attempt) row was accepted")
	}
}

func TestStageRunBlockedOnlyFromRunning(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.StageRepo()
	seedStageItem(t, repo)

	running := startStage(t, repo, StageFix)
	if err := repo.MarkBlocked(ctx, running, BlockerAgentNoCommit, "fix made no change"); err != nil {
		t.Fatalf("MarkBlocked: %v", err)
	}
	if err := repo.MarkBlocked(ctx, running, BlockerTimeout, "again"); !errors.Is(err, ErrStageRunNotRunning) {
		t.Fatalf("MarkBlocked on blocked = %v, want ErrStageRunNotRunning", err)
	}
	if err := repo.SetRunAgent(ctx, running, "ses-1", 9); !errors.Is(err, ErrStageRunNotRunning) {
		t.Fatalf("SetRunAgent on blocked = %v, want ErrStageRunNotRunning", err)
	}

	ended := startStage(t, repo, StageFix)
	if err := repo.EndRun(ctx, ended, StageStateDone, ""); err != nil {
		t.Fatalf("EndRun: %v", err)
	}
	if err := repo.MarkBlocked(ctx, ended, BlockerTimeout, "late"); !errors.Is(err, ErrStageRunNotRunning) {
		t.Fatalf("MarkBlocked on done = %v, want ErrStageRunNotRunning", err)
	}
	if err := repo.MarkBlocked(ctx, 999, BlockerTimeout, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MarkBlocked on missing = %v, want ErrNotFound", err)
	}

	runs, err := repo.ListRuns(ctx, stageTestItem)
	if err != nil || len(runs) != 2 {
		t.Fatalf("ListRuns = %+v, %v", runs, err)
	}
	if runs[0].State != StageStateBlocked || runs[0].BlockerCode != BlockerAgentNoCommit || runs[0].BlockerDetail != "fix made no change" {
		t.Fatalf("blocked run = %+v", runs[0])
	}
	if running, err := repo.ListRunning(ctx); err != nil || len(running) != 0 {
		t.Fatalf("ListRunning = %+v, %v", running, err)
	}
}

func TestStageRunSecondTerminalWriteLeavesRowUnchanged(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.StageRepo()
	seedStageItem(t, repo)
	id := startStage(t, repo, StageReview)
	if err := repo.SetRunAgent(ctx, id, "ses-1", 42); err != nil {
		t.Fatalf("SetRunAgent: %v", err)
	}
	if err := repo.EndRun(ctx, id, StageStateSkipped, "merge refused"); err != nil {
		t.Fatalf("EndRun: %v", err)
	}
	before := rawStageRun(t, s.db, id)

	for _, state := range []StageState{StageStateDone, StageStateFailed, StageStateSkipped, StageStateCancelled} {
		t.Run(string(state), func(t *testing.T) {
			if err := repo.EndRun(ctx, id, state, "second"); !errors.Is(err, ErrTerminalAlreadySet) {
				t.Fatalf("second EndRun = %v, want ErrTerminalAlreadySet", err)
			}
			if after := rawStageRun(t, s.db, id); !reflect.DeepEqual(before, after) {
				t.Fatalf("row changed:\nbefore %v\nafter  %v", before, after)
			}
		})
	}
	if err := repo.EndRun(ctx, id, StageStateRunning, ""); !errors.Is(err, ErrStageStateInvalid) {
		t.Fatalf("EndRun running = %v, want ErrStageStateInvalid", err)
	}
	if err := repo.EndRun(ctx, 999, StageStateDone, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("EndRun missing = %v, want ErrNotFound", err)
	}
}

func TestWorkItemState(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.StageRepo()
	if err := repo.SetWorkItemState(ctx, stageTestItem, WorkItemDone); !errors.Is(err, ErrNotFound) {
		t.Fatalf("SetWorkItemState on missing = %v, want ErrNotFound", err)
	}
	seedStageItem(t, repo)
	if err := repo.SetWorkItemState(ctx, stageTestItem, WorkItemAbandoned); err != nil {
		t.Fatalf("SetWorkItemState: %v", err)
	}
	got, err := repo.GetWorkItem(ctx, stageTestItem)
	if err != nil || got.State != WorkItemAbandoned || got.Kind != WorkItemPullRequest || got.Repo != "acme/widget" || got.Number != 7 {
		t.Fatalf("work item = %+v, %v", got, err)
	}
	seedStageItem(t, repo)
	if got, err := repo.GetWorkItem(ctx, stageTestItem); err != nil || got.State != WorkItemOpen {
		t.Fatalf("work item after a new run = %+v, %v", got, err)
	}
	if err := repo.SetWorkItemState(ctx, stageTestItem, WorkItemState("gone")); err == nil {
		t.Fatal("a state outside the CHECK constraint was accepted")
	}
	if _, err := repo.GetWorkItem(ctx, "acme/widget#8"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetWorkItem missing = %v, want ErrNotFound", err)
	}
}
