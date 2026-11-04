package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestStageInputMigrationUpgradesLiveV26Table(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	open := func() *SQLiteStore {
		t.Helper()
		s, err := OpenWithRetention(path, "", UsageRetention{Retention: time.Hour, MaxRows: 10}, time.Hour)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return s
	}
	s := open()
	for _, stmt := range []string{
		`DROP TABLE stage_input`,
		`CREATE TABLE stage_input (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			stage_run_id INTEGER NOT NULL,
			delivery_id  TEXT    NOT NULL,
			endpoint     TEXT    NOT NULL,
			state        TEXT    NOT NULL CHECK (state IN ('waiting','consumed','superseded','expired')),
			received_at  INTEGER NOT NULL,
			consumed_at  INTEGER NOT NULL DEFAULT 0,
			UNIQUE (stage_run_id, delivery_id)
		)`,
		`CREATE INDEX idx_stage_input_open ON stage_input (stage_run_id, state, received_at)`,
		`INSERT INTO stage_input (stage_run_id, delivery_id, endpoint, state, received_at) VALUES (7, 'd1', 'review', 'waiting', 100)`,
		`PRAGMA user_version=26`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			_ = s.Close()
			t.Fatalf("seed v26 table: %s: %v", stmt, err)
		}
	}
	_ = s.Close()

	s = open()
	defer func() { _ = s.Close() }()
	assertVersion(t, s, SchemaVersion)
	var deliveryID, endpoint, state, eventType string
	var body []byte
	if err := s.db.QueryRow(`SELECT delivery_id, endpoint, state, event_type, body FROM stage_input WHERE stage_run_id = 7`).Scan(&deliveryID, &endpoint, &state, &eventType, &body); err != nil {
		t.Fatalf("read upgraded row: %v", err)
	}
	if deliveryID != "d1" || endpoint != "review" || state != "waiting" || eventType != "" || len(body) != 0 {
		t.Fatalf("upgraded row = %q %q %q %q %q", deliveryID, endpoint, state, eventType, body)
	}
	if _, err := s.db.Exec(`INSERT INTO stage_input (stage_run_id, delivery_id, endpoint, state, received_at) VALUES (7, 'd1', 'fix', 'waiting', 101)`); err != nil {
		t.Fatalf("another endpoint's copy of the same delivery: %v", err)
	}
	if _, err := s.db.Exec(`INSERT INTO stage_input (stage_run_id, delivery_id, endpoint, state, received_at) VALUES (7, 'd1', 'review', 'waiting', 102)`); err == nil {
		t.Fatal("the same endpoint and delivery must stay unique")
	}
	var indexes int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_stage_input_open'`).Scan(&indexes); err != nil || indexes != 1 {
		t.Fatalf("idx_stage_input_open after rebuild = %d, %v", indexes, err)
	}
}

func blockedFallback(stage Stage) StageRunBlock {
	return StageRunBlock{WorkItemID: stageTestItem, Stage: stage, Code: BlockerAgentNoCommit, Detail: "fix made no change", TriggeredBy: "delivery:review/d0"}
}

func TestStartBlockedRunNumbersAttempts(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.StageRepo()
	seedStageItem(t, repo)
	startStage(t, repo, StageFix)
	id, err := repo.StartBlockedRun(ctx, StageRunBlock{WorkItemID: stageTestItem, Stage: StageFix, Code: BlockerWorkspaceDirty, Detail: "dirty", TriggeredBy: "delivery:fix/d2"})
	if err != nil {
		t.Fatalf("StartBlockedRun: %v", err)
	}
	run, err := repo.LatestBlockedRun(ctx, stageTestItem)
	if err != nil {
		t.Fatalf("LatestBlockedRun: %v", err)
	}
	if run.ID != id || run.Attempt != 2 || run.State != StageStateBlocked || run.BlockerCode != BlockerWorkspaceDirty ||
		run.BlockerDetail != "dirty" || run.TriggeredBy != "delivery:fix/d2" || run.StartedAt != 0 || run.EndedAt != 0 {
		t.Fatalf("blocked run = %+v", run)
	}
	if _, err := repo.LatestBlockedRun(ctx, "acme/widget#8"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LatestBlockedRun on an item without one = %v, want ErrNotFound", err)
	}
}

func TestAttachInputFindsOrCreatesBlockedRun(t *testing.T) {
	cases := []struct {
		name        string
		seed        func(t *testing.T, repo StageRepo) int64
		wantAttempt int
	}{
		{
			name:        "no blocked run creates one",
			seed:        func(*testing.T, StageRepo) int64 { return 0 },
			wantAttempt: 1,
		},
		{
			name: "newest blocked run is reused",
			seed: func(t *testing.T, repo StageRepo) int64 {
				t.Helper()
				id := startStage(t, repo, StageFix)
				if err := repo.MarkBlocked(context.Background(), id, BlockerAgentNoCommit, "no change"); err != nil {
					t.Fatal(err)
				}
				return id
			},
			wantAttempt: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := tempStore(t)
			ctx := context.Background()
			repo := s.StageRepo()
			seedStageItem(t, repo)
			seeded := tc.seed(t, repo)
			in := StageInput{DeliveryID: "d1", Endpoint: "review", EventType: "pull_request_review", Body: []byte("body-1")}
			got, added, err := repo.AttachInput(ctx, blockedFallback(StageFix), in)
			if err != nil || !added {
				t.Fatalf("AttachInput = %+v, %v, %v", got, added, err)
			}
			if seeded != 0 && got.StageRunID != seeded {
				t.Fatalf("input run = %d, want the blocked run %d", got.StageRunID, seeded)
			}
			runs, err := repo.ListRuns(ctx, stageTestItem)
			if err != nil || len(runs) != 1 || runs[0].State != StageStateBlocked || runs[0].Attempt != tc.wantAttempt {
				t.Fatalf("runs = %+v, %v", runs, err)
			}
			if got.State != StageInputWaiting || got.WorkItemID != stageTestItem || got.ReceivedAt == 0 || string(got.Body) != "body-1" {
				t.Fatalf("input = %+v", got)
			}
			again, added, err := repo.AttachInput(ctx, blockedFallback(StageFix), in)
			if err != nil || added || again.ID != got.ID {
				t.Fatalf("redelivery = %+v, %v, %v; want the stored row, not added", again, added, err)
			}
			waiting, err := repo.ListWaitingInputs(ctx, stageTestItem)
			if err != nil || len(waiting) != 1 {
				t.Fatalf("waiting = %+v, %v; want one input", waiting, err)
			}
		})
	}
}

func TestStageInputStateTransitions(t *testing.T) {
	cases := []struct {
		name     string
		state    StageInputState
		consumed bool
	}{
		{name: "consumed", state: StageInputConsumed, consumed: true},
		{name: "superseded", state: StageInputSuperseded},
		{name: "expired", state: StageInputExpired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := tempStore(t)
			ctx := context.Background()
			repo := s.StageRepo()
			seedStageItem(t, repo)
			in, _, err := repo.AttachInput(ctx, blockedFallback(StageReview), StageInput{DeliveryID: "d1", Endpoint: "review"})
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.SetInputState(ctx, in.ID, tc.state); err != nil {
				t.Fatalf("SetInputState: %v", err)
			}
			var state string
			var consumedAt int64
			if err := s.db.QueryRow(`SELECT state, consumed_at FROM stage_input WHERE id = ?`, in.ID).Scan(&state, &consumedAt); err != nil {
				t.Fatal(err)
			}
			if StageInputState(state) != tc.state || (consumedAt != 0) != tc.consumed {
				t.Fatalf("row = %s consumed_at %d", state, consumedAt)
			}
			if err := repo.SetInputState(ctx, in.ID, StageInputSuperseded); !errors.Is(err, ErrStageInputNotWaiting) {
				t.Fatalf("second transition = %v, want ErrStageInputNotWaiting", err)
			}
		})
	}
	s := tempStore(t)
	if err := s.StageRepo().SetInputState(context.Background(), 999, StageInputConsumed); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing input = %v, want ErrNotFound", err)
	}
	if err := s.StageRepo().SetInputState(context.Background(), 1, StageInputWaiting); !errors.Is(err, ErrStageStateInvalid) {
		t.Fatalf("back to waiting = %v, want ErrStageStateInvalid", err)
	}
}

func TestListWaitingAndExpiredInputs(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.StageRepo()
	seedStageItem(t, repo)
	var ids []int64
	for i, delivery := range []string{"d-old", "d-mid", "d-new"} {
		in, _, err := repo.AttachInput(ctx, blockedFallback(StageReview), StageInput{DeliveryID: delivery, Endpoint: "review"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`UPDATE stage_input SET received_at = ? WHERE id = ?`, int64(1000+i*100), in.ID); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, in.ID)
	}
	waiting, err := repo.ListWaitingInputs(ctx, stageTestItem)
	if err != nil || len(waiting) != 3 || waiting[0].DeliveryID != "d-new" || waiting[2].DeliveryID != "d-old" {
		t.Fatalf("waiting = %+v, %v; want newest first", waiting, err)
	}
	expired, err := repo.ListExpiredInputs(ctx, 1100, 10)
	if err != nil || len(expired) != 2 || expired[0].DeliveryID != "d-old" || expired[1].DeliveryID != "d-mid" {
		t.Fatalf("expired = %+v, %v", expired, err)
	}
	if err := repo.SetInputState(ctx, ids[0], StageInputExpired); err != nil {
		t.Fatal(err)
	}
	expired, err = repo.ListExpiredInputs(ctx, 1100, 10)
	if err != nil || len(expired) != 1 || expired[0].DeliveryID != "d-mid" {
		t.Fatalf("expired after settling = %+v, %v; only waiting inputs expire", expired, err)
	}
	if other, err := repo.ListWaitingInputs(ctx, "acme/widget#8"); err != nil || len(other) != 0 {
		t.Fatalf("other item = %+v, %v", other, err)
	}
}

func TestAttachInputKeepsEachEndpointCopy(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.StageRepo()
	seedStageItem(t, repo)
	for _, endpoint := range []string{"review", "fix", "review"} {
		if _, _, err := repo.AttachInput(ctx, blockedFallback(StageFix), StageInput{DeliveryID: "d-shared", Endpoint: endpoint}); err != nil {
			t.Fatalf("AttachInput %s: %v", endpoint, err)
		}
	}
	inputs, err := repo.ListInputs(ctx, stageTestItem)
	if err != nil || len(inputs) != 2 || inputs[0].Endpoint != "review" || inputs[1].Endpoint != "fix" {
		t.Fatalf("inputs = %+v, %v; want one per endpoint", inputs, err)
	}
}

func TestFailStaleKeepsWaitingInputReceipt(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	deliveries := s.WebhookDeliveryRepo()
	for _, id := range []string{"d-kept", "d-lost"} {
		if _, err := deliveries.Create(ctx, webhookDelivery(func(d *WebhookDelivery) { d.DeliveryID = id })); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
	}
	if _, err := s.db.Exec(`UPDATE webhook_delivery SET updated_at = 1`); err != nil {
		t.Fatal(err)
	}
	seedStageItem(t, s.StageRepo())
	in, _, err := s.StageRepo().AttachInput(ctx, blockedFallback(StageReview), StageInput{DeliveryID: "d-kept", Endpoint: "github-review"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.StageRepo().AttachInput(ctx, blockedFallback(StageReview), StageInput{DeliveryID: "d-lost", Endpoint: "other-endpoint"}); err != nil {
		t.Fatal(err)
	}
	failed, err := deliveries.FailStale(ctx, 500, "interrupted by restart")
	if err != nil || failed != 1 {
		t.Fatalf("FailStale = %d, %v; want only the receipt without a waiting input", failed, err)
	}
	statusOf := func(id string) WebhookStatus {
		t.Helper()
		d, err := deliveries.Get(ctx, "github-review", id)
		if err != nil || d == nil {
			t.Fatalf("Get %s: %+v, %v", id, d, err)
		}
		return d.Status
	}
	if got := statusOf("d-kept"); got != WebhookStatusReceived {
		t.Fatalf("kept receipt = %s, want received", got)
	}
	if got := statusOf("d-lost"); got != WebhookStatusFailed {
		t.Fatalf("receipt of another endpoint's input = %s, want failed", got)
	}
	if err := s.StageRepo().SetInputState(ctx, in.ID, StageInputSuperseded); err != nil {
		t.Fatal(err)
	}
	if failed, err := deliveries.FailStale(ctx, 500, "interrupted by restart"); err != nil || failed != 1 {
		t.Fatalf("FailStale after settling = %d, %v; want the receipt failed", failed, err)
	}
}
