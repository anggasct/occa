package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var ErrStageInputNotWaiting = errors.New("store: stage input not waiting")

type StageInputState string

const (
	StageInputWaiting    StageInputState = "waiting"
	StageInputConsumed   StageInputState = "consumed"
	StageInputSuperseded StageInputState = "superseded"
	StageInputExpired    StageInputState = "expired"
)

type StageInput struct {
	ID         int64
	StageRunID int64
	WorkItemID string
	DeliveryID string
	Endpoint   string
	EventType  string
	Body       []byte
	State      StageInputState
	ReceivedAt int64
	ConsumedAt int64
}

type StageRunBlock struct {
	WorkItemID  string
	Stage       Stage
	Code        BlockerCode
	Detail      string
	TriggeredBy string
}

func addStageInputPayload(_ *SQLiteStore, tx *sql.Tx) error {
	have, err := tableColumns(tx, "stage_input")
	if err != nil {
		return fmt.Errorf("store: migrate stage input payload: %w", err)
	}
	for _, col := range []struct{ name, ddl string }{
		{"event_type", `ALTER TABLE stage_input ADD COLUMN event_type TEXT NOT NULL DEFAULT '';`},
		{"body", `ALTER TABLE stage_input ADD COLUMN body BLOB NOT NULL DEFAULT x'';`},
	} {
		if have[col.name] {
			continue
		}
		if _, err := tx.Exec(col.ddl); err != nil {
			return fmt.Errorf("store: migrate stage input %s: %w", col.name, err)
		}
	}
	return nil
}

func tableColumns(tx *sql.Tx, table string) (map[string]bool, error) {
	rows, err := tx.Query(`SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}

func nextAttempt(ctx context.Context, tx *sql.Tx, workItemID string, stage Stage) (int, error) {
	var last int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(attempt), 0) FROM stage_run WHERE work_item_id = ? AND stage = ?`,
		workItemID, string(stage),
	).Scan(&last); err != nil {
		return 0, fmt.Errorf("store: stage run attempt: %w", err)
	}
	return last + 1, nil
}

func insertBlockedRun(ctx context.Context, tx *sql.Tx, block StageRunBlock) (int64, error) {
	attempt, err := nextAttempt(ctx, tx, block.WorkItemID, block.Stage)
	if err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx,
		`INSERT INTO stage_run (work_item_id, stage, attempt, state, blocker_code, blocker_detail, triggered_by, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		block.WorkItemID, string(block.Stage), attempt, string(StageStateBlocked), string(block.Code), block.Detail, block.TriggeredBy, time.Now().Unix(),
	)
	if err != nil {
		return 0, fmt.Errorf("store: blocked stage run insert: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("store: blocked stage run id: %w", err)
	}
	return id, nil
}

func (r *sqliteStageRepo) StartBlockedRun(ctx context.Context, block StageRunBlock) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: blocked stage run begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	id, err := insertBlockedRun(ctx, tx, block)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: blocked stage run commit: %w", err)
	}
	return id, nil
}

func (r *sqliteStageRepo) LatestBlockedRun(ctx context.Context, workItemID string) (*StageRun, error) {
	runs, err := r.listRuns(ctx,
		`SELECT `+stageRunColumns+` FROM stage_run WHERE work_item_id = ? AND state = ? ORDER BY id DESC LIMIT 1`,
		workItemID, string(StageStateBlocked))
	if err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return nil, fmt.Errorf("store: latest blocked run %s: %w", workItemID, ErrNotFound)
	}
	return &runs[0], nil
}

// AttachInput keeps a delivery on the work item's newest blocked run, creating
// that run from fallback when there is none. It returns the stored input and
// whether this call added it; a redelivery returns the row already stored.
func (r *sqliteStageRepo) AttachInput(ctx context.Context, fallback StageRunBlock, in StageInput) (StageInput, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return StageInput{}, false, fmt.Errorf("store: stage input begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var runID int64
	err = tx.QueryRowContext(ctx,
		`SELECT id FROM stage_run WHERE work_item_id = ? AND state = ? ORDER BY id DESC LIMIT 1`,
		fallback.WorkItemID, string(StageStateBlocked)).Scan(&runID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if runID, err = insertBlockedRun(ctx, tx, fallback); err != nil {
			return StageInput{}, false, err
		}
	case err != nil:
		return StageInput{}, false, fmt.Errorf("store: stage input run: %w", err)
	}
	body := in.Body
	if body == nil {
		body = []byte{}
	}
	res, err := tx.ExecContext(ctx,
		`INSERT INTO stage_input (stage_run_id, delivery_id, endpoint, state, received_at, event_type, body)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (stage_run_id, endpoint, delivery_id) DO NOTHING`,
		runID, in.DeliveryID, in.Endpoint, string(StageInputWaiting), time.Now().Unix(), in.EventType, body)
	if err != nil {
		return StageInput{}, false, fmt.Errorf("store: stage input insert: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return StageInput{}, false, fmt.Errorf("store: stage input insert rows: %w", err)
	}
	stored, err := scanStageInputs(tx.QueryContext(ctx,
		`SELECT `+stageInputColumns+` FROM stage_input i JOIN stage_run r ON r.id = i.stage_run_id
		 WHERE i.stage_run_id = ? AND i.endpoint = ? AND i.delivery_id = ?`, runID, in.Endpoint, in.DeliveryID))
	if err != nil {
		return StageInput{}, false, err
	}
	if len(stored) != 1 {
		return StageInput{}, false, fmt.Errorf("store: stage input %s: %w", in.DeliveryID, ErrNotFound)
	}
	if err := tx.Commit(); err != nil {
		return StageInput{}, false, fmt.Errorf("store: stage input commit: %w", err)
	}
	return stored[0], n > 0, nil
}

const stageInputColumns = `i.id, i.stage_run_id, r.work_item_id, i.delivery_id, i.endpoint, i.event_type, i.body,
	i.state, i.received_at, i.consumed_at`

func (r *sqliteStageRepo) ListInputs(ctx context.Context, workItemID string) ([]StageInput, error) {
	return scanStageInputs(r.db.QueryContext(ctx,
		`SELECT `+stageInputColumns+` FROM stage_input i JOIN stage_run r ON r.id = i.stage_run_id
		 WHERE r.work_item_id = ? ORDER BY i.id`, workItemID))
}

func (r *sqliteStageRepo) ListWaitingInputs(ctx context.Context, workItemID string) ([]StageInput, error) {
	return scanStageInputs(r.db.QueryContext(ctx,
		`SELECT `+stageInputColumns+` FROM stage_input i JOIN stage_run r ON r.id = i.stage_run_id
		 WHERE r.work_item_id = ? AND i.state = ?
		 ORDER BY i.received_at DESC, i.id DESC`,
		workItemID, string(StageInputWaiting)))
}

func (r *sqliteStageRepo) ListExpiredInputs(ctx context.Context, cutoff int64, limit int) ([]StageInput, error) {
	return scanStageInputs(r.db.QueryContext(ctx,
		`SELECT `+stageInputColumns+` FROM stage_input i JOIN stage_run r ON r.id = i.stage_run_id
		 WHERE i.state = ? AND i.received_at <= ?
		 ORDER BY i.received_at, i.id LIMIT ?`,
		string(StageInputWaiting), cutoff, limit))
}

// SetInputState settles a waiting input. Only a waiting input moves, so two
// unblocks racing for the same input cannot both run it.
func (r *sqliteStageRepo) SetInputState(ctx context.Context, id int64, state StageInputState) error {
	if state != StageInputConsumed && state != StageInputSuperseded && state != StageInputExpired {
		return fmt.Errorf("store: stage input state %q: %w", state, ErrStageStateInvalid)
	}
	consumedAt := int64(0)
	if state == StageInputConsumed {
		consumedAt = time.Now().Unix()
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: stage input state begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx,
		`UPDATE stage_input SET state = ?, consumed_at = ? WHERE id = ? AND state = ?`,
		string(state), consumedAt, id, string(StageInputWaiting))
	if err != nil {
		return fmt.Errorf("store: stage input state: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: stage input state rows: %w", err)
	}
	if n == 0 {
		var current string
		err := tx.QueryRowContext(ctx, `SELECT state FROM stage_input WHERE id = ?`, id).Scan(&current)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("store: stage input %d: %w", id, ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("store: stage input state read: %w", err)
		}
		return fmt.Errorf("store: stage input %d is %s: %w", id, current, ErrStageInputNotWaiting)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: stage input state commit: %w", err)
	}
	return nil
}

func scanStageInputs(rows *sql.Rows, err error) ([]StageInput, error) {
	if err != nil {
		return nil, fmt.Errorf("store: stage input list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []StageInput
	for rows.Next() {
		var in StageInput
		var state string
		if err := rows.Scan(&in.ID, &in.StageRunID, &in.WorkItemID, &in.DeliveryID, &in.Endpoint, &in.EventType, &in.Body,
			&state, &in.ReceivedAt, &in.ConsumedAt); err != nil {
			return nil, fmt.Errorf("store: stage input scan: %w", err)
		}
		in.State = StageInputState(state)
		out = append(out, in)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: stage input rows: %w", err)
	}
	return out, nil
}
