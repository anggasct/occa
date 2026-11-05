package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var ErrStageRunNotPending = errors.New("store: stage run not pending")

// QueueRun records a delivery waiting in its PR queue. A trigger that already
// has a pending run of the stage gets that run back, so a redelivery or a
// replay never adds a second attempt for the same request.
func (r *sqliteStageRepo) QueueRun(ctx context.Context, start StageRunStart) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: pending stage run begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var id int64
	err = tx.QueryRowContext(ctx,
		`SELECT id FROM stage_run WHERE work_item_id = ? AND stage = ? AND state = ? AND triggered_by = ?
		 ORDER BY id DESC LIMIT 1`,
		start.WorkItemID, string(start.Stage), string(StageStatePending), start.TriggeredBy).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("store: pending stage run lookup: %w", err)
	}
	attempt, err := nextAttempt(ctx, tx, start.WorkItemID, start.Stage)
	if err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx,
		`INSERT INTO stage_run (work_item_id, stage, attempt, state, triggered_by, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		start.WorkItemID, string(start.Stage), attempt, string(StageStatePending), start.TriggeredBy, time.Now().Unix(),
	)
	if err != nil {
		return 0, fmt.Errorf("store: pending stage run insert: %w", err)
	}
	if id, err = res.LastInsertId(); err != nil {
		return 0, fmt.Errorf("store: pending stage run id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: pending stage run commit: %w", err)
	}
	return id, nil
}

// PromotePending starts a pending run in place, so the delivery that was
// queued keeps its attempt instead of opening a new one.
func (r *sqliteStageRepo) PromotePending(ctx context.Context, runID int64, workspacePath string) error {
	now := time.Now().Unix()
	return r.updatePending(ctx, "promote", runID,
		`state = ?, started_at = ?, last_heartbeat_at = ?, workspace_path = ?`,
		string(StageStateRunning), now, now, workspacePath)
}

// EndPending ends a run that never started. Only a pending run moves, so a
// late hook can never end a run that has started meanwhile.
func (r *sqliteStageRepo) EndPending(ctx context.Context, runID int64, state StageState, detail string) error {
	if state != StageStateSkipped && state != StageStateCancelled {
		return fmt.Errorf("store: pending stage run end %q: %w", state, ErrStageStateInvalid)
	}
	return r.updatePending(ctx, "end pending", runID, `state = ?, blocker_detail = ?, ended_at = ?`,
		string(state), detail, time.Now().Unix())
}

func (r *sqliteStageRepo) updatePending(ctx context.Context, op string, runID int64, set string, setArgs ...any) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: stage run %s begin: %w", op, err)
	}
	defer func() { _ = tx.Rollback() }()
	args := append(setArgs, runID, string(StageStatePending))
	res, err := tx.ExecContext(ctx, `UPDATE stage_run SET `+set+` WHERE id = ? AND state = ?`, args...)
	if err != nil {
		return fmt.Errorf("store: stage run %s: %w", op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: stage run %s rows: %w", op, err)
	}
	if n == 0 {
		current, err := stageRunState(ctx, tx, runID)
		if err != nil {
			return fmt.Errorf("store: stage run %s %d: %w", op, runID, err)
		}
		if current.Terminal() {
			return fmt.Errorf("store: stage run %s %d: %w", op, runID, ErrTerminalAlreadySet)
		}
		return fmt.Errorf("store: stage run %s %d: %w", op, runID, ErrStageRunNotPending)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: stage run %s commit: %w", op, err)
	}
	return nil
}

// BlockPending records a block raised before the run started on the pending
// run it hit, so the stage keeps one attempt. With runID 0 it takes the newest
// pending run of the stage; ErrNotFound means there is none.
func (r *sqliteStageRepo) BlockPending(ctx context.Context, block StageRunBlock, runID int64) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: pending stage run block begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if runID == 0 {
		err := tx.QueryRowContext(ctx,
			`SELECT id FROM stage_run WHERE work_item_id = ? AND stage = ? AND state = ? ORDER BY id DESC LIMIT 1`,
			block.WorkItemID, string(block.Stage), string(StageStatePending)).Scan(&runID)
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("store: pending stage run block %s: %w", block.WorkItemID, ErrNotFound)
		}
		if err != nil {
			return 0, fmt.Errorf("store: pending stage run block lookup: %w", err)
		}
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE stage_run SET state = ?, blocker_code = ?, blocker_detail = ? WHERE id = ? AND state = ?`,
		string(StageStateBlocked), string(block.Code), block.Detail, runID, string(StageStatePending))
	if err != nil {
		return 0, fmt.Errorf("store: pending stage run block: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: pending stage run block rows: %w", err)
	}
	if n == 0 {
		current, err := stageRunState(ctx, tx, runID)
		if err != nil {
			return 0, fmt.Errorf("store: pending stage run block %d: %w", runID, err)
		}
		if current.Terminal() {
			return 0, fmt.Errorf("store: pending stage run block %d: %w", runID, ErrTerminalAlreadySet)
		}
		return 0, fmt.Errorf("store: pending stage run block %d: %w", runID, ErrStageRunNotPending)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: pending stage run block commit: %w", err)
	}
	return runID, nil
}

func (r *sqliteStageRepo) ListPending(ctx context.Context, limit int) ([]StageRun, error) {
	return r.listRuns(ctx, `SELECT `+stageRunColumns+` FROM stage_run WHERE state = ? ORDER BY id LIMIT ?`,
		string(StageStatePending), limit)
}
