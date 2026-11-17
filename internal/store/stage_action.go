package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type ActionRequest struct {
	ID             int64
	WorkItemID     string
	StageRunID     int64
	Action         string
	Actor          string
	Reason         string
	IdempotencyKey string
	Result         string
	Code           string
	Sentence       string
	Detail         string
	CreatedAt      int64
}

func addActionResultColumns(_ *SQLiteStore, tx *sql.Tx) error {
	var have int
	if err := tx.QueryRow(
		`SELECT COUNT(*) FROM pragma_table_info('action_request') WHERE name = 'code'`,
	).Scan(&have); err != nil {
		return fmt.Errorf("store: migrate action result columns: %w", err)
	}
	if have > 0 {
		return nil
	}
	for _, ddl := range []string{
		`ALTER TABLE action_request ADD COLUMN code TEXT NOT NULL DEFAULT '';`,
		`ALTER TABLE action_request ADD COLUMN sentence TEXT NOT NULL DEFAULT '';`,
		`ALTER TABLE action_request ADD COLUMN detail TEXT NOT NULL DEFAULT '';`,
	} {
		if _, err := tx.Exec(ddl); err != nil {
			return fmt.Errorf("store: migrate action result columns: %w", err)
		}
	}
	return nil
}

// RecordAction inserts an action request. A replay of the same idempotency
// key returns the stored row with replayed=true so the caller never fires
// the effect twice.
func (r *sqliteStageRepo) RecordAction(ctx context.Context, req ActionRequest) (ActionRequest, bool, error) {
	res, err := r.db.ExecContext(ctx,
		`INSERT INTO action_request (work_item_id, stage_run_id, action, actor, reason, idempotency_key, result, code, sentence, detail, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (idempotency_key) DO NOTHING`,
		req.WorkItemID, req.StageRunID, req.Action, req.Actor, req.Reason, req.IdempotencyKey,
		req.Result, req.Code, req.Sentence, req.Detail, time.Now().Unix(),
	)
	if err != nil {
		return ActionRequest{}, false, fmt.Errorf("store: action record: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return ActionRequest{}, false, fmt.Errorf("store: action record rows: %w", err)
	}
	if n == 1 {
		id, err := res.LastInsertId()
		if err != nil {
			return ActionRequest{}, false, fmt.Errorf("store: action record id: %w", err)
		}
		req.ID = id
		return req, false, nil
	}
	stored, err := r.getActionByKey(ctx, req.IdempotencyKey)
	if err != nil {
		return ActionRequest{}, false, fmt.Errorf("store: action replay lookup: %w", err)
	}
	return *stored, true, nil
}

func (r *sqliteStageRepo) getActionByKey(ctx context.Context, key string) (*ActionRequest, error) {
	var a ActionRequest
	err := r.db.QueryRowContext(ctx,
		`SELECT id, work_item_id, stage_run_id, action, actor, reason, idempotency_key, result, code, sentence, detail, created_at
		 FROM action_request WHERE idempotency_key = ?`, key,
	).Scan(&a.ID, &a.WorkItemID, &a.StageRunID, &a.Action, &a.Actor, &a.Reason, &a.IdempotencyKey, &a.Result, &a.Code, &a.Sentence, &a.Detail, &a.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("store: action key %s: %w", key, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("store: action by key: %w", err)
	}
	return &a, nil
}

func (r *sqliteStageRepo) GetAction(ctx context.Context, id int64) (*ActionRequest, error) {
	var a ActionRequest
	err := r.db.QueryRowContext(ctx,
		`SELECT id, work_item_id, stage_run_id, action, actor, reason, idempotency_key, result, code, sentence, detail, created_at
		 FROM action_request WHERE id = ?`, id,
	).Scan(&a.ID, &a.WorkItemID, &a.StageRunID, &a.Action, &a.Actor, &a.Reason, &a.IdempotencyKey, &a.Result, &a.Code, &a.Sentence, &a.Detail, &a.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("store: action %d: %w", id, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("store: action get: %w", err)
	}
	return &a, nil
}

// ListActions returns a work item's action requests, newest first.
func (r *sqliteStageRepo) ListActions(ctx context.Context, workItemID string) ([]ActionRequest, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, work_item_id, stage_run_id, action, actor, reason, idempotency_key, result, code, sentence, detail, created_at
		 FROM action_request WHERE work_item_id = ? ORDER BY id DESC`, workItemID)
	if err != nil {
		return nil, fmt.Errorf("store: action list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []ActionRequest
	for rows.Next() {
		var a ActionRequest
		if err := rows.Scan(&a.ID, &a.WorkItemID, &a.StageRunID, &a.Action, &a.Actor, &a.Reason, &a.IdempotencyKey, &a.Result, &a.Code, &a.Sentence, &a.Detail, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: action list scan: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: action list rows: %w", err)
	}
	return out, nil
}

func (r *sqliteStageRepo) SetActionResult(ctx context.Context, id int64, result, code, sentence, detail string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE action_request SET result = ?, code = ?, sentence = ?, detail = ? WHERE id = ?`,
		result, code, sentence, detail, id)
	if err != nil {
		return fmt.Errorf("store: action result: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: action result rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("store: action result %d: %w", id, ErrNotFound)
	}
	return nil
}

// ListLatestRuns returns the newest stage run per (work_item, stage) for the
// given items — one query, no per-item round trips.
func (r *sqliteStageRepo) ListLatestRuns(ctx context.Context, workItemIDs []string) (map[string][]StageRun, error) {
	if len(workItemIDs) == 0 {
		return map[string][]StageRun{}, nil
	}
	marks := ""
	args := make([]any, 0, len(workItemIDs))
	for i, id := range workItemIDs {
		if i > 0 {
			marks += ","
		}
		marks += "?"
		args = append(args, id)
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+stageRunColumns+` FROM stage_run s
		 WHERE s.work_item_id IN (`+marks+`)
		   AND s.id = (SELECT MAX(id) FROM stage_run WHERE work_item_id = s.work_item_id AND stage = s.stage)
		 ORDER BY s.work_item_id, s.stage`, args...)
	if err != nil {
		return nil, fmt.Errorf("store: latest stage runs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string][]StageRun{}
	for rows.Next() {
		var s StageRun
		var stage, state, code string
		if err := rows.Scan(&s.ID, &s.WorkItemID, &stage, &s.Attempt, &state, &code, &s.BlockerDetail,
			&s.StartedAt, &s.LastHeartbeatAt, &s.EndedAt, &s.AgentSessionID, &s.AgentPID, &s.WorkspacePath,
			&s.ArtifactRef, &s.GateResult, &s.TriggeredBy, &s.AutoRetryUsed, &s.ManualRetryCount, &s.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: latest stage run scan: %w", err)
		}
		s.Stage, s.State, s.BlockerCode = Stage(stage), StageState(state), BlockerCode(code)
		out[s.WorkItemID] = append(out[s.WorkItemID], s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: latest stage run rows: %w", err)
	}
	return out, nil
}

// ListOpenWorkItems returns open work items, optionally filtered by repo.
func (r *sqliteStageRepo) ListOpenWorkItems(ctx context.Context, repo string, limit int) ([]WorkItem, error) {
	if limit <= 0 {
		limit = 50
	}
	args := []any{string(WorkItemOpen)}
	query := `SELECT id, repo, number, kind, spec_ref, spec_sha, state, created_at, updated_at
		 FROM work_item WHERE state = ?`
	if repo != "" {
		query += ` AND repo = ?`
		args = append(args, repo)
	}
	query += ` ORDER BY updated_at DESC LIMIT ?`
	args = append(args, limit)
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: open work items: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []WorkItem
	for rows.Next() {
		var w WorkItem
		var kind, state string
		if err := rows.Scan(&w.ID, &w.Repo, &w.Number, &kind, &w.SpecRef, &w.SpecSHA, &state, &w.CreatedAt, &w.UpdatedAt); err != nil {
			return nil, fmt.Errorf("store: open work item scan: %w", err)
		}
		w.Kind, w.State = WorkItemKind(kind), WorkItemState(state)
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: open work item rows: %w", err)
	}
	return out, nil
}

// CountWaitingInputs returns waiting-input counts per work item in one query.
func (r *sqliteStageRepo) CountWaitingInputs(ctx context.Context, workItemIDs []string) (map[string]int, error) {
	if len(workItemIDs) == 0 {
		return map[string]int{}, nil
	}
	marks := ""
	args := make([]any, 0, len(workItemIDs)+1)
	for i, id := range workItemIDs {
		if i > 0 {
			marks += ","
		}
		marks += "?"
		args = append(args, id)
	}
	args = append(args, string(StageInputWaiting))
	rows, err := r.db.QueryContext(ctx,
		`SELECT sr.work_item_id, COUNT(*) FROM stage_input si
		 JOIN stage_run sr ON sr.id = si.stage_run_id
		 WHERE sr.work_item_id IN (`+marks+`) AND si.state = ?
		 GROUP BY sr.work_item_id`, args...)
	if err != nil {
		return nil, fmt.Errorf("store: waiting input counts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]int{}
	var itemID string
	var n int
	for rows.Next() {
		if err := rows.Scan(&itemID, &n); err != nil {
			return nil, fmt.Errorf("store: waiting input count scan: %w", err)
		}
		out[itemID] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: waiting input count rows: %w", err)
	}
	return out, nil
}
