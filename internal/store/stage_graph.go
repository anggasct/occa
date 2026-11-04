package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var (
	ErrTerminalAlreadySet = errors.New("store: stage run terminal state already set")
	ErrStageRunNotRunning = errors.New("store: stage run not running")
	ErrStageStateInvalid  = errors.New("store: stage state invalid")
)

type WorkItemKind string

const (
	WorkItemPullRequest WorkItemKind = "pull_request"
	WorkItemIssue       WorkItemKind = "issue"
)

type WorkItemState string

const (
	WorkItemOpen      WorkItemState = "open"
	WorkItemDone      WorkItemState = "done"
	WorkItemAbandoned WorkItemState = "abandoned"
)

type Stage string

const (
	StageReview Stage = "review"
	StageFix    Stage = "fix"
	StageMerge  Stage = "merge"
)

type StageState string

const (
	StageStatePending         StageState = "pending"
	StageStateRunning         StageState = "running"
	StageStateWaitingHuman    StageState = "waiting_human"
	StageStateWaitingExternal StageState = "waiting_external"
	StageStateBlocked         StageState = "blocked"
	StageStateDone            StageState = "done"
	StageStateFailed          StageState = "failed"
	StageStateSkipped         StageState = "skipped"
	StageStateCancelled       StageState = "cancelled"
)

func (s StageState) Terminal() bool {
	switch s {
	case StageStateDone, StageStateFailed, StageStateSkipped, StageStateCancelled:
		return true
	}
	return false
}

type BlockerCode string

const (
	BlockerWorkspaceDirty     BlockerCode = "workspace_dirty"
	BlockerRetriesExhausted   BlockerCode = "retries_exhausted"
	BlockerGateFailed         BlockerCode = "gate_failed"
	BlockerVerdictStale       BlockerCode = "verdict_stale"
	BlockerHeadUnchanged      BlockerCode = "head_unchanged"
	BlockerCIRed              BlockerCode = "ci_red"
	BlockerReviewPending      BlockerCode = "review_pending"
	BlockerApprovalMissing    BlockerCode = "approval_missing"
	BlockerEventMissing       BlockerCode = "event_missing"
	BlockerBindingMissing     BlockerCode = "binding_missing"
	BlockerAuthDenied         BlockerCode = "auth_denied"
	BlockerQuotaExceeded      BlockerCode = "quota_exceeded"
	BlockerAgentNoCommit      BlockerCode = "agent_no_commit"
	BlockerTimeout            BlockerCode = "timeout"
	BlockerMisclassifiedAbort BlockerCode = "misclassified_abort"
	BlockerProviderLimit      BlockerCode = "provider_limit"
	BlockerGitHubUnreachable  BlockerCode = "github_unreachable"
	BlockerMergeStarved       BlockerCode = "merge_starved"
)

type WorkItem struct {
	ID        string
	Repo      string
	Number    int
	Kind      WorkItemKind
	SpecRef   string
	SpecSHA   string
	State     WorkItemState
	CreatedAt int64
	UpdatedAt int64
}

type StageRun struct {
	ID               int64
	WorkItemID       string
	Stage            Stage
	Attempt          int
	State            StageState
	BlockerCode      BlockerCode
	BlockerDetail    string
	StartedAt        int64
	LastHeartbeatAt  int64
	EndedAt          int64
	AgentSessionID   string
	AgentPID         int
	WorkspacePath    string
	ArtifactRef      string
	GateResult       string
	TriggeredBy      string
	AutoRetryUsed    int
	ManualRetryCount int
	CreatedAt        int64
}

// Stalled is derived at read time and never stored, so the threshold stays a
// reader's decision.
func (r StageRun) Stalled(now time.Time, threshold time.Duration) bool {
	return r.State == StageStateRunning && threshold > 0 && now.Sub(time.Unix(r.LastHeartbeatAt, 0)) > threshold
}

type StageRunStart struct {
	WorkItemID    string
	Stage         Stage
	TriggeredBy   string
	WorkspacePath string
}

type StageRepo interface {
	UpsertWorkItem(ctx context.Context, item WorkItem) error
	SetWorkItemState(ctx context.Context, id string, state WorkItemState) error
	GetWorkItem(ctx context.Context, id string) (*WorkItem, error)
	StartRun(ctx context.Context, start StageRunStart) (int64, error)
	SetRunAgent(ctx context.Context, runID int64, sessionID string, pid int) error
	Heartbeat(ctx context.Context, runID int64, at time.Time) error
	MarkBlocked(ctx context.Context, runID int64, code BlockerCode, detail string) error
	EndRun(ctx context.Context, runID int64, state StageState, detail string) error
	ListRuns(ctx context.Context, workItemID string) ([]StageRun, error)
	ListRunning(ctx context.Context) ([]StageRun, error)
	StartBlockedRun(ctx context.Context, block StageRunBlock) (int64, error)
	LatestBlockedRun(ctx context.Context, workItemID string) (*StageRun, error)
	AttachInput(ctx context.Context, fallback StageRunBlock, in StageInput) (StageInput, bool, error)
	ListInputs(ctx context.Context, workItemID string) ([]StageInput, error)
	ListWaitingInputs(ctx context.Context, workItemID string) ([]StageInput, error)
	ListExpiredInputs(ctx context.Context, cutoff int64, limit int) ([]StageInput, error)
	SetInputState(ctx context.Context, inputID int64, state StageInputState) error
}

type sqliteStageRepo struct {
	db *sql.DB
}

func addStageGraph(_ *SQLiteStore, tx *sql.Tx) error {
	ddl := `
CREATE TABLE IF NOT EXISTS work_item (
    id             TEXT PRIMARY KEY,          -- <owner>/<repo>#<number>
    repo           TEXT NOT NULL,
    number         INTEGER NOT NULL,
    kind           TEXT NOT NULL CHECK (kind IN ('pull_request','issue')),
    spec_ref       TEXT NOT NULL DEFAULT '',
    spec_sha       TEXT NOT NULL DEFAULT '',
    state          TEXT NOT NULL CHECK (state IN ('open','done','abandoned')),
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS stage_run (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    work_item_id       TEXT    NOT NULL,
    stage              TEXT    NOT NULL,
    attempt            INTEGER NOT NULL,
    state              TEXT    NOT NULL CHECK (state IN
                         ('pending','running','waiting_human','waiting_external',
                          'blocked','done','failed','skipped','cancelled')),
    blocker_code       TEXT    NOT NULL DEFAULT '',
    blocker_detail     TEXT    NOT NULL DEFAULT '',
    started_at         INTEGER NOT NULL DEFAULT 0,
    last_heartbeat_at  INTEGER NOT NULL DEFAULT 0,
    ended_at           INTEGER NOT NULL DEFAULT 0,
    agent_session_id   TEXT    NOT NULL DEFAULT '',
    agent_pid          INTEGER NOT NULL DEFAULT 0,
    workspace_path     TEXT    NOT NULL DEFAULT '',
    artifact_ref       TEXT    NOT NULL DEFAULT '',
    gate_result        TEXT    NOT NULL DEFAULT '',
    triggered_by       TEXT    NOT NULL DEFAULT '',
    auto_retry_used    INTEGER NOT NULL DEFAULT 0,
    manual_retry_count INTEGER NOT NULL DEFAULT 0,
    created_at         INTEGER NOT NULL,
    UNIQUE (work_item_id, stage, attempt)
);

CREATE TABLE IF NOT EXISTS stage_input (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    stage_run_id INTEGER NOT NULL,
    delivery_id  TEXT    NOT NULL,
    endpoint     TEXT    NOT NULL,
    state        TEXT    NOT NULL CHECK (state IN ('waiting','consumed','superseded','expired')),
    received_at  INTEGER NOT NULL,
    consumed_at  INTEGER NOT NULL DEFAULT 0,
    UNIQUE (stage_run_id, endpoint, delivery_id)
);

CREATE TABLE IF NOT EXISTS action_request (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    work_item_id    TEXT    NOT NULL,
    stage_run_id    INTEGER NOT NULL,
    action          TEXT    NOT NULL,
    actor           TEXT    NOT NULL,
    reason          TEXT    NOT NULL DEFAULT '',
    idempotency_key TEXT    NOT NULL UNIQUE,
    result          TEXT    NOT NULL DEFAULT '',
    created_at      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stage_run_item ON stage_run (work_item_id, stage, attempt);
CREATE INDEX IF NOT EXISTS idx_stage_run_open ON stage_run (state, last_heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_stage_input_open ON stage_input (stage_run_id, state, received_at);
CREATE INDEX IF NOT EXISTS idx_action_request_item ON action_request (work_item_id, created_at);
`
	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("store: migrate stage graph: %w", err)
	}
	return nil
}

const stageRunColumns = `id, work_item_id, stage, attempt, state, blocker_code, blocker_detail,
	started_at, last_heartbeat_at, ended_at, agent_session_id, agent_pid, workspace_path,
	artifact_ref, gate_result, triggered_by, auto_retry_used, manual_retry_count, created_at`

func (r *sqliteStageRepo) UpsertWorkItem(ctx context.Context, item WorkItem) error {
	kind := item.Kind
	if kind == "" {
		kind = WorkItemPullRequest
	}
	now := time.Now().Unix()
	if _, err := r.db.ExecContext(ctx,
		`INSERT INTO work_item (id, repo, number, kind, spec_ref, spec_sha, state, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (id) DO UPDATE SET state = excluded.state, updated_at = excluded.updated_at`,
		item.ID, item.Repo, item.Number, string(kind), item.SpecRef, item.SpecSHA, string(WorkItemOpen), now, now,
	); err != nil {
		return fmt.Errorf("store: work item upsert: %w", err)
	}
	return nil
}

func (r *sqliteStageRepo) SetWorkItemState(ctx context.Context, id string, state WorkItemState) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE work_item SET state = ?, updated_at = ? WHERE id = ?`,
		string(state), time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("store: work item state: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: work item state rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("store: work item state %s: %w", id, ErrNotFound)
	}
	return nil
}

func (r *sqliteStageRepo) GetWorkItem(ctx context.Context, id string) (*WorkItem, error) {
	var w WorkItem
	var kind, state string
	err := r.db.QueryRowContext(ctx,
		`SELECT id, repo, number, kind, spec_ref, spec_sha, state, created_at, updated_at
		 FROM work_item WHERE id = ?`, id,
	).Scan(&w.ID, &w.Repo, &w.Number, &kind, &w.SpecRef, &w.SpecSHA, &state, &w.CreatedAt, &w.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("store: work item %s: %w", id, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("store: work item get: %w", err)
	}
	w.Kind, w.State = WorkItemKind(kind), WorkItemState(state)
	return &w, nil
}

// StartRun numbers the attempt inside the insert transaction, so two starts
// of the same stage can never claim the same attempt.
func (r *sqliteStageRepo) StartRun(ctx context.Context, start StageRunStart) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: stage run begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	attempt, err := nextAttempt(ctx, tx, start.WorkItemID, start.Stage)
	if err != nil {
		return 0, err
	}
	now := time.Now().Unix()
	res, err := tx.ExecContext(ctx,
		`INSERT INTO stage_run (work_item_id, stage, attempt, state, started_at, last_heartbeat_at, workspace_path, triggered_by, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		start.WorkItemID, string(start.Stage), attempt, string(StageStateRunning), now, now, start.WorkspacePath, start.TriggeredBy, now,
	)
	if err != nil {
		return 0, fmt.Errorf("store: stage run insert: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("store: stage run id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: stage run commit: %w", err)
	}
	return id, nil
}

func (r *sqliteStageRepo) SetRunAgent(ctx context.Context, runID int64, sessionID string, pid int) error {
	return r.updateRunning(ctx, "agent", runID, `agent_session_id = ?, agent_pid = ?`, sessionID, pid)
}

func (r *sqliteStageRepo) Heartbeat(ctx context.Context, runID int64, at time.Time) error {
	return r.updateRunning(ctx, "heartbeat", runID, `last_heartbeat_at = ?`, at.Unix())
}

func (r *sqliteStageRepo) MarkBlocked(ctx context.Context, runID int64, code BlockerCode, detail string) error {
	return r.updateRunning(ctx, "block", runID, `state = ?, blocker_code = ?, blocker_detail = ?`,
		string(StageStateBlocked), string(code), detail)
}

func (r *sqliteStageRepo) updateRunning(ctx context.Context, op string, runID int64, set string, setArgs ...any) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: stage run %s begin: %w", op, err)
	}
	defer func() { _ = tx.Rollback() }()
	args := append(setArgs, runID, string(StageStateRunning))
	res, err := tx.ExecContext(ctx, `UPDATE stage_run SET `+set+` WHERE id = ? AND state = ?`, args...)
	if err != nil {
		return fmt.Errorf("store: stage run %s: %w", op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: stage run %s rows: %w", op, err)
	}
	if n == 0 {
		if _, err := stageRunState(ctx, tx, runID); err != nil {
			return fmt.Errorf("store: stage run %s %d: %w", op, runID, err)
		}
		return fmt.Errorf("store: stage run %s %d: %w", op, runID, ErrStageRunNotRunning)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: stage run %s commit: %w", op, err)
	}
	return nil
}

// EndRun writes the one terminal state of a run. A run already terminal is
// left untouched and reported, so a second outcome never rewrites history.
func (r *sqliteStageRepo) EndRun(ctx context.Context, runID int64, state StageState, detail string) error {
	if !state.Terminal() {
		return fmt.Errorf("store: stage run end %q: %w", state, ErrStageStateInvalid)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: stage run end begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, err := stageRunState(ctx, tx, runID)
	if err != nil {
		return fmt.Errorf("store: stage run end: %w", err)
	}
	if current.Terminal() {
		return fmt.Errorf("store: stage run end %d: %w", runID, ErrTerminalAlreadySet)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE stage_run SET state = ?, blocker_detail = ?, ended_at = ? WHERE id = ?`,
		string(state), detail, time.Now().Unix(), runID,
	); err != nil {
		return fmt.Errorf("store: stage run end: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: stage run end commit: %w", err)
	}
	return nil
}

func stageRunState(ctx context.Context, tx *sql.Tx, runID int64) (StageState, error) {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT state FROM stage_run WHERE id = ?`, runID).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return StageState(state), nil
}

func (r *sqliteStageRepo) ListRuns(ctx context.Context, workItemID string) ([]StageRun, error) {
	return r.listRuns(ctx, `SELECT `+stageRunColumns+` FROM stage_run WHERE work_item_id = ? ORDER BY id`, workItemID)
}

func (r *sqliteStageRepo) ListRunning(ctx context.Context) ([]StageRun, error) {
	return r.listRuns(ctx, `SELECT `+stageRunColumns+` FROM stage_run WHERE state = ? ORDER BY id`, string(StageStateRunning))
}

func (r *sqliteStageRepo) listRuns(ctx context.Context, query string, args ...any) ([]StageRun, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: stage run list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []StageRun
	for rows.Next() {
		var s StageRun
		var stage, state, code string
		if err := rows.Scan(&s.ID, &s.WorkItemID, &stage, &s.Attempt, &state, &code, &s.BlockerDetail,
			&s.StartedAt, &s.LastHeartbeatAt, &s.EndedAt, &s.AgentSessionID, &s.AgentPID, &s.WorkspacePath,
			&s.ArtifactRef, &s.GateResult, &s.TriggeredBy, &s.AutoRetryUsed, &s.ManualRetryCount, &s.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: stage run scan: %w", err)
		}
		s.Stage, s.State, s.BlockerCode = Stage(stage), StageState(state), BlockerCode(code)
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: stage run rows: %w", err)
	}
	return out, nil
}
