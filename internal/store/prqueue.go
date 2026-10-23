package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type PRStatus string

const (
	PRStatusActive  PRStatus = "active"
	PRStatusBlocked PRStatus = "blocked"
	PRStatusDrained PRStatus = "drained"
)

type PRRun struct {
	PRKey          string
	ThreadID       string
	RootMsgID      string
	Status         PRStatus
	ActiveKind     string
	ActiveSession  string
	ActiveDelivery int64
	ActiveSince    int64
	RetryCount     int
	NextRetryAt    int64
	BlockedReason  string
	CreatedAt      int64
	UpdatedAt      int64
}

type PRQueueItem struct {
	ID         int64
	PRKey      string
	Endpoint   string
	EventType  string
	DeliveryID int64
	Priority   int
	EnqueuedAt int64
}

type PRQueueRepo interface {
	EnsurePR(ctx context.Context, prKey, threadID, rootMsgID string) (*PRRun, error)
	Get(ctx context.Context, prKey string) (*PRRun, error)
	Enqueue(ctx context.Context, prKey, endpoint, eventType string, deliveryID int64, priority int) (int64, error)
	DequeueNext(ctx context.Context, prKey string) (*PRQueueItem, error)
	SetActive(ctx context.Context, prKey, kind, sessionID string, deliveryID int64) error
	ClearActive(ctx context.Context, prKey string) error
	SetBlocked(ctx context.Context, prKey, reason string) error
	ListItems(ctx context.Context, prKey string) ([]PRQueueItem, error)
	PruneDrained(ctx context.Context, cutoff int64) (int, error)
	UpdateThread(ctx context.Context, prKey, threadID, rootMsgID string) error
	UpdateSession(ctx context.Context, prKey, sessionID string) error
	SetRetryState(ctx context.Context, prKey string, retryCount int, nextRetryAt int64) error
	SetResumePending(ctx context.Context, prKey, kind string, deliveryID int64) error
	Unblock(ctx context.Context, prKey string) error
	ListUnresolved(ctx context.Context) ([]PRRun, error)
	ClearQueue(ctx context.Context, prKey string) (int, error)
	DeleteItem(ctx context.Context, id int64) error
	RepairInvariants(ctx context.Context, prKey string) ([]string, error)
}

type sqlitePRQueueRepo struct {
	db *sql.DB
}

const prRunColumns = `pr_key, thread_id, root_msg_id, status, active_kind, active_session, active_delivery, active_since, retry_count, next_retry_at, blocked_reason, created_at, updated_at`

func scanPRRun(row interface{ Scan(...any) error }) (*PRRun, error) {
	var r PRRun
	var status, kind, session, reason string
	err := row.Scan(&r.PRKey, &r.ThreadID, &r.RootMsgID, &status, &kind, &session,
		&r.ActiveDelivery, &r.ActiveSince, &r.RetryCount, &r.NextRetryAt, &reason,
		&r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return nil, err
	}
	r.Status = PRStatus(status)
	r.ActiveKind = kind
	r.ActiveSession = session
	r.BlockedReason = reason
	return &r, nil
}

func (r *sqlitePRQueueRepo) EnsurePR(ctx context.Context, prKey, threadID, rootMsgID string) (*PRRun, error) {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO pr_queue (pr_key, thread_id, root_msg_id, status, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(pr_key) DO NOTHING`,
		prKey, threadID, rootMsgID, string(PRStatusActive), now, now,
	)
	if err != nil {
		return nil, fmt.Errorf("store: pr queue ensure: %w", err)
	}
	return r.Get(ctx, prKey)
}

func (r *sqlitePRQueueRepo) Get(ctx context.Context, prKey string) (*PRRun, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT `+prRunColumns+` FROM pr_queue WHERE pr_key = ?`, prKey)
	run, err := scanPRRun(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: pr queue get: %w", err)
	}
	return run, nil
}

func (r *sqlitePRQueueRepo) Enqueue(ctx context.Context, prKey, endpoint, eventType string, deliveryID int64, priority int) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: pr queue enqueue begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	now := time.Now().Unix()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO pr_queue (pr_key, thread_id, root_msg_id, status, created_at, updated_at)
		 VALUES (?, '', '', ?, ?, ?)
		 ON CONFLICT(pr_key) DO NOTHING`,
		prKey, string(PRStatusActive), now, now,
	); err != nil {
		return 0, fmt.Errorf("store: pr queue enqueue ensure: %w", err)
	}
	var id int64
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO pr_queue_items (pr_key, endpoint, event_type, delivery_id, priority, enqueued_at)
		 VALUES (?, ?, ?, ?, ?, ?) RETURNING id`,
		prKey, endpoint, eventType, deliveryID, priority, now,
	).Scan(&id); err != nil {
		return 0, fmt.Errorf("store: pr queue enqueue insert: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE pr_queue SET updated_at = ? WHERE pr_key = ?`, now, prKey); err != nil {
		return 0, fmt.Errorf("store: pr queue enqueue touch: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: pr queue enqueue commit: %w", err)
	}
	committed = true
	return id, nil
}

func (r *sqlitePRQueueRepo) DequeueNext(ctx context.Context, prKey string) (*PRQueueItem, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: pr queue dequeue begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	var item PRQueueItem
	err = tx.QueryRowContext(ctx,
		`SELECT id, pr_key, endpoint, event_type, delivery_id, priority, enqueued_at
		 FROM pr_queue_items WHERE pr_key = ? ORDER BY priority ASC, id ASC LIMIT 1`,
		prKey,
	).Scan(&item.ID, &item.PRKey, &item.Endpoint, &item.EventType, &item.DeliveryID, &item.Priority, &item.EnqueuedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: pr queue dequeue select: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pr_queue_items WHERE id = ?`, item.ID); err != nil {
		return nil, fmt.Errorf("store: pr queue dequeue delete: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: pr queue dequeue commit: %w", err)
	}
	committed = true
	return &item, nil
}

func (r *sqlitePRQueueRepo) SetActive(ctx context.Context, prKey, kind, sessionID string, deliveryID int64) error {
	now := time.Now().Unix()
	res, err := r.db.ExecContext(ctx,
		`UPDATE pr_queue
		 SET status = ?, active_kind = ?, active_session = ?, active_delivery = ?,
		     active_since = ?, blocked_reason = '', updated_at = ?
		 WHERE pr_key = ?`,
		string(PRStatusActive), kind, sessionID, deliveryID, now, now, prKey,
	)
	if err != nil {
		return fmt.Errorf("store: pr queue set active: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("store: pr queue set active rows: %w", err)
	} else if n == 0 {
		return fmt.Errorf("store: pr queue set active: unknown pr_key %q", prKey)
	}
	return nil
}

func (r *sqlitePRQueueRepo) ClearActive(ctx context.Context, prKey string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: pr queue clear begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	now := time.Now().Unix()
	var current string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM pr_queue WHERE pr_key = ?`, prKey).Scan(&current); err != nil {
		return fmt.Errorf("store: pr queue clear status read: %w", err)
	}
	if PRStatus(current) == PRStatusBlocked {
		if _, err := tx.ExecContext(ctx, `UPDATE pr_queue SET updated_at = ? WHERE pr_key = ?`, now, prKey); err != nil {
			return fmt.Errorf("store: pr queue clear blocked touch: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("store: pr queue clear commit: %w", err)
		}
		committed = true
		return nil
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE pr_queue
		 SET active_kind = '', active_session = '', active_delivery = 0,
		     active_since = 0, retry_count = 0, updated_at = ?
		 WHERE pr_key = ?`,
		now, prKey,
	); err != nil {
		return fmt.Errorf("store: pr queue clear update: %w", err)
	}
	var remaining int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pr_queue_items WHERE pr_key = ?`, prKey,
	).Scan(&remaining); err != nil {
		return fmt.Errorf("store: pr queue clear count: %w", err)
	}
	status := PRStatusActive
	if remaining == 0 {
		status = PRStatusDrained
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE pr_queue SET status = ?, updated_at = ? WHERE pr_key = ?`,
		string(status), now, prKey,
	); err != nil {
		return fmt.Errorf("store: pr queue clear status: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: pr queue clear commit: %w", err)
	}
	committed = true
	return nil
}

func (r *sqlitePRQueueRepo) SetBlocked(ctx context.Context, prKey, reason string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE pr_queue SET status = ?, blocked_reason = ?, updated_at = ? WHERE pr_key = ?`,
		string(PRStatusBlocked), reason, time.Now().Unix(), prKey,
	)
	if err != nil {
		return fmt.Errorf("store: pr queue set blocked: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("store: pr queue set blocked rows: %w", err)
	} else if n == 0 {
		return fmt.Errorf("store: pr queue set blocked: unknown pr_key %q", prKey)
	}
	return nil
}

func (r *sqlitePRQueueRepo) ListItems(ctx context.Context, prKey string) ([]PRQueueItem, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, pr_key, endpoint, event_type, delivery_id, priority, enqueued_at
		 FROM pr_queue_items WHERE pr_key = ? ORDER BY priority ASC, id ASC`,
		prKey,
	)
	if err != nil {
		return nil, fmt.Errorf("store: pr queue list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []PRQueueItem
	for rows.Next() {
		var item PRQueueItem
		if err := rows.Scan(&item.ID, &item.PRKey, &item.Endpoint, &item.EventType, &item.DeliveryID, &item.Priority, &item.EnqueuedAt); err != nil {
			return nil, fmt.Errorf("store: pr queue list scan: %w", err)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (r *sqlitePRQueueRepo) PruneDrained(ctx context.Context, cutoff int64) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: pr queue prune begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM pr_queue_items WHERE pr_key IN (
			SELECT pr_key FROM pr_queue WHERE status = ? AND updated_at < ?
		)`,
		string(PRStatusDrained), cutoff,
	); err != nil {
		return 0, fmt.Errorf("store: pr queue prune items: %w", err)
	}
	res, err := tx.ExecContext(ctx,
		`DELETE FROM pr_queue WHERE status = ? AND updated_at < ?`,
		string(PRStatusDrained), cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("store: pr queue prune: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: pr queue prune rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: pr queue prune commit: %w", err)
	}
	committed = true
	return int(n), nil
}

func (r *sqlitePRQueueRepo) UpdateThread(ctx context.Context, prKey, threadID, rootMsgID string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE pr_queue SET thread_id = ?, root_msg_id = ?, updated_at = ? WHERE pr_key = ?`,
		threadID, rootMsgID, time.Now().Unix(), prKey,
	)
	if err != nil {
		return fmt.Errorf("store: pr queue update thread: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("store: pr queue update thread rows: %w", err)
	} else if n == 0 {
		return fmt.Errorf("store: pr queue update thread: unknown pr_key %q", prKey)
	}
	return nil
}

func (r *sqlitePRQueueRepo) UpdateSession(ctx context.Context, prKey, sessionID string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE pr_queue SET active_session = ?, updated_at = ? WHERE pr_key = ?`,
		sessionID, time.Now().Unix(), prKey,
	)
	if err != nil {
		return fmt.Errorf("store: pr queue update session: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("store: pr queue update session rows: %w", err)
	} else if n == 0 {
		return fmt.Errorf("store: pr queue update session: unknown pr_key %q", prKey)
	}
	return nil
}

func (r *sqlitePRQueueRepo) SetRetryState(ctx context.Context, prKey string, retryCount int, nextRetryAt int64) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE pr_queue SET retry_count = ?, next_retry_at = ?, updated_at = ? WHERE pr_key = ?`,
		retryCount, nextRetryAt, time.Now().Unix(), prKey,
	)
	if err != nil {
		return fmt.Errorf("store: pr queue set retry state: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("store: pr queue set retry state rows: %w", err)
	} else if n == 0 {
		return fmt.Errorf("store: pr queue set retry state: unknown pr_key %q", prKey)
	}
	return nil
}

func (r *sqlitePRQueueRepo) SetResumePending(ctx context.Context, prKey, kind string, deliveryID int64) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE pr_queue
		 SET status = ?, active_kind = ?, active_session = '', active_delivery = ?,
		     active_since = 0, retry_count = 0, next_retry_at = 0, blocked_reason = ?, updated_at = ?
		 WHERE pr_key = ?`,
		string(PRStatusDrained), kind, deliveryID, "resume_pending", time.Now().Unix(), prKey,
	)
	if err != nil {
		return fmt.Errorf("store: pr queue set resume pending: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("store: pr queue set resume pending rows: %w", err)
	} else if n == 0 {
		return fmt.Errorf("store: pr queue set resume pending: unknown pr_key %q", prKey)
	}
	return nil
}

func (r *sqlitePRQueueRepo) Unblock(ctx context.Context, prKey string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE pr_queue SET status = ?, retry_count = 0, next_retry_at = 0, blocked_reason = '', updated_at = ? WHERE pr_key = ? AND status = ?`,
		string(PRStatusActive), time.Now().Unix(), prKey, string(PRStatusBlocked),
	)
	if err != nil {
		return fmt.Errorf("store: pr queue unblock: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("store: pr queue unblock rows: %w", err)
	} else if n == 0 {
		return fmt.Errorf("store: pr queue unblock: %q not blocked", prKey)
	}
	return nil
}

func (r *sqlitePRQueueRepo) DeleteItem(ctx context.Context, id int64) error {
	if _, err := r.db.ExecContext(ctx, `DELETE FROM pr_queue_items WHERE id = ?`, id); err != nil {
		return fmt.Errorf("store: pr queue delete item: %w", err)
	}
	return nil
}

func (r *sqlitePRQueueRepo) ListUnresolved(ctx context.Context) ([]PRRun, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+prRunColumns+` FROM pr_queue WHERE status != ? ORDER BY updated_at ASC`,
		string(PRStatusDrained),
	)
	if err != nil {
		return nil, fmt.Errorf("store: pr queue list unresolved: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []PRRun
	for rows.Next() {
		run, err := scanPRRun(rows)
		if err != nil {
			return nil, fmt.Errorf("store: pr queue list unresolved scan: %w", err)
		}
		out = append(out, *run)
	}
	return out, rows.Err()
}

func (r *sqlitePRQueueRepo) ClearQueue(ctx context.Context, prKey string) (int, error) {
	res, err := r.db.ExecContext(ctx, `DELETE FROM pr_queue_items WHERE pr_key = ?`, prKey)
	if err != nil {
		return 0, fmt.Errorf("store: pr queue clear queue: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: pr queue clear queue rows: %w", err)
	}
	return int(n), nil
}

var _ PRQueueRepo = (*sqlitePRQueueRepo)(nil)

// RepairInvariants fixes rows that claim an active run without carrying one.
// An active row is legitimate only while a run is stamped (kind, delivery,
// and start time) or queued items are waiting; a row with neither is a
// phantom that /retry cannot reach. An empty prKey sweeps every row; a
// non-empty prKey restricts the sweep to that PR.
func (r *sqlitePRQueueRepo) RepairInvariants(ctx context.Context, prKey string) ([]string, error) {
	query := `SELECT pr_key FROM pr_queue q
		 WHERE status = ?
		   AND (active_kind = '' OR active_delivery = 0 OR active_since = 0)
		   AND NOT EXISTS (SELECT 1 FROM pr_queue_items i WHERE i.pr_key = q.pr_key)`
	args := []any{string(PRStatusActive)}
	if strings.TrimSpace(prKey) != "" {
		query += ` AND q.pr_key = ?`
		args = append(args, strings.TrimSpace(prKey))
	}
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: pr queue invariant scan: %w", err)
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("store: pr queue invariant scan: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("store: pr queue invariant scan: %w", err)
	}
	_ = rows.Close()
	now := time.Now().Unix()
	for _, key := range keys {
		if _, err := r.db.ExecContext(ctx,
			`UPDATE pr_queue
			 SET status = ?, active_session = '', active_since = 0,
			     retry_count = 0, next_retry_at = 0, blocked_reason = ?, updated_at = ?
			 WHERE pr_key = ?`,
			string(PRStatusDrained), "resume_pending", now, key,
		); err != nil {
			return nil, fmt.Errorf("store: pr queue invariant repair: %w", err)
		}
	}
	return keys, nil
}
