package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type WebhookReplayStatus string

const (
	WebhookReplayPending    WebhookReplayStatus = "pending"
	WebhookReplayReplayed   WebhookReplayStatus = "replayed"
	WebhookReplaySuperseded WebhookReplayStatus = "superseded"
	WebhookReplayExpired    WebhookReplayStatus = "expired"
)

type WebhookReplay struct {
	ID            int64
	Endpoint      string
	DeliveryID    string
	EventType     string
	PRKey         string
	Body          []byte
	Reason        string
	Attempts      int
	NextAttemptAt int64
	ExpiresAt     int64
	Status        WebhookReplayStatus
	CreatedAt     int64
	UpdatedAt     int64
}

type WebhookReplayRepo interface {
	// Create stores a replay row. It reports whether the row was inserted; a
	// duplicate (endpoint, delivery_id) is not inserted again.
	Create(ctx context.Context, r WebhookReplay) (int64, bool, error)
	ListDue(ctx context.Context, now int64, limit int) ([]WebhookReplay, error)
	Mark(ctx context.Context, id int64, status WebhookReplayStatus, nextAttemptAt int64) error
	Count(ctx context.Context, status WebhookReplayStatus) (int, error)
	Prune(ctx context.Context, maxRows int, maxBytes int64) (int, error)
}

type sqliteWebhookReplayRepo struct {
	db *sql.DB
}

const replayColumns = `id, endpoint, delivery_id, event_type, pr_key, body, reason, attempts, next_attempt_at, expires_at, status, created_at, updated_at`

func scanWebhookReplay(row interface{ Scan(...any) error }) (*WebhookReplay, error) {
	var r WebhookReplay
	var status string
	err := row.Scan(&r.ID, &r.Endpoint, &r.DeliveryID, &r.EventType, &r.PRKey, &r.Body, &r.Reason,
		&r.Attempts, &r.NextAttemptAt, &r.ExpiresAt, &status, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return nil, err
	}
	r.Status = WebhookReplayStatus(status)
	return &r, nil
}

func (r *sqliteWebhookReplayRepo) Create(ctx context.Context, replay WebhookReplay) (int64, bool, error) {
	if replay.CreatedAt == 0 {
		replay.CreatedAt = time.Now().Unix()
	}
	if replay.UpdatedAt == 0 {
		replay.UpdatedAt = replay.CreatedAt
	}
	if replay.Status == "" {
		replay.Status = WebhookReplayPending
	}
	if replay.NextAttemptAt == 0 {
		replay.NextAttemptAt = replay.CreatedAt
	}
	var id int64
	err := r.db.QueryRowContext(ctx,
		`INSERT INTO webhook_replay (endpoint, delivery_id, event_type, pr_key, body, reason, attempts, next_attempt_at, expires_at, status, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(endpoint, delivery_id) DO NOTHING
		 RETURNING id`,
		replay.Endpoint, replay.DeliveryID, replay.EventType, replay.PRKey, replay.Body, replay.Reason,
		replay.Attempts, replay.NextAttemptAt, replay.ExpiresAt, string(replay.Status), replay.CreatedAt, replay.UpdatedAt,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("store: webhook replay create: %w", err)
	}
	return id, true, nil
}

func (r *sqliteWebhookReplayRepo) ListDue(ctx context.Context, now int64, limit int) ([]WebhookReplay, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+replayColumns+` FROM webhook_replay
		 WHERE status = ? AND next_attempt_at <= ?
		 ORDER BY next_attempt_at ASC, id ASC LIMIT ?`,
		string(WebhookReplayPending), now, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: webhook replay list due: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []WebhookReplay
	for rows.Next() {
		replay, err := scanWebhookReplay(rows)
		if err != nil {
			return nil, fmt.Errorf("store: webhook replay list due scan: %w", err)
		}
		out = append(out, *replay)
	}
	return out, rows.Err()
}

func (r *sqliteWebhookReplayRepo) Mark(ctx context.Context, id int64, status WebhookReplayStatus, nextAttemptAt int64) error {
	if _, err := r.db.ExecContext(ctx,
		`UPDATE webhook_replay SET status = ?, next_attempt_at = ?, attempts = attempts + 1, updated_at = ? WHERE id = ?`,
		string(status), nextAttemptAt, time.Now().Unix(), id,
	); err != nil {
		return fmt.Errorf("store: webhook replay mark: %w", err)
	}
	return nil
}

func (r *sqliteWebhookReplayRepo) Count(ctx context.Context, status WebhookReplayStatus) (int, error) {
	var count int
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM webhook_replay WHERE status = ?`, string(status),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("store: webhook replay count: %w", err)
	}
	return count, nil
}

// Prune enforces both retention bounds. Rows are removed oldest first, never
// by status, so a pending row can be dropped only when the store is over its
// bound — the alternative is unbounded disk growth.
func (r *sqliteWebhookReplayRepo) Prune(ctx context.Context, maxRows int, maxBytes int64) (int, error) {
	if maxRows <= 0 {
		maxRows = 200
	}
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: webhook replay prune begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	removed := 0
	res, err := tx.ExecContext(ctx,
		`DELETE FROM webhook_replay WHERE id NOT IN (
			SELECT id FROM webhook_replay ORDER BY id DESC LIMIT ?
		)`, maxRows,
	)
	if err != nil {
		return 0, fmt.Errorf("store: webhook replay prune rows: %w", err)
	}
	if n, err := res.RowsAffected(); err == nil {
		removed += int(n)
	}
	res, err = tx.ExecContext(ctx,
		`DELETE FROM webhook_replay WHERE id IN (
			SELECT id FROM (
				SELECT id, SUM(LENGTH(body)) OVER (ORDER BY id DESC) AS cumulative
				FROM webhook_replay
			) WHERE cumulative > ?
		)`, maxBytes,
	)
	if err != nil {
		return 0, fmt.Errorf("store: webhook replay prune bytes: %w", err)
	}
	if n, err := res.RowsAffected(); err == nil {
		removed += int(n)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: webhook replay prune commit: %w", err)
	}
	committed = true
	return removed, nil
}

var _ WebhookReplayRepo = (*sqliteWebhookReplayRepo)(nil)
