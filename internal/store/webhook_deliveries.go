package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type sqliteWebhookDeliveryRepo struct {
	db *sql.DB
}

const deliveryColumns = `id, endpoint, delivery_id, event_type, payload_hash, status, attempt, error_summary, created_at, updated_at, started_at, completed_at`

func scanWebhookDelivery(row interface{ Scan(...any) error }) (*WebhookDelivery, error) {
	var d WebhookDelivery
	var status string
	err := row.Scan(&d.ID, &d.Endpoint, &d.DeliveryID, &d.EventType, &d.PayloadHash, &status, &d.Attempt, &d.ErrorSummary, &d.CreatedAt, &d.UpdatedAt, &d.StartedAt, &d.CompletedAt)
	if err != nil {
		return nil, err
	}
	d.Status = WebhookStatus(status)
	return &d, nil
}

func (r *sqliteWebhookDeliveryRepo) Create(ctx context.Context, d WebhookDelivery) (bool, error) {
	if d.CreatedAt == 0 {
		d.CreatedAt = time.Now().Unix()
	}
	if d.UpdatedAt == 0 {
		d.UpdatedAt = d.CreatedAt
	}
	if d.Status == "" {
		d.Status = WebhookStatusReceived
	}
	if d.Attempt < 1 {
		d.Attempt = 1
	}
	err := r.db.QueryRowContext(ctx,
		`INSERT INTO webhook_delivery (endpoint, delivery_id, event_type, payload_hash, status, attempt, error_summary, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(endpoint, delivery_id) DO NOTHING
		 RETURNING id`,
		d.Endpoint, d.DeliveryID, d.EventType, d.PayloadHash, string(d.Status), d.Attempt, d.ErrorSummary, d.CreatedAt, d.UpdatedAt,
	).Scan(&d.ID)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := r.db.ExecContext(ctx,
			`UPDATE webhook_delivery SET attempt = attempt + 1 WHERE endpoint = ? AND delivery_id = ?`,
			d.Endpoint, d.DeliveryID,
		); err != nil {
			return false, fmt.Errorf("store: webhook delivery bump attempt: %w", err)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: webhook delivery create: %w", err)
	}
	return true, nil
}

func (r *sqliteWebhookDeliveryRepo) Get(ctx context.Context, endpoint, deliveryID string) (*WebhookDelivery, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT `+deliveryColumns+` FROM webhook_delivery WHERE endpoint = ? AND delivery_id = ?`,
		endpoint, deliveryID,
	)
	d, err := scanWebhookDelivery(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: webhook delivery get: %w", err)
	}
	return d, nil
}

func (r *sqliteWebhookDeliveryRepo) Transition(ctx context.Context, id int64, from []WebhookStatus, to WebhookStatus, summary string) (bool, error) {
	if len(from) == 0 {
		return false, fmt.Errorf("store: webhook delivery transition: empty from set")
	}
	now := time.Now().Unix()
	query := `UPDATE webhook_delivery SET status = ?, error_summary = ?, updated_at = ?`
	args := []any{string(to), summary, now}
	switch to {
	case WebhookStatusAccepted, WebhookStatusProcessing:
		query += `, started_at = ?`
		args = append(args, now)
	case WebhookStatusCompleted, WebhookStatusSkipped, WebhookStatusFailed:
		query += `, completed_at = ?`
		args = append(args, now)
	}
	query += ` WHERE id = ? AND status IN (` + strings.TrimSuffix(strings.Repeat("?,", len(from)), ",") + `)`
	args = append(args, id)
	for _, s := range from {
		args = append(args, string(s))
	}
	res, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("store: webhook delivery transition: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: webhook delivery transition rows: %w", err)
	}
	return n > 0, nil
}

// BumpAttempt increments the attempt counter of an in-flight delivery. It is
// used by the dispatcher's self-heal path so a re-executed delivery is
// observable as attempt 2 in the receipt. The update only succeeds while the
// delivery is still processing, so a delivery that raced to a terminal state
// in between keeps its final attempt count.
func (r *sqliteWebhookDeliveryRepo) BumpAttempt(ctx context.Context, id int64) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE webhook_delivery SET attempt = attempt + 1, updated_at = ? WHERE id = ? AND status = ?`,
		time.Now().Unix(), id, string(WebhookStatusProcessing),
	)
	if err != nil {
		return fmt.Errorf("store: webhook delivery bump attempt: %w", err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return fmt.Errorf("store: webhook delivery bump attempt: delivery %d not in processing", id)
	}
	return nil
}

func (r *sqliteWebhookDeliveryRepo) ClaimStale(ctx context.Context, id, cutoff int64) (bool, error) {
	now := time.Now().Unix()
	res, err := r.db.ExecContext(ctx,
		`UPDATE webhook_delivery
		 SET status = ?, started_at = ?, updated_at = ?
		 WHERE id = ? AND status IN (?, ?, ?) AND updated_at <= ?`,
		string(WebhookStatusProcessing), now, now, id,
		string(WebhookStatusReceived), string(WebhookStatusAccepted), string(WebhookStatusProcessing), cutoff,
	)
	if err != nil {
		return false, fmt.Errorf("store: webhook delivery stale claim: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: webhook delivery stale claim rows: %w", err)
	}
	return n > 0, nil
}

func (r *sqliteWebhookDeliveryRepo) List(ctx context.Context, limit int) ([]WebhookDelivery, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+deliveryColumns+` FROM webhook_delivery ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: webhook delivery list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []WebhookDelivery
	for rows.Next() {
		d, err := scanWebhookDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("store: webhook delivery list scan: %w", err)
		}
		out = append(out, *d)
	}
	return out, rows.Err()
}

func (r *sqliteWebhookDeliveryRepo) Prune(ctx context.Context, cutoff int64, keep int) (int, error) {
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM webhook_delivery
		 WHERE created_at < ? OR id NOT IN (
			SELECT id FROM webhook_delivery WHERE created_at >= ? ORDER BY id DESC LIMIT ?
		)`,
		cutoff, cutoff, keep,
	)
	if err != nil {
		return 0, fmt.Errorf("store: webhook delivery prune: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: webhook delivery prune rows: %w", err)
	}
	return int(n), nil
}

func (r *sqliteWebhookDeliveryRepo) FailStale(ctx context.Context, cutoff int64, summary string) (int, error) {
	now := time.Now().Unix()
	res, err := r.db.ExecContext(ctx,
		`UPDATE webhook_delivery
		 SET status = ?, error_summary = ?, completed_at = ?, updated_at = ?
		 WHERE status IN (?, ?, ?) AND updated_at <= ?`,
		string(WebhookStatusFailed), summary, now, now,
		string(WebhookStatusReceived), string(WebhookStatusAccepted), string(WebhookStatusProcessing), cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("store: webhook delivery fail stale: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: webhook delivery fail stale rows: %w", err)
	}
	return int(n), nil
}

var _ WebhookDeliveryRepo = (*sqliteWebhookDeliveryRepo)(nil)
