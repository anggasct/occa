package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const recoveryEventRetention = 30 * 24 * time.Hour

type sqliteRecoveryEventRepo struct {
	db *sql.DB
}

func (r *sqliteRecoveryEventRepo) Put(ctx context.Context, e RecoveryEvent) error {
	if e.CreatedAt == 0 {
		e.CreatedAt = time.Now().Unix()
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: recovery event begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO recovery_event (platform, channel_id, thread_id, user_id, workdir, trigger_kind, outcome, correlation_id, detail, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.Platform, e.ChannelID, e.ThreadID, e.UserID, e.Workdir, string(e.Trigger), string(e.Outcome), e.CorrelationID, e.Detail, e.CreatedAt,
	); err != nil {
		return fmt.Errorf("store: recovery event insert: %w", err)
	}

	cutoff := time.Now().Add(-recoveryEventRetention).Unix()
	if _, err := tx.ExecContext(ctx, `DELETE FROM recovery_event WHERE created_at < ?`, cutoff); err != nil {
		return fmt.Errorf("store: recovery event prune: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: recovery event commit: %w", err)
	}
	return nil
}

func (r *sqliteRecoveryEventRepo) List(ctx context.Context, limit int) ([]RecoveryEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, platform, channel_id, thread_id, user_id, workdir, trigger_kind, outcome, correlation_id, detail, created_at
		 FROM recovery_event ORDER BY created_at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: recovery event list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var events []RecoveryEvent
	for rows.Next() {
		var e RecoveryEvent
		var trigger, outcome string
		if err := rows.Scan(&e.ID, &e.Platform, &e.ChannelID, &e.ThreadID, &e.UserID, &e.Workdir, &trigger, &outcome, &e.CorrelationID, &e.Detail, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: recovery event scan: %w", err)
		}
		e.Trigger = RecoveryTrigger(trigger)
		e.Outcome = RecoveryOutcome(outcome)
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: recovery event rows: %w", err)
	}
	return events, nil
}
