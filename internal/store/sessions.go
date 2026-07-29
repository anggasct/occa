package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type sqliteSessionRepo struct {
	db *sql.DB
}

func (r *sqliteSessionRepo) Active(ctx context.Context, platform, channelID string) (string, error) {
	var sessionID string
	err := r.db.QueryRowContext(ctx,
		`SELECT opencode_session_id FROM session WHERE platform = ? AND channel_id = ? AND active = 1`,
		platform, channelID,
	).Scan(&sessionID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("store: session active: %w", err)
	}
	return sessionID, nil
}

func (r *sqliteSessionRepo) SetActive(ctx context.Context, platform, channelID, sessionID string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: session set active: begin: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()

	if _, err := tx.ExecContext(ctx,
		`UPDATE session SET active = 0, updated_at = ? WHERE platform = ? AND channel_id = ? AND active = 1`,
		now, platform, channelID,
	); err != nil {
		return fmt.Errorf("store: session set active: deactivate: %w", err)
	}

	res, err := tx.ExecContext(ctx,
		`UPDATE session SET active = 1, updated_at = ? WHERE platform = ? AND channel_id = ? AND opencode_session_id = ?`,
		now, platform, channelID, sessionID,
	)
	if err != nil {
		return fmt.Errorf("store: session set active: activate: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: session set active: rows affected: %w", err)
	}
	if rows == 0 {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO session (channel_id, platform, opencode_session_id, active, created_at, updated_at) VALUES (?, ?, ?, 1, ?, ?)`,
			channelID, platform, sessionID, now, now,
		); err != nil {
			return fmt.Errorf("store: session set active: insert: %w", err)
		}
	}

	return tx.Commit()
}

func (r *sqliteSessionRepo) List(ctx context.Context, platform, channelID string) ([]Session, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, channel_id, platform, opencode_session_id, active, created_at, updated_at FROM session WHERE platform = ? AND channel_id = ? ORDER BY created_at DESC`,
		platform, channelID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: session list: %w", err)
	}
	defer rows.Close()

	var sessions []Session
	for rows.Next() {
		var s Session
		var active int
		if err := rows.Scan(&s.ID, &s.ChannelID, &s.Platform, &s.OpenCodeSessionID, &active, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("store: session list: scan: %w", err)
		}
		s.Active = active == 1
		sessions = append(sessions, s)
	}
	return sessions, rows.Err()
}

func (r *sqliteSessionRepo) Delete(ctx context.Context, id int64) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM session WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("store: session delete: %w", err)
	}
	return nil
}

var _ SessionRepo = (*sqliteSessionRepo)(nil)
