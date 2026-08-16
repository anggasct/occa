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

func (r *sqliteSessionRepo) Active(ctx context.Context, platform, channelID, threadID, userID string) (string, int, error) {
	var sessionID string
	var agentPID int
	err := r.db.QueryRowContext(ctx,
		`SELECT agent_session_id, agent_pid FROM session WHERE platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ? AND active = 1`,
		platform, channelID, threadID, userID,
	).Scan(&sessionID, &agentPID)
	if err == sql.ErrNoRows {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, fmt.Errorf("store: session active: %w", err)
	}
	return sessionID, agentPID, nil
}

func (r *sqliteSessionRepo) SetActive(ctx context.Context, platform, channelID, threadID, userID, sessionID string, agentPID int) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: session set active: begin: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()

	if _, err := tx.ExecContext(ctx,
		`UPDATE session SET active = 0, updated_at = ? WHERE platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ? AND active = 1`,
		now, platform, channelID, threadID, userID,
	); err != nil {
		return fmt.Errorf("store: session set active: deactivate: %w", err)
	}

	res, err := tx.ExecContext(ctx,
		`UPDATE session SET active = 1, thread_id = ?, user_id = ?, agent_pid = ?, updated_at = ? WHERE platform = ? AND channel_id = ? AND agent_session_id = ?`,
		threadID, userID, agentPID, now, platform, channelID, sessionID,
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
			`INSERT INTO session (channel_id, platform, agent_session_id, thread_id, user_id, agent_pid, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)`,
			channelID, platform, sessionID, threadID, userID, agentPID, now, now,
		); err != nil {
			return fmt.Errorf("store: session set active: insert: %w", err)
		}
	}

	return tx.Commit()
}

func (r *sqliteSessionRepo) Deactivate(ctx context.Context, platform, channelID, threadID, userID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE session SET active = 0, updated_at = ? WHERE platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ? AND active = 1`,
		time.Now().Unix(), platform, channelID, threadID, userID,
	)
	if err != nil {
		return fmt.Errorf("store: session deactivate: %w", err)
	}
	return nil
}

func (r *sqliteSessionRepo) SetTitle(ctx context.Context, id int64, title string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE session SET title = ?, updated_at = ? WHERE id = ?`,
		title, time.Now().Unix(), id,
	)
	if err != nil {
		return fmt.Errorf("store: session set title: %w", err)
	}
	return nil
}

func (r *sqliteSessionRepo) SetModel(ctx context.Context, platform, channelID, threadID, userID, model string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE session SET model = ?, updated_at = ? WHERE platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ? AND active = 1`,
		model, time.Now().Unix(), platform, channelID, threadID, userID,
	)
	if err != nil {
		return fmt.Errorf("store: session set model: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: session set model: rows affected: %w", err)
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *sqliteSessionRepo) ActiveModel(ctx context.Context, platform, channelID, threadID, userID string) (string, error) {
	var model string
	err := r.db.QueryRowContext(ctx,
		`SELECT model FROM session WHERE platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ? AND active = 1`,
		platform, channelID, threadID, userID,
	).Scan(&model)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("store: session active model: %w", err)
	}
	return model, nil
}

func (r *sqliteSessionRepo) List(ctx context.Context, platform, channelID string) ([]Session, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, channel_id, platform, agent_session_id, thread_id, user_id, title, active, created_at, updated_at FROM session WHERE platform = ? AND channel_id = ? ORDER BY created_at DESC`,
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
		if err := rows.Scan(&s.ID, &s.ChannelID, &s.Platform, &s.AgentSessionID, &s.ThreadID, &s.UserID, &s.Title, &active, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("store: session list: scan: %w", err)
		}
		s.Active = active == 1
		sessions = append(sessions, s)
	}
	return sessions, rows.Err()
}

func (r *sqliteSessionRepo) ListConversation(ctx context.Context, platform, channelID, threadID, userID string) ([]Session, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, channel_id, platform, agent_session_id, thread_id, user_id, title, active, created_at, updated_at FROM session WHERE platform = ? AND channel_id = ? AND thread_id = ? AND user_id = ? ORDER BY created_at DESC`,
		platform, channelID, threadID, userID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: session list conversation: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var sessions []Session
	for rows.Next() {
		var s Session
		var active int
		if err := rows.Scan(&s.ID, &s.ChannelID, &s.Platform, &s.AgentSessionID, &s.ThreadID, &s.UserID, &s.Title, &active, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("store: session list conversation: scan: %w", err)
		}
		s.Active = active == 1
		sessions = append(sessions, s)
	}
	return sessions, rows.Err()
}

func (r *sqliteSessionRepo) ThreadChannel(ctx context.Context, platform, threadID string) (string, error) {
	var channelID string
	err := r.db.QueryRowContext(ctx,
		`SELECT channel_id FROM session WHERE platform = ? AND thread_id = ? ORDER BY created_at DESC LIMIT 1`,
		platform, threadID,
	).Scan(&channelID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("store: session thread channel: %w", err)
	}
	return channelID, nil
}

func (r *sqliteSessionRepo) Delete(ctx context.Context, id int64) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM session WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("store: session delete: %w", err)
	}
	return nil
}

var _ SessionRepo = (*sqliteSessionRepo)(nil)
