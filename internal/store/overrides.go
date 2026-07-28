package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type sqliteOverrideRepo struct {
	db *sql.DB
}

func (r *sqliteOverrideRepo) Get(ctx context.Context, channelID, userID string) (*UserOverride, error) {
	var o UserOverride
	err := r.db.QueryRowContext(ctx,
		`SELECT id, channel_id, user_id, role, model, created_at, updated_at FROM user_override WHERE channel_id = ? AND user_id = ?`,
		channelID, userID,
	).Scan(&o.ID, &o.ChannelID, &o.UserID, &o.Role, &o.Model, &o.CreatedAt, &o.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: override get: %w", err)
	}
	return &o, nil
}

func (r *sqliteOverrideRepo) Upsert(ctx context.Context, o *UserOverride) error {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO user_override (channel_id, user_id, role, model, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (channel_id, user_id) DO UPDATE SET role = excluded.role, model = excluded.model, updated_at = excluded.updated_at`,
		o.ChannelID, o.UserID, o.Role, o.Model, now, now,
	)
	if err != nil {
		return fmt.Errorf("store: override upsert: %w", err)
	}
	return nil
}

func (r *sqliteOverrideRepo) Delete(ctx context.Context, channelID, userID string) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM user_override WHERE channel_id = ? AND user_id = ?`,
		channelID, userID,
	)
	if err != nil {
		return fmt.Errorf("store: override delete: %w", err)
	}
	return nil
}

func (r *sqliteOverrideRepo) ListByChannel(ctx context.Context, channelID string) ([]UserOverride, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, channel_id, user_id, role, model, created_at, updated_at FROM user_override WHERE channel_id = ? ORDER BY created_at`,
		channelID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: override list: %w", err)
	}
	defer rows.Close()

	var overrides []UserOverride
	for rows.Next() {
		var o UserOverride
		if err := rows.Scan(&o.ID, &o.ChannelID, &o.UserID, &o.Role, &o.Model, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, fmt.Errorf("store: override list: scan: %w", err)
		}
		overrides = append(overrides, o)
	}
	return overrides, rows.Err()
}

var _ OverrideRepo = (*sqliteOverrideRepo)(nil)
