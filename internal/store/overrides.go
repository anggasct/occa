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

func (r *sqliteOverrideRepo) Get(ctx context.Context, platform, channelID, userID string) (*UserOverride, error) {
	var o UserOverride
	var model sql.NullString
	err := r.db.QueryRowContext(ctx,
		`SELECT id, channel_id, platform, user_id, role, model, created_at, updated_at FROM user_override WHERE platform = ? AND channel_id = ? AND user_id = ?`,
		platform, channelID, userID,
	).Scan(&o.ID, &o.ChannelID, &o.Platform, &o.UserID, &o.Role, &model, &o.CreatedAt, &o.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: override get: %w", err)
	}
	o.Model = model.String
	return &o, nil
}

// UpsertRole writes only the role column. On first touch for a (platform, channel, user)
// tuple, model starts unset (NULL); on conflict, the existing model is left untouched.
func (r *sqliteOverrideRepo) UpsertRole(ctx context.Context, platform, channelID, userID, role string) error {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO user_override (channel_id, platform, user_id, role, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (platform, channel_id, user_id) DO UPDATE SET role = excluded.role, updated_at = excluded.updated_at`,
		channelID, platform, userID, role, now, now,
	)
	if err != nil {
		return fmt.Errorf("store: override upsert role: %w", err)
	}
	return nil
}

// UpsertModel writes only the model column. A row created here (no prior role write)
// defaults role to "deny" — setting a model preference must never itself grant access.
func (r *sqliteOverrideRepo) UpsertModel(ctx context.Context, platform, channelID, userID, model string) error {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO user_override (channel_id, platform, user_id, role, model, created_at, updated_at)
		 VALUES (?, ?, ?, 'deny', ?, ?, ?)
		 ON CONFLICT (platform, channel_id, user_id) DO UPDATE SET model = excluded.model, updated_at = excluded.updated_at`,
		channelID, platform, userID, model, now, now,
	)
	if err != nil {
		return fmt.Errorf("store: override upsert model: %w", err)
	}
	return nil
}

func (r *sqliteOverrideRepo) Delete(ctx context.Context, platform, channelID, userID string) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM user_override WHERE platform = ? AND channel_id = ? AND user_id = ?`,
		platform, channelID, userID,
	)
	if err != nil {
		return fmt.Errorf("store: override delete: %w", err)
	}
	return nil
}

func (r *sqliteOverrideRepo) ListByChannel(ctx context.Context, platform, channelID string) ([]UserOverride, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, channel_id, platform, user_id, role, model, created_at, updated_at FROM user_override WHERE platform = ? AND channel_id = ? ORDER BY created_at`,
		platform, channelID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: override list: %w", err)
	}
	defer rows.Close()

	var overrides []UserOverride
	for rows.Next() {
		var o UserOverride
		var model sql.NullString
		if err := rows.Scan(&o.ID, &o.ChannelID, &o.Platform, &o.UserID, &o.Role, &model, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, fmt.Errorf("store: override list: scan: %w", err)
		}
		o.Model = model.String
		overrides = append(overrides, o)
	}
	return overrides, rows.Err()
}

var _ OverrideRepo = (*sqliteOverrideRepo)(nil)
