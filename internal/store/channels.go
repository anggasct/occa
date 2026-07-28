package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type sqliteChannelRepo struct {
	db *sql.DB
}

func (r *sqliteChannelRepo) Get(ctx context.Context, platform, channelID string) (*Channel, error) {
	var ch Channel
	err := r.db.QueryRowContext(ctx,
		`SELECT channel_id, platform, model, listen_mode, created_at, updated_at FROM channel WHERE platform = ? AND channel_id = ?`,
		platform, channelID,
	).Scan(&ch.ChannelID, &ch.Platform, &ch.Model, &ch.ListenMode, &ch.CreatedAt, &ch.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: channel get: %w", err)
	}
	return &ch, nil
}

func (r *sqliteChannelRepo) Upsert(ctx context.Context, ch *Channel) error {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO channel (channel_id, platform, model, listen_mode, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (channel_id, platform) DO UPDATE SET model = excluded.model, listen_mode = excluded.listen_mode, updated_at = excluded.updated_at`,
		ch.ChannelID, ch.Platform, ch.Model, ch.ListenMode, now, now,
	)
	if err != nil {
		return fmt.Errorf("store: channel upsert: %w", err)
	}
	return nil
}

var _ ChannelRepo = (*sqliteChannelRepo)(nil)
