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
	var model, workdir sql.NullString
	var autoThread int
	err := r.db.QueryRowContext(ctx,
		`SELECT channel_id, platform, model, listen_mode, workdir, auto_thread, created_at, updated_at FROM channel WHERE platform = ? AND channel_id = ?`,
		platform, channelID,
	).Scan(&ch.ChannelID, &ch.Platform, &model, &ch.ListenMode, &workdir, &autoThread, &ch.CreatedAt, &ch.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: channel get: %w", err)
	}
	ch.Model, ch.Workdir = model.String, workdir.String
	ch.AutoThread = autoThread == 1
	return &ch, nil
}

func (r *sqliteChannelRepo) UpsertModel(ctx context.Context, platform, channelID, model string) error {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO channel (channel_id, platform, model, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (channel_id, platform) DO UPDATE SET model = excluded.model, updated_at = excluded.updated_at`,
		channelID, platform, model, now, now,
	)
	if err != nil {
		return fmt.Errorf("store: channel upsert model: %w", err)
	}
	return nil
}

func (r *sqliteChannelRepo) UpsertListenMode(ctx context.Context, platform, channelID, listenMode string) error {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO channel (channel_id, platform, listen_mode, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (channel_id, platform) DO UPDATE SET listen_mode = excluded.listen_mode, updated_at = excluded.updated_at`,
		channelID, platform, listenMode, now, now,
	)
	if err != nil {
		return fmt.Errorf("store: channel upsert listen mode: %w", err)
	}
	return nil
}

func (r *sqliteChannelRepo) UpsertWorkdir(ctx context.Context, platform, channelID, workdir string) error {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO channel (channel_id, platform, workdir, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (channel_id, platform) DO UPDATE SET workdir = excluded.workdir, updated_at = excluded.updated_at`,
		channelID, platform, workdir, now, now,
	)
	if err != nil {
		return fmt.Errorf("store: channel upsert workdir: %w", err)
	}
	return nil
}

var _ ChannelRepo = (*sqliteChannelRepo)(nil)
