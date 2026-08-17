package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type sqliteThreadConfigRepo struct {
	db *sql.DB
}

func (r *sqliteThreadConfigRepo) Get(ctx context.Context, platform, channelID, threadID string) (*ThreadConfig, error) {
	var tc ThreadConfig
	err := r.db.QueryRowContext(ctx,
		`SELECT id, platform, channel_id, thread_id, workdir, model, listen_mode, created_at, updated_at
		   FROM thread_config
		  WHERE platform = ? AND channel_id = ? AND thread_id = ?`,
		platform, channelID, threadID,
	).Scan(&tc.ID, &tc.Platform, &tc.ChannelID, &tc.ThreadID, &tc.Workdir, &tc.Model, &tc.ListenMode, &tc.CreatedAt, &tc.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: thread config get: %w", err)
	}
	return &tc, nil
}

func (r *sqliteThreadConfigRepo) UpsertWorkdir(ctx context.Context, platform, channelID, threadID, workdir string) error {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO thread_config (platform, channel_id, thread_id, workdir, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (platform, channel_id, thread_id) DO UPDATE SET workdir = excluded.workdir, updated_at = excluded.updated_at`,
		platform, channelID, threadID, workdir, now, now,
	)
	if err != nil {
		return fmt.Errorf("store: thread config upsert workdir: %w", err)
	}
	return nil
}

func (r *sqliteThreadConfigRepo) UpsertModel(ctx context.Context, platform, channelID, threadID, model string) error {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO thread_config (platform, channel_id, thread_id, model, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (platform, channel_id, thread_id) DO UPDATE SET model = excluded.model, updated_at = excluded.updated_at`,
		platform, channelID, threadID, model, now, now,
	)
	if err != nil {
		return fmt.Errorf("store: thread config upsert model: %w", err)
	}
	return nil
}

func (r *sqliteThreadConfigRepo) UpsertListenMode(ctx context.Context, platform, channelID, threadID, mode string) error {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO thread_config (platform, channel_id, thread_id, listen_mode, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (platform, channel_id, thread_id) DO UPDATE SET listen_mode = excluded.listen_mode, updated_at = excluded.updated_at`,
		platform, channelID, threadID, mode, now, now,
	)
	if err != nil {
		return fmt.Errorf("store: thread config upsert listen mode: %w", err)
	}
	return nil
}

func (r *sqliteThreadConfigRepo) SnapshotFromChannel(ctx context.Context, platform, channelID, threadID, defaultWorkdir string) error {
	now := time.Now().Unix()
	_, err := r.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO thread_config (platform, channel_id, thread_id, workdir, model, listen_mode, created_at, updated_at)
		 VALUES (?, ?, ?,
		         COALESCE((SELECT workdir FROM channel WHERE platform = ? AND channel_id = ?), ?),
		         COALESCE((SELECT model FROM channel WHERE platform = ? AND channel_id = ?), ''),
		         COALESCE((SELECT listen_mode FROM channel WHERE platform = ? AND channel_id = ?), 'mention'),
		         ?, ?)`,
		platform, channelID, threadID,
		platform, channelID, defaultWorkdir,
		platform, channelID, platform, channelID, now, now,
	)
	if err != nil {
		return fmt.Errorf("store: thread config snapshot: %w", err)
	}
	return nil
}

var _ ThreadConfigRepo = (*sqliteThreadConfigRepo)(nil)
