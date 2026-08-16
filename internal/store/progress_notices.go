package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type sqliteProgressNoticeRepo struct {
	db *sql.DB
}

func (r *sqliteProgressNoticeRepo) Put(ctx context.Context, platform, channelID, threadID, messageID string) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO progress_notice (platform, channel_id, thread_id, message_id, created_at) VALUES (?, ?, ?, ?, ?)`,
		platform, channelID, threadID, messageID, time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("store: progress notice put: %w", err)
	}
	return nil
}

func (r *sqliteProgressNoticeRepo) List(ctx context.Context) ([]ProgressNotice, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, platform, channel_id, thread_id, message_id, created_at FROM progress_notice ORDER BY id`,
	)
	if err != nil {
		return nil, fmt.Errorf("store: progress notice list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var notices []ProgressNotice
	for rows.Next() {
		var n ProgressNotice
		if err := rows.Scan(&n.ID, &n.Platform, &n.ChannelID, &n.ThreadID, &n.MessageID, &n.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: progress notice scan: %w", err)
		}
		notices = append(notices, n)
	}
	return notices, rows.Err()
}

func (r *sqliteProgressNoticeRepo) Delete(ctx context.Context, platform, channelID, threadID, messageID string) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM progress_notice WHERE platform = ? AND channel_id = ? AND thread_id = ? AND message_id = ?`,
		platform, channelID, threadID, messageID,
	)
	if err != nil {
		return fmt.Errorf("store: progress notice delete: %w", err)
	}
	return nil
}

var _ ProgressNoticeRepo = (*sqliteProgressNoticeRepo)(nil)
