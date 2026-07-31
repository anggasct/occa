package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Schedule struct {
	ID             int64
	ChannelID      string
	Platform       string
	CronExpression string
	HumanSchedule  string
	Prompt         string
	Enabled        bool
	CreatedAt      int64
	UpdatedAt      int64
}

type ScheduleRepo interface {
	Create(ctx context.Context, s *Schedule) (int64, error)
	Delete(ctx context.Context, platform, channelID string, id int64) error
	List(ctx context.Context, platform, channelID string) ([]Schedule, error)
	ListAll(ctx context.Context) ([]Schedule, error)
}

type sqliteScheduleRepo struct {
	db *sql.DB
}

func (r *sqliteScheduleRepo) Create(ctx context.Context, s *Schedule) (int64, error) {
	now := time.Now().Unix()
	s.CreatedAt = now
	s.UpdatedAt = now
	enabled := 0
	if s.Enabled {
		enabled = 1
	}
	res, err := r.db.ExecContext(ctx,
		`INSERT INTO schedule (channel_id, platform, cron_expression, human_schedule, prompt, enabled, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		s.ChannelID, s.Platform, s.CronExpression, s.HumanSchedule, s.Prompt, enabled, now, now,
	)
	if err != nil {
		return 0, fmt.Errorf("store: schedule create: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("store: schedule create: %w", err)
	}
	s.ID = id
	return id, nil
}

func (r *sqliteScheduleRepo) Delete(ctx context.Context, platform, channelID string, id int64) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM schedule WHERE id = ? AND platform = ? AND channel_id = ?`, id, platform, channelID)
	if err != nil {
		return fmt.Errorf("store: schedule delete: %w", err)
	}
	return nil
}

func (r *sqliteScheduleRepo) List(ctx context.Context, platform, channelID string) ([]Schedule, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, channel_id, platform, cron_expression, human_schedule, prompt, enabled, created_at, updated_at
		 FROM schedule WHERE platform = ? AND channel_id = ? AND enabled = 1 ORDER BY id`,
		platform, channelID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: schedule list: %w", err)
	}
	defer rows.Close()
	return scanSchedules(rows)
}

func (r *sqliteScheduleRepo) ListAll(ctx context.Context) ([]Schedule, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, channel_id, platform, cron_expression, human_schedule, prompt, enabled, created_at, updated_at
		 FROM schedule WHERE enabled = 1 ORDER BY id`,
	)
	if err != nil {
		return nil, fmt.Errorf("store: schedule list all: %w", err)
	}
	defer rows.Close()
	return scanSchedules(rows)
}

func scanSchedules(rows *sql.Rows) ([]Schedule, error) {
	var schedules []Schedule
	for rows.Next() {
		var s Schedule
		var enabled int
		if err := rows.Scan(&s.ID, &s.ChannelID, &s.Platform, &s.CronExpression, &s.HumanSchedule, &s.Prompt, &enabled, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("store: schedule scan: %w", err)
		}
		s.Enabled = enabled == 1
		schedules = append(schedules, s)
	}
	return schedules, rows.Err()
}

var _ ScheduleRepo = (*sqliteScheduleRepo)(nil)
