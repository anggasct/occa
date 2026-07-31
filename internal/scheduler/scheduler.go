package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/store"
	"github.com/robfig/cron/v3"
)

type Executor func(ctx context.Context, platform, channelID, prompt string)

type Scheduler struct {
	cron     *cron.Cron
	store    store.ScheduleRepo
	executor Executor
	mu       sync.Mutex
	entryIDs map[int64]cron.EntryID
}

func New(st store.ScheduleRepo, executor Executor) *Scheduler {
	return &Scheduler{
		cron:     cron.New(),
		store:    st,
		executor: executor,
		entryIDs: make(map[int64]cron.EntryID),
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	schedules, err := s.store.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("scheduler: load schedules: %w", err)
	}
	for _, sched := range schedules {
		if err := s.register(sched); err != nil {
			slog.Warn("scheduler: skip schedule on load", "id", sched.ID, "error", err)
		}
	}
	s.cron.Start()
	slog.Info("scheduler started", "schedules", len(schedules))
	return nil
}

func (s *Scheduler) Stop() error {
	s.cron.Stop()
	return nil
}

func (s *Scheduler) AddSchedule(ctx context.Context, sched store.Schedule) (int64, error) {
	if _, err := cron.ParseStandard(sched.CronExpression); err != nil {
		return 0, fmt.Errorf("scheduler: invalid cron expression %q: %w", sched.CronExpression, err)
	}

	id, err := s.store.Create(ctx, &sched)
	if err != nil {
		return 0, err
	}
	sched.ID = id
	if err := s.register(sched); err != nil {
		_ = s.store.Delete(ctx, sched.Platform, sched.ChannelID, id)
		return 0, err
	}
	slog.Info("scheduler: schedule added", "id", id, "cron", sched.CronExpression, "channel", sched.ChannelID)
	return id, nil
}

func (s *Scheduler) RemoveSchedule(ctx context.Context, platform, channelID string, id int64) error {
	s.mu.Lock()
	entryID, ok := s.entryIDs[id]
	if ok {
		s.cron.Remove(entryID)
		delete(s.entryIDs, id)
	}
	s.mu.Unlock()
	return s.store.Delete(ctx, platform, channelID, id)
}

func (s *Scheduler) ListSchedules(ctx context.Context, platform, channelID string) ([]store.Schedule, error) {
	return s.store.List(ctx, platform, channelID)
}

func (s *Scheduler) register(sched store.Schedule) error {
	entryID, err := s.cron.AddFunc(sched.CronExpression, func() {
		slog.Info("scheduler: executing", "id", sched.ID, "prompt", sched.Prompt)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		s.executor(ctx, sched.Platform, sched.ChannelID, sched.Prompt)
	})
	if err != nil {
		return fmt.Errorf("scheduler: register cron: %w", err)
	}
	s.mu.Lock()
	s.entryIDs[sched.ID] = entryID
	s.mu.Unlock()
	return nil
}
