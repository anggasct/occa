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

const defaultStopGrace = 5 * time.Second

type Executor func(ctx context.Context, platform, channelID, prompt string)

type Scheduler struct {
	cron      *cron.Cron
	store     store.ScheduleRepo
	executor  Executor
	mu        sync.Mutex
	entryIDs  map[int64]cron.EntryID
	nextJobID int64
	active    []jobRef
	appCtx    context.Context
	stopGrace time.Duration
}

type jobRef struct {
	id     int64
	cancel context.CancelFunc
}

func New(st store.ScheduleRepo, executor Executor) *Scheduler {
	return &Scheduler{
		cron:      cron.New(),
		store:     st,
		executor:  executor,
		entryIDs:  make(map[int64]cron.EntryID),
		stopGrace: defaultStopGrace,
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.appCtx = ctx
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

// Stop waits for running jobs for a bounded grace period, then cancels them.
func (s *Scheduler) Stop() error {
	done := s.cron.Stop()

	s.mu.Lock()
	count := len(s.active)
	s.mu.Unlock()
	if count > 0 {
		slog.Info("scheduler: waiting for running jobs", "running", count)
	}

	select {
	case <-done.Done():
		return nil
	case <-time.After(s.stopGrace):
		s.mu.Lock()
		for _, ref := range s.active {
			ref.cancel()
		}
		s.mu.Unlock()
		slog.Warn("scheduler: grace period expired, cancelling running jobs", "running", count)

		select {
		case <-done.Done():
		case <-time.After(2 * time.Second):
		}
	}
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
		jobCtx, cancel := context.WithTimeout(s.appCtx, 10*time.Minute)
		s.mu.Lock()
		s.nextJobID++
		ref := jobRef{id: s.nextJobID, cancel: cancel}
		s.active = append(s.active, ref)
		s.mu.Unlock()
		defer func() {
			cancel()
			s.mu.Lock()
			for i, j := range s.active {
				if j.id == ref.id {
					s.active = append(s.active[:i], s.active[i+1:]...)
					break
				}
			}
			s.mu.Unlock()
		}()
		s.executor(jobCtx, sched.Platform, sched.ChannelID, sched.Prompt)
	})
	if err != nil {
		return fmt.Errorf("scheduler: register cron: %w", err)
	}
	s.mu.Lock()
	s.entryIDs[sched.ID] = entryID
	s.mu.Unlock()
	return nil
}
