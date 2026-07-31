package scheduler

import (
	"context"
	"testing"

	"github.com/anggasct/occa/internal/store"
)

type fakeScheduleStore struct {
	schedules []store.Schedule
	nextID    int64
}

func (f *fakeScheduleStore) Create(_ context.Context, s *store.Schedule) (int64, error) {
	f.nextID++
	s.ID = f.nextID
	f.schedules = append(f.schedules, *s)
	return s.ID, nil
}

func (f *fakeScheduleStore) Delete(_ context.Context, _, _ string, id int64) error {
	for i, s := range f.schedules {
		if s.ID == id {
			f.schedules = append(f.schedules[:i], f.schedules[i+1:]...)
			return nil
		}
	}
	return nil
}

func (f *fakeScheduleStore) List(_ context.Context, _, _ string) ([]store.Schedule, error) {
	return f.schedules, nil
}

func (f *fakeScheduleStore) ListAll(_ context.Context) ([]store.Schedule, error) {
	return f.schedules, nil
}

func TestSchedulerAddAndRemove(t *testing.T) {
	repo := &fakeScheduleStore{nextID: 0}
	executor := func(ctx context.Context, platform, channelID, prompt string) {}
	s := New(repo, executor)

	id, err := s.AddSchedule(context.Background(), store.Schedule{
		Platform:       "telegram",
		ChannelID:      "chat1",
		CronExpression: "0 9 * * 1-5",
		Prompt:         "run tests",
		Enabled:        true,
	})
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	if id != 1 {
		t.Fatalf("expected id 1, got %d", id)
	}

	schedules, err := s.ListSchedules(context.Background(), "telegram", "chat1")
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if len(schedules) != 1 {
		t.Fatalf("expected 1 schedule, got %d", len(schedules))
	}

	if err := s.RemoveSchedule(context.Background(), "telegram", "chat1", id); err != nil {
		t.Fatalf("RemoveSchedule: %v", err)
	}
}

func TestSchedulerInvalidCron(t *testing.T) {
	repo := &fakeScheduleStore{}
	executor := func(ctx context.Context, platform, channelID, prompt string) {}
	s := New(repo, executor)

	_, err := s.AddSchedule(context.Background(), store.Schedule{
		Platform:       "telegram",
		ChannelID:      "chat1",
		CronExpression: "invalid",
		Prompt:         "test",
		Enabled:        true,
	})
	if err == nil {
		t.Fatal("expected error for invalid cron expression")
	}

	if len(repo.schedules) != 0 {
		t.Fatalf("expected no persisted schedule for invalid cron, got %d", len(repo.schedules))
	}
}

func TestSchedulerStartReloadsFromStore(t *testing.T) {
	repo := &fakeScheduleStore{
		schedules: []store.Schedule{
			{ID: 1, Platform: "telegram", ChannelID: "chat1", CronExpression: "0 9 * * 1-5", Prompt: "daily", Enabled: true},
		},
	}
	executor := func(ctx context.Context, platform, channelID, prompt string) {}
	s := New(repo, executor)

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	if len(s.entryIDs) != 1 {
		t.Fatalf("expected 1 loaded schedule, got %d", len(s.entryIDs))
	}
}
