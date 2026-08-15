package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/store"
	"github.com/robfig/cron/v3"
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
	var enabled []store.Schedule
	for _, s := range f.schedules {
		if s.Enabled {
			enabled = append(enabled, s)
		}
	}
	return enabled, nil
}

func (f *fakeScheduleStore) Attribute(_ context.Context, id int64, platform, channelID string) error {
	for i, s := range f.schedules {
		if s.ID == id {
			f.schedules[i].Platform = platform
			f.schedules[i].ChannelID = channelID
			f.schedules[i].Enabled = true
			return nil
		}
	}
	return store.ErrNotFound
}

func (f *fakeScheduleStore) SweepPending(_ context.Context) (int64, error) {
	var count int64
	var kept []store.Schedule
	for _, s := range f.schedules {
		if s.Platform == "" && s.ChannelID == "" && !s.Enabled {
			count++
		} else {
			kept = append(kept, s)
		}
	}
	f.schedules = kept
	return count, nil
}

type failingSweepStore struct {
	*fakeScheduleStore
}

func (f *failingSweepStore) SweepPending(_ context.Context) (int64, error) {
	return 0, store.ErrNotFound
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

// newSecondsScheduler gives tests a second-resolution cron for fast firing;
// production keeps the 5-field standard grammar.
func newSecondsScheduler(repo store.ScheduleRepo, executor Executor) *Scheduler {
	s := New(repo, executor)
	s.cron = cron.New(cron.WithSeconds())
	return s
}

func TestStopWaitsForRunningJobThenCancels(t *testing.T) {
	repo := &fakeScheduleStore{nextID: 0}
	var startOnce, doneOnce sync.Once
	jobStarted := make(chan struct{})
	jobDone := make(chan struct{})
	executor := func(ctx context.Context, platform, channelID, prompt string) {
		startOnce.Do(func() { close(jobStarted) })
		<-ctx.Done()
		doneOnce.Do(func() { close(jobDone) })
	}
	s := newSecondsScheduler(repo, executor)
	s.stopGrace = time.Second
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := s.register(store.Schedule{
		Platform: "telegram", ChannelID: "chat1",
		CronExpression: "*/1 * * * * *", Prompt: "blocking",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	<-jobStarted

	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		_ = s.Stop()
	}()

	select {
	case <-jobDone:
		t.Fatal("job context was cancelled before the grace period expired")
	case <-time.After(500 * time.Millisecond):
	}

	<-stopDone
	select {
	case <-jobDone:
	case <-time.After(2 * time.Second):
		t.Fatal("job was not cancelled after the grace period")
	}
}

func TestStopWithoutJobsReturns(t *testing.T) {
	repo := &fakeScheduleStore{nextID: 0}
	s := New(repo, func(ctx context.Context, platform, channelID, prompt string) {})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = s.Stop() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop hung with no running jobs")
	}
}

func TestAppContextCancelsRunningJob(t *testing.T) {
	repo := &fakeScheduleStore{nextID: 0}
	var startOnce, doneOnce sync.Once
	jobStarted := make(chan struct{})
	jobDone := make(chan struct{})
	executor := func(ctx context.Context, platform, channelID, prompt string) {
		startOnce.Do(func() { close(jobStarted) })
		<-ctx.Done()
		doneOnce.Do(func() { close(jobDone) })
	}
	s := newSecondsScheduler(repo, executor)
	s.stopGrace = 30 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := s.register(store.Schedule{
		Platform: "telegram", ChannelID: "chat1",
		CronExpression: "*/1 * * * * *", Prompt: "blocking",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	<-jobStarted
	cancel()

	select {
	case <-jobDone:
	case <-time.After(2 * time.Second):
		t.Fatal("app context cancel did not reach the running job")
	}
}

func TestJobKeepsBoundedRunTimeout(t *testing.T) {
	repo := &fakeScheduleStore{nextID: 0}
	var startOnce sync.Once
	jobStarted := make(chan struct{})
	deadline := make(chan time.Duration, 1)
	executor := func(ctx context.Context, platform, channelID, prompt string) {
		startOnce.Do(func() { close(jobStarted) })
		if dl, ok := ctx.Deadline(); ok {
			select {
			case deadline <- time.Until(dl):
			default:
			}
		}
	}
	s := newSecondsScheduler(repo, executor)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := s.register(store.Schedule{
		Platform: "telegram", ChannelID: "chat1",
		CronExpression: "*/1 * * * * *", Prompt: "run",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	<-jobStarted
	select {
	case d := <-deadline:
		if d > 11*time.Minute || d < 9*time.Minute {
			t.Fatalf("unexpected run deadline: %v", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("job did not report its deadline")
	}
}

func TestSchedulerStartSweepsPending(t *testing.T) {
	repo := &fakeScheduleStore{
		schedules: []store.Schedule{
			{ID: 1, Platform: "", ChannelID: "", CronExpression: "0 9 * * 1-5", Prompt: "stray", Enabled: false},
			{ID: 2, Platform: "telegram", ChannelID: "c1", CronExpression: "0 9 * * 1-5", Prompt: "active", Enabled: true},
		},
	}
	s := New(repo, func(ctx context.Context, platform, channelID, prompt string) {})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = s.Stop() }()

	if len(repo.schedules) != 1 || repo.schedules[0].ID != 2 {
		t.Fatalf("expected stray pending schedule to be swept, got: %+v", repo.schedules)
	}
}

func TestSchedulerStartSweepErrorFailsStartup(t *testing.T) {
	repo := &failingSweepStore{fakeScheduleStore: &fakeScheduleStore{nextID: 0}}
	s := New(repo, func(ctx context.Context, platform, channelID, prompt string) {})
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("expected Start to fail when pending-row cleanup errors")
	}
}
