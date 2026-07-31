package mcpserver

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/anggasct/occa/internal/scheduler"
	"github.com/anggasct/occa/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type fakeStore struct {
	mu        sync.Mutex
	schedules []store.Schedule
}

func (f *fakeStore) Create(_ context.Context, s *store.Schedule) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s.ID = int64(len(f.schedules)) + 1
	f.schedules = append(f.schedules, *s)
	return s.ID, nil
}

func (f *fakeStore) Delete(_ context.Context, _, _ string, _ int64) error { return nil }

func (f *fakeStore) List(_ context.Context, _, _ string) ([]store.Schedule, error) {
	return f.schedules, nil
}

func (f *fakeStore) ListAll(_ context.Context) ([]store.Schedule, error) {
	return f.schedules, nil
}

func TestMCPServerNoChannelContext(t *testing.T) {
	repo := &fakeStore{}
	executor := func(ctx context.Context, platform, channelID, prompt string) {}
	sched := scheduler.New(repo, executor)
	srv := New(sched)

	result, _, err := srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		CronExpression: "0 9 * * 1-5",
		Prompt:         "test",
		HumanSchedule:  "daily at 9am",
	})
	if err != nil {
		t.Fatalf("handleScheduleTask: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error when platform/channel_id missing")
	}
}

func TestMCPServerWithChannelInput(t *testing.T) {
	repo := &fakeStore{}
	executor := func(ctx context.Context, platform, channelID, prompt string) {}
	sched := scheduler.New(repo, executor)
	srv := New(sched)

	result, _, err := srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		Platform:       "telegram",
		ChannelID:      "chat123",
		CronExpression: "0 9 * * 1-5",
		Prompt:         "run tests",
		HumanSchedule:  "weekdays at 9am",
	})
	if err != nil {
		t.Fatalf("handleScheduleTask: %v", err)
	}
	if result.IsError {
		t.Fatalf("expected success, got error: %v", result.Content)
	}

	text := ""
	if len(result.Content) > 0 {
		if tc, ok := result.Content[0].(*mcp.TextContent); ok {
			text = tc.Text
		}
	}
	if !strings.Contains(text, "Scheduled") {
		t.Fatalf("expected 'Scheduled' in result, got: %s", text)
	}
	if len(repo.schedules) != 1 {
		t.Fatalf("expected 1 schedule, got %d", len(repo.schedules))
	}
	s := repo.schedules[0]
	if s.Platform != "telegram" || s.ChannelID != "chat123" {
		t.Fatalf("wrong context: platform=%s channel=%s", s.Platform, s.ChannelID)
	}
}

func TestMCPServerConcurrentNoRace(t *testing.T) {
	repo := &fakeStore{}
	executor := func(ctx context.Context, platform, channelID, prompt string) {}
	sched := scheduler.New(repo, executor)
	srv := New(sched)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
			Platform:       "telegram",
			ChannelID:      "chatA",
			CronExpression: "0 9 * * 1-5",
			Prompt:         "from A",
			HumanSchedule:  "daily",
		})
	}()
	_, _, _ = srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		Platform:       "discord",
		ChannelID:      "chatB",
		CronExpression: "0 10 * * 1-5",
		Prompt:         "from B",
		HumanSchedule:  "daily",
	})
	<-done

	if len(repo.schedules) != 2 {
		t.Fatalf("expected 2 schedules, got %d", len(repo.schedules))
	}
}
