package mcpserver

import (
	"context"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/scheduler"
	"github.com/anggasct/occa/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type fakeStore struct {
	schedules []store.Schedule
}

func (f *fakeStore) Create(_ context.Context, s *store.Schedule) (int64, error) {
	s.ID = 1
	f.schedules = append(f.schedules, *s)
	return 1, nil
}

func (f *fakeStore) Delete(_ context.Context, _, _ string, _ int64) error { return nil }

func (f *fakeStore) List(_ context.Context, _, _ string) ([]store.Schedule, error) {
	return f.schedules, nil
}

func (f *fakeStore) ListAll(_ context.Context) ([]store.Schedule, error) {
	return f.schedules, nil
}

func TestMCPServerNoContext(t *testing.T) {
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
		t.Fatal("expected error when no context is set")
	}
}

func TestMCPServerWithContext(t *testing.T) {
	repo := &fakeStore{}
	executor := func(ctx context.Context, platform, channelID, prompt string) {}
	sched := scheduler.New(repo, executor)
	srv := New(sched)

	srv.SetContext("telegram", "chat123")

	result, _, err := srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		CronExpression: "0 9 * * 1-5",
		Prompt:         "run tests",
		HumanSchedule:  "weekdays at 9am",
	})
	if err != nil {
		t.Fatalf("handleScheduleTask: %v", err)
	}
	if result.IsError {
		t.Fatalf("expected success, got error")
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
		t.Fatalf("expected 1 schedule created, got %d", len(repo.schedules))
	}
	s := repo.schedules[0]
	if s.Platform != "telegram" || s.ChannelID != "chat123" {
		t.Fatalf("schedule created with wrong context: platform=%s channel=%s", s.Platform, s.ChannelID)
	}
}

func TestMCPContextMostRecent(t *testing.T) {
	repo := &fakeStore{}
	executor := func(ctx context.Context, platform, channelID, prompt string) {}
	sched := scheduler.New(repo, executor)
	srv := New(sched)

	srv.SetContext("telegram", "chatA")
	srv.SetContext("discord", "chatB")

	platform, channelID := srv.mostRecentContext()
	if platform != "discord" || channelID != "chatB" {
		t.Fatalf("expected discord/chatB as most recent, got %s/%s", platform, channelID)
	}
}
