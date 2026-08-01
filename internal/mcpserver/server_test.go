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

func newTestServer() (*Server, *fakeStore, *TokenStore) {
	repo := &fakeStore{}
	executor := func(ctx context.Context, platform, channelID, prompt string) {}
	sched := scheduler.New(repo, executor)
	tokens := NewTokenStore()
	srv := New(sched, tokens)
	return srv, repo, tokens
}

func TestMCPServerInvalidToken(t *testing.T) {
	srv, _, _ := newTestServer()

	result, _, err := srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		ScheduleToken:  "bogus",
		CronExpression: "0 9 * * 1-5",
		Prompt:         "test",
	})
	if err != nil {
		t.Fatalf("handleScheduleTask: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error for invalid token")
	}
}

func TestMCPServerValidToken(t *testing.T) {
	srv, repo, tokens := newTestServer()
	token := tokens.Generate("telegram", "chat123")

	result, _, err := srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		ScheduleToken:  token,
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
		t.Fatalf("expected 1 schedule, got %d", len(repo.schedules))
	}
	s := repo.schedules[0]
	if s.Platform != "telegram" || s.ChannelID != "chat123" {
		t.Fatalf("wrong attribution: platform=%s channel=%s", s.Platform, s.ChannelID)
	}
}

func TestMCPServerTokenCannotBeUsedForOtherChannel(t *testing.T) {
	srv, repo, tokens := newTestServer()
	tokenA := tokens.Generate("telegram", "chatA")
	tokenB := tokens.Generate("discord", "chatB")

	_, _, _ = srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		ScheduleToken:  tokenA,
		CronExpression: "0 9 * * 1-5",
		Prompt:         "from A",
	})
	_, _, _ = srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		ScheduleToken:  tokenB,
		CronExpression: "0 10 * * 1-5",
		Prompt:         "from B",
	})

	if len(repo.schedules) != 2 {
		t.Fatalf("expected 2 schedules, got %d", len(repo.schedules))
	}
	if repo.schedules[0].ChannelID != "chatA" {
		t.Fatalf("schedule 0 should be chatA, got %s", repo.schedules[0].ChannelID)
	}
	if repo.schedules[1].ChannelID != "chatB" {
		t.Fatalf("schedule 1 should be chatB, got %s", repo.schedules[1].ChannelID)
	}
}

func TestMCPServerForgedTokenRejected(t *testing.T) {
	srv, repo, tokens := newTestServer()
	_ = tokens.Generate("telegram", "chatA")

	result, _, _ := srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		ScheduleToken:  "forged-token-not-in-store",
		CronExpression: "0 9 * * 1-5",
		Prompt:         "attack",
	})
	if !result.IsError {
		t.Fatal("forged token should be rejected")
	}
	if len(repo.schedules) != 0 {
		t.Fatal("no schedule should be created with forged token")
	}
}

func TestServerTimeoutsSet(t *testing.T) {
	srv, _, _ := newTestServer()
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	s := srv.httpSrv
	if s.ReadHeaderTimeout <= 0 || s.ReadTimeout <= 0 || s.WriteTimeout <= 0 || s.IdleTimeout <= 0 {
		t.Fatalf("server timeouts not set: %+v", s)
	}
}
