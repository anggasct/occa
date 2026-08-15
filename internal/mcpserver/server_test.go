package mcpserver

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

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

func (f *fakeStore) Delete(_ context.Context, _, _ string, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.schedules {
		if s.ID == id {
			f.schedules = append(f.schedules[:i], f.schedules[i+1:]...)
			return nil
		}
	}
	return nil
}

func (f *fakeStore) List(_ context.Context, _, _ string) ([]store.Schedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.schedules, nil
}

func (f *fakeStore) ListAll(_ context.Context) ([]store.Schedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.schedules, nil
}

func (f *fakeStore) AttributePending(_ context.Context, platform, channelID, cronExpression, prompt, humanSchedule string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.schedules {
		s := &f.schedules[i]
		if s.Platform == "" && s.ChannelID == "" && !s.Enabled &&
			s.CronExpression == cronExpression && s.Prompt == prompt && s.HumanSchedule == humanSchedule {
			s.Platform = platform
			s.ChannelID = channelID
			s.Enabled = true
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeStore) Attributed(_ context.Context, id int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.schedules {
		if f.schedules[i].ID == id {
			return f.schedules[i].Platform != "", nil
		}
	}
	return false, nil
}

func (f *fakeStore) SweepPending(_ context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func newTestServer() (*Server, *fakeStore) {
	repo := &fakeStore{}
	executor := func(ctx context.Context, platform, channelID, prompt string) {}
	sched := scheduler.New(repo, executor)
	srv := New(sched)
	return srv, repo
}

func TestMCPServerHappyPath(t *testing.T) {
	srv, repo := newTestServer()
	cronExpr := "0 9 * * 1-5"
	prompt := "run tests"
	humanSched := "weekdays at 9am"

	// The relay observes the tool call and stamps the pending row while the
	// handler polls; the retry covers the event-before-row window.
	go func() {
		for {
			ok, err := repo.AttributePending(context.Background(), "telegram", "chat123", cronExpr, prompt, humanSched)
			if err == nil && ok {
				return
			}
		}
	}()

	result, _, err := srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		CronExpression: cronExpr,
		Prompt:         prompt,
		HumanSchedule:  humanSched,
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
	if !strings.Contains(text, "Scheduled (ID: 1)") {
		t.Fatalf("expected 'Scheduled (ID: 1)' in result, got: %s", text)
	}
	if len(repo.schedules) != 1 {
		t.Fatalf("expected 1 schedule, got %d", len(repo.schedules))
	}
	s := repo.schedules[0]
	if s.Platform != "telegram" || s.ChannelID != "chat123" || !s.Enabled {
		t.Fatalf("wrong attribution or state: %+v", s)
	}
}

func TestMCPServerTimeoutPath(t *testing.T) {
	srv, repo := newTestServer()

	result, _, err := srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		CronExpression: "0 9 * * 1-5",
		Prompt:         "unattributed",
		HumanSchedule:  "weekdays at 9am",
	})
	if err != nil {
		t.Fatalf("handleScheduleTask: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result on attribution timeout")
	}

	if len(repo.schedules) != 0 {
		t.Fatalf("expected pending row to be deleted on timeout, got %d rows", len(repo.schedules))
	}
}

func TestMCPServerConcurrentIdenticalCalls(t *testing.T) {
	srv, repo := newTestServer()
	cronExpr := "0 9 * * 1-5"
	prompt := "same prompt"
	humanSched := "weekdays at 9am"

	// Two conversations issue identical schedule_task calls. The relay stamps
	// the oldest pending row per conversation; AttributePending consumes the
	// oldest matching row, so the two stamps pair one-to-one with the two
	// pending rows even though the handlers create them concurrently.
	var wg sync.WaitGroup
	results := make([]*mcp.CallToolResult, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, _, _ := srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
				CronExpression: cronExpr, Prompt: prompt, HumanSchedule: humanSched,
			})
			results[i] = res
		}(i)
	}

	relayStamp := func(platform, channelID string) {
		for i := 0; i < 50; i++ {
			ok, err := repo.AttributePending(context.Background(), platform, channelID, cronExpr, prompt, humanSched)
			if err == nil && ok {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	relayStamp("telegram", "c1")
	relayStamp("discord", "c2")

	wg.Wait()

	if results[0].IsError || results[1].IsError {
		t.Fatalf("expected both calls to succeed: %+v %+v", results[0], results[1])
	}
	if len(repo.schedules) != 2 {
		t.Fatalf("expected 2 schedules, got %d", len(repo.schedules))
	}
	byChannel := map[string]string{}
	for _, s := range repo.schedules {
		if !s.Enabled {
			t.Fatalf("schedule not enabled: %+v", s)
		}
		byChannel[s.ChannelID] = s.Platform
	}
	if byChannel["c1"] != "telegram" || byChannel["c2"] != "discord" {
		t.Fatalf("rows attributed to wrong conversation: %+v", byChannel)
	}
}

func TestMCPServerInvalidCronAndEmptyPrompt(t *testing.T) {
	srv, repo := newTestServer()

	result, _, _ := srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		CronExpression: "invalid cron",
		Prompt:         "test",
	})
	if !result.IsError {
		t.Fatal("expected error for invalid cron")
	}

	result, _, _ = srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		CronExpression: "0 9 * * 1-5",
		Prompt:         "   ",
	})
	if !result.IsError {
		t.Fatal("expected error for empty prompt")
	}

	if len(repo.schedules) != 0 {
		t.Fatalf("expected 0 schedules created, got %d", len(repo.schedules))
	}
}

func TestServerTimeoutsSet(t *testing.T) {
	srv, _ := newTestServer()
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	s := srv.httpSrv
	if s.ReadHeaderTimeout <= 0 || s.ReadTimeout <= 0 || s.WriteTimeout <= 0 || s.IdleTimeout <= 0 {
		t.Fatalf("server timeouts not set: %+v", s)
	}
}
