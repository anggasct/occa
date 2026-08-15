package mcpserver

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/anggasct/occa/internal/attribution"
	"github.com/anggasct/occa/internal/scheduler"
	"github.com/anggasct/occa/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type fakeStore struct {
	mu        sync.Mutex
	schedules []store.Schedule
	failList  bool
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
	if f.failList {
		return nil, store.ErrNotFound
	}
	out := make([]store.Schedule, len(f.schedules))
	copy(out, f.schedules)
	return out, nil
}

func (f *fakeStore) ListAll(_ context.Context) ([]store.Schedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.Schedule, len(f.schedules))
	copy(out, f.schedules)
	return out, nil
}

func (f *fakeStore) Attribute(_ context.Context, id int64, platform, channelID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func newTestServer() (*Server, *fakeStore, *attribution.Store) {
	repo := &fakeStore{}
	executor := func(ctx context.Context, platform, channelID, prompt string) {}
	sched := scheduler.New(repo, executor)
	attrib := attribution.NewStore()
	srv := New(sched, attrib)
	return srv, repo, attrib
}

func fp(cronExpr, prompt, humanSched string) string {
	return attribution.Fingerprint(cronExpr, prompt, humanSched)
}

func TestMCPServerHappyPath(t *testing.T) {
	srv, repo, attrib := newTestServer()
	cronExpr := "0 9 * * 1-5"
	prompt := "run tests"
	humanSched := "weekdays at 9am"

	// The relay observes the tool call first and records the conversation.
	attrib.Put(fp(cronExpr, prompt, humanSched), "telegram", "chat123")

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

func TestMCPServerEventBeforeHandler(t *testing.T) {
	srv, repo, attrib := newTestServer()
	cronExpr := "0 9 * * 1-5"
	prompt := "run tests"
	humanSched := "weekdays at 9am"

	// The relay records the conversation while the handler is already
	// polling (within the bounded window).
	go func() {
		attrib.Put(fp(cronExpr, prompt, humanSched), "telegram", "chat123")
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
	if len(repo.schedules) != 1 || repo.schedules[0].Platform != "telegram" || repo.schedules[0].ChannelID != "chat123" {
		t.Fatalf("expected attributed schedule, got: %+v", repo.schedules)
	}
}

func TestMCPServerTimeoutPath(t *testing.T) {
	srv, repo, _ := newTestServer()

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
	srv, repo, attrib := newTestServer()
	cronExpr := "0 9 * * 1-5"
	prompt := "same prompt"
	humanSched := "weekdays at 9am"

	// Two conversations issue identical schedule_task calls. The relay
	// records both conversations; the FIFO pop consumes each entry exactly
	// once, so each row is stamped with one conversation and both jobs fire
	// to their originating chats regardless of handler goroutine order.
	attrib.Put(fp(cronExpr, prompt, humanSched), "telegram", "c1")
	attrib.Put(fp(cronExpr, prompt, humanSched), "discord", "c2")

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

func TestMCPServerPostAttributeFailureCleansRow(t *testing.T) {
	// Regression (review round 4): if the follow-up List fails AFTER the row
	// is stamped with platform/channel, the handler must not leave an
	// enabled schedule behind while returning an error.
	srv, repo, attrib := newTestServer()
	repo.failList = true
	cronExpr := "0 9 * * 1-5"
	prompt := "run tests"
	humanSched := "weekdays at 9am"
	attrib.Put(fp(cronExpr, prompt, humanSched), "telegram", "chat123")

	result, _, err := srv.handleScheduleTask(context.Background(), nil, scheduleTaskInput{
		CronExpression: cronExpr,
		Prompt:         prompt,
		HumanSchedule:  humanSched,
	})
	if err != nil {
		t.Fatalf("handleScheduleTask: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result when attribution registration fails")
	}
	if len(repo.schedules) != 0 {
		t.Fatalf("expected no enabled row to remain, got: %+v", repo.schedules)
	}
}

func TestMCPServerInvalidCronAndEmptyPrompt(t *testing.T) {
	srv, repo, _ := newTestServer()

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
