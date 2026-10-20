package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

func TestRetryPolicyBudgets(t *testing.T) {
	max, waits := retryPolicyFor(failureModel)
	if max != 3 || len(waits) != 3 || waits[0] != 30*time.Second || waits[1] != 2*time.Minute || waits[2] != 5*time.Minute {
		t.Fatalf("model policy = %d %v", max, waits)
	}
	max, waits = retryPolicyFor(failureStall)
	if max != 1 || len(waits) != 1 || waits[0] != 0 {
		t.Fatalf("stall policy = %d %v", max, waits)
	}
	max, waits = retryPolicyFor(failureBuild)
	if max != 2 || len(waits) != 2 || waits[0] != 60*time.Second || waits[1] != 5*time.Minute {
		t.Fatalf("build policy = %d %v", max, waits)
	}
}

func TestClassifyFailure(t *testing.T) {
	var empty relay.TurnProgress
	_ = empty
	if got := classifyFailure(relay.ErrWebhookAgentResponse, &WebhookWorkContext{}, 2*time.Minute, false); got != failureModel {
		t.Fatalf("agent response error = %v, want model", got)
	}
	if got := classifyFailure(relay.ErrUnreachable, nil, 2*time.Minute, false); got != failureModel {
		t.Fatalf("unreachable = %v, want model", got)
	}
	if got := classifyFailure(context.DeadlineExceeded, &WebhookWorkContext{}, 2*time.Minute, true); got != failureStall {
		t.Fatalf("timeout without deltas = %v, want stall", got)
	}
	stale := &WebhookWorkContext{Progress: relay.TurnProgress{DeltaCount: 5, FirstDeltaAt: time.Now().Add(-10 * time.Minute), LastDeltaAt: time.Now().Add(-5 * time.Minute)}}
	if got := classifyFailure(context.DeadlineExceeded, stale, 2*time.Minute, true); got != failureStall {
		t.Fatalf("timeout with stale deltas = %v, want stall", got)
	}
	fresh := &WebhookWorkContext{Progress: relay.TurnProgress{DeltaCount: 50, FirstDeltaAt: time.Now().Add(-time.Minute), LastDeltaAt: time.Now()}}
	if got := classifyFailure(context.DeadlineExceeded, fresh, 2*time.Minute, true); got != failureBuild {
		t.Fatalf("timeout with fresh deltas = %v, want build (long generation)", got)
	}
	if got := classifyFailure(errors.New("boom"), &WebhookWorkContext{}, 2*time.Minute, false); got != failureBuild {
		t.Fatalf("generic error = %v, want build", got)
	}
}

func newRetryHarness(t *testing.T, exec func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error) (*Server, *store.SQLiteStore, string) {
	t.Helper()
	cfg := config.WebhookConfig{
		Bind:    "127.0.0.1:0",
		Policy:  testPolicy(),
		Runtime: testRuntime(),
		Endpoints: []config.EndpointConfig{
			prTestEndpoint("review", "/review", config.AutomationFull),
			prTestEndpoint("fix", "/fix", config.AutomationFull),
			prTestEndpoint("merge", "/merge", config.AutomationFull),
			prTestEndpoint("merged", "/merged", config.AutomationFull),
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return srv, st, ts.URL
}

func TestRetryModelErrorThreeTimesThenBlocked(t *testing.T) {
	var calls int64
	var mu sync.Mutex
	var mentioned []string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		if workCtx.ThreadID == "" {
			workCtx.ThreadID = "thread-pr"
		}
		return fmt.Errorf("model blew up: %w", relay.ErrWebhookAgentResponse)
	}
	srv, st, url := newRetryHarness(t, exec)
	srv.retryBackoffs = map[failureClass][]time.Duration{
		failureModel: {50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond},
		failureStall: {0},
		failureBuild: {50 * time.Millisecond, 50 * time.Millisecond},
	}
	srv.notifier = func(ctx context.Context, platform, channelID, text string) error {
		mu.Lock()
		mentioned = append(mentioned, text)
		mu.Unlock()
		return nil
	}
	srv.SetMentionResolver(func(ctx context.Context, platform, name string) (string, bool) {
		if name == "operator" {
			return "user-42", true
		}
		return "", false
	})
	var mentionCalls []string
	srv.SetMentionNotifier(func(ctx context.Context, platform, channelID, text, mentionUserID string) error {
		mu.Lock()
		mentionCalls = append(mentionCalls, channelID+"|"+mentionUserID+"|"+text)
		mu.Unlock()
		return nil
	})
	srv.escalationMention = "operator"
	body := prReviewBody(7, "feat/x", "owner", "commented", "c0ffee-retry-1")
	if resp := post(t, url+"/review?secret=s3cret", "del-retry-1", "pull_request_review", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-retry-1", store.WebhookStatusFailed)
	if got := atomic.LoadInt64(&calls); got != 4 {
		t.Fatalf("executor calls = %d, want 1 + 3 retries", got)
	}
	run, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	if err != nil || run == nil || run.Status != store.PRStatusBlocked || run.RetryCount != 3 {
		t.Fatalf("pr row = %+v, %v; want blocked with retry_count 3", run, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(mentionCalls) != 1 {
		t.Fatalf("mention notifications = %d, want exactly one: %v", len(mentionCalls), mentionCalls)
	}
	if !strings.HasPrefix(mentionCalls[0], "chat1|user-42|") {
		t.Fatalf("mention notification = %q, want the channel and the resolved user id", mentionCalls[0])
	}
	for _, want := range []string{"needs you", "Action: /retry · /skip · /cancel", "@operator"} {
		if !strings.Contains(mentionCalls[0], want) {
			t.Errorf("escalation copy %q must contain %q", mentionCalls[0], want)
		}
	}
	mentions := 0
	for _, text := range mentioned {
		if strings.Contains(text, "@operator") {
			mentions++
		}
	}
	if mentions != 0 {
		t.Fatalf("plain notifications must stay mention-free, got %d: %v", mentions, mentioned)
	}
}

func TestStallImmediateResumeSameSession(t *testing.T) {
	var mu sync.Mutex
	var sessions []string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		if workCtx.ThreadID == "" {
			workCtx.ThreadID = "thread-pr"
		}
		if workCtx.SessionID == "" {
			workCtx.SessionID = "sess-41"
		}
		mu.Lock()
		sessions = append(sessions, workCtx.SessionID)
		n := len(sessions)
		mu.Unlock()
		if n == 1 {
			return context.DeadlineExceeded
		}
		return nil
	}
	_, st, url := newRetryHarness(t, exec)
	body := prReviewBody(7, "feat/x", "owner", "commented", "c0ffee-stall-1")
	if resp := post(t, url+"/review?secret=s3cret", "del-stall-1", "pull_request_review", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-stall-1", store.WebhookStatusCompleted)
	mu.Lock()
	defer mu.Unlock()
	if len(sessions) != 2 {
		t.Fatalf("expected immediate retry (2 turns), got %d", len(sessions))
	}
	if sessions[1] != "sess-41" {
		t.Fatalf("retry did not resume same session: %v", sessions)
	}
}

func TestNoRetryWithoutPRKey(t *testing.T) {
	var calls int64
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		return errors.New("agent exploded")
	}
	cfg := config.WebhookConfig{
		Bind:    "127.0.0.1:0",
		Policy:  testPolicy(),
		Runtime: testRuntime(),
		Endpoints: []config.EndpointConfig{{
			Name: "custom-ep", Path: "/custom", Secret: "s3cret", Platform: "telegram",
			ChannelID: "chat1", Prompt: "do it", Workflow: "custom", Admit: defaultCatchAllAdmit(),
		}},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	body := prReviewBody(9, "feat/z", "owner", "commented", "c0ffee-nokey-1")
	if resp := post(t, ts.URL+"/custom?secret=s3cret", "del-nokey-1", "pull_request_review", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "custom-ep", "del-nokey-1", store.WebhookStatusFailed)
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("custom endpoint must not retry, calls = %d", got)
	}
}
