package webhook

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

// TestRetryExecutionFailsClosedWhenPRStateUnresolvable asserts a retry whose
// live PR state cannot be read is refused instead of started: a second agent
// turn on a merged PR is the incident the guard exists to stop.
func TestRetryExecutionFailsClosedWhenPRStateUnresolvable(t *testing.T) {
	logs := captureServerLogs(t)
	var calls int64
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		return errors.New("agent exploded")
	}
	srv, st, _, url := newIsolationServer(t, exec)
	var state atomic.Value
	state.Store(PRStateOpen)
	srv.SetPRStateResolver(func(ctx context.Context, repo, number string) (string, error) {
		current, _ := state.Load().(string)
		if current == "" {
			return "", errors.New("gh unreachable")
		}
		return current, nil
	})
	srv.retryBackoffs = map[failureClass][]time.Duration{
		failureBuild: {2 * time.Second},
	}

	if resp := post(t, url+"/fix?secret=s3cret", "del-retry-unknown", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffeeb")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForLog(t, logs, "webhook: retry scheduled")
	state.Store("")

	waitForDeliveryStatus(t, st, "github-widget-fix", "del-retry-unknown", store.WebhookStatusSkipped)
	receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-fix", "del-retry-unknown")
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v", err)
	}
	if !strings.Contains(receipt.ErrorSummary, "unresolvable") {
		t.Fatalf("refused retry summary = %q, want the unresolvable state", receipt.ErrorSummary)
	}
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("an unresolvable PR state must not start a second agent turn, executor calls = %d", got)
	}
	if len(logs.all("webhook: retry refused, PR state unresolvable")) == 0 {
		t.Fatal("the refused retry must be logged")
	}
}

// TestWatchdogReportsNoLiveAgentInsteadOfAbortFailure asserts a workdir
// without a running agent is reported as "nothing to abort" instead of an
// abort that failed against a session that is already gone.
func TestWatchdogReportsNoLiveAgentInsteadOfAbortFailure(t *testing.T) {
	logs := captureServerLogs(t)
	srv, st := newWatchdogServer(t, func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	})
	srv.SetDispatcherStuckAfter(time.Minute)
	srv.SetDispatcherWatchdogInterval(time.Hour)

	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	item := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-no-live-agent", eventType: "pull_request", payloadHash: "hash-no-live-agent", prKey: "acme/widget#7"}
	d := &dispatcher{server: srv, key: "pr:acme/widget#7", lanes: makePRLanes(8)}
	d.setActive(item)
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()
	d.mu.Lock()
	d.lastActivity = time.Now().Add(-10 * time.Minute)
	d.activeStartedAt = time.Now().Add(-10 * time.Minute)
	d.mu.Unlock()
	run := srv.beginActiveRun(item.prKey, ep.Name, item.deliveryID, "/tmp/widget/worktree")
	run.setSession("ses-no-live-agent-1")

	created, err := st.WebhookDeliveryRepo().Create(context.Background(), store.WebhookDelivery{
		Endpoint: ep.Name, DeliveryID: item.deliveryID, EventType: item.eventType, PayloadHash: item.payloadHash,
	})
	if err != nil || !created {
		t.Fatalf("create receipt: %v", err)
	}
	receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, item.deliveryID)
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v", err)
	}
	if ok, tErr := st.WebhookDeliveryRepo().Transition(context.Background(), receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusProcessing, ""); tErr != nil || !ok {
		t.Fatalf("claim receipt: %v %v", ok, tErr)
	}

	srv.SetSessionLiveness(func(ctx context.Context, sessionID, workdir string) (bool, error) {
		return false, nil
	})
	srv.SetSessionAborter(func(ctx context.Context, sessionID, workdir string) error {
		return ErrNoLiveAgent
	})

	if n := srv.sweepStuckDispatchers(time.Now()); n != 1 {
		t.Fatalf("sweep handled %d dispatchers, want 1", n)
	}
	failed, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, item.deliveryID)
	if err != nil || failed == nil || failed.Status != store.WebhookStatusFailed {
		t.Fatalf("stuck delivery receipt = %+v %v; want failed", failed, err)
	}
	if !strings.Contains(failed.ErrorSummary, "no session to abort") {
		t.Fatalf("stuck delivery summary = %q, want no session to abort", failed.ErrorSummary)
	}
	if len(logs.all("webhook: dispatcher stuck, session abort failed, still running")) != 0 {
		t.Fatal("a missing agent must not be reported as a failed abort")
	}
	if len(logs.all("webhook: dispatcher stuck, no session to abort")) != 1 {
		t.Fatal("the missing agent must be reported as nothing to abort")
	}
}

// TestFixRoundQueuesExactlyOneReview asserts a fix round that moved the PR
// head queues one review for that PR, and a round that changed nothing
// queues none.
func TestFixRoundQueuesExactlyOneReview(t *testing.T) {
	var mu sync.Mutex
	reviewRuns := 0
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		if workCtx != nil && workCtx.Workflow == "review" {
			mu.Lock()
			reviewRuns++
			mu.Unlock()
		}
		return nil
	}
	reviewEp := config.EndpointConfig{
		Name:           "github-widget-review",
		Path:           "/review",
		Secret:         "s3cret",
		Platform:       "discord",
		ChannelID:      "chan-1",
		Prompt:         "review",
		Workflow:       "review",
		Thread:         true,
		Repository:     "acme/widget",
		CommentTrigger: []string{"please re-review"},
		Limits:         &config.EndpointLimits{MaxRunsPerPR: 3, Window: time.Hour},
		Admit:          defaultCatchAllAdmit(),
	}
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{reviewEp}),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "rereview.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	srv.SetPRHeadResolver(&fakePRHeadResolver{head: PRHead{
		Branch: "feat/x",
		SHA:    "abcdef0123456789abcdef0123456789abcdef01",
	}})

	srv.queueReviewAfterFix("acme/widget#181", "0123456789abcdef0123456789abcdef01234567")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		runs := reviewRuns
		mu.Unlock()
		if runs == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	runs := reviewRuns
	mu.Unlock()
	if runs != 1 {
		t.Fatalf("review runs = %d, want exactly one review for the fix round", runs)
	}

	// A round that moved nothing must not queue a second review.
	srv.queueReviewAfterFix("acme/widget#181", "abcdef0123456789abcdef0123456789abcdef01")
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	runs = reviewRuns
	mu.Unlock()
	if runs != 1 {
		t.Fatalf("review runs = %d, want no review for a round that changed nothing", runs)
	}
}

// TestCommentTriggeredReviewReachesTerminalStateAfterDeniedToolCall asserts
// a review whose tool call is denied ends the run and leaves the delivery in a
// terminal state instead of hanging.
func TestCommentTriggeredReviewReachesTerminalStateAfterDeniedToolCall(t *testing.T) {
	var calls int64
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		return errors.New("permission denied: no handler")
	}
	reviewEp := config.EndpointConfig{
		Name:           "github-widget-review",
		Path:           "/review",
		Secret:         "s3cret",
		Platform:       "discord",
		ChannelID:      "chan-1",
		Prompt:         "review",
		Workflow:       "review",
		Thread:         true,
		Repository:     "acme/widget",
		CommentTrigger: []string{"please re-review"},
		Limits:         &config.EndpointLimits{MaxRunsPerPR: 3, Window: time.Hour},
		Admit:          defaultCatchAllAdmit(),
	}
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{reviewEp}),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "denied.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { return "msg-1", nil })
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error { return nil })
	srv.SetNotifier(func(_ context.Context, _, _, _ string) error { return nil })
	setOpenPRState(srv)
	srv.retryBackoffs = map[failureClass][]time.Duration{
		failureBuild: {10 * time.Millisecond},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	body := `{"action":"created","repository":{"full_name":"acme/widget"},"issue":{"number":181,"state":"open","pull_request":{"html_url":"https://github.com/acme/widget/pull/181"}},"comment":{"body":"please re-review","user":{"login":"kumasct"}}}`
	if resp := post(t, ts.URL+"/review?secret=s3cret", "del-denied", "issue_comment", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-denied", store.WebhookStatusFailed)
	if got := atomic.LoadInt64(&calls); got < 1 {
		t.Fatalf("executor calls = %d, want the denied run to have run", got)
	}
}

// TestWatchdogAbortsWorkspaceLessRunWithReportedWorkdir asserts a delivery
// without a git workspace still reaches its agent: the run reports the
// workdir the agent runs in, and the watchdog aborts that session.
func TestWatchdogAbortsWorkspaceLessRunWithReportedWorkdir(t *testing.T) {
	logs := captureServerLogs(t)
	srv, _ := newWatchdogServer(t, func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	})
	srv.SetDispatcherStuckAfter(time.Minute)
	srv.SetDispatcherWatchdogInterval(time.Hour)

	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	item := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-no-workspace", eventType: "pull_request", payloadHash: "hash-no-workspace", prKey: "acme/widget#7"}
	d := &dispatcher{server: srv, key: "pr:acme/widget#7", lanes: makePRLanes(8)}
	d.setActive(item)
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()
	d.mu.Lock()
	d.lastActivity = time.Now().Add(-10 * time.Minute)
	d.activeStartedAt = time.Now().Add(-10 * time.Minute)
	d.mu.Unlock()
	run := srv.beginActiveRun(item.prKey, ep.Name, item.deliveryID, "")
	run.setSession("ses-no-workspace-1")
	// The delivery has no lease: the agent reports the workdir it runs in.
	run.setWorkdir("/tmp/widget/agent")

	abortedWorkdir := ""
	srv.SetSessionLiveness(func(ctx context.Context, sessionID, workdir string) (bool, error) {
		return true, nil
	})
	srv.SetSessionAborter(func(ctx context.Context, sessionID, workdir string) error {
		abortedWorkdir = workdir
		return nil
	})

	if n := srv.sweepStuckDispatchers(time.Now()); n != 1 {
		t.Fatalf("sweep handled %d dispatchers, want 1", n)
	}
	if abortedWorkdir != "/tmp/widget/agent" {
		t.Fatalf("aborted workdir = %q, want the workdir the agent reported", abortedWorkdir)
	}
	if len(logs.all("webhook: dispatcher stuck, session aborted")) != 1 {
		t.Fatal("the watchdog must report the aborted session")
	}
}
