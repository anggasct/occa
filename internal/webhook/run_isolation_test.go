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

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func (h *logCapture) all(message string) []capturedLogRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []capturedLogRecord
	for _, record := range h.records {
		if record.msg == message {
			out = append(out, record)
		}
	}
	return out
}

func attrValue(record capturedLogRecord, key string) string {
	value, ok := record.attrs[key]
	if !ok {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

func newIsolationServer(t *testing.T, exec Executor) (*Server, *store.SQLiteStore, *surfaceRecorder, string) {
	t.Helper()
	fixEp := prTestEndpoint("fix", "/fix", config.AutomationFull)
	fixEp.Workspace = config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/tmp/widget", Mode: config.WorkspaceModeIsolated}
	fixEp.Repository = "github.com/acme/widget"
	mergedEp := prTestEndpoint("merged", "/merged", config.AutomationFull)
	mergedEp.Repository = "github.com/acme/widget"
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{fixEp, mergedEp}),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	setOpenPRState(srv)
	rec := &surfaceRecorder{}
	srv.SetSenderWithButtons(func(_ context.Context, _, channelID, text string, buttons []channel.Button) (string, error) {
		rec.add(&rec.sent, surfaceMessage{channelID: channelID, messageID: "parent-1", text: text, buttons: buttons})
		return "parent-1", nil
	})
	srv.SetEditorWithButtons(func(_ context.Context, _, channelID, messageID, text string, buttons []channel.Button) error {
		rec.add(&rec.edited, surfaceMessage{channelID: channelID, messageID: messageID, text: text, buttons: buttons})
		return nil
	})
	srv.SetSender(func(_ context.Context, _, channelID, text string) (string, error) {
		rec.add(&rec.threadSent, surfaceMessage{channelID: channelID, messageID: "status-1", text: text})
		return "status-1", nil
	})
	srv.SetEditor(func(_ context.Context, _, channelID, messageID, text string) error {
		rec.add(&rec.edited, surfaceMessage{channelID: channelID, messageID: messageID, text: text})
		return nil
	})
	srv.SetNotifier(func(_ context.Context, _, channelID, text string) error {
		rec.add(&rec.notified, surfaceMessage{channelID: channelID, text: text})
		return nil
	})
	srv.SetWorkspaceResolver(workspaceResolverFunc(func(ctx context.Context, req WorkspaceRequest) (*WorkspaceLease, error) {
		return &WorkspaceLease{Path: "/tmp/widget/worktree", Mode: req.Mode}, nil
	}))
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return srv, st, rec, ts.URL
}

func TestWatchdogAbortsSessionBeforeKeyRelease(t *testing.T) {
	logs := captureServerLogs(t)
	srv, _ := newWatchdogServer(t, func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	})
	srv.SetDispatcherStuckAfter(time.Minute)
	srv.SetDispatcherWatchdogInterval(time.Hour)
	srv.SetSessionLiveness(func(ctx context.Context, sessionID, workdir string) (bool, error) {
		return false, nil
	})

	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	item := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-stuck-abort", eventType: "pull_request", payloadHash: "hash-abort", prKey: "acme/widget#7"}
	d := &dispatcher{server: srv, key: "pr:acme/widget#7", lanes: makePRLanes(8)}
	d.setActive(item)
	d.mu.Lock()
	d.lastActivity = time.Now().Add(-10 * time.Minute)
	d.activeStartedAt = time.Now().Add(-10 * time.Minute)
	d.mu.Unlock()
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()
	run := srv.beginActiveRun(item.prKey, ep.Name, item.deliveryID, "/tmp/widget/worktree")
	run.setSession("ses-stuck-1")
	srv.TaskRegistry().Upsert("anggasct/occa", "fix/watchdog", "root-1", "thread-1", TaskDispatched)

	var mu sync.Mutex
	var order []string
	var card string
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		mu.Lock()
		order = append(order, "card")
		card = text
		mu.Unlock()
		return nil
	})
	keyHeldAtAbort := false
	srv.SetSessionAborter(func(ctx context.Context, sessionID, workdir string) error {
		mu.Lock()
		order = append(order, "abort")
		mu.Unlock()
		srv.dispatchMu.Lock()
		keyHeldAtAbort = srv.dispatchers[d.key] == d
		srv.dispatchMu.Unlock()
		return nil
	})

	if n := srv.sweepStuckDispatchers(time.Now()); n != 1 {
		t.Fatalf("sweep handled %d dispatchers, want 1", n)
	}
	mu.Lock()
	joined := strings.Join(order, ",")
	mu.Unlock()
	if joined != "abort,card" {
		t.Fatalf("session must be aborted before the card and the key release, order = %q", joined)
	}
	if !keyHeldAtAbort {
		t.Fatal("the key must still be held while the session is aborted")
	}
	if !strings.Contains(card, "aborted the agent session") {
		t.Fatalf("stuck card must state the session abort: %q", card)
	}
	records := logs.all("webhook: dispatcher stuck, key released")
	if len(records) != 1 {
		t.Fatalf("stuck log lines = %d, want 1", len(records))
	}
	if got := attrValue(records[0], "session_abort_ok"); got != "true" {
		t.Fatalf("session_abort_ok = %q, want true", got)
	}
	if got := attrValue(records[0], "session_id"); got != "ses-stuck-1" {
		t.Fatalf("session_id = %q, want ses-stuck-1", got)
	}
	srv.dispatchMu.Lock()
	released := srv.dispatchers[d.key] != d
	srv.dispatchMu.Unlock()
	if !released {
		t.Fatal("the key must be released after the abort")
	}
}

func TestWatchdogReleasesKeyWhenAbortFails(t *testing.T) {
	logs := captureServerLogs(t)
	srv, st := newWatchdogServer(t, func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	})
	srv.SetDispatcherStuckAfter(time.Minute)
	srv.SetDispatcherWatchdogInterval(time.Hour)
	srv.SetSessionLiveness(func(ctx context.Context, sessionID, workdir string) (bool, error) {
		return false, nil
	})

	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	item := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-abort-fail", eventType: "pull_request", payloadHash: "hash-abort-fail", prKey: "acme/widget#7"}
	d := &dispatcher{server: srv, key: "pr:acme/widget#7", lanes: makePRLanes(8)}
	d.setActive(item)
	d.mu.Lock()
	d.lastActivity = time.Now().Add(-10 * time.Minute)
	d.activeStartedAt = time.Now().Add(-10 * time.Minute)
	d.mu.Unlock()
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()
	run := srv.beginActiveRun(item.prKey, ep.Name, item.deliveryID, "/tmp/widget/worktree")
	run.setSession("ses-stuck-2")

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

	var card string
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		card = text
		return nil
	})
	srv.TaskRegistry().Upsert("anggasct/occa", "fix/watchdog", "root-1", "thread-1", TaskDispatched)
	srv.SetSessionAborter(func(ctx context.Context, sessionID, workdir string) error {
		return errors.New("abort refused")
	})

	if n := srv.sweepStuckDispatchers(time.Now()); n != 1 {
		t.Fatalf("sweep handled %d dispatchers, want 1", n)
	}
	srv.dispatchMu.Lock()
	released := srv.dispatchers[d.key] != d
	srv.dispatchMu.Unlock()
	if !released {
		t.Fatal("a failed abort must still release the key")
	}
	records := logs.all("webhook: dispatcher stuck, key released")
	if len(records) != 1 {
		t.Fatalf("stuck log lines = %d, want 1", len(records))
	}
	if got := attrValue(records[0], "session_abort_ok"); got != "false" {
		t.Fatalf("session_abort_ok = %q, want false", got)
	}
	failed, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, item.deliveryID)
	if err != nil || failed == nil || failed.Status != store.WebhookStatusFailed {
		t.Fatalf("stuck delivery receipt = %+v %v; want failed", failed, err)
	}
	if !strings.Contains(failed.ErrorSummary, "stuck") || !strings.Contains(failed.ErrorSummary, "session abort failed") {
		t.Fatalf("stuck delivery summary = %q", failed.ErrorSummary)
	}
	if strings.Contains(failed.ErrorSummary, "session aborted") {
		t.Fatalf("a failed abort must not be reported as an aborted session: %q", failed.ErrorSummary)
	}
	if !strings.Contains(card, "could not abort the agent session") {
		t.Fatalf("stuck card must report the failed abort: %q", card)
	}
}

func TestWatchdogKeepsLiveRunAlive(t *testing.T) {
	logs := captureServerLogs(t)
	srv, _ := newWatchdogServer(t, func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	})
	srv.SetDispatcherStuckAfter(time.Minute)
	srv.SetDispatcherWatchdogInterval(time.Hour)
	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	item := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-live", eventType: "pull_request", payloadHash: "hash-live", prKey: "acme/widget#7"}
	d := &dispatcher{server: srv, key: "pr:acme/widget#7", lanes: makePRLanes(8)}
	d.setActive(item)
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()

	d.touch()
	if n := srv.sweepStuckDispatchers(time.Now()); n != 0 {
		t.Fatalf("a run with a fresh heartbeat must not be declared stuck, sweep = %d", n)
	}

	d.mu.Lock()
	d.lastActivity = time.Now().Add(-10 * time.Minute)
	d.activeStartedAt = time.Now().Add(-10 * time.Minute)
	d.mu.Unlock()
	run := srv.beginActiveRun(item.prKey, ep.Name, item.deliveryID, "/tmp/widget/worktree")
	run.setSession("ses-live-1")
	srv.SetSessionLiveness(func(ctx context.Context, sessionID, workdir string) (bool, error) {
		return true, nil
	})
	aborted := false
	srv.SetSessionAborter(func(ctx context.Context, sessionID, workdir string) error {
		aborted = true
		return nil
	})
	if n := srv.sweepStuckDispatchers(time.Now()); n != 1 {
		t.Fatalf("a live but silent agent must fall back to the elapsed rule, sweep = %d", n)
	}
	if !aborted {
		t.Fatal("the watchdog must abort the session it declares stuck")
	}
	if len(logs.all("webhook: dispatcher stuck, session aborted")) != 1 {
		t.Fatal("the watchdog must report the aborted session")
	}
}

func TestWatchdogAbortedDeliveryIsFailedAndNotRerun(t *testing.T) {
	logs := captureServerLogs(t)
	var calls int64
	release := make(chan struct{})
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		if workCtx.ReportSession != nil {
			workCtx.ReportSession("ses-watchdog-4")
		}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return errors.New("session aborted")
	}
	srv, st, _, url := newIsolationServer(t, exec)
	srv.SetDispatcherStuckAfter(50 * time.Millisecond)
	srv.SetDispatcherWatchdogInterval(time.Hour)
	srv.SetSessionLiveness(func(ctx context.Context, sessionID, workdir string) (bool, error) {
		return false, nil
	})
	srv.SetSessionAborter(func(ctx context.Context, sessionID, workdir string) error {
		close(release)
		return nil
	})

	if resp := post(t, url+"/fix?secret=s3cret", "del-stuck-4", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee4")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-fix", "del-stuck-4", store.WebhookStatusProcessing)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n := srv.sweepStuckDispatchers(time.Now()); n == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	failed, err := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-fix", "del-stuck-4")
	if err != nil || failed == nil || failed.Status != store.WebhookStatusFailed {
		t.Fatalf("watchdog delivery receipt = %+v %v; want failed", failed, err)
	}
	if !strings.Contains(failed.ErrorSummary, "stuck") || !strings.Contains(failed.ErrorSummary, "session aborted") {
		t.Fatalf("watchdog delivery summary = %q", failed.ErrorSummary)
	}
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("a watchdog aborted delivery must not run again, executor calls = %d", got)
	}
	if len(logs.all("webhook: delivery aborted by watchdog")) == 0 {
		t.Fatal("the watchdog abort must be logged")
	}
}

func TestRetryRefusedWhenLivePRMerged(t *testing.T) {
	var calls int64
	release := make(chan struct{})
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		if workCtx.DeliveryID == "del-merged-retry" {
			<-release
		}
		return errors.New("agent exploded")
	}
	srv, st, _, url := newIsolationServer(t, exec)
	setPRState(srv, PRStateMerged, nil)

	if resp := post(t, url+"/fix?secret=s3cret", "del-merged-retry", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee5")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-fix", "del-merged-retry", store.WebhookStatusProcessing)
	if resp := post(t, url+"/fix?secret=s3cret", "del-queued-retry", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee6")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post queued: status %d", resp.StatusCode)
	}
	close(release)

	waitForDeliveryStatus(t, st, "github-widget-fix", "del-merged-retry", store.WebhookStatusSkipped)
	receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-fix", "del-merged-retry")
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v", err)
	}
	if !strings.Contains(receipt.ErrorSummary, "PR already merged") {
		t.Fatalf("refused retry summary = %q, want PR already merged", receipt.ErrorSummary)
	}
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("a merged PR must not run again, executor calls = %d", got)
	}
	if queued, qerr := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-fix", "del-queued-retry"); qerr != nil || queued == nil || queued.Status != store.WebhookStatusSkipped || !strings.Contains(queued.ErrorSummary, "queue drained") {
		t.Fatalf("the drained queue item must settle as skipped, queued receipt = %+v %v", queued, qerr)
	}
	run, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	if err != nil || run == nil {
		t.Fatalf("pr row = %+v %v", run, err)
	}
	if run.RetryCount != 0 {
		t.Fatalf("a refused retry must not consume the retry budget, retry_count = %d", run.RetryCount)
	}
}

func TestRetryRefusedWhenLivePRClosed(t *testing.T) {
	var calls int64
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		return errors.New("agent exploded")
	}
	srv, st, _, url := newIsolationServer(t, exec)
	setPRState(srv, PRStateClosed, nil)

	if resp := post(t, url+"/fix?secret=s3cret", "del-closed-retry", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee7")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-fix", "del-closed-retry", store.WebhookStatusSkipped)
	receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-fix", "del-closed-retry")
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v", err)
	}
	if !strings.Contains(receipt.ErrorSummary, "PR already closed") {
		t.Fatalf("refused retry summary = %q, want PR already closed", receipt.ErrorSummary)
	}
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("a closed PR must not run again, executor calls = %d", got)
	}
}

func TestRetryFailsClosedWhenLivePRStateUnknown(t *testing.T) {
	logs := captureServerLogs(t)
	var calls int64
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		return errors.New("agent exploded")
	}
	srv, st, _, url := newIsolationServer(t, exec)
	setPRState(srv, "", errors.New("gh unreachable"))

	if resp := post(t, url+"/fix?secret=s3cret", "del-unknown-state", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee8")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-fix", "del-unknown-state", store.WebhookStatusFailed)
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("an unresolvable PR state must not re-run, executor calls = %d", got)
	}
	run, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	if err != nil || run == nil || run.Status != store.PRStatusBlocked {
		t.Fatalf("pr row = %+v %v; want blocked", run, err)
	}
	if len(logs.all("webhook: retry refused, PR state unresolvable")) == 0 {
		t.Fatal("an unresolvable PR state must be logged")
	}
}

func TestLivePRStateResolvesPerDecision(t *testing.T) {
	var lookups int64
	srv, st, _, _ := newIsolationServer(t, func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	})
	srv.SetPRStateResolver(func(ctx context.Context, repo, number string) (string, error) {
		atomic.AddInt64(&lookups, 1)
		return PRStateOpen, nil
	})
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "", ""); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	srv.retryBackoffs = map[failureClass][]time.Duration{
		failureBuild: {50 * time.Millisecond},
	}
	ep := prTestEndpoint("fix", "/fix", config.AutomationFull)
	for i := 0; i < 3; i++ {
		srv.planRetry(ep, "acme/widget#7", 0, errors.New("boom"), &WebhookWorkContext{}, false, false)
	}
	if got := atomic.LoadInt64(&lookups); got != 3 {
		t.Fatalf("live PR state lookups = %d, want one per retry decision", got)
	}
}

func TestLivePRStateDecisionDoesNotReuseEarlierReading(t *testing.T) {
	var lookups int64
	var state atomic.Value
	state.Store(PRStateOpen)
	srv, st, _, _ := newIsolationServer(t, func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	})
	srv.SetPRStateResolver(func(ctx context.Context, repo, number string) (string, error) {
		atomic.AddInt64(&lookups, 1)
		return state.Load().(string), nil
	})
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "", ""); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	srv.retryBackoffs = map[failureClass][]time.Duration{
		failureBuild: {50 * time.Millisecond},
	}
	ep := prTestEndpoint("fix", "/fix", config.AutomationFull)
	srv.planRetry(ep, "acme/widget#7", 0, errors.New("boom"), &WebhookWorkContext{}, false, false)
	if got := atomic.LoadInt64(&lookups); got != 1 {
		t.Fatalf("live PR state lookups = %d, want 1 for the first decision", got)
	}
	state.Store(PRStateMerged)
	row, err := st.PRQueueRepo().Get(ctx, "acme/widget#7")
	if err != nil || row == nil {
		t.Fatalf("pr row = %+v %v", row, err)
	}
	if srv.planRetry(ep, "acme/widget#7", 0, errors.New("boom"), &WebhookWorkContext{}, false, false) {
		t.Fatal("a retry on a merged PR must be refused")
	}
	if got := atomic.LoadInt64(&lookups); got != 2 {
		t.Fatalf("live PR state lookups = %d, want the next decision to resolve again", got)
	}
}

func TestMergedDeliveryRunsWithoutAgentTurn(t *testing.T) {
	logs := captureServerLogs(t)
	turn := make(chan struct{})
	var mu sync.Mutex
	var turnPRKey, turnThreadID, turnRootID string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		mu.Lock()
		turnPRKey = workCtx.PRKey
		turnThreadID = workCtx.ThreadID
		turnRootID = workCtx.RootMessageID
		mu.Unlock()
		close(turn)
		return nil
	}
	srv, st, _, url := newIsolationServer(t, exec)
	setPRState(srv, PRStateMerged, nil)

	if resp := post(t, url+"/merged?secret=s3cret", "del-merged-lane", "pull_request", mergedBody(7)); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-merged", "del-merged-lane", store.WebhookStatusCompleted)
	select {
	case <-turn:
	case <-time.After(5 * time.Second):
		t.Fatal("the post-merge work never ran")
	}
	mu.Lock()
	prKey, threadID, rootID := turnPRKey, turnThreadID, turnRootID
	mu.Unlock()
	if prKey != "" || threadID != "" || rootID != "" {
		t.Fatalf("the post-merge work must not run as a queue-lane turn: pr_key=%q thread=%q root=%q", prKey, threadID, rootID)
	}
	rec, ok := srv.tasks.Lookup("acme/widget", "feat/x")
	if !ok || !isTaskTerminal(rec.Status) {
		t.Fatalf("task record = %+v %v; want a terminal status", rec, ok)
	}
	run, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	if err != nil || run == nil {
		t.Fatalf("pr row = %+v %v", run, err)
	}
	if run.Status != store.PRStatusDrained {
		t.Fatalf("pr queue status = %q, want drained", run.Status)
	}
	if run.ActiveSession != "" || run.ActiveDelivery != 0 {
		t.Fatalf("the merged delivery must hold no execution key: %+v", run)
	}
	if len(logs.all("webhook: merged delivery closed without agent turn")) != 1 {
		t.Fatal("the merged delivery must be recorded as closed without an agent turn")
	}
	fallbacks := logs.all("webhook: merged lane falls back to channel root")
	if len(fallbacks) != 1 {
		t.Fatalf("channel-root fallback lines = %d, want one", len(fallbacks))
	}
	if reason := attrValue(fallbacks[0], "reason"); reason != "no PR thread binding" {
		t.Fatalf("fallback reason = %q, want no PR thread binding", reason)
	}
}

func TestMergedDeliveryAbortsActiveSession(t *testing.T) {
	logs := captureServerLogs(t)
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	}
	srv, st, _, url := newIsolationServer(t, exec)
	setPRState(srv, PRStateMerged, nil)
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "", ""); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	if err := st.PRQueueRepo().SetActive(ctx, "acme/widget#7", "fix", "ses-legacy-1", 0); err != nil {
		t.Fatalf("set active: %v", err)
	}
	var abortedSession, abortedWorkdir string
	srv.SetSessionAborter(func(ctx context.Context, sessionID, workdir string) error {
		abortedSession = sessionID
		abortedWorkdir = workdir
		return nil
	})

	if resp := post(t, url+"/merged?secret=s3cret", "del-merged-abort", "pull_request", mergedBody(7)); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-merged", "del-merged-abort", store.WebhookStatusCompleted)
	if abortedSession != "ses-legacy-1" {
		t.Fatalf("aborted session = %q, want ses-legacy-1", abortedSession)
	}
	if abortedWorkdir == "" {
		t.Fatal("the abort needs a workdir")
	}
	records := logs.all("webhook: merged delivery aborted active session")
	if len(records) != 1 {
		t.Fatalf("abort outcome log lines = %d, want 1", len(records))
	}
	if got := attrValue(records[0], "session_abort_ok"); got != "true" {
		t.Fatalf("session_abort_ok = %q, want true", got)
	}
}

func TestMergedDocsUpdateWritesNoSurface(t *testing.T) {
	turn := make(chan struct{})
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		close(turn)
		return nil
	}
	srv, _, rec, _ := newIsolationServer(t, exec)
	setPRState(srv, PRStateMerged, nil)
	ep := srv.endpoints["/merged"]
	ctx := context.Background()
	prKey := "acme/widget#7"
	task := srv.tasks.Upsert("acme/widget", "feat/x", "parent-1", "thread-9", TaskClaimed)
	srv.tasks.LinkPRThread(prKey, task)
	envelope := normalizeWebhook([]byte(mergedBody(7)), "pull_request", "del-merged-surface", srv.verdicts, false, "", ep.CommentTrigger)
	created, err := srv.deliveries.Create(ctx, store.WebhookDelivery{Endpoint: ep.Name, DeliveryID: "del-merged-surface", EventType: "pull_request", PayloadHash: "hash-merged-surface"})
	if err != nil || !created {
		t.Fatalf("create receipt: %v", err)
	}
	receipt, err := srv.deliveries.Get(ctx, ep.Name, "del-merged-surface")
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v", err)
	}
	item := dispatchItem{ep: ep, body: []byte(mergedBody(7)), deliveryID: "del-merged-surface", eventType: "pull_request", payloadHash: "hash-merged-surface", prKey: prKey}
	srv.handleMergedDelivery(item, receipt, envelope, nil)
	sentBefore, editedBefore, _, _ := rec.snapshot()

	select {
	case <-turn:
	case <-time.After(5 * time.Second):
		t.Fatal("the post-merge work never ran")
	}
	time.Sleep(200 * time.Millisecond)
	sent, edited, _, _ := rec.snapshot()
	parentWrites := append(append([]surfaceMessage(nil), sent...), edited...)
	if len(parentWrites) != 1 {
		t.Fatalf("parent writes = %d, want the single terminal phase line", len(parentWrites))
	}
	if !strings.Contains(parentWrites[0].text, "merged") {
		t.Fatalf("parent line = %q, want the merged phase", parentWrites[0].text)
	}
	if len(rec.threadSent) != 1 {
		t.Fatalf("thread state line writes = %d, want the single terminal state line", len(rec.threadSent))
	}
	if len(sent) != len(sentBefore) || len(edited) != len(editedBefore) {
		t.Fatalf("the post-merge work must not write to the surface: %+v %+v", sent, edited)
	}
}

func TestTerminalPRRefusesSurfaceWrites(t *testing.T) {
	logs := captureServerLogs(t)
	srv, _, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	ctx := context.Background()
	prKey := "acme/widget#181"
	envelope := mergeSurfaceEnvelope()

	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseMerged)
	srv.ensureThreadStatus(ctx, ep.Platform, ep.ChannelID, "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{Phase: PhaseMerged})
	sent, edits, _, _ := rec.snapshot()
	parentsBefore := len(sent)
	threadBefore := len(rec.threadSent)
	editsBefore := len(edits)

	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseReview)
	srv.ensureThreadStatus(ctx, ep.Platform, ep.ChannelID, "thread-9", "acme/widget", "feat/x", prKey, ThreadStatusUpdate{Phase: PhaseReview})

	sent, edits, _, _ = rec.snapshot()
	if len(sent) != parentsBefore {
		t.Fatalf("a terminal PR must not write the parent again: %+v", sent)
	}
	if len(rec.threadSent) != threadBefore {
		t.Fatalf("a terminal PR must not write the thread state line again: %+v", rec.threadSent)
	}
	if len(edits) != editsBefore {
		t.Fatalf("a terminal PR must not edit the parent or the state line: %+v", edits)
	}
	records := logs.all("webhook: write refused, PR terminal")
	if len(records) != 2 {
		t.Fatalf("refused write log lines = %d, want the parent and the state line", len(records))
	}
	surfaces := map[string]bool{}
	for _, record := range records {
		surfaces[attrValue(record, "surface")] = true
	}
	if !surfaces["parent"] || !surfaces["thread status"] {
		t.Fatalf("refused write surfaces = %v, want parent and thread status", surfaces)
	}
}

func TestPR199IncidentShape(t *testing.T) {
	logs := captureServerLogs(t)
	var calls int64
	var mu sync.Mutex
	turns := map[string]int{}
	release := make(chan struct{})
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		mu.Lock()
		turns[workCtx.DeliveryID]++
		mu.Unlock()
		if workCtx.ReportSession != nil {
			workCtx.ReportSession("ses-incident-199")
		}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return errors.New("provider stall")
	}
	srv, st, rec, url := newIsolationServer(t, exec)
	srv.SetDispatcherStuckAfter(80 * time.Millisecond)
	srv.SetDispatcherWatchdogInterval(time.Hour)
	srv.SetSessionLiveness(func(ctx context.Context, sessionID, workdir string) (bool, error) {
		return false, nil
	})

	var prState atomic.Value
	prState.Store(PRStateOpen)
	srv.SetPRStateResolver(func(ctx context.Context, repo, number string) (string, error) {
		state, _ := prState.Load().(string)
		if state == "" {
			return "", errors.New("gh unreachable")
		}
		return state, nil
	})
	srv.SetSessionAborter(func(ctx context.Context, sessionID, workdir string) error {
		return nil
	})

	if resp := post(t, url+"/fix?secret=s3cret", "del-incident-199", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee9")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post fix: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-fix", "del-incident-199", store.WebhookStatusProcessing)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n := srv.sweepStuckDispatchers(time.Now()); n == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	failed, err := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-fix", "del-incident-199")
	if err != nil || failed == nil || failed.Status != store.WebhookStatusFailed {
		t.Fatalf("incident receipt = %+v %v; want failed", failed, err)
	}
	if !strings.Contains(failed.ErrorSummary, "stuck") || !strings.Contains(failed.ErrorSummary, "session aborted") {
		t.Fatalf("incident summary = %q", failed.ErrorSummary)
	}

	prState.Store(PRStateMerged)
	if resp := post(t, url+"/merged?secret=s3cret", "del-incident-merged", "pull_request", mergedBody(7)); resp.StatusCode != http.StatusOK {
		t.Fatalf("post merged: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-merged", "del-incident-merged", store.WebhookStatusCompleted)
	waitForSurfacePhase(t, srv, "acme/widget#7", PhaseMerged)

	sentBefore, editedBefore, _, _ := rec.snapshot()
	threadBefore := len(rec.threadSent)

	close(release)
	waitForDeliveryStatus(t, st, "github-widget-fix", "del-incident-199", store.WebhookStatusFailed)
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	fixTurns := turns["del-incident-199"]
	mu.Unlock()
	if fixTurns != 1 {
		t.Fatalf("no second agent turn may start on a merged PR, fix turns = %d", fixTurns)
	}
	records := logs.all("webhook: retry refused, PR terminal")
	if len(records) == 0 {
		t.Fatal("the retry must be refused for a merged PR")
	}
	if got := attrValue(records[0], "pr_state"); got != PRStateMerged {
		t.Fatalf("refused retry pr_state = %q, want merged", got)
	}
	sentAfter, editedAfter, _, _ := rec.snapshot()
	if len(sentAfter) != len(sentBefore) || len(editedAfter) != len(editedBefore) {
		t.Fatalf("no surface write may follow the terminal state: sent=%d edited=%d", len(sentAfter), len(editedAfter))
	}
	if len(rec.threadSent) != threadBefore {
		t.Fatalf("no thread state line may follow the terminal state: %d", len(rec.threadSent))
	}
}

func waitForSurfacePhase(t *testing.T, srv *Server, prKey string, want Phase) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, phase, _, ok := srv.prSurfaceState(prKey); ok && phase == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, phase, _, _ := srv.prSurfaceState(prKey)
	t.Fatalf("surface phase = %q, want %q", phase, want)
}

func waitForLog(t *testing.T, logs *logCapture, message string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(logs.all(message)) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("log line %q never appeared", message)
}

func TestRetryExecutionRefusedWhenPRMergedAfterScheduling(t *testing.T) {
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
		return state.Load().(string), nil
	})
	srv.retryBackoffs = map[failureClass][]time.Duration{
		failureBuild: {2 * time.Second},
	}

	if resp := post(t, url+"/fix?secret=s3cret", "del-retry-merged", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffeea")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForLog(t, logs, "webhook: retry scheduled")
	state.Store(PRStateMerged)

	waitForDeliveryStatus(t, st, "github-widget-fix", "del-retry-merged", store.WebhookStatusSkipped)
	receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-fix", "del-retry-merged")
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v", err)
	}
	if !strings.Contains(receipt.ErrorSummary, "PR already merged") {
		t.Fatalf("refused retry summary = %q, want PR already merged", receipt.ErrorSummary)
	}
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("a merged PR must not start a second agent turn, executor calls = %d", got)
	}
}

func TestWatchdogKeepsMutableWorkspaceRunAlive(t *testing.T) {
	logs := captureServerLogs(t)
	srv, _ := newWatchdogServer(t, func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	})
	srv.SetDispatcherStuckAfter(time.Minute)
	srv.SetDispatcherWatchdogInterval(time.Hour)

	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	item := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-mutable", eventType: "pull_request", payloadHash: "hash-mutable", prKey: "acme/widget#7"}
	d := &dispatcher{server: srv, key: "pr:acme/widget#7", lanes: makePRLanes(8)}
	d.setActive(item)
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()
	run := srv.beginActiveRun(item.prKey, ep.Name, item.deliveryID, "/tmp/widget/shared")
	run.setSession("ses-mutable-1")

	aborted := false
	srv.SetSessionAborter(func(ctx context.Context, sessionID, workdir string) error {
		aborted = true
		return nil
	})
	srv.SetSessionLiveness(func(ctx context.Context, sessionID, workdir string) (bool, error) {
		return true, nil
	})

	// A live session with a fresh heartbeat is not stuck, in a shared
	// workspace as well as in an isolated one.
	d.touch()
	if n := srv.sweepStuckDispatchers(time.Now()); n != 0 {
		t.Fatalf("a live agent on a shared workspace must not be declared stuck, sweep = %d", n)
	}
	if aborted {
		t.Fatal("a live agent must not be aborted")
	}
	if len(logs.all("webhook: dispatcher stuck, key released")) != 0 {
		t.Fatal("a live run must not log a stuck dispatcher")
	}

	// A live session that stopped reporting progress falls back to the
	// elapsed rule: the watchdog declares it stuck and aborts it.
	d.mu.Lock()
	d.lastActivity = time.Now().Add(-10 * time.Minute)
	d.activeStartedAt = time.Now().Add(-10 * time.Minute)
	d.mu.Unlock()
	if n := srv.sweepStuckDispatchers(time.Now()); n != 1 {
		t.Fatalf("a silent agent on a shared workspace must be declared stuck, sweep = %d", n)
	}
	if !aborted {
		t.Fatal("the watchdog must abort the session it declares stuck")
	}
}

func TestWatchdogDoesNotAbortRunWithUnknownLiveness(t *testing.T) {
	logs := captureServerLogs(t)
	srv, st := newWatchdogServer(t, func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	})
	srv.SetDispatcherStuckAfter(time.Minute)
	srv.SetDispatcherWatchdogInterval(time.Hour)

	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	item := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-unknown-liveness", eventType: "pull_request", payloadHash: "hash-unknown", prKey: "acme/widget#7"}
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
	run.setSession("ses-unknown-1")

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

	aborted := false
	srv.SetSessionAborter(func(ctx context.Context, sessionID, workdir string) error {
		aborted = true
		return nil
	})

	if n := srv.sweepStuckDispatchers(time.Now()); n != 1 {
		t.Fatalf("sweep handled %d dispatchers, want 1", n)
	}
	if aborted {
		t.Fatal("a run whose liveness could not be established must not be aborted")
	}
	after, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, item.deliveryID)
	if err != nil || after == nil {
		t.Fatalf("get receipt: %v", err)
	}
	if after.Status != store.WebhookStatusFailed {
		t.Fatalf("a stuck delivery must reach a terminal receipt: %+v", after)
	}
	if strings.Contains(after.ErrorSummary, "session aborted") {
		t.Fatalf("a run with unknown liveness must not claim an abort: %q", after.ErrorSummary)
	}
	if !strings.Contains(after.ErrorSummary, "stuck") {
		t.Fatalf("stuck delivery summary = %q", after.ErrorSummary)
	}
	records := logs.all("webhook: dispatcher stuck, session not aborted, still running")
	if len(records) != 1 {
		t.Fatalf("session outcome log lines = %d, want 1", len(records))
	}
}

func TestWatchdogReceiptDoesNotClaimAbortWhenAbortImpossible(t *testing.T) {
	logs := captureServerLogs(t)
	srv, st := newWatchdogServer(t, func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	})
	srv.SetDispatcherStuckAfter(time.Minute)
	srv.SetDispatcherWatchdogInterval(time.Hour)
	srv.SetSessionLiveness(func(ctx context.Context, sessionID, workdir string) (bool, error) {
		return false, nil
	})

	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	item := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-no-abort", eventType: "pull_request", payloadHash: "hash-no-abort", prKey: "acme/widget#7"}
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
	run.setSession("ses-no-abort-1")

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

	if n := srv.sweepStuckDispatchers(time.Now()); n != 1 {
		t.Fatalf("sweep handled %d dispatchers, want 1", n)
	}
	failed, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, item.deliveryID)
	if err != nil || failed == nil || failed.Status != store.WebhookStatusFailed {
		t.Fatalf("stuck delivery receipt = %+v %v; want failed", failed, err)
	}
	if strings.Contains(failed.ErrorSummary, "session aborted") {
		t.Fatalf("a delivery must not claim an aborted session when no abort ran: %q", failed.ErrorSummary)
	}
	if !strings.Contains(failed.ErrorSummary, "stuck") {
		t.Fatalf("stuck delivery summary = %q", failed.ErrorSummary)
	}
	records := logs.all("webhook: dispatcher stuck, no session to abort")
	if len(records) != 1 {
		t.Fatalf("session outcome log lines = %d, want 1", len(records))
	}
	if got := attrValue(records[0], "session_still_running"); got != "true" {
		t.Fatalf("session_still_running = %q, want true", got)
	}
	if len(logs.all("webhook: dispatcher stuck, session aborted")) != 0 {
		t.Fatal("the log must not claim an aborted session when no abort ran")
	}
}
