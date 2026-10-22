package webhook

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func laneItem(workflow, deliveryID string, lane int, eventType, action string, prKey string) dispatchItem {
	return dispatchItem{
		ep:          config.EndpointConfig{Name: "github", Workflow: workflow, Platform: "telegram", ChannelID: "chat1"},
		body:        []byte(fmt.Sprintf(`{"action":%q}`, action)),
		deliveryID:  deliveryID,
		eventType:   eventType,
		payloadHash: deliveryID,
		prKey:       prKey,
		lane:        lane,
		actionClass: stateActionClass(eventType, action),
	}
}

func TestAdmissionVerdictEvictsStateEvent(t *testing.T) {
	srv, _, _ := newTestServerFull(t, nil)
	d := newDispatcher(srv, "pr:acme/widget#7")
	d.push(laneItem("custom", "state-1", 3, "pull_request", "opened", "acme/widget#7"))
	d.push(laneItem("custom", "state-2", 3, "pull_request", "opened", "acme/widget#7"))

	review := laneItem("review", "review-1", 1, "pull_request_review", "submitted", "acme/widget#7")
	decision := d.admit(review, 2, 2, 0)
	if !decision.admitted {
		t.Fatal("review refused a budget filled by state events")
	}
	if decision.evicted == nil || decision.evicted.deliveryID != "state-1" {
		t.Fatalf("oldest state event not evicted: %+v", decision.evicted)
	}
	d.push(review)
	if got := d.queueLen(); got > 2 {
		t.Fatalf("queue len = %d, want <= budget", got)
	}
}

func TestAdmissionStateShareCapsLane(t *testing.T) {
	srv, _, _ := newTestServerFull(t, nil)
	d := newDispatcher(srv, "pr:acme/widget#7")
	d.push(laneItem("custom", "state-1", 3, "pull_request", "opened", "acme/widget#7"))
	d.push(laneItem("custom", "state-2", 3, "check_suite", "", "acme/widget#7"))

	third := laneItem("custom", "state-3", 3, "issue_comment", "created", "acme/widget#7")
	if decision := d.admit(third, 8, 2, 8); decision.admitted {
		t.Fatal("state event admitted past its share while the budget had room")
	}
}

func TestAdmissionStateEventNeverEvictsVerdicts(t *testing.T) {
	srv, _, _ := newTestServerFull(t, nil)
	d := newDispatcher(srv, "pr:acme/widget#7")
	d.push(laneItem("review", "review-1", 1, "pull_request_review", "submitted", "acme/widget#7"))
	d.push(laneItem("fix", "fix-1", 2, "pull_request_review", "submitted", "acme/widget#7"))

	state := laneItem("custom", "state-1", 3, "pull_request", "opened", "acme/widget#7")
	if decision := d.admit(state, 2, 8, 0); decision.admitted {
		t.Fatal("state event admitted by evicting a verdict/fix")
	}
}

func TestAdmissionPriorityBorrowsOverflow(t *testing.T) {
	srv, _, _ := newTestServerFull(t, nil)
	d := newDispatcher(srv, "pr:acme/widget#7")
	d.push(laneItem("review", "review-1", 1, "pull_request_review", "submitted", "acme/widget#7"))
	d.push(laneItem("fix", "fix-1", 2, "pull_request_review", "submitted", "acme/widget#7"))

	if decision := d.admit(laneItem("review", "review-2", 1, "pull_request_review", "submitted", "acme/widget#7"), 2, 8, 1); !decision.admitted {
		t.Fatal("verdict refused inside the overflow window")
	}
	if decision := d.admit(laneItem("review", "review-3", 1, "pull_request_review", "submitted", "acme/widget#7"), 2, 8, 0); decision.admitted {
		t.Fatal("verdict admitted past the hard ceiling")
	}
}

func TestAdmissionCoalescesStateShapesOnly(t *testing.T) {
	srv, _, _ := newTestServerFull(t, nil)
	d := newDispatcher(srv, "pr:acme/widget#7")
	for i := 0; i < 5; i++ {
		item := laneItem("custom", fmt.Sprintf("state-%d", i), 3, "pull_request", "edited", "acme/widget#7")
		decision := d.admit(item, 8, 8, 0)
		if !decision.admitted {
			t.Fatalf("state event %d refused: %+v", i, decision)
		}
		d.push(item)
	}
	if got := d.laneLen(3); got != 1 {
		t.Fatalf("state lane len = %d, want 1 after coalescing", got)
	}
	for i := 0; i < 3; i++ {
		item := laneItem("custom", fmt.Sprintf("review-%d", i), 3, "pull_request_review", "submitted", "acme/widget#7")
		if decision := d.admit(item, 8, 8, 0); !decision.admitted {
			t.Fatalf("review event %d refused", i)
		}
		d.push(item)
	}
	if got := d.laneLen(3); got != 4 {
		t.Fatalf("review events were coalesced: lane len = %d, want 4", got)
	}
}

func TestAdmissionPopOrderUnchanged(t *testing.T) {
	srv, _, _ := newTestServerFull(t, nil)
	d := newDispatcher(srv, "pr:acme/widget#7")
	for _, lane := range []int{3, 2, 1, 0} {
		d.push(laneItem("custom", fmt.Sprintf("d-%d", lane), lane, "pull_request", "opened", "acme/widget#7"))
	}
	want := []int{0, 1, 2, 3}
	for _, lane := range want {
		item, ok := d.pop()
		if !ok {
			t.Fatalf("lane %d missing", lane)
		}
		if item.lane != lane {
			t.Fatalf("pop order: got lane %d, want %d", item.lane, lane)
		}
	}
	if _, ok := d.pop(); ok {
		t.Fatal("queue not empty")
	}
}

func TestRejectedDeliveryIsRecordedAndReplayable(t *testing.T) {
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	srv.SetWebhookReplayRepo(st.WebhookReplayRepo())
	release := make(chan struct{})
	defer close(release)
	srv.executor = func(ctx context.Context, _, _, _ string, _ *WebhookWorkContext) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var notices []string
	srv.SetNotifier(func(_ context.Context, _, _, text string) error {
		notices = append(notices, text)
		return nil
	})

	ep := srv.endpointsByName["github"]
	srv.maxQueuedPerKey = 1

	rejected := laneItem("custom", "delivery-rejected", 3, "pull_request", "edited", "")
	rejected.ep = ep
	if _, ok := srv.enqueue(ep, WebhookExecutionKey{}, rejected); ok {
		t.Fatal("delivery admitted past the state share")
	}

	ctx := context.Background()
	pending, err := st.WebhookReplayRepo().Count(ctx, store.WebhookReplayPending)
	if err != nil {
		t.Fatalf("count replay: %v", err)
	}
	if pending != 1 {
		t.Fatalf("pending replay rows = %d, want 1", pending)
	}
	delivery, err := st.WebhookDeliveryRepo().Get(ctx, "github", "delivery-rejected")
	if err != nil || delivery == nil {
		t.Fatalf("rejected delivery row missing: %v", err)
	}
	if delivery.Status != store.WebhookStatusRejected || delivery.ErrorSummary == "" {
		t.Fatalf("rejected delivery row = %+v", delivery)
	}
	if got := countRejectionNotices(notices); got != 1 {
		t.Fatalf("rejection notices = %d, want 1", got)
	}
	srv.recordRejection(ep, rejected, "state lane at capacity")
	if got := countRejectionNotices(notices); got != 1 {
		t.Fatalf("duplicate rejection posted %d notices, want 1", got)
	}
}

func countRejectionNotices(notices []string) int {
	count := 0
	for _, text := range notices {
		if strings.Contains(text, "WEBHOOK REJECTED") {
			count++
		}
	}
	return count
}

func TestReplayReenqueuesThroughAdmission(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	srv.SetWebhookReplayRepo(st.WebhookReplayRepo())
	srv.SetReplayChecker(func(context.Context, store.WebhookReplay) (replayDecision, error) {
		return replayGo, nil
	})
	now := time.Now().Unix()
	if _, _, err := st.WebhookReplayRepo().Create(context.Background(), store.WebhookReplay{
		Endpoint: "github", DeliveryID: "del-replay", EventType: "pull_request",
		Body: []byte(`{"action":"opened"}`), Reason: "state lane at capacity",
		NextAttemptAt: now - 1, ExpiresAt: now + 600, Status: store.WebhookReplayPending,
	}); err != nil {
		t.Fatalf("create replay: %v", err)
	}

	srv.replayPending(context.Background())

	rows, err := st.WebhookReplayRepo().ListDue(context.Background(), now+1, 10)
	if err != nil {
		t.Fatalf("list due: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("replay row still pending: %+v", rows)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && exec.callCount() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if exec.callCount() != 1 {
		t.Fatalf("replayed delivery executions = %d, want 1", exec.callCount())
	}
}

func TestReplaySkipsSupersededVerdict(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	srv.SetWebhookReplayRepo(st.WebhookReplayRepo())
	srv.SetReplayChecker(func(context.Context, store.WebhookReplay) (replayDecision, error) {
		return replaySuperseded, nil
	})
	now := time.Now().Unix()
	if _, _, err := st.WebhookReplayRepo().Create(context.Background(), store.WebhookReplay{
		Endpoint: "github", DeliveryID: "del-stale", EventType: "pull_request_review",
		Body: []byte(`{"review":{"id":1}}`), Reason: "state lane at capacity",
		NextAttemptAt: now - 1, ExpiresAt: now + 600, Status: store.WebhookReplayPending,
	}); err != nil {
		t.Fatalf("create replay: %v", err)
	}

	srv.replayPending(context.Background())
	if exec.callCount() != 0 {
		t.Fatalf("superseded verdict executed %d times", exec.callCount())
	}
}

func TestReplayExpiresAndWarns(t *testing.T) {
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	srv.SetWebhookReplayRepo(st.WebhookReplayRepo())
	release := make(chan struct{})
	defer close(release)
	srv.executor = func(ctx context.Context, _, _, _ string, _ *WebhookWorkContext) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var notices []string
	srv.SetNotifier(func(_ context.Context, _, _, text string) error {
		notices = append(notices, text)
		return nil
	})
	now := time.Now().Unix()
	if _, _, err := st.WebhookReplayRepo().Create(context.Background(), store.WebhookReplay{
		Endpoint: "github", DeliveryID: "del-expired", EventType: "pull_request",
		Body: []byte(`{"action":"opened"}`), Reason: "state lane at capacity",
		NextAttemptAt: now - 1, ExpiresAt: now - 1, Status: store.WebhookReplayPending,
	}); err != nil {
		t.Fatalf("create replay: %v", err)
	}

	srv.replayPending(context.Background())
	expired, err := st.WebhookReplayRepo().Count(context.Background(), store.WebhookReplayExpired)
	if err != nil || expired != 1 {
		t.Fatalf("expired rows = %d, err = %v", expired, err)
	}
	if len(notices) != 1 {
		t.Fatalf("lost notices = %d, want 1", len(notices))
	}
}

func TestReplayStoreIsBounded(t *testing.T) {
	_, _, st := newTestServerFull(t, nil)
	now := time.Now().Unix()
	for i := 0; i < 5; i++ {
		if _, _, err := st.WebhookReplayRepo().Create(context.Background(), store.WebhookReplay{
			Endpoint: "github", DeliveryID: fmt.Sprintf("del-%d", i), EventType: "pull_request",
			Body: []byte("0123456789"), Reason: "rejected", NextAttemptAt: now, ExpiresAt: now + 600,
		}); err != nil {
			t.Fatalf("create replay %d: %v", i, err)
		}
	}
	if _, err := st.WebhookReplayRepo().Prune(context.Background(), 3, 1<<20); err != nil {
		t.Fatalf("prune: %v", err)
	}
	total := 0
	for _, status := range []store.WebhookReplayStatus{store.WebhookReplayPending, store.WebhookReplayReplayed, store.WebhookReplaySuperseded, store.WebhookReplayExpired} {
		n, err := st.WebhookReplayRepo().Count(context.Background(), status)
		if err != nil {
			t.Fatalf("count %s: %v", status, err)
		}
		total += n
	}
	if total != 3 {
		t.Fatalf("rows after prune = %d, want 3", total)
	}
}

func TestQueueStatsReportsReplayState(t *testing.T) {
	srv, _, st := newTestServerFull(t, nil)
	srv.SetWebhookReplayRepo(st.WebhookReplayRepo())
	srv.maxQueuedPerKey = 12
	now := time.Now().Unix()
	if _, _, err := st.WebhookReplayRepo().Create(context.Background(), store.WebhookReplay{
		Endpoint: "github", DeliveryID: "del-pending", EventType: "pull_request",
		Body: []byte("{}"), Reason: "rejected", NextAttemptAt: now, ExpiresAt: now + 600,
	}); err != nil {
		t.Fatalf("create pending: %v", err)
	}
	if _, _, err := st.WebhookReplayRepo().Create(context.Background(), store.WebhookReplay{
		Endpoint: "github", DeliveryID: "del-expired", EventType: "pull_request",
		Body: []byte("{}"), Reason: "lost", Status: store.WebhookReplayExpired,
	}); err != nil {
		t.Fatalf("create expired: %v", err)
	}
	stats := srv.QueueStats(context.Background())
	if stats.QueueBudget != 12 || stats.RejectedPending != 1 || stats.ReplayExpired != 1 {
		t.Fatalf("queue stats = %+v", stats)
	}
}

func TestMergeGateIsHeadScoped(t *testing.T) {
	srv, _, _ := newTestServerFull(t, nil)
	prKey := "acme/widget#7"
	srv.recordMergeGate(prKey, WebhookEnvelope{
		"review_verdict": "request_changes",
		"review_commit":  "d41c600",
		"head_sha":       "d41c600",
	})
	if _, blocked := srv.mergeGateBlocked(prKey); !blocked {
		t.Fatal("review on the current head does not block")
	}

	srv.recordMergeGate(prKey, WebhookEnvelope{"head_sha": "2017dbb4"})
	if _, blocked := srv.mergeGateBlocked(prKey); blocked {
		t.Fatal("stale review verdict blocked a newer head")
	}

	srv.recordMergeGate(prKey, WebhookEnvelope{
		"review_verdict": "request_changes",
		"review_commit":  "2017dbb4",
		"head_sha":       "2017dbb4",
	})
	srv.recordMergeGate(prKey, WebhookEnvelope{
		"review_verdict": "approved",
		"review_commit":  "2017dbb4",
		"head_sha":       "2017dbb4",
	})
	if verdict, blocked := srv.mergeGateBlocked(prKey); blocked {
		t.Fatalf("approval on the current head did not clear the gate: %s", verdict)
	}
}

func TestMergeGateApprovalRecordedBeforeAdmission(t *testing.T) {
	ep := config.EndpointConfig{
		Name: "github-merge", Path: "/merge", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1",
		Workflow: "merge", Prompt: "Merge", Thread: true, Repository: "github.com/acme/widget",
		Admit:      []config.AdmitRule{{Event: "pull_request"}},
		Automation: &config.RepoAutomation{Default: config.AutomationFull},
	}
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{ep})
	srv.SetPRQueueRepo(st.PRQueueRepo())
	srv.SetWebhookReplayRepo(st.WebhookReplayRepo())
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	srv.recordMergeGate("acme/widget#7", WebhookEnvelope{
		"review_verdict": "request_changes",
		"review_commit":  "d41c600",
		"head_sha":       "d41c600",
	})
	if _, blocked := srv.mergeGateBlocked("acme/widget#7"); !blocked {
		t.Fatal("setup: gate not blocked")
	}

	body := strings.Replace(prReviewBody(7, "feat/x", "owner", "approved", "2017dbb4"), "abc1234def5678", "2017dbb4", 1)
	if resp := post(t, ts.URL+"/merge?secret=s3cret", "del-merge-approved", "pull_request_review", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("approved review returned %d", resp.StatusCode)
	}
	waitForReceipt(t, st, store.WebhookStatusSkipped)
	if verdict := srv.mergeGateVerdict("acme/widget#7"); verdict != "approved" {
		t.Fatalf("approval not recorded before admission: verdict = %q", verdict)
	}
}

func TestReplayExpiresAfterAttempts(t *testing.T) {
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	srv.SetWebhookReplayRepo(st.WebhookReplayRepo())
	srv.SetReplayChecker(func(context.Context, store.WebhookReplay) (replayDecision, error) {
		return replayUndecidable, nil
	})
	now := time.Now().Unix()
	if _, _, err := st.WebhookReplayRepo().Create(context.Background(), store.WebhookReplay{
		Endpoint: "github", DeliveryID: "del-exhausted", EventType: "pull_request",
		Body: []byte(`{"action":"opened"}`), Reason: "state lane at capacity", Attempts: srv.replayMaxAttempts - 1,
		NextAttemptAt: now - 1, ExpiresAt: now + 600, Status: store.WebhookReplayPending,
	}); err != nil {
		t.Fatalf("create replay: %v", err)
	}

	srv.replayPending(context.Background())
	expired, err := st.WebhookReplayRepo().Count(context.Background(), store.WebhookReplayExpired)
	if err != nil || expired != 1 {
		t.Fatalf("expired rows = %d, err = %v", expired, err)
	}
}
