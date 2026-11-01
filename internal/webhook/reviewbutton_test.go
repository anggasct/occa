package webhook

import (
	"context"
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

const (
	reviewButtonPR   = "acme/widget#7"
	reviewButtonHead = "abcdef0123456789abcdef0123456789abcdef01"
)

type reviewButtonHarness struct {
	srv        *Server
	st         *store.SQLiteStore
	reviewEp   config.EndpointConfig
	reviewRuns int64
	mu         sync.Mutex
	notes      []string
	targets    []string
}

func newReviewButtonHarness(t *testing.T) *reviewButtonHarness {
	t.Helper()
	h := &reviewButtonHarness{}
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		if workCtx != nil && workCtx.Workflow == "review" {
			atomic.AddInt64(&h.reviewRuns, 1)
		}
		return nil
	}
	reviewEp := prTestEndpoint("review", "/review", config.AutomationFull)
	reviewEp.CommentTrigger = []string{"please re-review"}
	reviewEp.Limits = &config.EndpointLimits{MaxRunsPerPR: 6, Window: time.Hour}
	fixEp := prTestEndpoint("fix", "/fix", config.AutomationFull)
	h.reviewEp = reviewEp
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{reviewEp, fixEp}),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "reviewbutton.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	h.st = st
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { return "msg-1", nil })
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error { return nil })
	srv.SetNotifier(func(_ context.Context, _, target, text string) error {
		h.mu.Lock()
		h.notes = append(h.notes, text)
		h.targets = append(h.targets, target)
		h.mu.Unlock()
		return nil
	})
	srv.SetOperatorAuthorizer(func(_, userID string) bool { return userID == "op" })
	setOpenPRState(srv)
	srv.SetPRHeadResolver(&fakePRHeadResolver{head: PRHead{Branch: "feat/x", SHA: reviewButtonHead}})
	rec := srv.tasks.Upsert("acme/widget", "feat/x", "root-7", "thread-7", TaskPROpened)
	srv.tasks.LinkPRThread(reviewButtonPR, rec)
	h.srv = srv
	return h
}

func (h *reviewButtonHarness) press(t *testing.T, userID string) {
	t.Helper()
	msg := channel.IncomingMessage{Platform: "telegram", ChannelID: "chat1", UserID: userID, IsCallback: true}
	if err := h.srv.RequestReview(context.Background(), msg, "acme/widget", "feat/x"); err != nil {
		t.Fatalf("RequestReview: %v", err)
	}
}

func (h *reviewButtonHarness) lastNote(t *testing.T) string {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.notes) == 0 {
		t.Fatal("the press posted nothing")
	}
	return h.notes[len(h.notes)-1]
}

// park installs a dispatcher for the PR that does not run, so a test can see
// what a press queued and fake what is already active.
func (h *reviewButtonHarness) park(active string) *dispatcher {
	d := &dispatcher{server: h.srv, key: "pr:" + reviewButtonPR, lanes: makePRLanes(8), notify: make(chan struct{}, 1), lastActivity: time.Now()}
	if active != "" {
		ep := prTestEndpoint(active, "/"+active, config.AutomationFull)
		d.setActive(dispatchItem{ep: ep, deliveryID: "active-" + active, prKey: reviewButtonPR, lane: laneOfWorkflow(active)})
	}
	h.srv.dispatchMu.Lock()
	h.srv.dispatchers[d.key] = d
	h.srv.dispatchMu.Unlock()
	return d
}

func queuedReviews(d *dispatcher) []dispatchItem {
	lane := d.lanes[laneOfWorkflow("review")]
	lane.mu.Lock()
	defer lane.mu.Unlock()
	return append([]dispatchItem(nil), lane.items...)
}

func wantNote(t *testing.T, h *reviewButtonHarness, want string) {
	t.Helper()
	if got := h.lastNote(t); !strings.Contains(got, want) {
		t.Fatalf("reply = %q, want it to contain %q", got, want)
	}
}

// AC-04 end to end, then AC-07: the review runs once, posts in the PR thread,
// and a press on the same head afterwards is refused as already reviewed.
func TestReviewButtonQueuesOneReviewThenRefusesSameHead(t *testing.T) {
	h := newReviewButtonHarness(t)
	h.press(t, "op")
	wantNote(t, h, "⏳ Review queued for acme/widget#7.")
	h.mu.Lock()
	target := h.targets[len(h.targets)-1]
	h.mu.Unlock()
	if target != "thread-7" {
		t.Fatalf("reply target = %q, want the PR thread", target)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt64(&h.reviewRuns) < 1 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := atomic.LoadInt64(&h.reviewRuns); got != 1 {
		t.Fatalf("review runs = %d, want 1", got)
	}
	waitForReviewKey(t, h, reviewButtonPR+"@"+reviewButtonHead)
	waitForPRIdle(t, h.srv, reviewButtonPR)

	h.press(t, "op")
	wantNote(t, h, "✅ acme/widget#7@"+shortRevision(reviewButtonHead)+" was already reviewed")
	wantNote(t, h, "/retry")
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt64(&h.reviewRuns); got != 1 {
		t.Fatalf("review runs after the refused press = %d, want 1", got)
	}
}

func waitForPRIdle(t *testing.T, srv *Server, prKey string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if busy := srv.prDispatchState(prKey); busy.activeKind == "" && busy.queued == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s never went idle", prKey)
}

func waitForReviewKey(t *testing.T, h *reviewButtonHarness, key string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if prior, err := h.st.WebhookDeliveryRepo().FindReviewDuplicate(context.Background(), h.reviewEp.Name, key, 0, 0); err == nil && prior != nil && prior.Status == store.WebhookStatusCompleted {
			if !strings.HasPrefix(prior.DeliveryID, reviewButtonDeliveryPrefix) {
				t.Fatalf("review delivery id = %q, want the button prefix", prior.DeliveryID)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no completed review delivery keyed %q", key)
}

// AC-04 (shape) + AC-05: the queued item is an internal comment trigger with
// no head; a second press finds it queued and adds nothing.
func TestReviewButtonSecondPressFindsItQueued(t *testing.T) {
	h := newReviewButtonHarness(t)
	d := h.park("")
	h.press(t, "op")
	wantNote(t, h, "⏳ Review queued for acme/widget#7.")
	h.press(t, "op")
	wantNote(t, h, "👀 A review of acme/widget#7 is already queued.")
	items := queuedReviews(d)
	if len(items) != 1 {
		t.Fatalf("queued reviews = %d, want 1", len(items))
	}
	item := items[0]
	if !strings.HasPrefix(item.deliveryID, reviewButtonDeliveryPrefix) || item.eventType != "issue_comment" || item.ep.Name != h.reviewEp.Name {
		t.Fatalf("queued item = %+v", item)
	}
	if strings.Contains(string(item.body), reviewButtonHead) || !strings.Contains(string(item.body), `"head_revision":""`) {
		t.Fatalf("a button review must leave its head to the run: %s", item.body)
	}
}

// AC-06: a review already running queues nothing.
func TestReviewButtonReviewRunning(t *testing.T) {
	h := newReviewButtonHarness(t)
	d := h.park("review")
	h.press(t, "op")
	wantNote(t, h, "👀 A review of acme/widget#7 is already running.")
	if n := len(queuedReviews(d)); n != 0 {
		t.Fatalf("queued reviews = %d, want 0", n)
	}
}

// AC-08: during a fix the head is not checked, the review queues behind it.
func TestReviewButtonQueuesBehindFix(t *testing.T) {
	h := newReviewButtonHarness(t)
	seedReviewedHead(t, h)
	d := h.park("fix")
	h.press(t, "op")
	wantNote(t, h, "⏳ Review queued for acme/widget#7. It runs after the current fix run.")
	if n := len(queuedReviews(d)); n != 1 {
		t.Fatalf("queued reviews = %d, want 1", n)
	}
}

// AC-07 with a parked dispatcher: a reviewed head and no fix refuses.
func TestReviewButtonRefusesReviewedHead(t *testing.T) {
	h := newReviewButtonHarness(t)
	seedReviewedHead(t, h)
	d := h.park("")
	h.press(t, "op")
	wantNote(t, h, "was already reviewed")
	if n := len(queuedReviews(d)); n != 0 {
		t.Fatalf("queued reviews = %d, want 0", n)
	}
}

func seedReviewedHead(t *testing.T, h *reviewButtonHarness) {
	t.Helper()
	id := seedProcessingDelivery(t, h.st, h.reviewEp.Name, "del-reviewed", "pull_request_review")
	if err := h.st.WebhookDeliveryRepo().SetReviewKey(context.Background(), id, reviewButtonPR+"@"+reviewButtonHead); err != nil {
		t.Fatalf("SetReviewKey: %v", err)
	}
}

// AC-09, AC-10, AC-11: refusals queue nothing.
func TestReviewButtonRefusals(t *testing.T) {
	cases := []struct {
		name  string
		setup func(h *reviewButtonHarness)
		user  string
		want  string
	}{
		{"merged", func(h *reviewButtonHarness) { setPRState(h.srv, PRStateMerged, nil) }, "op", "⛔ acme/widget#7 is merged — nothing to review."},
		{"state unknown", func(h *reviewButtonHarness) { setPRState(h.srv, "", context.DeadlineExceeded) }, "op", "⚠️ Could not read the live state of acme/widget#7"},
		{"not operator", func(h *reviewButtonHarness) {}, "stranger", "⛔ Not allowed."},
		{"no pr", func(h *reviewButtonHarness) {
			h.srv.tasks = NewTaskThreadRegistry()
		}, "op", "No open PR for acme/widget#feat/x yet."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newReviewButtonHarness(t)
			d := h.park("")
			tc.setup(h)
			h.press(t, tc.user)
			wantNote(t, h, tc.want)
			if n := len(queuedReviews(d)); n != 0 {
				t.Fatalf("queued reviews = %d, want 0", n)
			}
		})
	}
}

// AC-12: concurrent presses queue exactly one review.
func TestReviewButtonConcurrentPressesQueueOne(t *testing.T) {
	h := newReviewButtonHarness(t)
	d := h.park("")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := channel.IncomingMessage{Platform: "telegram", ChannelID: "chat1", UserID: "op", IsCallback: true}
			_ = h.srv.RequestReview(context.Background(), msg, "acme/widget", "feat/x")
		}()
	}
	wg.Wait()
	if n := len(queuedReviews(d)); n != 1 {
		t.Fatalf("queued reviews after 20 presses = %d, want 1", n)
	}
}

// AC-01, AC-02.
func TestPhaseParentButtonsCarryReview(t *testing.T) {
	buttons := PhaseParentButtons("discord", "guild", "https://github.com/acme/widget/pull/7", "thread-7", "acme/widget", "feat/x", true)
	var labels []string
	for _, b := range buttons {
		labels = append(labels, b.Label)
	}
	if got := strings.Join(labels, ","); got != "Open PR,Open thread,Merge,Review,Re-dispatch" {
		t.Fatalf("PR card buttons = %s", got)
	}
	review := buttons[3]
	if review.Value != "task:review:acme/widget#feat/x" {
		t.Fatalf("review value = %q", review.Value)
	}
	action, repo, branch, ok := ParseTaskCallback(review.Value)
	if !ok || action != "review" || repo != "acme/widget" || branch != "feat/x" {
		t.Fatalf("ParseTaskCallback = %q %q %q %v", action, repo, branch, ok)
	}
	plain := TaskActionButtons("acme/widget", "feat/x")
	if len(plain) != 2 || plain[0].Label != "Merge" || plain[1].Label != "Re-dispatch" {
		t.Fatalf("dispatch card buttons changed: %+v", plain)
	}
}
