package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

func isolatedReviewEndpoint() config.EndpointConfig {
	return config.EndpointConfig{
		Name:           "review",
		Path:           "/review",
		Secret:         "s3cret",
		Platform:       "telegram",
		ChannelID:      "chat1",
		Prompt:         "review {{.webhook.head_branch}}",
		Workflow:       "review",
		Repository:     "o/r",
		Workspace:      config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/tmp/repo", Mode: config.WorkspaceModeIsolated},
		CommentTrigger: []string{"please re-review"},
		Admit: []config.AdmitRule{
			{Event: "issue_comment", Actions: []string{"created"}, Require: []string{"comment_trigger", "pr_open"}},
		},
		Limits: &config.EndpointLimits{MaxRunsPerPR: 3, Window: time.Hour},
	}
}

// IMP-116 AC-01: a Review-button body carries the branch but no head; the
// isolated review lane needs the live head, so it is enriched.
func TestReviewButtonBodyIsEnrichedWithHead(t *testing.T) {
	ep := isolatedReviewEndpoint()
	srv, _, _ := newTestServerFull(t, []config.EndpointConfig{ep})
	const head = "abcdef0123456789abcdef0123456789abcdef01"
	fake := &fakePRHeadResolver{head: PRHead{Branch: "feat/x", SHA: head, HeadRepo: "o/r", BaseBranch: "main"}}
	srv.SetPRHeadResolver(fake)

	body := internalReviewCommentBody("o/r", "7", "please re-review", "https://github.com/o/r/pull/7", "feat/x", "")
	enriched, err := srv.enrichCommentBody(dispatchItem{ep: ep, body: body, deliveryID: reviewButtonDeliveryPrefix + "1", eventType: "issue_comment"})
	if err != nil {
		t.Fatalf("enrichCommentBody: %v", err)
	}
	if key := ExtractExecutionKey(enriched); key.Branch != "feat/x" || key.HeadRevision != head {
		t.Fatalf("enriched key = %+v, want the live head", key)
	}
	if fake.calls != 1 {
		t.Fatalf("resolver calls = %d, want 1", fake.calls)
	}

	withHead := internalReviewCommentBody("o/r", "7", "please re-review", "https://github.com/o/r/pull/7", "feat/x", head)
	if _, err := srv.enrichCommentBody(dispatchItem{ep: ep, body: withHead, deliveryID: "internal-review-x", eventType: "issue_comment"}); err != nil {
		t.Fatalf("enrichCommentBody with head: %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("a body with branch and head must not be looked up (calls = %d)", fake.calls)
	}
}

// IMP-116 AC-02: the posted check — SKIP replies, posted reviews, lookup
// errors and silent runs.
func TestVerifyReviewPosted(t *testing.T) {
	srv, _, _ := newTestServerFull(t, []config.EndpointConfig{isolatedReviewEndpoint()})
	ep := isolatedReviewEndpoint()
	start := time.Date(2026, 11, 1, 15, 25, 0, 0, time.UTC)
	cases := []struct {
		name    string
		reply   string
		posted  bool
		err     error
		want    error
		lookups int
	}{
		{"silent run", "The implementation matches the spec.", false, nil, errReviewNotPosted, 1},
		{"posted", "Verdict: APPROVED", true, nil, nil, 1},
		{"skip line", "SKIP", false, nil, nil, 0},
		{"status skip", "📦 Project: x\n✅ Status: SKIP — no actionable REQUEST_CHANGES findings.", false, nil, nil, 0},
		{"lookup error", "done", false, errors.New("gh down"), nil, 1},
	}
	for _, tc := range cases {
		lookups := 0
		var since time.Time
		srv.reviewPosted = func(ctx context.Context, repo, number string, s time.Time) (bool, error) {
			lookups++
			since = s
			if repo != "o/r" || number != "7" {
				t.Fatalf("%s: lookup for %s#%s", tc.name, repo, number)
			}
			return tc.posted, tc.err
		}
		got := srv.verifyReviewPosted(context.Background(), ep, &WebhookWorkContext{PRKey: "o/r#7", AgentReply: tc.reply}, start)
		if !errors.Is(got, tc.want) && got != tc.want {
			t.Errorf("%s: err = %v, want %v", tc.name, got, tc.want)
		}
		if lookups != tc.lookups {
			t.Errorf("%s: lookups = %d, want %d", tc.name, lookups, tc.lookups)
		}
		if lookups > 0 && !since.Equal(start.Add(-time.Minute)) {
			t.Errorf("%s: since = %v, want one minute before the run start", tc.name, since)
		}
	}
}

func TestReviewedSince(t *testing.T) {
	since := time.Date(2026, 11, 1, 15, 24, 0, 0, time.UTC)
	lines := "anggasct\t2026-11-01T15:30:00Z\nkumasct\t2026-11-01T15:00:00Z\n"
	if reviewedSince(lines, "kumasct", since) {
		t.Fatal("a review before the run start does not count")
	}
	if reviewedSince(lines, "occa-bot", since) {
		t.Fatal("another account's review does not count")
	}
	if !reviewedSince(lines+"KumaSCT\t2026-11-01T15:28:00Z\n", "kumasct", since) {
		t.Fatal("occa's review after the start counts (login case-insensitive)")
	}
	if reviewedSince("", "kumasct", since) {
		t.Fatal("no reviews")
	}
}

// IMP-116 AC-02/AC-03: a review lane run that posts nothing is retried once,
// then blocks with the plain copy; the failed delivery is no dedupe hit.
func TestReviewNotPostedRetriesThenBlocks(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	reviewEp := prTestEndpoint("review", "/review", config.AutomationFull)
	srv, st, url := newPRTestServer(t, rec, []config.EndpointConfig{reviewEp})
	var lookups int64
	srv.reviewPosted = func(ctx context.Context, repo, number string, since time.Time) (bool, error) {
		atomic.AddInt64(&lookups, 1)
		return false, nil
	}
	if resp := post(t, url+"/review?secret=s3cret", "del-silent", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-silent", store.WebhookStatusFailed)
	if calls := len(rec.order); calls != 2 {
		t.Fatalf("review runs = %d, want 2 (one retry)", calls)
	}
	row, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	if err != nil || row == nil || row.Status != store.PRStatusBlocked || !strings.Contains(row.BlockedReason, "review not posted") {
		t.Fatalf("pr row = %+v %v, want blocked on review not posted", row, err)
	}
	copyText := describeBlock(row.BlockedReason)
	if copyText.Headline != "The review run failed 1 time in a row: the review was not posted on GitHub." {
		t.Fatalf("block copy = %+v", copyText)
	}
	receipt, _ := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-review", "del-silent")
	if receipt == nil || !strings.Contains(receipt.ErrorSummary, "review not posted") {
		t.Fatalf("receipt = %+v", receipt)
	}
	envelope := normalizeWebhook([]byte(prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1")), "pull_request_review", "del-next", srv.verdicts, false, "", nil)
	key := reviewDedupeKey(envelope)
	if key == "" {
		t.Fatal("no review dedupe key for the test body")
	}
	if prior, err := st.WebhookDeliveryRepo().FindReviewDuplicate(context.Background(), "github-widget-review", key, 0, 0); err != nil || prior != nil {
		t.Fatalf("a failed review must not be a dedupe hit: %+v %v", prior, err)
	}
}

// IMP-116 AC-05: a delivery that moved an unknown card to queued and was then
// skipped before running puts the card on the phase of the PR's latest run;
// with no run to go back to the card stays queued.
func TestQueuedCardSettlesAfterEarlySkip(t *testing.T) {
	srv, st, _ := newSurfaceServer(t)
	ctx := context.Background()
	ep := srv.endpoints["/review"]
	envelope := WebhookEnvelope{"repository": "acme/widget", "pr_number": "181", "head_branch": "feat/x"}

	srv.markPRQueued(truthPR, ep, "del-dup-1", envelope)
	if _, phase, _, _ := srv.prSurfaceState(truthPR); phase != PhaseQueued {
		t.Fatalf("phase = %q, want queued", phase)
	}
	srv.settleQueuedAfterSkip(ep, truthPR, "del-dup-1", envelope)
	if _, phase, _, _ := srv.prSurfaceState(truthPR); phase != PhaseQueued {
		t.Fatalf("no earlier run: phase = %q, want queued", phase)
	}

	id := seedProcessingDelivery(t, st, ep.Name, "del-review-done", "pull_request")
	if err := st.WebhookDeliveryRepo().SetPRKey(ctx, id, truthPR); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.WebhookDeliveryRepo().Transition(ctx, id, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusCompleted, ""); err != nil || !ok {
		t.Fatalf("complete: %v %v", ok, err)
	}
	// A restart forgets the card's phase; the store keeps the runs.
	restarted, _ := newSurfaceServerOn(t, st)
	restarted.markPRQueued(truthPR, ep, "del-dup-2", envelope)
	restarted.settleQueuedAfterSkip(restarted.endpoints["/fix"], truthPR, "del-dup-2", envelope)
	if _, phase, _, _ := restarted.prSurfaceState(truthPR); phase != PhaseQueued {
		t.Fatalf("another endpoint's skip must not settle the card: phase = %q", phase)
	}
	restarted.settleQueuedAfterSkip(ep, truthPR, "del-dup-2", envelope)
	if _, phase, _, _ := restarted.prSurfaceState(truthPR); phase != PhaseReview {
		t.Fatalf("phase = %q, want the latest run's review phase", phase)
	}
}

// IMP-116 B4: a new run of a PR whose card was left on failed edits the card
// again; a blocked PR stays frozen.
func TestNewRunReopensFailedCard(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		srv, st, _ := newSurfaceServer(t)
		srv.reviewPosted = func(context.Context, string, string, time.Time) (bool, error) { return true, nil }
		ctx := context.Background()
		ep := srv.endpoints["/review"]
		ep.Admit = defaultCatchAllAdmit()
		srv.endpoints["/review"] = ep
		srv.syncPhaseParent(ctx, ep, truthPR, surfaceEnvelope(), PhaseFailed)
		if blocked {
			if _, err := st.PRQueueRepo().EnsurePR(ctx, truthPR, "thread-1", "root-1"); err != nil {
				t.Fatal(err)
			}
			if err := st.PRQueueRepo().SetBlocked(ctx, truthPR, "fix made no change — operator action required"); err != nil {
				t.Fatal(err)
			}
		}
		body := []byte(prReviewBody(181, "feat/x", "owner", "commented", "c0ffee"))
		id := seedProcessingDelivery(t, st, ep.Name, "del-rerun", "pull_request_review")
		if err := srv.executeDelivery(ep, body, id, "del-rerun", "pull_request_review", 1, nil, func() bool { return false }, nil); err != nil {
			t.Fatalf("execute review: %v", err)
		}
		_, phase, _, _ := srv.prSurfaceState(truthPR)
		if !blocked && phase != PhaseReview {
			t.Fatalf("failed card: phase = %q, want the new review run", phase)
		}
		if blocked && phase != PhaseFailed {
			t.Fatalf("blocked PR: phase = %q, want the card left frozen", phase)
		}
	}
}

type resumeCall struct {
	prompt string
	resume string
}

type resumeRecorder struct {
	mu    sync.Mutex
	calls []resumeCall
	reply func(n int, workCtx *WebhookWorkContext) error
}

func (r *resumeRecorder) exec(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
	r.mu.Lock()
	r.calls = append(r.calls, resumeCall{prompt: prompt, resume: workCtx.ResumeSessionID})
	n := len(r.calls)
	r.mu.Unlock()
	if workCtx.ResumeSessionID != "" {
		workCtx.SessionID = workCtx.ResumeSessionID
	} else {
		workCtx.SessionID = fmt.Sprintf("ses-%d", n)
	}
	return r.reply(n, workCtx)
}

func (r *resumeRecorder) snapshot() []resumeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]resumeCall(nil), r.calls...)
}

func newResumeServer(t *testing.T, rr *resumeRecorder, postedFrom int64) (*Server, *store.SQLiteStore, string) {
	t.Helper()
	reviewEp := prTestEndpoint("review", "/review", config.AutomationFull)
	srv, st, url := newPRTestServer(t, newPRRecorder("thread-pr", nil), []config.EndpointConfig{reviewEp})
	srv.executor = rr.exec
	srv.ghLoginCached = "occa-bot"
	srv.retryBackoffs = map[failureClass][]time.Duration{failureModel: {0, 0, 0}}
	var lookups int64
	srv.reviewPosted = func(ctx context.Context, repo, number string, since time.Time) (bool, error) {
		return atomic.AddInt64(&lookups, 1) >= postedFrom, nil
	}
	return srv, st, url
}

func TestReviewNotPostedRetryResumesSession(t *testing.T) {
	rr := &resumeRecorder{reply: func(int, *WebhookWorkContext) error { return nil }}
	_, st, url := newResumeServer(t, rr, 2)
	if resp := post(t, url+"/review?secret=s3cret", "del-resume", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-resume", store.WebhookStatusCompleted)
	calls := rr.snapshot()
	if len(calls) != 2 {
		t.Fatalf("executor calls = %d, want 2", len(calls))
	}
	if calls[0].resume != "" || calls[1].resume != "ses-1" {
		t.Fatalf("resume ids = %q, %q; want fresh then ses-1", calls[0].resume, calls[1].resume)
	}
	for _, want := range []string{"acme/widget#7", "@occa-bot", "gh pr review 7 --repo acme/widget", "SKIP"} {
		if !strings.Contains(calls[1].prompt, want) {
			t.Fatalf("correction prompt misses %q:\n%s", want, calls[1].prompt)
		}
	}
	if strings.Contains(calls[1].prompt, "untrusted_payload") {
		t.Fatal("the correction prompt must not resend the delivery prompt")
	}
}

func TestReviewResumeFallsBackToFreshRun(t *testing.T) {
	rr := &resumeRecorder{reply: func(n int, workCtx *WebhookWorkContext) error {
		if n == 2 {
			return fmt.Errorf("%w: session gone", relay.ErrWebhookPrompt)
		}
		return nil
	}}
	_, st, url := newResumeServer(t, rr, 2)
	if resp := post(t, url+"/review?secret=s3cret", "del-fallback", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-fallback", store.WebhookStatusCompleted)
	calls := rr.snapshot()
	if len(calls) != 3 {
		t.Fatalf("executor calls = %d, want 3", len(calls))
	}
	if calls[1].resume != "ses-1" || calls[2].resume != "" {
		t.Fatalf("resume ids = %q, %q; want ses-1 then fresh", calls[1].resume, calls[2].resume)
	}
	if calls[2].prompt != calls[0].prompt {
		t.Fatalf("fallback prompt differs from the full prompt:\n%s", calls[2].prompt)
	}
}

func TestReviewModelErrorRetryRunsFresh(t *testing.T) {
	rr := &resumeRecorder{reply: func(n int, workCtx *WebhookWorkContext) error {
		if n == 1 {
			return relay.ErrWebhookAgentResponse
		}
		return nil
	}}
	_, st, url := newResumeServer(t, rr, 1)
	if resp := post(t, url+"/review?secret=s3cret", "del-model", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-model", store.WebhookStatusCompleted)
	calls := rr.snapshot()
	if len(calls) != 2 || calls[1].resume != "" || calls[1].prompt != calls[0].prompt {
		t.Fatalf("calls = %+v, want a fresh full retry", calls)
	}
}
