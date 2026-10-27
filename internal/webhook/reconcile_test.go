package webhook

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func TestGHReviewVerdictScopesCurrentHead(t *testing.T) {
	review := func(login, state, commit, submitted string) ghReconcileReview {
		var r ghReconcileReview
		r.Author.Login = login
		r.State = state
		r.Commit.OID = commit
		r.SubmittedAt = submitted
		return r
	}
	cases := []struct {
		name    string
		head    string
		reviews []ghReconcileReview
		want    string
	}{
		{
			name:    "approval on previous head does not approve new head",
			head:    "bbbbbbb",
			reviews: []ghReconcileReview{review("alice", "APPROVED", "aaaaaaa", "2026-10-23T10:00:00Z")},
			want:    "",
		},
		{
			name: "approval on current head wins",
			head: "bbbbbbb",
			reviews: []ghReconcileReview{
				review("alice", "APPROVED", "aaaaaaa", "2026-10-23T10:00:00Z"),
				review("alice", "APPROVED", "bbbbbbb", "2026-10-23T11:00:00Z"),
			},
			want: "approved",
		},
		{
			name:    "change request on previous head does not block new head review",
			head:    "bbbbbbb",
			reviews: []ghReconcileReview{review("alice", "CHANGES_REQUESTED", "aaaaaaa", "2026-10-23T10:00:00Z")},
			want:    "",
		},
		{
			name:    "change request on current head blocks",
			head:    "bbbbbbb",
			reviews: []ghReconcileReview{review("alice", "CHANGES_REQUESTED", "bbbbbbb", "2026-10-23T10:00:00Z")},
			want:    "changes_requested",
		},
		{
			name: "dismissed review on current head clears approval",
			head: "bbbbbbb",
			reviews: []ghReconcileReview{
				review("alice", "APPROVED", "bbbbbbb", "2026-10-23T10:00:00Z"),
				review("alice", "DISMISSED", "bbbbbbb", "2026-10-23T11:00:00Z"),
			},
			want: "",
		},
		{
			name:    "unknown head never approves",
			head:    "",
			reviews: []ghReconcileReview{review("alice", "APPROVED", "aaaaaaa", "2026-10-23T10:00:00Z")},
			want:    "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ghReviewVerdictForHead(tc.reviews, tc.head, "", nil); got != tc.want {
				t.Fatalf("verdict = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReconcilePRRoutesReviewLaneOncePerHead(t *testing.T) {
	srv, st, _ := newSurfaceServer(t)
	ctx := context.Background()
	reviewEP := srv.endpoints["/review"]
	reviewEP.CommentTrigger = []string{"please re-review"}
	srv.endpoints["/review"] = reviewEP
	prKey := "acme/widget#181"
	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-181", "root-181"); err != nil {
		t.Fatalf("ensure pr binding: %v", err)
	}
	facts := ReconcileFacts{
		State:         "OPEN",
		HeadSHA:       "c0ffee81",
		HeadRef:       "feat/x",
		BaseRef:       "main",
		Title:         "T",
		URL:           "https://github.com/acme/widget/pull/181",
		Author:        "owner",
		HeadRepo:      "acme/widget",
		CIGreen:       true,
		ReviewVerdict: "",
	}
	srv.SetReconcileFacts(func(context.Context, string, string) (ReconcileFacts, error) {
		return facts, nil
	})
	row := store.PRRun{PRKey: prKey, ThreadID: "thread-181"}
	srv.reconcilePR(ctx, row)
	prefix := "reconcile-review-" + normalizeRevision(facts.HeadSHA) + "-"
	waitForReconcileDelivery(t, st, "github-widget-review", prefix, 1)
	srv.reconcilePR(ctx, row)
	time.Sleep(50 * time.Millisecond)
	if got := countReconcileDeliveries(t, st, "github-widget-review", prefix); got != 1 {
		t.Fatalf("review reconcile deliveries = %d, want 1 after a second tick", got)
	}
}

func TestReconcilePRSkipsClosedAndMergedPRs(t *testing.T) {
	srv, st, _ := newSurfaceServer(t)
	ctx := context.Background()
	for i, state := range []string{"CLOSED", "MERGED"} {
		number := 190 + i
		prKey := fmt.Sprintf("acme/widget#%d", number)
		if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, fmt.Sprintf("thread-%d", number), ""); err != nil {
			t.Fatalf("ensure pr binding: %v", err)
		}
		facts := ReconcileFacts{
			State:         state,
			HeadSHA:       fmt.Sprintf("c0ffee%02d", i),
			HeadRef:       "feat/x",
			BaseRef:       "main",
			Title:         "T",
			URL:           fmt.Sprintf("https://github.com/acme/widget/pull/%d", number),
			CIGreen:       true,
			ReviewVerdict: "approved",
		}
		srv.SetReconcileFacts(func(context.Context, string, string) (ReconcileFacts, error) {
			return facts, nil
		})
		srv.reconcilePR(ctx, store.PRRun{PRKey: prKey, ThreadID: fmt.Sprintf("thread-%d", number)})
	}
	time.Sleep(50 * time.Millisecond)
	for i := range []string{"CLOSED", "MERGED"} {
		prefix := fmt.Sprintf("reconcile-merge-c0ffee%02d-", i)
		if got := countReconcileDeliveries(t, st, "github-widget-merge", prefix); got != 0 {
			t.Fatalf("%s reconcile deliveries = %d, want 0", prefix, got)
		}
	}
}

func TestReconcileFloorSurvivesRestart(t *testing.T) {
	srv, st, _ := newSurfaceServer(t)
	ctx := context.Background()
	floor := 3 * time.Second
	srv.reconcileFloor = floor
	prKey := "acme/widget#205"
	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-205", "root-205"); err != nil {
		t.Fatalf("ensure pr binding: %v", err)
	}
	facts := ReconcileFacts{
		State:         "OPEN",
		HeadSHA:       "c0ffee205",
		HeadRef:       "feat/x",
		BaseRef:       "main",
		Title:         "T",
		URL:           "https://github.com/acme/widget/pull/205",
		CIGreen:       true,
		ReviewVerdict: "approved",
	}
	waitForBucketMidpoint(t, floor)
	startBucket := time.Now().UnixNano() / int64(floor)
	srv.reconcileEnqueue(ctx, store.PRRun{PRKey: prKey, ThreadID: "thread-205"}, "merge", facts, "test")
	prefix := "reconcile-merge-" + normalizeRevision(facts.HeadSHA) + "-"
	waitForReconcileDelivery(t, st, "github-widget-merge", prefix, 1)

	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: []config.EndpointConfig{surfaceEndpoint("merge", "github-widget-merge", "/merge")},
	}
	restarted := New(cfg, func(context.Context, string, string, string, *WebhookWorkContext) error { return nil }, st.WebhookDeliveryRepo())
	restarted.SetPRQueueRepo(st.PRQueueRepo())
	restarted.reconcileFloor = floor
	for time.Now().UnixNano()/int64(floor) == startBucket {
		time.Sleep(10 * time.Millisecond)
	}
	restarted.reconcileEnqueue(ctx, store.PRRun{PRKey: prKey, ThreadID: "thread-205"}, "merge", facts, "test")
	time.Sleep(50 * time.Millisecond)
	if got := countReconcileDeliveries(t, st, "github-widget-merge", prefix); got != 1 {
		t.Fatalf("merge reconcile deliveries = %d, want 1 across restart", got)
	}
}

func countReconcileDeliveries(t *testing.T, st *store.SQLiteStore, endpoint, prefix string) int {
	t.Helper()
	rows, err := st.WebhookDeliveryRepo().List(context.Background(), 500)
	if err != nil {
		t.Fatalf("list deliveries: %v", err)
	}
	count := 0
	for _, row := range rows {
		if row.Endpoint == endpoint && strings.HasPrefix(row.DeliveryID, prefix) {
			count++
		}
	}
	return count
}

func waitForReconcileDelivery(t *testing.T, st *store.SQLiteStore, endpoint, prefix string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if countReconcileDeliveries(t, st, endpoint, prefix) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("reconcile deliveries for %s never reached %d", prefix, want)
}

func waitForBucketMidpoint(t *testing.T, floor time.Duration) {
	t.Helper()
	target := floor / 2
	deadline := time.Now().Add(floor + time.Second)
	for time.Now().Before(deadline) {
		remainder := time.Duration(time.Now().UnixNano() % int64(floor))
		if remainder >= target && remainder < target+100*time.Millisecond {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("never reached the midpoint of a reconcile bucket")
}

func TestReconcileReviewDeliveryBypassesCommentAuthorsAllowlist(t *testing.T) {
	ep := config.EndpointConfig{
		Name:           "github-widget-review",
		Workflow:       "review",
		Repository:     "acme/widget",
		CommentTrigger: []string{"please re-review"},
		CommentAuthors: []string{"anggasct"},
		Admit: []config.AdmitRule{
			{Event: "issue_comment", Actions: []string{"created"}, Require: []string{"comment_trigger", "pr_open"}},
		},
	}
	facts := ReconcileFacts{
		State:    "OPEN",
		HeadSHA:  "c0ffee81",
		HeadRef:  "feat/x",
		BaseRef:  "main",
		Title:    "T",
		URL:      "https://github.com/acme/widget/pull/181",
		Author:   "owner",
		HeadRepo: "acme/widget",
	}
	policy := policyFromConfig(testPolicy())
	body, eventType, err := reconcileBody(ep, "review", "acme/widget", facts)
	if err != nil {
		t.Fatalf("reconcile body: %v", err)
	}
	internal := normalizeWebhook(body, eventType, "reconcile-review-c0ffee81-1", testVerdicts(), false, "", ep.CommentTrigger)
	if ok, why := admitDelivery(ep, policy, internal); !ok {
		t.Fatalf("reconcile review rejected: %s", why)
	}

	externalBody := []byte(`{"action":"created","issue":{"number":181,"state":"open","pull_request":{"html_url":"https://github.com/acme/widget/pull/181"}},"comment":{"body":"please re-review","user":{"login":"kumasct"}},"repository":{"full_name":"acme/widget"}}`)
	external := normalizeWebhook(externalBody, "issue_comment", "9bccf660-cbe5-11f1-9d3c-1197f0c92220", testVerdicts(), false, "", ep.CommentTrigger)
	if ok, _ := admitDelivery(ep, policy, external); ok {
		t.Fatal("external comment from a non-allowlisted login must stay gated")
	}
}

func TestReconcilePRAdmitsReviewWithCommentAuthorsAllowlist(t *testing.T) {
	srv, st, _ := newSurfaceServer(t)
	ctx := context.Background()
	reviewEP := srv.endpoints["/review"]
	reviewEP.CommentTrigger = []string{"please re-review"}
	reviewEP.CommentAuthors = []string{"anggasct"}
	srv.endpoints["/review"] = reviewEP
	prKey := "acme/widget#182"
	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-182", "root-182"); err != nil {
		t.Fatalf("ensure pr binding: %v", err)
	}
	facts := ReconcileFacts{
		State:    "OPEN",
		HeadSHA:  "c0ffee82",
		HeadRef:  "feat/x",
		BaseRef:  "main",
		Title:    "T",
		URL:      "https://github.com/acme/widget/pull/182",
		Author:   "owner",
		HeadRepo: "acme/widget",
		CIGreen:  true,
	}
	srv.SetReconcileFacts(func(context.Context, string, string) (ReconcileFacts, error) {
		return facts, nil
	})
	srv.reconcilePR(ctx, store.PRRun{PRKey: prKey, ThreadID: "thread-182"})
	prefix := "reconcile-review-" + normalizeRevision(facts.HeadSHA) + "-"
	waitForReconcileDelivery(t, st, "github-widget-review", prefix, 1)
}

func TestReconcileIgnoresBodyVerdictFromUntrustedAuthor(t *testing.T) {
	const head = "c0ffee217"
	const prAuthor = "reviewbot"
	trustLogins := policyFromConfig(testPolicy()).trustLogins
	review := func(login string) ghReconcileReview {
		var r ghReconcileReview
		r.State = "COMMENTED"
		r.Body = "**Verdict:** APPROVED"
		r.Commit.OID = head
		r.Author.Login = login
		r.SubmittedAt = "2026-10-27T10:00:00Z"
		return r
	}
	if got := ghReviewVerdictForHead([]ghReconcileReview{review("mallory")}, head, prAuthor, trustLogins); got != "" {
		t.Fatalf("untrusted body verdict = %q, want empty", got)
	}
	trusted := ghReviewVerdictForHead([]ghReconcileReview{review(prAuthor)}, head, prAuthor, trustLogins)
	if trusted != "approved" {
		t.Fatalf("trusted self-review verdict = %q, want approved", trusted)
	}

	srv, st, _ := newSurfaceServer(t)
	ctx := context.Background()
	prKey := "acme/widget#217"
	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-217", "root-217"); err != nil {
		t.Fatalf("ensure pr binding: %v", err)
	}
	facts := ReconcileFacts{
		State:         "OPEN",
		HeadSHA:       head,
		HeadRef:       "feat/x",
		BaseRef:       "main",
		Title:         "T",
		URL:           "https://github.com/acme/widget/pull/217",
		Author:        prAuthor,
		HeadRepo:      "acme/widget",
		CIGreen:       true,
		ReviewVerdict: "",
	}
	srv.SetReconcileFacts(func(context.Context, string, string) (ReconcileFacts, error) {
		return facts, nil
	})
	row := store.PRRun{PRKey: prKey, ThreadID: "thread-217"}
	srv.reconcilePR(ctx, row)
	time.Sleep(50 * time.Millisecond)
	mergePrefix := "reconcile-merge-" + normalizeRevision(head) + "-"
	if got := countReconcileDeliveries(t, st, "github-widget-merge", mergePrefix); got != 0 {
		t.Fatalf("untrusted body verdict enqueued %d merge deliveries, want 0", got)
	}

	facts.ReviewVerdict = trusted
	srv.SetReconcileFacts(func(context.Context, string, string) (ReconcileFacts, error) {
		return facts, nil
	})
	srv.reconcilePR(ctx, row)
	waitForReconcileDelivery(t, st, "github-widget-merge", mergePrefix, 1)
}

func TestReconcileCountsBodyVerdict(t *testing.T) {
	const head = "bbbbbbb"
	const prAuthor = "reviewbot"
	trustLogins := map[string]bool{prAuthor: true}
	review := func(state, body, commit, author string) ghReconcileReview {
		var r ghReconcileReview
		r.State = state
		r.Body = body
		r.Commit.OID = commit
		r.Author.Login = author
		r.SubmittedAt = "2026-10-27T10:00:00Z"
		return r
	}
	cases := []struct {
		name   string
		review ghReconcileReview
		want   string
	}{
		{"commented approved", review("COMMENTED", "**Verdict:** APPROVED\n\n## Findings\n- none", head, prAuthor), "approved"},
		{"commented request changes", review("COMMENTED", "**Verdict:** REQUEST_CHANGES", head, prAuthor), "changes_requested"},
		{"commented spec unclear", review("COMMENTED", "**Verdict:** SPEC_UNCLEAR", head, prAuthor), "spec_unclear"},
		{"commented blocked", review("COMMENTED", "**Verdict:** BLOCKED", head, prAuthor), "blocked"},
		{"commented no verdict", review("COMMENTED", "looks good to me", head, prAuthor), ""},
		{"commented verdict on other head", review("COMMENTED", "**Verdict:** APPROVED", "aaaaaaa", prAuthor), ""},
		{"commented verdict from untrusted author", review("COMMENTED", "**Verdict:** APPROVED", head, "mallory"), ""},
		{"commented verdict from trusted non-author", review("COMMENTED", "**Verdict:** APPROVED", head, "otherbot"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ghReviewVerdictForHead([]ghReconcileReview{tc.review}, head, prAuthor, trustLogins); got != tc.want {
				t.Fatalf("verdict = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReconcileBodyVerdictRespectsLatestPerAuthor(t *testing.T) {
	const head = "bbbbbbb"
	const prAuthor = "reviewbot"
	trustLogins := map[string]bool{prAuthor: true, "teammate": true}
	review := func(state, body, author string) ghReconcileReview {
		var r ghReconcileReview
		r.State = state
		r.Body = body
		r.Commit.OID = head
		r.Author.Login = author
		return r
	}
	approved := review("COMMENTED", "**Verdict:** APPROVED", prAuthor)
	approved.SubmittedAt = "2026-10-27T10:00:00Z"
	blocked := review("COMMENTED", "**Verdict:** BLOCKED", prAuthor)
	blocked.SubmittedAt = "2026-10-27T11:00:00Z"
	if got := ghReviewVerdictForHead([]ghReconcileReview{approved, blocked}, head, prAuthor, trustLogins); got != "blocked" {
		t.Fatalf("latest body verdict = %q, want blocked", got)
	}
	dismissed := review("DISMISSED", "", prAuthor)
	dismissed.SubmittedAt = "2026-10-27T12:00:00Z"
	if got := ghReviewVerdictForHead([]ghReconcileReview{approved, dismissed}, head, prAuthor, trustLogins); got != "" {
		t.Fatalf("dismissed verdict = %q, want empty", got)
	}
	untrusted := review("COMMENTED", "**Verdict:** BLOCKED", "teammate")
	untrusted.SubmittedAt = "2026-10-27T13:00:00Z"
	if got := ghReviewVerdictForHead([]ghReconcileReview{approved, untrusted}, head, prAuthor, trustLogins); got != "approved" {
		t.Fatalf("untrusted body verdict overrode trusted = %q, want approved", got)
	}
}

func TestReviewVerdictObservationPicksLatestVerdictHead(t *testing.T) {
	const prAuthor = "reviewbot"
	trustLogins := map[string]bool{prAuthor: true}
	review := func(state, body, commit, submitted, author string) ghReconcileReview {
		var r ghReconcileReview
		r.State = state
		r.Body = body
		r.Commit.OID = commit
		r.SubmittedAt = submitted
		r.Author.Login = author
		return r
	}
	reviews := []ghReconcileReview{
		review("APPROVED", "", "aaaaaaa", "2026-10-27T10:00:00Z", prAuthor),
		review("COMMENTED", "**Verdict:** REQUEST_CHANGES", "bbbbbbb", "2026-10-27T11:00:00Z", prAuthor),
		review("COMMENTED", "no verdict here", "ccccccc", "2026-10-27T12:00:00Z", prAuthor),
	}
	if got := latestVerdictReviewHead(reviews, prAuthor, trustLogins); got != "bbbbbbb" {
		t.Fatalf("latest verdict head = %q, want bbbbbbb", got)
	}
	none := []ghReconcileReview{review("COMMENTED", "no verdict here", "ccccccc", "2026-10-27T12:00:00Z", prAuthor)}
	if got := latestVerdictReviewHead(none, prAuthor, trustLogins); got != "" {
		t.Fatalf("verdict head without a verdict = %q, want empty", got)
	}
	untrusted := []ghReconcileReview{review("COMMENTED", "**Verdict:** APPROVED", "ddddddd", "2026-10-27T13:00:00Z", "mallory")}
	if got := latestVerdictReviewHead(untrusted, prAuthor, trustLogins); got != "" {
		t.Fatalf("untrusted verdict head = %q, want empty", got)
	}
}
