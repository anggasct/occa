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

const codeMergeHead = "c0ffee1234567890c0ffee1234567890c0ffee12"

func okFacts() mergeFacts {
	return mergeFacts{State: "OPEN", HeadSHA: codeMergeHead, MergeState: "CLEAN", HeadVerdict: "approved", Checks: checkStateGreen}
}

// IMP-113 AC-02, AC-03, AC-04: every gate refuses with its reason.
func TestMergeGateRefusal(t *testing.T) {
	mod := func(f func(*mergeFacts)) mergeFacts { m := okFacts(); f(&m); return m }
	cases := []struct {
		name  string
		facts mergeFacts
		want  string
	}{
		{"ready", okFacts(), ""},
		{"no checks at all", mod(func(m *mergeFacts) { m.Checks = checkStateNone }), ""},
		{"unstable is mergeable", mod(func(m *mergeFacts) { m.MergeState = "UNSTABLE" }), ""},
		{"github review decision", mod(func(m *mergeFacts) { m.HeadVerdict = ""; m.ReviewDecision = "APPROVED" }), ""},
		{"closed", mod(func(m *mergeFacts) { m.State = "CLOSED" }), "PR is closed"},
		{"draft", mod(func(m *mergeFacts) { m.Draft = true }), "PR is a draft"},
		{"approval only on an older head", mod(func(m *mergeFacts) { m.HeadVerdict = "" }), "no approval on the current head"},
		{"changes requested on head", mod(func(m *mergeFacts) { m.HeadVerdict = "changes_requested" }), "no approval on the current head"},
		{"pending", mod(func(m *mergeFacts) { m.Checks = checkStatePending }), "checks still running"},
		{"red", mod(func(m *mergeFacts) { m.Checks = checkStateRed }), "checks failing"},
		{"dirty", mod(func(m *mergeFacts) { m.MergeState = "DIRTY" }), "conflicts with the base branch"},
		{"behind", mod(func(m *mergeFacts) { m.MergeState = "BEHIND" }), "branch is behind the base branch"},
		{"blocked", mod(func(m *mergeFacts) { m.MergeState = "BLOCKED" }), "blocked by branch protection"},
		{"unknown", mod(func(m *mergeFacts) { m.MergeState = "UNKNOWN" }), "mergeability not known yet"},
	}
	for _, tc := range cases {
		if got := mergeGateRefusal(tc.facts); got != tc.want {
			t.Errorf("%s: refusal = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestChecksStateAndMethod(t *testing.T) {
	run := func(status, conclusion string) ghCheckContext {
		return ghCheckContext{TypeName: "CheckRun", Status: status, Conclusion: conclusion}
	}
	ctxState := func(state string) ghCheckContext { return ghCheckContext{TypeName: "StatusContext", State: state} }
	cases := []struct {
		checks []ghCheckContext
		want   string
	}{
		{nil, checkStateNone},
		{[]ghCheckContext{run("COMPLETED", "SUCCESS"), run("COMPLETED", "SKIPPED"), ctxState("SUCCESS")}, checkStateGreen},
		{[]ghCheckContext{run("COMPLETED", "SUCCESS"), run("IN_PROGRESS", "")}, checkStatePending},
		{[]ghCheckContext{run("IN_PROGRESS", ""), run("COMPLETED", "FAILURE")}, checkStateRed},
		{[]ghCheckContext{ctxState("PENDING")}, checkStatePending},
		{[]ghCheckContext{ctxState("ERROR")}, checkStateRed},
	}
	for i, tc := range cases {
		if got := checksState(tc.checks); got != tc.want {
			t.Errorf("case %d: checksState = %q, want %q", i, got, tc.want)
		}
	}
	if pickMergeMethod(map[string]bool{"squash": true, "rebase": true}) != "squash" || pickMergeMethod(map[string]bool{"merge": true, "squash": true}) != "merge" || pickMergeMethod(nil) != "" {
		t.Fatal("pickMergeMethod must follow merge > squash > rebase")
	}
}

type codeMergeHarness struct {
	srv        *Server
	st         *store.SQLiteStore
	url        string
	agentCalls int64
	merged     atomic.Bool
	mu         sync.Mutex
	merges     []string
	facts      mergeFacts
}

func newCodeMergeHarness(t *testing.T, policy config.MergePolicy) *codeMergeHarness {
	t.Helper()
	h := &codeMergeHarness{facts: okFacts()}
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&h.agentCalls, 1)
		return nil
	}
	mergeEp := prTestEndpoint("merge", "/merge", config.AutomationFull)
	mergeEp.Merge = policy
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{mergeEp}),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "codemerge.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	h.st = st
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { return "msg-1", nil })
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error { return nil })
	srv.SetNotifier(func(_ context.Context, _, _, _ string) error { return nil })
	srv.SetPRStateResolver(func(ctx context.Context, repo, number string) (string, error) {
		if h.merged.Load() {
			return PRStateMerged, nil
		}
		return PRStateOpen, nil
	})
	srv.SetReconcileFacts(func(ctx context.Context, repo, number string) (ReconcileFacts, error) {
		return ReconcileFacts{State: "OPEN", HeadSHA: codeMergeHead, ApprovalCandidate: true}, nil
	})
	srv.mergeFacts = func(ctx context.Context, repo, number string) (mergeFacts, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.facts, nil
	}
	srv.mergeMethods = func(ctx context.Context, repo string) ([]string, error) {
		return []string{"squash", "rebase"}, nil
	}
	srv.prMerger = func(ctx context.Context, repo, number, method, head string, deleteBranch bool) error {
		h.mu.Lock()
		h.merges = append(h.merges, strings.Join([]string{repo, number, method, head, boolWord(deleteBranch)}, " "))
		h.mu.Unlock()
		h.merged.Store(true)
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	h.srv, h.url = srv, ts.URL
	return h
}

func boolWord(b bool) string {
	if b {
		return "delete"
	}
	return "keep"
}

func (h *codeMergeHarness) approve(t *testing.T, delivery string) *store.WebhookDelivery {
	t.Helper()
	if resp := post(t, h.url+"/merge?secret=s3cret", delivery, "pull_request_review", prReviewBody(7, "feat/x", "owner", "approved", codeMergeHead)); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		d, err := h.st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-merge", delivery)
		if err == nil && d != nil && (d.Status == store.WebhookStatusCompleted || d.Status == store.WebhookStatusSkipped || d.Status == store.WebhookStatusFailed) {
			return d
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("delivery %s never finished", delivery)
	return nil
}

// IMP-113 AC-02, AC-05: a ready PR merges in code — one gh merge pinned to
// the head, the configured method, no agent turn — and completes through
// the existing merge confirmation.
func TestCodeMergeMergesWithoutAgent(t *testing.T) {
	h := newCodeMergeHarness(t, config.MergePolicy{Code: true, Method: "rebase", DeleteBranch: true})
	d := h.approve(t, "del-code-merge")
	if d.Status != store.WebhookStatusCompleted {
		t.Fatalf("delivery = %s %q, want completed", d.Status, d.ErrorSummary)
	}
	if got := atomic.LoadInt64(&h.agentCalls); got != 0 {
		t.Fatalf("agent calls = %d, want 0", got)
	}
	if strings.Join(h.merges, ";") != "acme/widget 7 rebase "+codeMergeHead+" delete" {
		t.Fatalf("merges = %v", h.merges)
	}
}

// IMP-113 AC-02: no configured method follows the repository's allowed ones.
func TestCodeMergeMethodFromRepository(t *testing.T) {
	h := newCodeMergeHarness(t, config.MergePolicy{Code: true})
	if d := h.approve(t, "del-code-method"); d.Status != store.WebhookStatusCompleted {
		t.Fatalf("delivery = %s %q", d.Status, d.ErrorSummary)
	}
	if strings.Join(h.merges, ";") != "acme/widget 7 squash "+codeMergeHead+" keep" {
		t.Fatalf("merges = %v", h.merges)
	}
}

// IMP-113 AC-02: a refused gate skips with its reason and merges nothing.
func TestCodeMergeRefusalSkips(t *testing.T) {
	h := newCodeMergeHarness(t, config.MergePolicy{Code: true})
	h.facts.Checks = checkStatePending
	d := h.approve(t, "del-code-pending")
	if d.Status != store.WebhookStatusSkipped || !strings.Contains(d.ErrorSummary, "Reason: merge skipped: checks still running") {
		t.Fatalf("delivery = %s %q", d.Status, d.ErrorSummary)
	}
	if len(h.merges) != 0 || atomic.LoadInt64(&h.agentCalls) != 0 {
		t.Fatalf("a refused gate must merge nothing and run no agent: merges=%v agent=%d", h.merges, h.agentCalls)
	}
}

// IMP-113 AC-06: the agent keeps the lane when code merge is off.
func TestAgentMergeWhenCodeMergeOff(t *testing.T) {
	h := newCodeMergeHarness(t, config.MergePolicy{Code: false})
	h.approve(t, "del-agent-merge")
	if got := atomic.LoadInt64(&h.agentCalls); got != 1 {
		t.Fatalf("agent calls = %d, want 1", got)
	}
	if len(h.merges) != 0 {
		t.Fatalf("code merge ran with code merge off: %v", h.merges)
	}
}

// IMP-113 AC-07: the built-in merged prompt is replaced by one summary line.
func TestMergedSummaryReplacesBuiltinPrompt(t *testing.T) {
	var agent int64
	srv, _, _ := operatorTestServer(t, nil)
	srv.executor = func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&agent, 1)
		return nil
	}
	var mu sync.Mutex
	var lines []string
	srv.SetNotifier(func(_ context.Context, _, target, text string) error {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, target+"|"+text)
		return nil
	})
	srv.mergedSummaryFacts = func(ctx context.Context, repo, number string) (mergedSummary, error) {
		return mergedSummary{Title: "Add widgets", Commits: 3, ChangedFiles: 1, Additions: 40, Deletions: 2, MergedBy: "octo", MergeCommit: "abcdef0123456789"}, nil
	}
	ep := srv.endpointsByName["github-widget-merged"]
	ep.PromptSource = "builtin"
	ep.Prompt = config.BuiltinPrompt("merged")
	envelope := WebhookEnvelope{"repository": "acme/widget", "pr_number": "7", "title": "event title"}
	item := dispatchItem{ep: ep, body: []byte(mergedBody(7)), deliveryID: "del-merged-sum", eventType: "pull_request"}
	srv.runMergedDocsUpdate(context.Background(), ep, item, envelope, "thread-7", "root-7")
	if atomic.LoadInt64(&agent) != 0 {
		t.Fatal("the built-in merged prompt must not start an agent")
	}
	want := "chat1:thread-7|✅ Merged acme/widget#7 — Add widgets\n3 commits · 1 file (+40 −2) · merged by octo · " + shortRevision("abcdef0123456789")
	if len(lines) != 1 || lines[0] != want {
		t.Fatalf("summary lines = %q, want %q", lines, want)
	}

	srv.mergedSummaryFacts = func(ctx context.Context, repo, number string) (mergedSummary, error) {
		return mergedSummary{}, errors.New("gh down")
	}
	lines = nil
	srv.runMergedDocsUpdate(context.Background(), ep, item, envelope, "thread-7", "root-7")
	if len(lines) != 1 || lines[0] != "chat1:thread-7|✅ Merged acme/widget#7 — event title" {
		t.Fatalf("fallback line = %q", lines)
	}

	custom := ep
	custom.PromptSource = "file"
	custom.Prompt = "write the release note"
	srv.runMergedDocsUpdate(context.Background(), custom, item, envelope, "thread-7", "root-7")
	if atomic.LoadInt64(&agent) != 1 {
		t.Fatal("a custom merged prompt keeps its agent turn")
	}
}

// IMP-113 AC-08: review and CI-fix deliveries carry the fetched context.
func TestPrepareLaneContext(t *testing.T) {
	srv, _, _ := operatorTestServer(t, nil)
	srv.priorReviews = func(ctx context.Context, repo, number string) (string, error) {
		return "— APPROVED by octo on c0ffee1 (2026-11-01)\nlooks good", nil
	}
	srv.ciFailures = func(ctx context.Context, repo, number string) (string, error) {
		return "## test\nFAIL TestThing", nil
	}
	work := &WebhookWorkContext{PRKey: "acme/widget#7"}

	review := WebhookEnvelope{"prior_reviews": "", "ci_failures": "", "fix_kind": ""}
	srv.prepareLaneContext(context.Background(), srv.endpointsByName["github-widget-review"], review, work, nil)
	if review["prior_reviews"] != "— APPROVED by octo on c0ffee1 (2026-11-01)\nlooks good" || review["ci_failures"] != "" {
		t.Fatalf("review envelope = %+v", review)
	}
	rendered, err := renderTemplate(config.BuiltinPrompt("review"), map[string]any{"webhook": withEnvelopeDefaults(review)})
	if err != nil || !strings.Contains(rendered, "Earlier reviews on this PR") || !strings.Contains(rendered, "looks good") {
		t.Fatalf("review prompt = %q %v", rendered, err)
	}

	fix := WebhookEnvelope{"fix_kind": fixKindCI}
	srv.prepareLaneContext(context.Background(), srv.endpointsByName["github-widget-fix"], fix, work, nil)
	if fix["ci_failures"] != "## test\nFAIL TestThing" {
		t.Fatalf("fix envelope = %+v", fix)
	}
	rendered, err = renderTemplate(config.BuiltinPrompt("fix"), map[string]any{"webhook": withEnvelopeDefaults(fix)})
	if err != nil || !strings.Contains(rendered, "Failing checks, fetched by occa") || !strings.Contains(rendered, "FAIL TestThing") {
		t.Fatalf("fix prompt = %q %v", rendered, err)
	}

	srv.priorReviews = func(ctx context.Context, repo, number string) (string, error) { return "", errors.New("gh down") }
	failed := WebhookEnvelope{}
	srv.prepareLaneContext(context.Background(), srv.endpointsByName["github-widget-review"], failed, work, nil)
	if failed["prior_reviews"] != "" {
		t.Fatalf("a failed lookup must leave the field empty: %+v", failed)
	}
}

// withEnvelopeDefaults fills the keys the built-in prompts read, as the
// normalizer does for a real delivery.
func withEnvelopeDefaults(e WebhookEnvelope) WebhookEnvelope {
	base := normalizeWebhook([]byte(`{}`), "pull_request", "d", nil, false, "", nil)
	for k, v := range e {
		base[k] = v
	}
	return base
}

// IMP-113 AC-08: the last three reviews with a body, oldest first, clipped.
func TestFormatPriorReviews(t *testing.T) {
	raw := `[
	 {"state":"COMMENTED","body":"","submitted_at":"2026-10-30T01:00:00Z","commit_id":"aaaaaaa1","user":{"login":"bot"}},
	 {"state":"CHANGES_REQUESTED","body":"F-1 fix the thing","submitted_at":"2026-10-30T02:00:00Z","commit_id":"bbbbbbb2","user":{"login":"rev"}},
	 {"state":"COMMENTED","body":"note","submitted_at":"2026-10-31T02:00:00Z","commit_id":"ccccccc3","user":{"login":"rev"}},
	 {"state":"APPROVED","body":"` + strings.Repeat("x", 2000) + `","submitted_at":"2026-11-01T02:00:00Z","commit_id":"ddddddd4","user":{"login":"rev"}},
	 {"state":"COMMENTED","body":"last","submitted_at":"2026-11-01T03:00:00Z","commit_id":"eeeeeee5","user":{"login":"rev"}}
	]`
	got, err := formatPriorReviews([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "F-1") || !strings.HasPrefix(got, "— COMMENTED by rev on ccccccc3 (2026-10-31)\nnote") || !strings.HasSuffix(got, "— COMMENTED by rev on eeeeeee5 (2026-11-01)\nlast") {
		t.Fatalf("prior reviews = %q", got)
	}
	if strings.Count(got, "x") > priorReviewRunes {
		t.Fatalf("a review body must be clipped to %d runes", priorReviewRunes)
	}
}
