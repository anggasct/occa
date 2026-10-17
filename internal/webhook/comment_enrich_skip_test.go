package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func prodLikeFixEndpoint() config.EndpointConfig {
	return config.EndpointConfig{
		Name:           "github-occa-review-fix",
		Path:           "/webhooks/github-occa-review-fix",
		Secret:         "s3cret",
		Platform:       "discord",
		ChannelID:      "chat1",
		Prompt:         "fix {{.webhook.head_branch}}",
		Workflow:       "fix",
		Repository:     "github.com/anggasct/occa",
		Workspace:      config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/tmp/repo", Mode: config.WorkspaceModeMutable},
		CommentTrigger: []string{"fix ci", "fix review", "please fix ci", "please fix review"},
		NotifySkip:     boolPtr(false),
		Admit: []config.AdmitRule{
			{Event: "pull_request_review", Actions: []string{"submitted"}, ReviewState: []string{"changes_requested"}},
			{Event: "issue_comment", Actions: []string{"created"}, Require: []string{"comment_trigger", "pr_open"}},
		},
		Limits: &config.EndpointLimits{MaxRunsPerPR: 3, Window: time.Hour},
	}
}

func realisticIssueCommentBody(prNumber int, commentBody string) []byte {
	payload := map[string]any{
		"action": "created",
		"issue": map[string]any{
			"number": prNumber,
			"title":  "keep one living parent per task",
			"state":  "open",
			"user":   map[string]any{"login": "author"},
			"pull_request": map[string]any{
				"url":      "https://api.github.com/repos/anggasct/occa/pulls/178",
				"html_url": "https://github.com/anggasct/occa/pull/178",
			},
		},
		"comment": map[string]any{
			"id":       3456789,
			"body":     commentBody,
			"user":     map[string]any{"login": "reviewer"},
			"html_url": "https://github.com/anggasct/occa/pull/178#issuecomment-3456789",
		},
		"repository": map[string]any{
			"full_name":      "anggasct/occa",
			"default_branch": "main",
		},
		"sender": map[string]any{"login": "reviewer"},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return encoded
}

func TestRealFixTriggerEnrichesViaResolver(t *testing.T) {
	ep := prodLikeFixEndpoint()
	srv, _, _ := newTestServerFull(t, []config.EndpointConfig{ep})
	fake := &fakePRHeadResolver{head: PRHead{Branch: "fix/task-thread-ops-complete", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "anggasct/occa", BaseBranch: "main"}}
	srv.SetPRHeadResolver(fake)

	body := realisticIssueCommentBody(178, "please fix review")
	item := dispatchItem{ep: ep, body: body, deliveryID: "del-real-trigger", eventType: "issue_comment"}
	enriched, err := srv.enrichCommentBody(item)
	if err != nil {
		t.Fatalf("enrichCommentBody: %v", err)
	}
	key := ExtractExecutionKey(enriched)
	if key.Branch != "fix/task-thread-ops-complete" || key.Repository != "anggasct/occa" {
		t.Fatalf("enriched key = %+v, want branch fix/task-thread-ops-complete repo anggasct/occa", key)
	}
	if fake.calls != 1 || fake.lastRepo != "anggasct/occa" || fake.lastNumber != 178 {
		t.Fatalf("resolver calls=%d repo=%q number=%d, want 1 anggasct/occa 178", fake.calls, fake.lastRepo, fake.lastNumber)
	}
}

func TestNonTriggerCommentsSkipEnrichmentWithoutLookup(t *testing.T) {
	ep := prodLikeFixEndpoint()
	srv, _, _ := newTestServerFull(t, []config.EndpointConfig{ep})
	fake := &fakePRHeadResolver{head: PRHead{Branch: "fix/x", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "anggasct/occa"}}
	srv.SetPRHeadResolver(fake)

	bodies := map[string][]byte{
		"bot progress report": realisticIssueCommentBody(178, "## Fix Applied\n\nSummary of changes..."),
		"re-review on fix":    realisticIssueCommentBody(178, "please re-review"),
	}
	for name, body := range bodies {
		fake.calls = 0
		item := dispatchItem{ep: ep, body: body, deliveryID: "del-" + name, eventType: "issue_comment"}
		out, err := srv.enrichCommentBody(item)
		if err != nil {
			t.Fatalf("%s: enrichCommentBody: %v", name, err)
		}
		if string(out) != string(body) {
			t.Fatalf("%s: non-trigger body must pass through unchanged", name)
		}
		if fake.calls != 0 {
			t.Fatalf("%s: resolver calls = %d, want 0", name, fake.calls)
		}
	}
}

func quietFixServer(t *testing.T, ep config.EndpointConfig) (*Server, *fakeExecutor, *store.SQLiteStore, *fakeWorkspaceResolver, *countingNotifier) {
	t.Helper()
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	workspace := &fakeWorkspaceResolver{path: "/tmp/wt-fix"}
	srv.SetWorkspaceResolver(workspace)
	notifier := &countingNotifier{}
	srv.SetNotifier(notifier.notify)
	srv.SetSender(notifier.send)
	srv.SetThreadStarter(notifier.startThread)
	return srv, exec, st, workspace, notifier
}

type countingNotifier struct {
	mu             sync.Mutex
	notifyCalls    int
	sendCalls      int
	threadCalls    int
	lastNotifyText string
}

func (c *countingNotifier) notify(ctx context.Context, platform, channelID, text string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notifyCalls++
	c.lastNotifyText = text
	return nil
}

func (c *countingNotifier) send(ctx context.Context, platform, channelID, text string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sendCalls++
	return "msg-1", nil
}

func (c *countingNotifier) startThread(ctx context.Context, platform, channelID, messageID, name string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.threadCalls++
	return "thread-1", nil
}

func (c *countingNotifier) totals() (notify, send, thread int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.notifyCalls, c.sendCalls, c.threadCalls
}

func TestNonTriggerBotReportSkipsQuietlyEndToEnd(t *testing.T) {
	ep := prodLikeFixEndpoint()
	srv, exec, st, workspace, notifier := quietFixServer(t, ep)
	srv.SetPRHeadResolver(&fakePRHeadResolver{head: PRHead{Branch: "fix/x", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "anggasct/occa"}})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := string(realisticIssueCommentBody(178, "## Fix Applied\n\nAll checks pass."))
	if resp := post(t, ts.URL+"/webhooks/github-occa-review-fix?secret=s3cret", "del-bot-report", "issue_comment", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceiptDeliveryID(t, st, "del-bot-report", store.WebhookStatusSkipped)
	if !strings.Contains(receipt.ErrorSummary, "admitted no rule") {
		t.Fatalf("skip reason = %q, want admitted no rule", receipt.ErrorSummary)
	}
	if exec.callCount() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.callCount())
	}
	if workspace.callCount() != 0 {
		t.Fatalf("workspace calls = %d, want 0", workspace.callCount())
	}
	if notify, send, thread := notifier.totals(); notify != 0 || send != 0 || thread != 0 {
		t.Fatalf("notifier/send/thread = %d/%d/%d, want 0/0/0 for quiet skip", notify, send, thread)
	}
	if got := len(srv.TaskRegistry().Snapshot()); got != 0 {
		t.Fatalf("task threads = %d, want 0 for quiet skip", got)
	}
}

func TestReReviewOnFixSkipsQuietlyEndToEnd(t *testing.T) {
	ep := prodLikeFixEndpoint()
	srv, exec, st, workspace, notifier := quietFixServer(t, ep)
	resolver := &fakePRHeadResolver{head: PRHead{Branch: "fix/x", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "anggasct/occa"}}
	srv.SetPRHeadResolver(resolver)

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := string(realisticIssueCommentBody(178, "please re-review"))
	if resp := post(t, ts.URL+"/webhooks/github-occa-review-fix?secret=s3cret", "del-rereview-on-fix", "issue_comment", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceiptDeliveryID(t, st, "del-rereview-on-fix", store.WebhookStatusSkipped)
	if !strings.Contains(receipt.ErrorSummary, "admitted no rule") {
		t.Fatalf("skip reason = %q, want admitted no rule", receipt.ErrorSummary)
	}
	if exec.callCount() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.callCount())
	}
	if workspace.callCount() != 0 {
		t.Fatalf("workspace calls = %d, want 0", workspace.callCount())
	}
	if resolver.calls != 0 {
		t.Fatalf("resolver calls = %d, want 0 for non-trigger", resolver.calls)
	}
	if notify, send, thread := notifier.totals(); notify != 0 || send != 0 || thread != 0 {
		t.Fatalf("notifier/send/thread = %d/%d/%d, want 0/0/0 for quiet skip", notify, send, thread)
	}
}

func TestGenuineFixTriggerCompletesEndToEnd(t *testing.T) {
	ep := prodLikeFixEndpoint()
	srv, exec, st, workspace, _ := quietFixServer(t, ep)
	srv.SetPRHeadResolver(&fakePRHeadResolver{head: PRHead{Branch: "fix/task-thread-ops-complete", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "anggasct/occa", BaseBranch: "main"}})
	srv.SetWorkspaceResolver(workspace)

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := string(realisticIssueCommentBody(178, "please fix review"))
	if resp := post(t, ts.URL+"/webhooks/github-occa-review-fix?secret=s3cret", "del-fix-trigger", "issue_comment", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	waitForReceiptDeliveryID(t, st, "del-fix-trigger", store.WebhookStatusCompleted)
	if exec.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.callCount())
	}
	if workspace.callCount() != 1 {
		t.Fatalf("workspace calls = %d, want 1", workspace.callCount())
	}
	call := exec.getCalls()[0]
	if call.workCtx.Key.Branch != "fix/task-thread-ops-complete" || call.workCtx.Key.Repository != "anggasct/occa" {
		t.Fatalf("execution key = %+v, want branch fix/task-thread-ops-complete repo anggasct/occa", call.workCtx.Key)
	}
}
