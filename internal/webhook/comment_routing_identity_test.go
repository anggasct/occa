package webhook

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func reviewNoWorkspaceEndpoint() config.EndpointConfig {
	return config.EndpointConfig{
		Name:           "github-occa-review",
		Path:           "/webhooks/github-occa-review",
		Secret:         "s3cret",
		Platform:       "discord",
		ChannelID:      "chat1",
		Prompt:         "review {{.webhook.head_branch}}",
		Workflow:       "review",
		Repository:     "github.com/anggasct/occa",
		Workspace:      config.EndpointWorkspace{},
		CommentTrigger: []string{"please re-review"},
		Admit: []config.AdmitRule{
			{Event: "pull_request", Actions: []string{"opened", "reopened", "ready_for_review"}},
			{Event: "issue_comment", Actions: []string{"created"}, Require: []string{"comment_trigger", "pr_open"}},
		},
		Limits: &config.EndpointLimits{MaxRunsPerPR: 3, Window: time.Hour},
	}
}

func TestReviewRoutingIdentityWithoutWorkspace(t *testing.T) {
	ep := reviewNoWorkspaceEndpoint()
	srv, _, _ := newTestServerFull(t, []config.EndpointConfig{ep})
	fake := &fakePRHeadResolver{head: PRHead{Branch: "fix/turn-delivery", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "anggasct/occa", BaseBranch: "main"}}
	srv.SetPRHeadResolver(fake)

	body := realisticIssueCommentBody(178, "please re-review")
	item := dispatchItem{ep: ep, body: body, deliveryID: "del-routing", eventType: "issue_comment"}
	out, err := srv.enrichCommentBody(item)
	if err != nil {
		t.Fatalf("enrichCommentBody: %v", err)
	}
	if string(out) != string(body) {
		t.Fatalf("workspace enrichment must stay skipped without a workspace")
	}
	if fake.calls != 0 {
		t.Fatalf("enrichment resolver calls = %d, want 0", fake.calls)
	}
	envelope := normalizeWebhook(body, "issue_comment", "del-routing", srv.verdicts, false, "", ep.CommentTrigger)
	repo, branch, ok := srv.commentRoutingIdentity(body, "issue_comment", ep, envelope)
	if !ok {
		t.Fatalf("routing identity must resolve for a re-review comment")
	}
	if repo != "anggasct/occa" || branch != "fix/turn-delivery" {
		t.Fatalf("routing identity = %q#%q, want anggasct/occa#fix/turn-delivery", repo, branch)
	}
	if fake.calls != 1 {
		t.Fatalf("routing resolver calls = %d, want 1", fake.calls)
	}
}

func TestReviewRoutingSkipsNonTriggerWithoutLookup(t *testing.T) {
	ep := reviewNoWorkspaceEndpoint()
	srv, _, _ := newTestServerFull(t, []config.EndpointConfig{ep})
	fake := &fakePRHeadResolver{head: PRHead{Branch: "fix/turn-delivery", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "anggasct/occa"}}
	srv.SetPRHeadResolver(fake)

	body := realisticIssueCommentBody(178, "looks good to me")
	envelope := normalizeWebhook(body, "issue_comment", "del-routing-quiet", srv.verdicts, false, "", ep.CommentTrigger)
	if _, _, ok := srv.commentRoutingIdentity(body, "issue_comment", ep, envelope); ok {
		t.Fatalf("non-trigger comment must not yield routing identity")
	}
	if fake.calls != 0 {
		t.Fatalf("resolver calls = %d, want 0", fake.calls)
	}
}

func TestReviewCommentDeliveryLandsInTaskThread(t *testing.T) {
	ep := reviewNoWorkspaceEndpoint()
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	srv.SetPRHeadResolver(&fakePRHeadResolver{head: PRHead{Branch: "fix/turn-delivery", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "anggasct/occa", BaseBranch: "main"}})
	srv.TaskRegistry().Upsert("anggasct/occa", "fix/turn-delivery", "root-1", "thread-1", TaskDispatched)

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := string(realisticIssueCommentBody(178, "please re-review"))
	if resp := post(t, ts.URL+"/webhooks/github-occa-review?secret=s3cret", "del-review-routing", "issue_comment", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	waitForReceiptDeliveryID(t, st, "del-review-routing", store.WebhookStatusCompleted)
	if exec.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.callCount())
	}
	call := exec.getCalls()[0]
	if call.workCtx.ThreadID != "thread-1" || call.workCtx.RootMessageID != "root-1" {
		t.Fatalf("workCtx thread = %q root = %q, want thread-1/root-1", call.workCtx.ThreadID, call.workCtx.RootMessageID)
	}
	if !call.workCtx.Key.IsZero() {
		t.Fatalf("workCtx key = %+v, want zero (routing only, no workspace binding)", call.workCtx.Key)
	}
	out := buf.String()
	if !strings.Contains(out, "task thread resolve") || !strings.Contains(out, "review") || !strings.Contains(out, "thread-1") {
		t.Fatalf("resolve INFO line with caller=review and thread-1 missing, got %q", out)
	}
	if !strings.Contains(out, "anggasct/occa#fix/turn-delivery") {
		t.Fatalf("resolve line must carry canonical key, got %q", out)
	}
}
