package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

type fakePRHeadResolver struct {
	head       PRHead
	err        error
	calls      int
	lastRepo   string
	lastNumber int
}

func (f *fakePRHeadResolver) ResolvePRHead(ctx context.Context, repo string, number int) (PRHead, error) {
	f.calls++
	f.lastRepo = repo
	f.lastNumber = number
	if f.err != nil {
		return PRHead{}, f.err
	}
	return f.head, nil
}

func mutableFixEndpoint() config.EndpointConfig {
	return config.EndpointConfig{
		Name:           "review-fix",
		Path:           "/fix",
		Secret:         "s3cret",
		Platform:       "telegram",
		ChannelID:      "chat1",
		Prompt:         "fix {{.webhook.head_branch}}",
		Workflow:       "fix",
		Repository:     "o/r",
		Workspace:      config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/tmp/repo", Mode: config.WorkspaceModeMutable},
		CommentTrigger: []string{"please fix review", "please fix ci"},
		Admit: []config.AdmitRule{
			{Event: "pull_request_review", Actions: []string{"submitted"}, ReviewState: []string{"changes_requested"}},
			{Event: "issue_comment", Actions: []string{"created"}, Require: []string{"comment_trigger", "pr_open"}},
		},
		Limits: &config.EndpointLimits{MaxRunsPerPR: 3, Window: time.Hour},
	}
}

func commentFixBody(prNumber int, comment string) []byte {
	payload := map[string]any{
		"action":     "created",
		"repository": map[string]any{"full_name": "o/r"},
		"issue": map[string]any{
			"number": prNumber,
			"title":  "Fix it",
			"state":  "open",
			"pull_request": map[string]any{
				"html_url": fmt.Sprintf("https://github.com/o/r/pull/%d", prNumber),
				"url":      fmt.Sprintf("https://api.github.com/repos/o/r/pulls/%d", prNumber),
			},
		},
		"comment": map[string]any{
			"body": comment,
			"user": map[string]any{"login": "someone"},
		},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return encoded
}

func TestEnrichmentPopulatesHeadIdentity(t *testing.T) {
	ep := mutableFixEndpoint()
	srv, _, _ := newTestServerFull(t, []config.EndpointConfig{ep})
	fake := &fakePRHeadResolver{head: PRHead{Branch: "feat/fix-7", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "o/r", BaseBranch: "main"}}
	srv.SetPRHeadResolver(fake)

	body := commentFixBody(7, "please fix review")
	item := dispatchItem{ep: ep, body: body, deliveryID: "del-1", eventType: "issue_comment"}
	enriched, err := srv.enrichCommentBody(item)
	if err != nil {
		t.Fatalf("enrichCommentBody: %v", err)
	}
	key := ExtractExecutionKey(enriched)
	if key.Repository != "o/r" || key.Branch != "feat/fix-7" || key.HeadRevision != "abcdef0123456789abcdef0123456789abcdef01" {
		t.Fatalf("enriched key = %+v", key)
	}
	if key.HeadRepository != "o/r" {
		t.Fatalf("enriched head repo = %q, want o/r", key.HeadRepository)
	}
	envelope := normalizeWebhook(enriched, "issue_comment", "del-1", srv.verdicts, false, "", ep.CommentTrigger)
	if stringValue(envelope["head_branch"]) != "feat/fix-7" {
		t.Fatalf("envelope head_branch = %q, want feat/fix-7", stringValue(envelope["head_branch"]))
	}
	if fake.calls != 1 || fake.lastRepo != "o/r" || fake.lastNumber != 7 {
		t.Fatalf("resolver calls=%d repo=%q number=%d", fake.calls, fake.lastRepo, fake.lastNumber)
	}
}

func TestEnrichmentFailureFailsClosed(t *testing.T) {
	ep := mutableFixEndpoint()
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	resolver := &fakePRHeadResolver{err: errors.New("rate limited")}
	srv.SetPRHeadResolver(resolver)
	workspace := &fakeWorkspaceResolver{path: "/tmp/wt"}
	srv.SetWorkspaceResolver(workspace)

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/fix?secret=s3cret", "del-fail", "issue_comment", string(commentFixBody(7, "please fix review"))); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceiptDeliveryID(t, st, "del-fail", store.WebhookStatusFailed)
	if !strings.HasPrefix(receipt.ErrorSummary, "pr head enrichment failed:") {
		t.Fatalf("summary = %q, want prefix pr head enrichment failed:", receipt.ErrorSummary)
	}
	if exec.callCount() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.callCount())
	}
	if workspace.callCount() != 0 {
		t.Fatalf("workspace calls = %d, want 0", workspace.callCount())
	}
}

func TestEnrichmentForkFailsClosed(t *testing.T) {
	ep := mutableFixEndpoint()
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	srv.SetPRHeadResolver(&fakePRHeadResolver{head: PRHead{Branch: "feat/fork", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "fork/r", BaseBranch: "main"}})
	workspace := &fakeWorkspaceResolver{path: "/tmp/wt"}
	srv.SetWorkspaceResolver(workspace)

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/fix?secret=s3cret", "del-fork", "issue_comment", string(commentFixBody(7, "please fix review"))); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceiptDeliveryID(t, st, "del-fork", store.WebhookStatusFailed)
	if !strings.HasPrefix(receipt.ErrorSummary, "pr head enrichment failed:") {
		t.Fatalf("fork summary = %q, want enrichment prefix", receipt.ErrorSummary)
	}
	if exec.callCount() != 0 {
		t.Fatalf("fork executor calls = %d, want 0", exec.callCount())
	}
	if workspace.callCount() != 0 {
		t.Fatalf("fork workspace calls = %d, want 0", workspace.callCount())
	}
}

func TestNonPRCommentSkippedWithoutLookup(t *testing.T) {
	ep := mutableFixEndpoint()
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	resolver := &fakePRHeadResolver{head: PRHead{Branch: "feat/x", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "o/r"}}
	srv.SetPRHeadResolver(resolver)
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{path: "/tmp/wt"})

	body := `{"action":"created","repository":{"full_name":"o/r"},"issue":{"number":9,"title":"Question","state":"open"},"comment":{"body":"please fix review","user":{"login":"someone"}}}`
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/fix?secret=s3cret", "del-nonpr", "issue_comment", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceiptDeliveryID(t, st, "del-nonpr", store.WebhookStatusSkipped)
	if !strings.Contains(receipt.ErrorSummary, "admitted no rule") {
		t.Fatalf("non-PR skip reason = %q, want admitted no rule", receipt.ErrorSummary)
	}
	if exec.callCount() != 0 {
		t.Fatalf("non-PR executor calls = %d, want 0", exec.callCount())
	}
	if resolver.calls != 0 {
		t.Fatalf("non-PR resolver calls = %d, want 0", resolver.calls)
	}
}

func TestEnrichedCommentCompletesWithHeadBranch(t *testing.T) {
	ep := mutableFixEndpoint()
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	srv.SetPRHeadResolver(&fakePRHeadResolver{head: PRHead{Branch: "feat/fix-7", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "o/r", BaseBranch: "main"}})
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{path: "/tmp/wt-fix"})

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/fix?secret=s3cret", "del-ok", "issue_comment", string(commentFixBody(7, "please fix review"))); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	waitForReceiptDeliveryID(t, st, "del-ok", store.WebhookStatusCompleted)
	if exec.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.callCount())
	}
	call := exec.getCalls()[0]
	if call.workCtx.Key.Branch != "feat/fix-7" || call.workCtx.Key.Repository != "o/r" {
		t.Fatalf("execution key = %+v", call.workCtx.Key)
	}
	if call.workCtx.Worktree != "/tmp/wt-fix" {
		t.Fatalf("worktree = %q", call.workCtx.Worktree)
	}
	if !strings.Contains(call.prompt, "feat/fix-7") {
		t.Fatalf("prompt missing head branch: %q", call.prompt)
	}
}

func TestEnrichedCommentRateCapIntact(t *testing.T) {
	ep := mutableFixEndpoint()
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	srv.SetPRHeadResolver(&fakePRHeadResolver{head: PRHead{Branch: "feat/fix-7", SHA: "abcdef0123456789abcdef0123456789abcdef01", HeadRepo: "o/r", BaseBranch: "main"}})
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{path: "/tmp/wt"})

	body := commentFixBody(7, "please fix review")
	enriched, err := srv.enrichCommentBody(dispatchItem{ep: ep, body: body, deliveryID: "seed", eventType: "issue_comment"})
	if err != nil {
		t.Fatalf("enrich seed: %v", err)
	}
	for i := 1; i <= 3; i++ {
		delID := fmt.Sprintf("cap-%d", i)
		id := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, delID)
		if err := srv.executeDelivery(ep, enriched, id, delID, "issue_comment", 1, nil, nil); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	if exec.callCount() != 3 {
		t.Fatalf("calls = %d, want 3", exec.callCount())
	}
	id4 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "cap-4")
	if err := srv.executeDelivery(ep, enriched, id4, "cap-4", "issue_comment", 1, nil, nil); err != nil {
		t.Fatalf("delivery 4: %v", err)
	}
	if exec.callCount() != 3 {
		t.Fatalf("capped calls = %d, want 3", exec.callCount())
	}
}
