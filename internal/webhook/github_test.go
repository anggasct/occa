package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

type fakePullRequestResolver struct {
	key WebhookExecutionKey
	ref GitHubPullRequestRef
	err error
}

func (f *fakePullRequestResolver) ResolvePullRequest(_ context.Context, ref GitHubPullRequestRef) (WebhookExecutionKey, error) {
	f.ref = ref
	return f.key, f.err
}

func TestGitHubPullRequestResolverFetchesAndValidatesHeadScope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/repos/anggasct/occa/pulls/125" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"number":125,"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"contributor/occa"},"ref":"fix/webhook-permissions"}}`))
	}))
	defer srv.Close()

	resolver := NewGitHubPullRequestResolver(srv.Client())
	resolver.apiBase = srv.URL
	key, err := resolver.ResolvePullRequest(context.Background(), GitHubPullRequestRef{
		Repository: "anggasct/occa",
		Number:     "125",
		HTMLURL:    "https://github.com/anggasct/occa/pull/125",
	})
	if err != nil {
		t.Fatalf("ResolvePullRequest: %v", err)
	}
	want := WebhookExecutionKey{Repository: "anggasct/occa", HeadRepository: "contributor/occa", Branch: "fix/webhook-permissions"}
	if key != want {
		t.Fatalf("key = %+v, want %+v", key, want)
	}
}

func TestExtractIssueCommentPullRequestRefRequiresMatchingPRURL(t *testing.T) {
	body := []byte(`{"repository":{"full_name":"anggasct/occa"},"issue":{"number":125,"pull_request":{"html_url":"https://github.com/other/occa/pull/125"}}}`)
	if _, err := extractIssueCommentPullRequestRef(body); err == nil {
		t.Fatal("mismatched issue comment PR URL was accepted")
	}
}

func TestIssueCommentReviewerResolvesCurrentPRBeforeWorktreePolicy(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{{
		Name:      "github",
		Path:      "/github",
		Secret:    "secret",
		Workflow:  "github_reviewer",
		Platform:  "telegram",
		ChannelID: "chat",
		Prompt:    "review {{.webhook.pr_number}}",
	}})
	resolver := &fakePullRequestResolver{key: WebhookExecutionKey{
		Repository:     "anggasct/occa",
		HeadRepository: "anggasct/occa",
		Branch:         "fix/webhook-permissions",
	}}
	srv.SetPullRequestResolver(resolver)
	srv.SetWorktreeResolver(&fakeWorktreeResolver{worktree: "/projects/occa/.worktree/pr-125"})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := `{"action":"created","repository":{"full_name":"anggasct/occa"},"issue":{"number":125,"pull_request":{"html_url":"https://github.com/anggasct/occa/pull/125"}},"comment":{"body":"please re-review @kumasct"}}`
	if response := post(t, ts.URL+"/github?secret=secret", "issue-comment-review", "issue_comment", body); response.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", response.StatusCode)
	}
	waitForReceipt(t, st, store.WebhookStatusCompleted)

	if resolver.ref != (GitHubPullRequestRef{Repository: "anggasct/occa", Number: "125", HTMLURL: "https://github.com/anggasct/occa/pull/125"}) {
		t.Fatalf("resolver ref = %+v", resolver.ref)
	}
	calls := exec.getCalls()
	if len(calls) != 1 {
		t.Fatalf("executor calls = %d, want 1", len(calls))
	}
	if calls[0].workCtx.Key != resolver.key || calls[0].workCtx.Worktree != "/projects/occa/.worktree/pr-125" {
		t.Fatalf("execution context = %+v", calls[0].workCtx)
	}
	if len(calls[0].workCtx.PermissionRuleset) == 0 {
		t.Fatal("reviewer execution did not receive a permission policy")
	}
}
