package webhook

import (
	"fmt"
	"testing"

	"github.com/anggasct/occa/internal/config"
)

func commentEnvelope(t *testing.T, author, body string) WebhookEnvelope {
	t.Helper()
	payload := fmt.Sprintf(`{"action":"created","issue":{"number":9,"title":"t","state":"open","pull_request":{"html_url":"https://github.com/acme/widget/pull/9"}},"comment":{"body":%q,"user":{"login":%q}},"repository":{"full_name":"acme/widget"}}`, body, author)
	return normalizeWebhook([]byte(payload), "issue_comment", "del-ca", testVerdicts(), false, "", []string{"fix ci"})
}

func fixCommentEndpoint(authors []string) config.EndpointConfig {
	return config.EndpointConfig{
		Name:           "github-widget-pr-fix",
		Workflow:       "fix",
		CommentTrigger: []string{"fix ci"},
		CommentAuthors: authors,
		Admit: []config.AdmitRule{
			{Event: "issue_comment", Actions: []string{"created"}, Require: []string{"comment_trigger", "pr_open"}},
		},
	}
}

func TestAdmitCommentAuthorsGate(t *testing.T) {
	policy := policyFromConfig(config.WebhookPolicy{
		Verdicts: map[string][]string{
			"approved":        {"approved"},
			"request_changes": {"request changes"},
		},
	})
	ep := fixCommentEndpoint([]string{"anggasct"})

	if ok, reason := admitDelivery(ep, policy, commentEnvelope(t, "anggasct", "fix ci please")); !ok {
		t.Fatalf("allowlisted author rejected: %s", reason)
	}
	ok, reason := admitDelivery(ep, policy, commentEnvelope(t, "kumasct", "fix ci please"))
	if ok {
		t.Fatal("non-allowlisted author was admitted")
	}
	if reason == "" || reason == "endpoint github-widget-pr-fix admitted no rule for issue_comment.created" {
		t.Fatalf("reason = %q, want a comment_authors rejection", reason)
	}

	open := fixCommentEndpoint(nil)
	if ok, reason := admitDelivery(open, policy, commentEnvelope(t, "kumasct", "fix ci please")); !ok {
		t.Fatalf("empty allowlist must keep historical behaviour: %s", reason)
	}
}

func TestAutomationAllowsCommentTriggeredFix(t *testing.T) {
	reviewOnly := &config.RepoAutomation{Default: config.AutomationReviewOnly}
	srv := &Server{}
	key := WebhookExecutionKey{Repository: "acme/widget", Branch: "feat/x"}

	unconfigured := config.EndpointConfig{Workflow: "fix", Automation: reviewOnly}
	triggered := commentEnvelope(t, "anggasct", "fix ci")
	triggered["pr_author"] = "anggasct"
	if srv.automationAllows(unconfigured, key, triggered) {
		t.Fatal("comment-triggered fix must stay blocked in review-only mode without a comment_authors allowlist")
	}

	configured := config.EndpointConfig{Workflow: "fix", Automation: reviewOnly, CommentAuthors: []string{"anggasct"}}
	if !srv.automationAllows(configured, key, triggered) {
		t.Fatal("allowlisted comment-triggered fix must run in review-only mode")
	}
	stranger := commentEnvelope(t, "kumasct", "fix ci")
	stranger["pr_author"] = "kumasct"
	if srv.automationAllows(configured, key, stranger) {
		t.Fatal("non-allowlisted commenter must not unlock fix in review-only mode")
	}

	plain := commentEnvelope(t, "anggasct", "lgtm")
	plain["pr_author"] = "anggasct"
	if srv.automationAllows(configured, key, plain) {
		t.Fatal("fix without a matched comment trigger must stay blocked in review-only mode")
	}
	merge := config.EndpointConfig{Workflow: "merge", Automation: reviewOnly, CommentAuthors: []string{"anggasct"}}
	if srv.automationAllows(merge, key, triggered) {
		t.Fatal("comment trigger must not unlock merge in review-only mode")
	}
}
