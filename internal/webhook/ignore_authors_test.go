package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func pullRequestBody(author string) string {
	return `{"action":"opened","repository":{"full_name":"acme/widget"},` +
		`"pull_request":{"number":7,"state":"open","user":{"login":"` + author + `"},` +
		`"head":{"ref":"feat/x","sha":"abc1234def5678","repo":{"full_name":"acme/widget"}},` +
		`"base":{"ref":"main","repo":{"full_name":"acme/widget"}}}}`
}

func reviewBody(prAuthor, reviewUser, state string) string {
	return `{"action":"submitted","repository":{"full_name":"acme/widget"},` +
		`"pull_request":{"number":7,"state":"open","user":{"login":"` + prAuthor + `"},` +
		`"head":{"ref":"feat/x","sha":"abc1234def5678","repo":{"full_name":"acme/widget"}},` +
		`"base":{"ref":"main","repo":{"full_name":"acme/widget"}}},` +
		`"review":{"state":"` + state + `","body":"changes","commit_id":"abc1234def5678","user":{"login":"` + reviewUser + `"}}}`
}

func TestIgnoreAuthorsSkipsPullRequestAuthor(t *testing.T) {
	policy := goldenPolicy()
	for name, ignored := range map[string][]string{
		"exact bot login":      {"dependabot[bot]"},
		"bare login matches":   {"dependabot"},
		"case does not matter": {"Dependabot[Bot]"},
	} {
		t.Run(name, func(t *testing.T) {
			ep := goldenEndpoint("review", []string{"please re-review"}, goldenReviewerRules(), nil)
			ep.IgnoreAuthors = ignored
			env := normalizeWebhook([]byte(pullRequestBody("dependabot[bot]")), "pull_request", "d-ignore", policy.verdicts, false, "", nil)
			allowed, reason := admitDelivery(ep, policy, env)
			if allowed {
				t.Fatal("ignored author was admitted")
			}
			if reason != "ignored author dependabot[bot]" {
				t.Fatalf("reason = %q, want ignored author dependabot[bot]", reason)
			}
		})
	}
}

func TestIgnoreAuthorsAdmitsOtherAuthors(t *testing.T) {
	policy := goldenPolicy()
	ep := goldenEndpoint("review", []string{"please re-review"}, goldenReviewerRules(), nil)
	ep.IgnoreAuthors = []string{"dependabot[bot]"}
	env := normalizeWebhook([]byte(pullRequestBody("anggasct")), "pull_request", "d-human", policy.verdicts, false, "", nil)
	if allowed, reason := admitDelivery(ep, policy, env); !allowed {
		t.Fatalf("human author rejected: %s", reason)
	}
}

func TestIgnoreAuthorsJudgesTheReviewActor(t *testing.T) {
	policy := goldenPolicy()
	ep := goldenEndpoint("fix", nil, goldenFixRules(), nil)
	ep.IgnoreAuthors = []string{"dependabot"}

	botReview := normalizeWebhook([]byte(reviewBody("anggasct", "dependabot[bot]", "changes_requested")), "pull_request_review", "d-bot", policy.verdicts, false, "", nil)
	allowed, reason := admitDelivery(ep, policy, botReview)
	if allowed {
		t.Fatal("review by an ignored author was admitted on a human PR")
	}
	if reason != "ignored author dependabot[bot]" {
		t.Fatalf("reason = %q, want ignored author dependabot[bot]", reason)
	}

	humanReview := normalizeWebhook([]byte(reviewBody("dependabot[bot]", "kumasct", "changes_requested")), "pull_request_review", "d-human", policy.verdicts, false, "", nil)
	if allowed, reason := admitDelivery(ep, policy, humanReview); !allowed {
		t.Fatalf("human review on a bot PR rejected: %s", reason)
	}
}

func TestIgnoreAuthorsAdmitsTrustedCommentOnBotPR(t *testing.T) {
	policy := goldenPolicy()
	ep := fixCommentEndpoint([]string{"anggasct"})
	ep.IgnoreAuthors = []string{"dependabot[bot]"}
	env := commentEnvelope(t, "anggasct", "fix ci please")
	env["pr_author"] = "dependabot[bot]"
	if allowed, reason := admitDelivery(ep, policy, env); !allowed {
		t.Fatalf("trusted human comment on a bot PR rejected: %s", reason)
	}
}

func TestIgnoreAuthorsEmptyListKeepsAdmission(t *testing.T) {
	policy := goldenPolicy()
	ep := goldenEndpoint("review", []string{"please re-review"}, goldenReviewerRules(), nil)
	env := normalizeWebhook([]byte(pullRequestBody("anggasct")), "pull_request", "d-empty", policy.verdicts, false, "", nil)
	if allowed, reason := admitDelivery(ep, policy, env); !allowed {
		t.Fatalf("empty ignore list rejected a human PR: %s", reason)
	}

	rejected := normalizeWebhook([]byte(`{"action":"synchronize","pull_request":{"number":9,"state":"open"}}`), "pull_request", "d-sync", policy.verdicts, false, "", nil)
	if allowed, reason := admitDelivery(ep, policy, rejected); allowed || reason != "endpoint golden-review admitted no rule for pull_request.synchronize" {
		t.Fatalf("empty ignore list changed admission: allowed=%v reason=%q", allowed, reason)
	}
}

func TestIgnoreAuthorsSkipLeavesNoActiveQueueRowOrSession(t *testing.T) {
	ep := config.EndpointConfig{
		Name: "github-widget-review", Path: "/review", Secret: "s3cret",
		Platform: "telegram", ChannelID: "chat1", Prompt: "review",
		Workflow: "review", Thread: true, Repository: "github.com/acme/widget",
		Admit:         defaultCatchAllAdmit(),
		Automation:    &config.RepoAutomation{Default: config.AutomationReviewOnly},
		IgnoreAuthors: []string{"dependabot"},
	}
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	srv.SetPRQueueRepo(st.PRQueueRepo())

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/review?secret=s3cret", "del-ignored", "pull_request", pullRequestBody("dependabot[bot]")); resp.StatusCode != http.StatusOK {
		t.Fatalf("ignored delivery returned %d", resp.StatusCode)
	}
	waitForReceiptDeliveryID(t, st, "del-ignored", store.WebhookStatusSkipped)

	receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-ignored")
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v %v", receipt, err)
	}
	if !strings.Contains(receipt.ErrorSummary, "ignored author dependabot[bot]") {
		t.Fatalf("skip reason = %q, want ignored author dependabot[bot]", receipt.ErrorSummary)
	}
	if exec.callCount() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.callCount())
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		row, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
		if err != nil {
			t.Fatalf("get queue row: %v", err)
		}
		if row == nil || row.Status != store.PRStatusActive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ignored delivery left an active queue row: %+v", row)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if items, err := st.PRQueueRepo().ListItems(context.Background(), "acme/widget#7"); err != nil || len(items) != 0 {
		t.Fatalf("queue items = %+v err=%v, want none", items, err)
	}
}
