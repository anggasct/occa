package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func TestNormalizeWebhookGitHubEvents(t *testing.T) {
	tests := []struct {
		name   string
		event  string
		body   string
		assert func(t *testing.T, got WebhookEnvelope)
	}{
		{
			name:  "pull request",
			event: "pull_request",
			body:  `{"action":"opened","repository":{"full_name":"acme/widgets"},"pull_request":{"number":42,"html_url":"https://github.com/acme/widgets/pull/42","title":"Improve widgets","head":{"ref":"fix/widgets","repo":{"full_name":"acme/widgets"}},"base":{"ref":"main","repo":{"full_name":"acme/widgets"}},"merged":false,"merge_commit_sha":""}}`,
			assert: func(t *testing.T, got WebhookEnvelope) {
				if got["repository"] != "acme/widgets" || got["pr_number"] != "42" || got["head_branch"] != "fix/widgets" || got["base_branch"] != "main" {
					t.Fatalf("unexpected pull request envelope: %#v", got)
				}
			},
		},
		{
			name:  "pull request review",
			event: "pull_request_review",
			body:  `{"action":"submitted","repository":{"full_name":"acme/widgets"},"pull_request":{"number":43,"html_url":"https://github.com/acme/widgets/pull/43","title":"Review widgets","user":{"login":"author"},"head":{"ref":"fix/review","repo":{"full_name":"acme/widgets"}},"base":{"ref":"main","repo":{"full_name":"acme/widgets"}}},"review":{"state":"changes_requested","body":"Please fix the test.","user":{"login":"reviewer"}}}`,
			assert: func(t *testing.T, got WebhookEnvelope) {
				if got["pr_number"] != "43" || got["pr_author"] != "author" || got["review_state"] != "changes_requested" || got["review_user"] != "reviewer" || got["comment_body"] != "Please fix the test." {
					t.Fatalf("unexpected review envelope: %#v", got)
				}
			},
		},
		{
			name:  "issue comment re-review",
			event: "issue_comment",
			body:  `{"action":"created","repository":{"full_name":"acme/widgets"},"issue":{"number":44,"html_url":"https://github.com/acme/widgets/issues/44","title":"Improve widgets","pull_request":{"html_url":"https://github.com/acme/widgets/pull/44"}},"comment":{"body":"Please re-review this PR","user":{"login":"maintainer"}}}`,
			assert: func(t *testing.T, got WebhookEnvelope) {
				if got["pr_number"] != "44" || got["pr_url"] != "https://github.com/acme/widgets/pull/44" || got["comment_trigger"] != "please re-review" || got["review_user"] != "maintainer" {
					t.Fatalf("unexpected issue comment envelope: %#v", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeWebhook([]byte(tt.body), tt.event, "delivery-1", false, "")
			tt.assert(t, got)
			for key, value := range got {
				if value == nil {
					t.Fatalf("envelope field %q is nil", key)
				}
			}
		})
	}
}

func TestNormalizeWebhookMissingFieldsNeverRendersNoValue(t *testing.T) {
	envelope := normalizeWebhook([]byte(`{"action":"opened","repository":{"full_name":"acme/widgets"}}`), "pull_request", "delivery-1", false, "")
	prompt, err := renderTemplate(`{{.webhook.repository}}|{{.webhook.pr_number}}|{{.webhook.title}}|{{.webhook.comment_trigger}}`, map[string]any{"webhook": envelope})
	if err != nil {
		t.Fatalf("render normalized envelope: %v", err)
	}
	if strings.Contains(prompt, "<no value>") {
		t.Fatalf("normalized prompt contains unresolved value: %q", prompt)
	}
}

func TestNormalizeWebhookNonPullRequestIssueCommentKeepsPRFieldsEmpty(t *testing.T) {
	envelope := normalizeWebhook([]byte(`{"action":"created","repository":{"full_name":"acme/widgets"},"issue":{"number":44,"title":"Question"},"comment":{"body":"please re-review"}}`), "issue_comment", "delivery-1", false, "")
	if envelope["pr_number"] != "" || envelope["pr_url"] != "" || envelope["comment_trigger"] != "" {
		t.Fatalf("non-PR issue comment envelope = %#v, want empty PR and trigger fields", envelope)
	}
}

func TestRenderTemplateRejectsStaticNoValue(t *testing.T) {
	if _, err := renderTemplate("literal <no value>", nil); err == nil {
		t.Fatal("renderTemplate accepted a static unresolved-value placeholder")
	}
}

func TestWebhookWorkflowGateMatrix(t *testing.T) {
	tests := []struct {
		workflow string
		event    string
		body     string
		allowed  bool
	}{
		{"github_reviewer", "pull_request", `{"action":"opened","pull_request":{"number":1}}`, true},
		{"github_reviewer", "issue_comment", `{"action":"created","issue":{"number":2,"pull_request":{"html_url":"https://example/pull/2"}},"comment":{"body":"please re-review"}}`, true},
		{"github_reviewer", "pull_request_review", `{"action":"submitted","review":{"state":"approved"}}`, false},
		{"github_fix", "pull_request_review", `{"action":"submitted","review":{"state":"changes_requested"}}`, true},
		{"github_fix", "pull_request", `{"action":"closed","pull_request":{"merged":true}}`, false},
		{"github_merge", "pull_request_review", `{"action":"submitted","review":{"state":"approved"}}`, true},
		{"github_fix", "pull_request_review", `{"action":"submitted","pull_request":{"user":{"login":"kumasct"}},"review":{"state":"commented","body":"**Verdict:** REQUEST_CHANGES\n\nActionable findings: 1","user":{"login":"kumasct"}}}`, true},
		{"github_merge", "pull_request_review", `{"action":"submitted","pull_request":{"user":{"login":"kumasct"}},"review":{"state":"commented","body":"**Verdict:** APPROVED\n\nNo actionable findings.","user":{"login":"kumasct"}}}`, true},
		{"github_merge", "pull_request_review", `{"action":"submitted","pull_request":{"user":{"login":"kumasct"}},"review":{"state":"commented","body":"**Verdict:** APPROVED\n\n### Findings\n\nNo blocking findings.","user":{"login":"kumasct"}}}`, true},
		{"github_merge", "pull_request_review", `{"action":"submitted","pull_request":{"user":{"login":"kumasct"}},"review":{"state":"commented","body":"**Verdict:** APPROVED\n\nActionable findings: 1","user":{"login":"kumasct"}}}`, false},
		{"github_merge", "pull_request_review", `{"action":"submitted","pull_request":{"user":{"login":"other"}},"review":{"state":"commented","body":"**Verdict:** APPROVED\n\nNo actionable findings.","user":{"login":"kumasct"}}}`, false},
		{"github_merge", "issue_comment", `{"action":"created","issue":{"number":2},"comment":{"body":"please re-review"}}`, false},
		{"github_merged", "pull_request", `{"action":"closed","pull_request":{"merged":true}}`, true},
		{"github_merged", "pull_request_review", `{"action":"submitted","review":{"state":"approved"}}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.workflow+"/"+tt.event, func(t *testing.T) {
			allowed, _ := workflowAllows(tt.workflow, normalizeWebhook([]byte(tt.body), tt.event, "d", false, ""))
			if allowed != tt.allowed {
				t.Fatalf("workflowAllows = %v, want %v", allowed, tt.allowed)
			}
		})
	}
}

// A pull_request synchronize push produces no execution packet.
func TestWebhookGateRejectsSynchronizeAction(t *testing.T) {
	for _, workflow := range []string{"github_reviewer", "github_fix", "github_merge", "github_merged", ""} {
		allowed, reason := workflowAllows(workflow, normalizeWebhook(
			[]byte(`{"action":"synchronize","pull_request":{"number":9,"state":"open"}}`), "pull_request", "d", false, ""))
		if workflow != "github_reviewer" {
			continue // other workflows already reject every pull_request action
		}
		if allowed {
			t.Fatalf("synchronize must never spawn execution (workflow=%s)", workflow)
		}
		if !strings.Contains(reason, "synchronize") {
			t.Fatalf("skip reason must mention synchronize, got %q", reason)
		}
	}
}

// An issue_comment re-review trigger on a closed or merged PR produces no
// execution packet; an open PR still executes. All three PR states are
// covered.
func TestWebhookGateSkipsReReviewOnClosedOrMergedPR(t *testing.T) {
	tests := []struct {
		name    string
		state   string
		merged  bool
		allowed bool
	}{
		{"open PR executes", "open", false, true},
		{"closed PR skipped", "closed", false, false},
		{"merged PR skipped", "closed", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"action":"created","issue":{"number":5,"state":%q,"pull_request":{"html_url":"https://example/pull/5","merged":%t}},"comment":{"body":"please re-review"}}`, tt.state, tt.merged)
			envelope := normalizeWebhook([]byte(body), "issue_comment", "d", false, "")
			wantState := tt.state
			if tt.merged {
				wantState = "merged" // merged flag wins over issue.state
			}
			if got := envelope["pr_state"]; got != wantState {
				t.Fatalf("pr_state = %v, want %q", got, wantState)
			}
			allowed, reason := workflowAllows("github_reviewer", envelope)
			if allowed != tt.allowed {
				t.Fatalf("workflowAllows = %v, want %v (pr_state=%v)", allowed, tt.allowed, envelope["pr_state"])
			}
			if !allowed && !strings.Contains(reason, "no re-review execution") {
				t.Fatalf("skip reason must name the terminal PR state, got %q", reason)
			}
		})
	}
}

func TestWebhookAuditNotificationsHaveSeparator(t *testing.T) {
	for _, status := range []string{"COMPLETED", "SKIP", "FAILED"} {
		t.Run(status, func(t *testing.T) {
			var got string
			srv := &Server{}
			srv.SetNotifier(func(_ context.Context, _, _, text string) error {
				got = text
				return nil
			})

			srv.emitAudit(context.Background(), config.EndpointConfig{Platform: "telegram", ChannelID: "chat"}, WebhookEnvelope{
				"event_type": "pull_request",
				"action":     "opened",
			}, status, "", "")

			if !strings.HasSuffix(got, "\n"+webhookMessageSeparator) && got != webhookMessageSeparator {
				t.Fatalf("audit notification = %q, want one trailing separator", got)
			}
			if !strings.Contains(got, "Status: "+status) {
				t.Fatalf("audit notification = %q, missing status %q", got, status)
			}
		})
	}
}

func TestWebhookMergeSkipsFormalFindingsSelfReview(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{{
		Name:      "github",
		Path:      "/github",
		Secret:    "secret",
		Workflow:  "github_merge",
		Platform:  "telegram",
		ChannelID: "chat",
		Prompt:    "must not run",
	}})
	audit := make(chan string, 1)
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		audit <- text
		return nil
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := `{"action":"submitted","repository":{"full_name":"anggasct/occa"},"pull_request":{"number":123,"html_url":"https://github.com/anggasct/occa/pull/123","title":"Webhook UX","user":{"login":"kumasct"}},"review":{"state":"commented","body":"**Verdict:** APPROVED\n\n### Findings\n\n- **Severity:** critical\n- **Type:** security\n- **Problem:** The merge gate must not accept this review.","user":{"login":"kumasct"}}}`
	if response := post(t, ts.URL+"/github?secret=secret", "formal-findings", "pull_request_review", body); response.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", response.StatusCode)
	}
	waitForReceipt(t, st, store.WebhookStatusSkipped)
	if exec.callCount() != 0 {
		t.Fatalf("self-review with formal findings invoked executor %d times", exec.callCount())
	}
	select {
	case summary := <-audit:
		if !strings.Contains(summary, "Status: SKIP") || strings.Contains(summary, "Status: COMPLETED") {
			t.Fatalf("unexpected audit summary: %q", summary)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("formal findings self-review did not emit an audit summary")
	}
}

func TestWebhookMergeAllowsNoBlockingFindingsSelfReview(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{{
		Name:      "github",
		Path:      "/github",
		Secret:    "secret",
		Workflow:  "github_merge",
		Platform:  "telegram",
		ChannelID: "chat",
		Prompt:    "must run",
	}})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := `{"action":"submitted","repository":{"full_name":"anggasct/occa"},"pull_request":{"number":123,"html_url":"https://github.com/anggasct/occa/pull/123","title":"Webhook UX","user":{"login":"kumasct"}},"review":{"state":"commented","body":"**Verdict:** APPROVED\n\n### Findings\n\nNo blocking findings.","user":{"login":"kumasct"}}}`
	if response := post(t, ts.URL+"/github?secret=secret", "clean-approval", "pull_request_review", body); response.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", response.StatusCode)
	}
	waitForReceipt(t, st, store.WebhookStatusCompleted)
	if exec.callCount() != 1 {
		t.Fatalf("clean self-review invoked executor %d times, want 1", exec.callCount())
	}
}

func TestWebhookAcceptedDeliveryAuditsOnceAndReplayIsSilent(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{{
		Name:      "github",
		Path:      "/github",
		Secret:    "secret",
		Platform:  "telegram",
		ChannelID: "chat",
		Prompt:    "analyze",
	}})
	audit := make(chan string, 2)
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		audit <- text
		return nil
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := `{"action":"opened","repository":{"full_name":"anggasct/occa"},"pull_request":{"number":123,"title":"Webhook UX"}}`
	if response := post(t, ts.URL+"/github?secret=secret", "accepted-once", "pull_request", body); response.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", response.StatusCode)
	}
	waitForReceipt(t, st, store.WebhookStatusCompleted)
	select {
	case summary := <-audit:
		for _, want := range []string{"Repo: anggasct/occa", "PR: #123 — Webhook UX", "Status: COMPLETED", "Delivery: accepted-once"} {
			if !strings.Contains(summary, want) {
				t.Fatalf("audit summary missing %q: %q", want, summary)
			}
		}
		if strings.Contains(summary, "secret") || strings.Contains(summary, "<no value>") {
			t.Fatalf("audit summary leaked sensitive or unresolved content: %q", summary)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("accepted delivery did not emit an audit summary")
	}
	if exec.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.callCount())
	}

	if response := post(t, ts.URL+"/github?secret=secret", "accepted-once", "pull_request", body); response.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d, want 200", response.StatusCode)
	}
	select {
	case duplicate := <-audit:
		t.Fatalf("duplicate delivery emitted audit summary: %q", duplicate)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWebhookWorkflowMismatchSkipsAndNotifiesWithoutExecutor(t *testing.T) {
	tests := []struct {
		workflow string
		event    string
		body     string
	}{
		{"github_reviewer", "pull_request_review", `{"action":"submitted","review":{"state":"approved"}}`},
		{"github_fix", "pull_request", `{"action":"closed","pull_request":{"merged":true}}`},
		{"github_merge", "issue_comment", `{"action":"created","issue":{"number":7,"pull_request":{"html_url":"https://example/pull/7"}},"comment":{"body":"please re-review"}}`},
		{"github_merged", "pull_request_review", `{"action":"submitted","review":{"state":"approved"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.workflow, func(t *testing.T) {
			srv, exec, st := newTestServerFull(t, []config.EndpointConfig{{Name: "github", Path: "/github", Secret: "secret", Workflow: tt.workflow, Platform: "telegram", ChannelID: "chat", Prompt: "must not run"}})
			var mu sync.Mutex
			var notifications []string
			srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
				mu.Lock()
				notifications = append(notifications, text)
				mu.Unlock()
				return nil
			})
			mux := http.NewServeMux()
			mux.HandleFunc("/", srv.handleRequest)
			ts := httptest.NewServer(mux)
			defer ts.Close()

			if response := post(t, ts.URL+"/github?secret=secret", "delivery-"+tt.workflow, tt.event, tt.body); response.StatusCode != 200 {
				t.Fatalf("POST status = %d, want 200", response.StatusCode)
			}
			waitForReceipt(t, st, store.WebhookStatusSkipped)
			if exec.callCount() != 0 {
				t.Fatalf("mismatched workflow invoked executor %d times", exec.callCount())
			}
			mu.Lock()
			defer mu.Unlock()
			if len(notifications) != 1 || strings.Contains(notifications[0], "secret") || strings.Contains(notifications[0], "Raw") {
				t.Fatalf("unexpected skip notifications: %#v", notifications)
			}
		})
	}
}

func TestFormatThreadName(t *testing.T) {
	tests := []struct {
		name     string
		envelope WebhookEnvelope
		workflow string
		want     string
	}{
		{
			name: "pr and branch",
			envelope: WebhookEnvelope{
				"pr_number":   "42",
				"head_branch": "feat/my-branch",
			},
			workflow: "github_reviewer",
			want:     "PR #42: github_reviewer (feat/my-branch)",
		},
		{
			name: "pr only",
			envelope: WebhookEnvelope{
				"pr_number": "42",
			},
			workflow: "github_reviewer",
			want:     "PR #42: github_reviewer",
		},
		{
			name: "branch only",
			envelope: WebhookEnvelope{
				"head_branch": "main",
			},
			workflow: "github_fix",
			want:     "github_fix: main",
		},
		{
			name: "delivery id fallback",
			envelope: WebhookEnvelope{
				"delivery_id": "1234567890abcdef",
			},
			workflow: "github_merge",
			want:     "github_merge (12345678)",
		},
		{
			name:     "empty envelope",
			envelope: WebhookEnvelope{},
			workflow: "github_reviewer",
			want:     "github_reviewer",
		},
		{
			name: "clamped to 100 runes",
			envelope: WebhookEnvelope{
				"pr_number":   "999",
				"head_branch": strings.Repeat("x", 120),
			},
			workflow: "long_workflow",
			want:     clipRunes("PR #999: long_workflow ("+strings.Repeat("x", 120)+")", 100),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatThreadName(tt.envelope, tt.workflow)
			if got != tt.want {
				t.Errorf("FormatThreadName = %q, want %q", got, tt.want)
			}
			if len([]rune(got)) > 100 {
				t.Errorf("FormatThreadName length = %d > 100", len([]rune(got)))
			}
		})
	}
}

func TestFormatRootCard(t *testing.T) {
	envelope := WebhookEnvelope{
		"repository":  "anggasct/occa",
		"pr_number":   "56",
		"title":       "feat: test",
		"event_type":  "pull_request",
		"delivery_id": "del-1",
	}

	running := FormatRootCard(envelope, "github_reviewer", "RUNNING", "", "", "discord")
	if !strings.Contains(running, "Status: RUNNING") {
		t.Errorf("running root card missing Status: RUNNING: %s", running)
	}
	if strings.Contains(running, "➡️ Details in thread:") {
		t.Errorf("running root card without threadID should not contain thread link: %s", running)
	}

	completed := FormatRootCard(envelope, "github_reviewer", "✅ COMPLETED (1m 45s)", "", "thread-789", "discord")
	if !strings.Contains(completed, "Status: ✅ COMPLETED (1m 45s)") {
		t.Errorf("completed root card missing status: %s", completed)
	}
	if !strings.Contains(completed, "➡️ Details in thread: <#thread-789>") {
		t.Errorf("completed root card missing thread link: %s", completed)
	}

	telegramCard := FormatRootCard(envelope, "github_reviewer", "⏳ Starting agent analysis...", "", "777", "telegram")
	if strings.Contains(telegramCard, "Details in thread:") || strings.Contains(telegramCard, "<#") {
		t.Errorf("telegram root card must not contain discord thread link: %s", telegramCard)
	}
}

func TestEmitAuditRootCardEditing(t *testing.T) {
	envelope := WebhookEnvelope{
		"repository":  "anggasct/occa",
		"pr_number":   "56",
		"title":       "feat: test",
		"event_type":  "pull_request",
		"delivery_id": "del-1",
	}
	ep := config.EndpointConfig{Platform: "discord", ChannelID: "chan-1", Workflow: "github_reviewer"}

	t.Run("successful root card edit avoids notifier", func(t *testing.T) {
		var editedChannel, editedMsgID, editedContent string
		var notifierCalled bool

		srv := &Server{}
		srv.SetEditor(func(ctx context.Context, platform, channelID, messageID, text string) error {
			editedChannel = channelID
			editedMsgID = messageID
			editedContent = text
			return nil
		})
		srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
			notifierCalled = true
			return nil
		})

		workCtx := &WebhookWorkContext{
			RootMessageID: "root-msg-123",
			ThreadID:      "thread-456",
			StartTime:     time.Now().Add(-105 * time.Second),
		}

		srv.emitAudit(context.Background(), ep, envelope, "COMPLETED", "", "", workCtx)

		if notifierCalled {
			t.Error("notifier was called when editor succeeded")
		}
		if editedChannel != "chan-1" || editedMsgID != "root-msg-123" {
			t.Errorf("editedChannel=%q, editedMsgID=%q, want chan-1 / root-msg-123", editedChannel, editedMsgID)
		}
		if !strings.Contains(editedContent, "✅ COMPLETED (1m 45s)") {
			t.Errorf("editedContent missing completed duration: %s", editedContent)
		}
		if !strings.Contains(editedContent, "➡️ Details in thread: <#thread-456>") {
			t.Errorf("editedContent missing thread link: %s", editedContent)
		}
	})

	t.Run("telegram topic card omits discord thread link", func(t *testing.T) {
		var editedChannel, editedContent string
		tgEp := config.EndpointConfig{Platform: "telegram", ChannelID: "-10012345", Workflow: "github_reviewer"}

		srv := &Server{}
		srv.SetEditor(func(ctx context.Context, platform, channelID, messageID, text string) error {
			editedChannel = channelID
			editedContent = text
			return nil
		})
		srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
			t.Error("notifier should not be called when editor succeeds")
			return nil
		})

		workCtx := &WebhookWorkContext{
			RootMessageID: "tg-root-1",
			ThreadID:      "777",
			StartTime:     time.Now().Add(-30 * time.Second),
		}

		srv.emitAudit(context.Background(), tgEp, envelope, "COMPLETED", "", "", workCtx)

		if editedChannel != "-10012345:777" {
			t.Errorf("editedChannel = %q, want -10012345:777", editedChannel)
		}
		if strings.Contains(editedContent, "Details in thread:") || strings.Contains(editedContent, "<#") {
			t.Errorf("telegram card must not contain discord thread link: %s", editedContent)
		}
	})

	t.Run("failed root card edit falls back to notifier", func(t *testing.T) {
		var notifierContent string
		srv := &Server{}
		srv.SetEditor(func(ctx context.Context, platform, channelID, messageID, text string) error {
			return errors.New("discord permission error")
		})
		srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
			notifierContent = text
			return nil
		})

		workCtx := &WebhookWorkContext{
			RootMessageID: "root-msg-123",
			ThreadID:      "thread-456",
			StartTime:     time.Now().Add(-30 * time.Second),
		}

		srv.emitAudit(context.Background(), ep, envelope, "FAILED", "some error", "", workCtx)

		if notifierContent == "" {
			t.Fatal("notifier was not called on editor failure")
		}
		if !strings.Contains(notifierContent, "Status: FAILED") {
			t.Errorf("fallback notification missing Status: FAILED: %s", notifierContent)
		}
	})

	t.Run("empty root message id uses notifier directly", func(t *testing.T) {
		var editorCalled, notifierCalled bool
		srv := &Server{}
		srv.SetEditor(func(ctx context.Context, platform, channelID, messageID, text string) error {
			editorCalled = true
			return nil
		})
		srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
			notifierCalled = true
			return nil
		})

		srv.emitAudit(context.Background(), ep, envelope, "COMPLETED", "", "")

		if editorCalled {
			t.Error("editor was called when RootMessageID was empty")
		}
		if !notifierCalled {
			t.Error("notifier was not called when RootMessageID was empty")
		}
	})
}
