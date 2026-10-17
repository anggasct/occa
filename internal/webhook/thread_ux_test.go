package webhook

import (
	"context"
	"strings"
	"testing"
)

func TestFormatPhaseBannerSingleLine(t *testing.T) {
	cases := map[string]string{
		"review": "🔍 REVIEW started",
		"fix":    "🔨 FIX in progress",
		"merge":  "🔀 MERGE checking gate",
		"merged": "✅ DONE",
	}
	for workflow, want := range cases {
		got := FormatPhaseBanner(workflow, "")
		if got != want {
			t.Errorf("FormatPhaseBanner(%q) = %q, want %q", workflow, got, want)
		}
		if strings.Contains(got, "\n") {
			t.Errorf("banner %q must be one line", got)
		}
		if strings.Contains(got, "━━━") || strings.Contains(got, webhookMessageSeparator) {
			t.Errorf("banner %q must not carry a card separator", got)
		}
	}
	got := FormatPhaseBanner("fix", "F-002, F-003")
	if !strings.Contains(got, "F-002") || !strings.Contains(got, "🔨 FIX in progress") {
		t.Errorf("fix banner with detail = %q, want findings inline", got)
	}
	if strings.Contains(got, "\n") {
		t.Errorf("fix banner must stay one line, got %q", got)
	}
}

func TestFormatThreadStatusShape(t *testing.T) {
	got := FormatThreadStatus("🔨 FIX", "Step 10 (fix-ci run)", "2 CI jobs still red", "push + re-review")
	if !strings.HasPrefix(got, "📌 STATUS: ") {
		t.Fatalf("STATUS must start with pinned marker, got %q", got)
	}
	for _, want := range []string{"🔨 FIX", "Step 10 (fix-ci run)", "2 CI jobs still red", "next: push + re-review"} {
		if !strings.Contains(got, want) {
			t.Errorf("STATUS %q must contain %q", got, want)
		}
	}
	if lines := strings.Count(got, "\n") + 1; lines > 4 {
		t.Errorf("STATUS must be max 4 lines, got %d: %q", lines, got)
	}
	if strings.Contains(got, "━━━") {
		t.Errorf("STATUS must not carry a card separator: %q", got)
	}
}

func TestFormatThreadSlimCardShape(t *testing.T) {
	envelope := WebhookEnvelope{
		"repository":     "acme/widget",
		"head_branch":    "feat/x",
		"pr_number":      "42",
		"pr_url":         "https://github.com/acme/widget/pull/42",
		"review_verdict": "REQUEST_CHANGES",
		"delivery_id":    "del-12345678",
		"model":          "openrouter/x",
	}
	got := FormatThreadSlimCard(envelope, "fix", TaskPROpened, nil)
	if !strings.Contains(got, "REQUEST_CHANGES") {
		t.Errorf("slim card must carry verdict, got %q", got)
	}
	if !strings.Contains(got, "[Open PR](https://github.com/acme/widget/pull/42)") {
		t.Errorf("slim card must carry PR link, got %q", got)
	}
	if !ThreadTextHasNoLeaks(got) {
		t.Errorf("slim card must not leak Delivery/Model/separator, got %q", got)
	}
	if strings.Contains(got, "del-12345678") {
		t.Errorf("slim card must not carry delivery ID, got %q", got)
	}
}

func TestFormatThreadOpenerZeroComponents(t *testing.T) {
	envelope := WebhookEnvelope{
		"repository":  "acme/widget",
		"head_branch": "feat/x",
		"pr_number":   "42",
		"pr_url":      "https://github.com/acme/widget/pull/42",
	}
	opener := FormatThreadOpener(envelope, "review", TaskClaimed, nil)
	if !strings.Contains(opener, "[Open PR]") {
		t.Errorf("opener must carry inline PR link as plain text, got %q", opener)
	}
	if !ThreadTextHasNoLeaks(opener) {
		t.Errorf("opener must have no Delivery/Model/separator leaks, got %q", opener)
	}
}

func TestFormatGateOutcomeStatesOutcome(t *testing.T) {
	red := WebhookEnvelope{"conclusion": "failure", "pr_state": "open"}
	if got := FormatGateOutcome(red, "merge", "COMPLETED"); got != "gate checked, CI red, fix running" {
		t.Errorf("red gate = %q, want CI red outcome", got)
	}
	merged := WebhookEnvelope{"pr_state": "merged", "merged": true}
	if got := FormatGateOutcome(merged, "merged", "COMPLETED"); got != "gate checked, merged" {
		t.Errorf("merged gate = %q, want merged outcome", got)
	}
	changes := WebhookEnvelope{"review_verdict": "REQUEST_CHANGES"}
	if got := FormatGateOutcome(changes, "merge", "COMPLETED"); !strings.Contains(got, "fix running") {
		t.Errorf("changes-requested gate must name fix running, got %q", got)
	}
	green := WebhookEnvelope{"pr_state": "open"}
	if got := FormatGateOutcome(green, "merge", "COMPLETED"); got == "COMPLETED" || strings.Contains(got, "✅ COMPLETED") {
		t.Errorf("gate must not be bare COMPLETED, got %q", got)
	}
}

func TestThreadStatusEditsInPlace(t *testing.T) {
	var sent []string
	var edited []string
	srv := &Server{tasks: NewTaskThreadRegistry()}
	srv.SetSender(func(_ context.Context, _, _, text string) (string, error) {
		sent = append(sent, text)
		return "status-1", nil
	})
	srv.SetEditor(func(_ context.Context, _, _, msgID, text string) error {
		edited = append(edited, msgID+":"+text)
		return nil
	})
	ctx := context.Background()
	first := srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-1", "acme/widget", "feat/x", "review", "review run del-1", "analyzing")
	if first != "status-1" {
		t.Fatalf("first STATUS must send, got %q", first)
	}
	if len(sent) != 1 || len(edited) != 0 {
		t.Fatalf("first STATUS must append once: sent=%d edited=%d", len(sent), len(edited))
	}
	second := srv.ensureThreadStatus(ctx, "discord", "chan-1", "thread-1", "acme/widget", "feat/x", "review", "review run del-1", "still analyzing")
	if second != "status-1" {
		t.Fatalf("second STATUS must reuse message ID, got %q", second)
	}
	if len(sent) != 1 {
		t.Errorf("second STATUS must not append, sent=%d", len(sent))
	}
	if len(edited) != 1 {
		t.Fatalf("second STATUS must edit in place, edited=%d", len(edited))
	}
	if !strings.HasPrefix(sent[0], "📌 STATUS: ") || !strings.Contains(edited[0], "📌 STATUS: ") {
		t.Errorf("STATUS texts must carry pinned marker: sent=%q edited=%q", sent[0], edited[0])
	}
}

func TestThreadSlimCardHasNoParentLeaks(t *testing.T) {
	envelope := WebhookEnvelope{
		"repository":  "acme/widget",
		"head_branch": "feat/x",
		"pr_number":   "7",
		"pr_url":      "https://github.com/acme/widget/pull/7",
		"delivery_id": "del-999",
		"model":       "agent/session default",
	}
	for _, status := range []TaskStatus{TaskClaimed, TaskPROpened, TaskMerged, TaskFailed} {
		got := FormatThreadSlimCard(envelope, "review", status, nil)
		if !ThreadTextHasNoLeaks(got) {
			t.Errorf("slim card %q leaks parent-only fields: %q", status, got)
		}
	}
}
