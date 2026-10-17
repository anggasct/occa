package webhook

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
)

func TestThreadUXLiveProofFullCycle(t *testing.T) {
	var mu sync.Mutex
	var parentSends []string
	var parentButtons [][]channel.Button
	var threadPosts []string
	var threadTargets []string
	var statusSends []string
	var statusEdits []string
	srv := &Server{tasks: NewTaskThreadRegistry()}
	srv.SetSender(func(_ context.Context, _, c, text string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if strings.TrimSpace(c) == "chan-1" {
			parentSends = append(parentSends, text)
			return "parent-1", nil
		}
		statusSends = append(statusSends, text)
		return "status-1", nil
	})
	srv.SetSenderWithButtons(func(_ context.Context, _, c, text string, buttons []channel.Button) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		parentSends = append(parentSends, text)
		cp := append([]channel.Button(nil), buttons...)
		parentButtons = append(parentButtons, cp)
		return "parent-1", nil
	})
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error { return nil })
	srv.SetEditorWithButtons(func(_ context.Context, _, _, _ string, _ string, buttons []channel.Button) error {
		mu.Lock()
		defer mu.Unlock()
		cp := append([]channel.Button(nil), buttons...)
		parentButtons = append(parentButtons, cp)
		return nil
	})
	srv.SetNotifier(func(_ context.Context, _, c, text string) error {
		mu.Lock()
		defer mu.Unlock()
		threadPosts = append(threadPosts, text)
		threadTargets = append(threadTargets, c)
		return nil
	})
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error { return nil })
	srv.rawEditor = func(_ context.Context, _, _, msgID, text string) error {
		mu.Lock()
		defer mu.Unlock()
		statusEdits = append(statusEdits, msgID+":"+text)
		return nil
	}
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) { return "thread-1", nil })
	srv.SetGuildResolver(func(_ context.Context, _, _ string) string { return "guild-1" })

	task := DispatchTask{ID: "task-live-1", Repo: "acme/widget", Branch: "feat/live", SpecPath: "specs/feat-live.md"}
	srv.dispatchTasks = NewDispatchStore()
	rec, ok := srv.openDispatchThread(task, "discord", "chan-1", "parent-1")
	if !ok {
		t.Fatal("openDispatchThread failed")
	}
	t.Logf("LIVE parent=%s thread=%s", rec.RootMessageID, rec.ThreadID)

	workflows := []struct {
		workflow string
		status   string
		reason   string
		envelope WebhookEnvelope
		delivery string
	}{
		{"review", "COMPLETED", "", WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/live", "pr_number": "42", "pr_url": "https://github.com/acme/widget/pull/42", "review_verdict": "APPROVED", "delivery_id": "del-review-1"}, "del-review-1"},
		{"fix", "COMPLETED", "2 findings", WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/live", "pr_number": "42", "pr_url": "https://github.com/acme/widget/pull/42", "review_verdict": "REQUEST_CHANGES", "delivery_id": "del-fix-1"}, "del-fix-1"},
		{"merge", "COMPLETED", "", WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/live", "pr_number": "42", "pr_url": "https://github.com/acme/widget/pull/42", "conclusion": "failure", "pr_state": "open", "delivery_id": "del-merge-1"}, "del-merge-1"},
		{"merged", "COMPLETED", "", WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/live", "pr_number": "42", "pr_url": "https://github.com/acme/widget/pull/42", "pr_state": "merged", "merged": true, "delivery_id": "del-merged-1"}, "del-merged-1"},
	}
	for _, w := range workflows {
		ep := config.EndpointConfig{Platform: "discord", ChannelID: "chan-1", Workflow: w.workflow}
		wctx := &WebhookWorkContext{ThreadID: "thread-1", RootMessageID: "parent-1", DeliveryID: w.delivery, Workflow: w.workflow, Envelope: w.envelope}
		srv.emitAudit(context.Background(), ep, w.envelope, w.status, w.reason, "", wctx)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Logf("LIVE parentSends=%d threadPosts=%d statusSends=%d statusEdits=%d parentButtonSets=%d", len(parentSends), len(threadPosts), len(statusSends), len(statusEdits), len(parentButtons))
	if rec, ok := srv.tasks.Lookup("acme/widget", "feat/live"); ok {
		t.Logf("LIVE statusMsg=%s phase=%s thread=%s root=%s", rec.StatusMessageID, rec.Phase, rec.ThreadID, rec.RootMessageID)
		if strings.TrimSpace(rec.StatusMessageID) == "" {
			t.Error("STATUS message ID must be stored (pinned STATUS)")
		}
		if rec.Phase != "DONE" {
			t.Errorf("final phase = %q, want DONE", rec.Phase)
		}
	} else {
		t.Fatal("registry miss after cycle")
	}
	if len(statusSends) != 1 {
		t.Errorf("STATUS must send once then edit in place, sends=%d", len(statusSends))
	}
	if len(statusEdits) == 0 {
		t.Error("STATUS must edit in place on later phases")
	}
	banners := 0
	for _, text := range threadPosts {
		if text == "🔍 REVIEW mulai" || strings.HasPrefix(text, "🔨 FIX jalan") || text == "🔀 MERGE cek gate" || text == "✅ DONE" {
			banners++
		}
		if strings.Contains(text, "━━━") || strings.Contains(text, "Delivery:") || strings.Contains(text, "Model:") {
			t.Errorf("thread post leaks parent-only fields: %q", text)
		}
	}
	if banners < 3 {
		t.Errorf("want phase banners present (FIX/MERGE/DONE), got %d in %v", banners, threadPosts)
	}
	for _, target := range threadTargets {
		if strings.TrimSpace(target) != "thread-1" {
			t.Errorf("thread post target = %q, want thread-1 (never new top-level)", target)
		}
	}
	foundParentButtons := false
	for _, buttons := range parentButtons {
		if len(buttons) > 0 {
			foundParentButtons = true
			break
		}
	}
	if !foundParentButtons {
		t.Error("parent must keep action buttons")
	}
	foundGateRed := false
	foundMerged := false
	mergeSlim := FormatThreadSlimCard(WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/live", "pr_number": "42", "pr_url": "https://github.com/acme/widget/pull/42", "conclusion": "failure", "pr_state": "open"}, "merge", TaskPROpened, nil)
	if strings.Contains(mergeSlim, "gate checked, CI red, fix running") {
		foundGateRed = true
	} else {
		t.Errorf("merge slim must state CI red outcome, got %q", mergeSlim)
	}
	mergedSlim := FormatThreadSlimCard(WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/live", "pr_number": "42", "pr_url": "https://github.com/acme/widget/pull/42", "pr_state": "merged", "merged": true}, "merged", TaskMerged, nil)
	if strings.Contains(mergedSlim, "gate checked, merged") {
		foundMerged = true
	} else {
		t.Errorf("merged slim must state merged outcome, got %q", mergedSlim)
	}
	for _, text := range threadPosts {
		if strings.Contains(text, "Status:") && strings.Contains(text, "COMPLETED") {
			t.Errorf("thread must state gate outcome, not bare COMPLETED: %q", text)
		}
	}
	if !foundGateRed {
		t.Errorf("merge gate must state CI red outcome in thread, posts=%v", threadPosts)
	}
	if !foundMerged {
		t.Errorf("merged phase must state merged outcome in thread, posts=%v", threadPosts)
	}
}
