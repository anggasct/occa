package webhook

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/render"
	"github.com/anggasct/occa/internal/store"
)

func TestSurfaceDiscipline_AC01(t *testing.T) {
	var parentWrites int
	var threadPosts int
	var statusText string
	srv := &Server{tasks: NewTaskThreadRegistry()}
	srv.SetNotifier(func(_ context.Context, _, target, _ string) error {
		if target == "thread-1" {
			threadPosts++
		}
		return nil
	})
	srv.SetSender(func(_ context.Context, _, target, text string) (string, error) {
		if target == "thread-1" && strings.Contains(text, threadStatusPrefix) {
			parentWrites++
			statusText = text
		}
		return "status-1", nil
	})
	srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskClaimed)
	srv.tasks.SetPhase("acme/widget", "feat/x", "REVIEW")

	changed := srv.postPhaseBannerIfChanged(context.Background(), "discord", "chan-1", "thread-1", "acme/widget", "feat/x", "fix", "")
	if !changed {
		t.Fatal("a phase change must be recorded")
	}
	if threadPosts != 0 {
		t.Fatalf("phase change posted %d banner messages, want 0", threadPosts)
	}
	srv.ensureThreadStatus(context.Background(), "discord", "chan-1", "thread-1", "acme/widget", "feat/x", "", ThreadStatusUpdate{Phase: PhaseFix})
	if parentWrites != 1 {
		t.Fatalf("status messages = %d, want 1", parentWrites)
	}
	if !strings.Contains(statusText, "fix") {
		t.Fatalf("status text = %q, want the FIX phase", statusText)
	}
}

func TestSurfaceDiscipline_AC02(t *testing.T) {
	var sends, edits, banners int
	srv := &Server{tasks: NewTaskThreadRegistry()}
	srv.SetNotifier(func(_ context.Context, _, _, _ string) error {
		banners++
		return nil
	})
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) {
		sends++
		return "status-1", nil
	})
	srv.SetEditor(func(_ context.Context, _, _, _ string, _ string) error {
		edits++
		return nil
	})

	phases := []struct {
		workflow string
		phase    Phase
	}{{"review", PhaseReview}, {"fix", PhaseFix}, {"merged", PhaseMerged}}
	for _, step := range phases {
		srv.postPhaseBannerIfChanged(context.Background(), "discord", "chan-1", "thread-1", "acme/widget", "feat/x", step.workflow, "")
		srv.ensureThreadStatus(context.Background(), "discord", "chan-1", "thread-1", "acme/widget", "feat/x", "", ThreadStatusUpdate{Phase: step.phase})
	}
	if banners != 0 {
		t.Fatalf("banner messages = %d, want 0", banners)
	}
	if sends != 1 || edits != 2 {
		t.Fatalf("status writes = %d sends + %d edits, want 1 + 2 in one message", sends, edits)
	}
}

func TestSurfaceDiscipline_AC03(t *testing.T) {
	ep := config.EndpointConfig{Name: "github", Path: "/github", Secret: "secret", Platform: "discord", ChannelID: "chan-1", Prompt: "p", Admit: defaultCatchAllAdmit()}
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{ep})
	var sends, posts int
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { sends++; return "x", nil })
	srv.SetNotifier(func(_ context.Context, _, _, _ string) error { posts++; return nil })
	id := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-ac03")
	envelope := WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/x", "pr_number": "7", "event_type": "pull_request", "delivery_id": "del-ac03"}
	srv.markSkipped(id, ep, envelope, "admitted no rule for pull_request.opened")
	if sends != 0 || posts != 0 {
		t.Fatalf("skip wrote channel=%d thread=%d, want silent", sends, posts)
	}
	receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-ac03")
	if err != nil || receipt == nil || receipt.Status != store.WebhookStatusSkipped {
		t.Fatalf("receipt = %+v err=%v, want skipped", receipt, err)
	}
	if !strings.Contains(receipt.ErrorSummary, "admitted no rule") {
		t.Fatalf("error_summary = %q, want the admit reason", receipt.ErrorSummary)
	}
}

func TestSurfaceDiscipline_AC04(t *testing.T) {
	ep := config.EndpointConfig{Name: "github", Path: "/github", Secret: "secret", Platform: "discord", ChannelID: "chan-1", Prompt: "p", Admit: defaultCatchAllAdmit()}
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{ep})
	rec := srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskClaimed)
	srv.tasks.LinkPRThread("acme/widget#7", rec)
	srv.tasks.SetSurface("acme/widget#7", SurfaceUpdate{
		RootMessageID: "root-1",
		Repo:          "acme/widget",
		PRNumber:      "7",
		Branch:        "feat/x",
		ThreadID:      "thread-1",
		Phase:         string(PhaseReview),
	})
	beforeRoot, beforePhase, _, _ := srv.tasks.SurfaceState("acme/widget#7")
	var writes int
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { writes++; return "x", nil })
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error { writes++; return nil })
	srv.SetNotifier(func(_ context.Context, _, _, _ string) error { writes++; return nil })

	id := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-ac04")
	envelope := WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/x", "pr_number": "7", "event_type": "pull_request", "delivery_id": "del-ac04"}
	srv.markSkipped(id, ep, envelope, "admitted no rule")

	afterRoot, afterPhase, _, _ := srv.tasks.SurfaceState("acme/widget#7")
	if writes != 0 {
		t.Fatalf("skip wrote %d messages to a bound PR, want 0", writes)
	}
	if beforeRoot != afterRoot || beforePhase != afterPhase {
		t.Fatalf("bound PR surface changed: root %q->%q phase %q->%q", beforeRoot, afterRoot, beforePhase, afterPhase)
	}
}

func TestSurfaceDiscipline_AC08(t *testing.T) {
	var channelSends, threadPosts int
	var statusTargets []string
	srv := &Server{tasks: NewTaskThreadRegistry()}
	srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskClaimed)
	srv.SetSender(func(_ context.Context, _, target, text string) (string, error) {
		if target == "chan-1" {
			channelSends++
		}
		if target == "thread-1" && strings.Contains(text, threadStatusPrefix) {
			statusTargets = append(statusTargets, target)
		}
		return "status-1", nil
	})
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error { return nil })
	srv.SetNotifier(func(_ context.Context, _, target, _ string) error {
		if target == "thread-1" {
			threadPosts++
		}
		return nil
	})
	ep := config.EndpointConfig{Platform: "discord", ChannelID: "chan-1", Workflow: "review"}
	envelope := WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/x", "delivery_id": "del-ac08"}
	srv.emitAudit(context.Background(), ep, envelope, "COMPLETED", "", "")

	if channelSends != 0 {
		t.Fatalf("channel messages = %d, want 0", channelSends)
	}
	if threadPosts != 1 {
		t.Fatalf("thread messages = %d, want 1", threadPosts)
	}
	if len(statusTargets) != 1 {
		t.Fatalf("status writes = %d, want the single status line updated", len(statusTargets))
	}
}

func TestSurfaceDiscipline_AC08TaskThreadHonorsPostedOutcome(t *testing.T) {
	tests := []struct {
		name           string
		status         string
		executorPosted bool
		outcomePosted  bool
	}{
		{name: "executor posted the completion and the audit skips", status: "COMPLETED", executorPosted: true, outcomePosted: true},
		{name: "audit owns the only completion when the run posted none", status: "COMPLETED", executorPosted: false, outcomePosted: false},
		{name: "executor posted the failure and the audit skips", status: "FAILED", executorPosted: true, outcomePosted: true},
		{name: "audit owns the only failure when the run posted none", status: "FAILED", executorPosted: false, outcomePosted: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var threadPosts, statusWrites int
			srv := &Server{tasks: NewTaskThreadRegistry()}
			srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskClaimed)
			srv.SetSender(func(_ context.Context, _, target, text string) (string, error) {
				if target == "thread-1" && strings.Contains(text, threadStatusPrefix) {
					statusWrites++
				}
				return "status-1", nil
			})
			srv.SetEditor(func(_ context.Context, _, _, _, _ string) error { return nil })
			srv.SetNotifier(func(_ context.Context, _, target, _ string) error {
				if target == "thread-1" {
					threadPosts++
				}
				return nil
			})
			ep := config.EndpointConfig{Platform: "discord", ChannelID: "chan-1", Workflow: "review"}
			envelope := WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/x", "delivery_id": "del-ac08-full"}
			if tc.executorPosted {
				slimStatus := TaskClaimed
				if tc.status == "FAILED" {
					slimStatus = TaskFailed
				}
				slim := FormatThreadSlimCard(envelope, ep.Workflow, slimStatus, nil)
				if err := srv.notifyThread(context.Background(), "discord", "thread-1", slim); err != nil {
					t.Fatalf("executor outcome post: %v", err)
				}
			}
			workCtx := &WebhookWorkContext{
				RootMessageID: "root-1",
				ThreadID:      "thread-1",
				OutcomePosted: tc.outcomePosted,
			}
			srv.emitAudit(context.Background(), ep, envelope, tc.status, "", "", workCtx)

			if threadPosts != 1 {
				t.Fatalf("thread outcome posts = %d, want exactly one", threadPosts)
			}
			if statusWrites != 1 {
				t.Fatalf("status writes = %d, want the thread status line updated once", statusWrites)
			}
		})
	}
}

func TestSurfaceDiscipline_AC09(t *testing.T) {
	var sends, edits int
	srv := &Server{tasks: NewTaskThreadRegistry()}
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { sends++; return "card-1", nil })
	srv.SetEditor(func(_ context.Context, _, _, messageID, _ string) error {
		edits++
		if messageID != "card-1" {
			t.Errorf("edit target = %q, want card-1", messageID)
		}
		return nil
	})
	ep := config.EndpointConfig{Platform: "discord", ChannelID: "chan-1", Workflow: "review"}
	envelope := WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/x", "delivery_id": "del-ac09"}
	srv.emitAudit(context.Background(), ep, envelope, "COMPLETED", "", "")
	srv.emitAudit(context.Background(), ep, envelope, "COMPLETED", "", "")
	if sends != 1 || edits != 1 {
		t.Fatalf("channel writes = %d sends + %d edits, want 1 card posted and then edited", sends, edits)
	}
}

func TestSurfaceDiscipline_AC10(t *testing.T) {
	envelope := WebhookEnvelope{
		"repository":  "acme/widget",
		"head_branch": "feat/x",
		"pr_number":   "7",
		"delivery_id": "del-uuid-ac10",
		"event_type":  "pull_request",
		"action":      "opened",
	}
	body := "Delivery: del-uuid-ac10\nEvent: pull_request.opened\nbody"
	cards := []string{
		terminalRootCard(envelope, "review", "COMPLETED", "", "thread-1", "discord", 0),
		FormatPhaseParent("acme/widget", "7", PhaseReview, nil),
		WithTaskHeader(body, envelope, "review", TaskClaimed, nil),
		WithTaskHeaderWithButtons(body, envelope, "review", TaskClaimed, nil, "discord", nil),
	}
	for i, card := range cards {
		for _, banned := range []string{"del-uuid-ac10", "Delivery:", "Event:", "pull_request"} {
			if strings.Contains(card, banned) {
				t.Errorf("root card %d carries %q: %q", i, banned, card)
			}
		}
	}
}

type surfaceSink struct {
	srv      *Server
	platform string
	target   string
	sent     []string
}

func (s *surfaceSink) Send(ctx context.Context, text string) (relay.EditHandle, error) {
	s.sent = append(s.sent, text)
	if err := s.srv.notifyThread(ctx, s.platform, s.target, text); err != nil {
		return nil, err
	}
	return &surfaceHandle{sink: s}, nil
}

func (s *surfaceSink) SendWithButtons(ctx context.Context, text string, _ []channel.Button) (relay.EditHandle, error) {
	return s.Send(ctx, text)
}

func (s *surfaceSink) SendTyping(context.Context) error { return nil }

type surfaceHandle struct {
	sink *surfaceSink
}

func (h *surfaceHandle) Ref() channel.MessageRef { return nil }

func (h *surfaceHandle) Edit(_ context.Context, text string) error {
	h.sink.sent = append(h.sink.sent, text)
	return nil
}

func (h *surfaceHandle) EditWithButtons(ctx context.Context, text string, _ []channel.Button) error {
	return h.Edit(ctx, text)
}

func TestSurfaceDiscipline_AC11(t *testing.T) {
	var sends, edits int
	srv := &Server{tasks: NewTaskThreadRegistry()}
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { sends++; return "status-1", nil })
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error { edits++; return nil })
	sink := &surfaceSink{srv: srv, platform: "discord", target: "thread-1"}
	streamer := relay.NewStreamerWithSink(sink, render.New(), render.Discord, time.Minute)
	streamer.SetVerboseSteps(false)
	streamer.SetNarration(false)

	events := make(chan relay.Event, 8)
	events <- relay.Event{Type: relay.EventTool, Delta: "bash", ToolContext: "gh pr view"}
	events <- relay.Event{Type: relay.EventTool, Delta: "read_file", ToolContext: "main.go"}
	events <- relay.Event{Type: relay.EventDelta, Delta: "I'll review this PR."}
	events <- relay.Event{Type: relay.EventReasoning}
	events <- relay.Event{Type: relay.EventDelta, Delta: "Verdict: APPROVED"}
	events <- relay.Event{Type: relay.EventDone}
	close(events)
	if err := streamer.Run(context.Background(), events); err != nil {
		t.Fatalf("streamer run: %v", err)
	}
	if len(sink.sent) != 2 {
		t.Fatalf("thread messages = %d, want one rollup plus one outcome: %v", len(sink.sent), sink.sent)
	}
	if !strings.Contains(sink.sent[1], "Verdict: APPROVED") {
		t.Fatalf("outcome message = %q, want the final segment", sink.sent[1])
	}
	if !streamer.OutcomePosted() {
		t.Fatal("streamer must record the outcome it posted")
	}

	prKey := "acme/widget#7"
	srv.tasks.SetSurface(prKey, SurfaceUpdate{
		RootMessageID: "root-1",
		Repo:          "acme/widget",
		PRNumber:      "7",
		Branch:        "feat/x",
		ThreadID:      "thread-1",
		Phase:         string(PhaseReview),
	})
	ep := config.EndpointConfig{Platform: "discord", ChannelID: "chan-1", Workflow: "review"}
	envelope := WebhookEnvelope{
		"repository":  "acme/widget",
		"head_branch": "feat/x",
		"pr_number":   "7",
		"event_type":  "pull_request",
	}
	workCtx := &WebhookWorkContext{
		RootMessageID: "root-1",
		ThreadID:      "thread-1",
		PRKey:         prKey,
		OutcomePosted: true,
	}
	srv.emitAudit(context.Background(), ep, envelope, "COMPLETED", "", "", workCtx)
	if len(sink.sent) != 2 {
		t.Fatalf("thread messages after the completion audit = %d, want no second outcome: %v", len(sink.sent), sink.sent)
	}

	srv.ensureThreadStatus(context.Background(), "discord", "chan-1", "thread-1", "acme/widget", "feat/x", "", ThreadStatusUpdate{Phase: PhaseFix})
	if sends != 1 || edits != 1 {
		t.Fatalf("status writes = %d sends + %d edits, want one status line edited once", sends, edits)
	}
}

func TestSurfaceDiscipline_AC12(t *testing.T) {
	var mentionUser, channelTarget, text string
	srv := &Server{}
	srv.escalationMention = "operator"
	srv.SetMentionResolver(func(_ context.Context, _, name string) (string, bool) {
		if name != "operator" {
			return "", false
		}
		return "user-42", true
	})
	srv.SetMentionNotifier(func(_ context.Context, _, channelID, body, userID string) error {
		channelTarget = channelID
		mentionUser = userID
		text = body
		return nil
	})
	ep := config.EndpointConfig{Platform: "discord", ChannelID: "chan-1"}
	srv.escalateToChannel(context.Background(), ep, "acme/widget", "7", PhaseBlocked, "fix run failed", "retry exhausted")
	if channelTarget != "chan-1" || mentionUser != "user-42" {
		t.Fatalf("escalation target/mention = %q/%q, want chan-1/user-42", channelTarget, mentionUser)
	}
	if !strings.Contains(text, "acme/widget#7") {
		t.Fatalf("escalation copy = %q, want the PR key", text)
	}
}

type fakeSessionRows struct {
	mu          sync.Mutex
	sessionID   string
	pid         int
	deactivated int
}

func (f *fakeSessionRows) Active(context.Context, string, string, string, string) (string, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sessionID, f.pid, nil
}

func (f *fakeSessionRows) Deactivate(context.Context, string, string, string, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deactivated++
	return nil
}

func TestSurfaceDiscipline_AC14(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.tasks.UpsertWithSpec("acme/widget", "feat/x", "root-1", "thread-1", TaskClaimed, "specs/orig.md")
	srv.SetBranchChecker(func(context.Context, string, string) (BranchPRResult, error) { return BranchPRResult{}, nil })
	srv.SetBranchExistsChecker(func(context.Context, string, string) (bool, error) { return true, nil })
	var posts []string
	srv.SetNotifier(func(_ context.Context, _, target, text string) error {
		if target == "thread-1" {
			posts = append(posts, text)
		}
		return nil
	})
	rec := postDispatch(t, srv, "dispatch-token", dispatchJSON(t, map[string]any{
		"repo":   "acme/widget",
		"branch": "feat/x",
		"brief":  "Implement the surface discipline.",
		"mode":   "redispatch",
	}))
	if rec.Code != http.StatusCreated {
		t.Fatalf("redispatch status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	if len(posts) != 2 {
		t.Fatalf("thread posts = %d, want separator plus brief: %v", len(posts), posts)
	}
	if !strings.Contains(strings.ToLower(posts[0]), "re-dispatch") {
		t.Fatalf("first post = %q, want the separator", posts[0])
	}
	if !strings.Contains(posts[1], "Implement the surface discipline.") {
		t.Fatalf("second post = %q, want the brief", posts[1])
	}
	envelope := minimalEnvelope()
	injectTaskContext(envelope, WebhookExecutionKey{Repository: "acme/widget", Branch: "feat/x"}, srv.dispatchTasks)
	if envelope["task_brief"] != "Implement the surface discipline." {
		t.Fatalf("claim brief = %v, want the re-dispatched brief", envelope["task_brief"])
	}

	empty := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	empty.tasks.UpsertWithSpec("acme/widget", "feat/x", "root-1", "thread-1", TaskClaimed, "")
	refused := postDispatch(t, empty, "dispatch-token", dispatchJSON(t, map[string]any{
		"repo":   "acme/widget",
		"branch": "feat/x",
		"mode":   "redispatch",
	}))
	if refused.Code != http.StatusConflict {
		t.Fatalf("empty redispatch status = %d, want 409: %s", refused.Code, refused.Body.String())
	}
	if !strings.Contains(refused.Body.String(), "no brief") {
		t.Fatalf("empty redispatch error = %q, want no brief", refused.Body.String())
	}
}

func TestSurfaceDiscipline_AC15(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.tasks.UpsertWithSpec("acme/widget", "feat/x", "root-1", "thread-1", TaskClaimed, "specs/orig.md")
	rows := &fakeSessionRows{sessionID: "sess-live", pid: 4242}
	srv.SetSessionRowStore(rows)
	srv.SetProcessAlive(func(pid int) bool { return pid == 4242 })
	var posts int
	srv.SetNotifier(func(_ context.Context, _, _, _ string) error { posts++; return nil })

	rec := postDispatch(t, srv, "dispatch-token", dispatchJSON(t, map[string]any{
		"repo":   "acme/widget",
		"branch": "feat/x",
		"brief":  "start another run",
		"mode":   "redispatch",
	}))
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "run in progress") {
		t.Fatalf("error = %q, want run in progress", rec.Body.String())
	}
	if got := srv.dispatchTasks.Count(); got != 0 {
		t.Fatalf("dispatch tasks = %d, want 0 (no run spawned)", got)
	}
	if posts != 0 {
		t.Fatalf("thread posts = %d, want 0", posts)
	}
}

func TestSurfaceDiscipline_AC16(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.tasks.UpsertWithSpec("acme/widget", "feat/x", "root-1", "thread-1", TaskClaimed, "specs/orig.md")
	rows := &fakeSessionRows{sessionID: "sess-dead", pid: 999999}
	srv.SetSessionRowStore(rows)
	srv.SetProcessAlive(func(int) bool { return false })
	srv.SetBranchExistsChecker(func(context.Context, string, string) (bool, error) { return true, nil })

	rec := postDispatch(t, srv, "dispatch-token", dispatchJSON(t, map[string]any{
		"repo":   "acme/widget",
		"branch": "feat/x",
		"brief":  "start again",
		"mode":   "redispatch",
	}))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 after a dead session: %s", rec.Code, rec.Body.String())
	}
	rows.mu.Lock()
	deactivated := rows.deactivated
	rows.mu.Unlock()
	if deactivated != 1 {
		t.Fatalf("stale session deactivations = %d, want 1", deactivated)
	}
	if got := srv.dispatchTasks.Count(); got != 1 {
		t.Fatalf("dispatch tasks = %d, want the re-dispatch admitted", got)
	}
}
