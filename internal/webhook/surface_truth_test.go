package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

const (
	truthForum  = "forum-1"
	truthLegacy = "legacy-1"
	truthPR     = "acme/widget#181"
)

type parentEditLog struct {
	mu    sync.Mutex
	calls []surfaceMessage
}

func (l *parentEditLog) add(msg surfaceMessage) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, msg)
}

func (l *parentEditLog) take() []surfaceMessage {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.calls
	l.calls = nil
	return out
}

func moveSurfaceServerToForum(srv *Server) {
	for path, ep := range srv.endpoints {
		ep.ChannelID = truthForum
		ep.ForumChannelID = truthForum
		ep.LegacyChannelID = truthLegacy
		srv.endpoints[path] = ep
		srv.endpointsByName[ep.Name] = ep
	}
}

func parentEditor(srv *Server, edits *parentEditLog, missing func(channelID, messageID string) bool) {
	editor := func(_ context.Context, _, channelID, messageID, text string, buttons []channel.Button) error {
		edits.add(surfaceMessage{channelID: channelID, messageID: messageID, text: text, buttons: buttons})
		if missing(channelID, messageID) {
			return fmt.Errorf("%w: unknown message", channel.ErrMessageNotFound)
		}
		return nil
	}
	srv.SetEditorWithButtons(editor)
	srv.SetEditor(func(ctx context.Context, platform, channelID, messageID, text string) error {
		return editor(ctx, platform, channelID, messageID, text, nil)
	})
}

func seedLegacyParent(t *testing.T, srv *Server, st *store.SQLiteStore, rootID string) time.Time {
	t.Helper()
	now := time.Now()
	delivery := seedRunDelivery(t, st, truthPR, now.Add(-5*time.Minute), now)
	seedActiveRun(t, st, truthPR, "thread-9", rootID, delivery.ID)
	seedParentSurface(srv, truthPR, "thread-9", rootID, PhaseReview, now)
	return now
}

func TestSurfaceHealEditsLegacyCardInPlace(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	moveSurfaceServerToForum(srv)
	now := seedLegacyParent(t, srv, st, "legacy-root")
	edits := &parentEditLog{}
	parentEditor(srv, edits, func(channelID, _ string) bool { return channelID != truthLegacy })
	ctx := context.Background()

	srv.syncPhaseParent(ctx, srv.endpoints["/fix"], truthPR, surfaceEnvelope(), PhaseFix)

	calls := edits.take()
	if len(calls) != 2 || calls[0].channelID != truthForum || calls[1].channelID != truthLegacy || calls[1].messageID != "legacy-root" {
		t.Fatalf("edit calls = %+v, want forum then legacy for the same message", calls)
	}
	if len(rec.sent) != 0 {
		t.Fatalf("parent sends = %d, want none", len(rec.sent))
	}
	if detail, _ := srv.tasks.SurfaceDetail(truthPR); detail.RootMessageID != "legacy-root" {
		t.Fatalf("surface root = %q, want legacy-root kept", detail.RootMessageID)
	}
	if row, _ := st.PRQueueRepo().Get(ctx, truthPR); row == nil || row.RootMsgID != "legacy-root" {
		t.Fatalf("persisted root = %+v, want legacy-root", row)
	}

	srv.syncPhaseParent(ctx, srv.endpoints["/merge"], truthPR, surfaceEnvelope(), PhaseMerge)
	if calls := edits.take(); len(calls) != 1 || calls[0].channelID != truthLegacy {
		t.Fatalf("next phase write = %+v, want one edit at the legacy channel", calls)
	}

	srv.refreshRunAges(ctx, now.Add(2*time.Minute))
	ageParentEdits := 0
	for _, call := range edits.take() {
		if call.messageID != "legacy-root" {
			continue
		}
		ageParentEdits++
		if call.channelID != truthLegacy {
			t.Fatalf("age tick edited the parent at %q, want the legacy channel", call.channelID)
		}
	}
	if ageParentEdits != 1 {
		t.Fatalf("age tick parent edits = %d, want exactly one at the legacy channel", ageParentEdits)
	}
}

func TestSurfaceHealReanchorsLostParentOnce(t *testing.T) {
	logs := captureServerLogs(t)
	srv, st, rec := newSurfaceServer(t)
	moveSurfaceServerToForum(srv)
	now := seedLegacyParent(t, srv, st, "gone-root")
	edits := &parentEditLog{}
	parentEditor(srv, edits, func(string, string) bool { return true })
	ctx := context.Background()

	srv.syncPhaseParent(ctx, srv.endpoints["/fix"], truthPR, surfaceEnvelope(), PhaseFix)

	if len(rec.sent) != 1 {
		t.Fatalf("parent sends = %d, want exactly one re-anchor", len(rec.sent))
	}
	row, _ := st.PRQueueRepo().Get(ctx, truthPR)
	if row == nil || row.RootMsgID != "parent-1" || row.ThreadID != "parent-1" {
		t.Fatalf("persisted binding = %+v, want the new forum post as root and thread", row)
	}
	if detail, _ := srv.tasks.SurfaceDetail(truthPR); detail.RootMessageID != "parent-1" || detail.ThreadID != "parent-1" {
		t.Fatalf("surface = %+v, want parent-1 as root and thread", detail)
	}
	if len(logs.all("webhook: parent surface re-anchored")) != 1 {
		t.Fatal("want one re-anchor line")
	}

	edits.take()
	srv.syncPhaseParent(ctx, srv.endpoints["/merge"], truthPR, surfaceEnvelope(), PhaseMerge)
	if len(rec.sent) != 1 {
		t.Fatalf("parent sends after a second loss = %d, want still one", len(rec.sent))
	}
	if warns := logs.all("webhook: phase parent edit failed"); len(warns) != 0 {
		t.Fatalf("cooldown-suppressed phase write logged WARN: %+v", warns)
	}
	if debug := logs.all("webhook: parent surface lost again, re-anchor cooling down"); len(debug) != 1 {
		t.Fatalf("cooldown DEBUG lines = %d, want exactly one", len(debug))
	}
	edits.take()
	srv.refreshRunAges(ctx, now.Add(2*time.Minute))
	for _, call := range edits.take() {
		if call.messageID == "parent-1" || call.messageID == "gone-root" {
			t.Fatalf("age tick edited a lost parent during cooldown: %+v", call)
		}
	}
}

func TestSurfaceHealHealthyParentEditsInPlace(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	moveSurfaceServerToForum(srv)
	seedLegacyParent(t, srv, st, "parent-1")
	edits := &parentEditLog{}
	parentEditor(srv, edits, func(string, string) bool { return false })

	srv.syncPhaseParent(context.Background(), srv.endpoints["/fix"], truthPR, surfaceEnvelope(), PhaseFix)

	if calls := edits.take(); len(calls) != 1 || calls[0].channelID != truthForum {
		t.Fatalf("edit calls = %+v, want one edit in the delivery channel", calls)
	}
	if len(rec.sent) != 0 {
		t.Fatalf("parent sends = %d, want none", len(rec.sent))
	}
}

func mergedSurfaceWithOpenEvent(t *testing.T, state string, err error) (*Server, *logCapture, *parentEditLog, *int) {
	t.Helper()
	logs := captureServerLogs(t)
	srv, st, _ := newSurfaceServer(t)
	now := time.Now()
	delivery := seedRunDelivery(t, st, truthPR, now.Add(-5*time.Minute), now)
	seedActiveRun(t, st, truthPR, "thread-9", "parent-1", delivery.ID)
	seedParentSurface(srv, truthPR, "thread-9", "parent-1", PhaseMerged, now)
	srv.tasks.SetSurface(truthPR, SurfaceUpdate{Phase: string(PhaseMerged)})
	lookups := 0
	srv.SetPRStateResolver(func(context.Context, string, string) (string, error) {
		lookups++
		return state, err
	})
	edits := &parentEditLog{}
	parentEditor(srv, edits, func(string, string) bool { return false })
	return srv, logs, edits, &lookups
}

func openEnvelope() WebhookEnvelope {
	envelope := surfaceEnvelope()
	envelope["pr_state"] = "open"
	envelope["merged"] = false
	return envelope
}

func TestMergedRepairRefusedWhenLiveStateIsMerged(t *testing.T) {
	srv, logs, edits, _ := mergedSurfaceWithOpenEvent(t, PRStateMerged, nil)

	srv.syncPhaseParent(context.Background(), srv.endpoints["/review"], truthPR, openEnvelope(), PhaseQueued)

	if _, phase, _, _ := srv.tasks.SurfaceState(truthPR); phase != PhaseMerged {
		t.Fatalf("phase = %q, want merged kept", phase)
	}
	if calls := edits.take(); len(calls) != 0 {
		t.Fatalf("edits = %+v, want no write", calls)
	}
	refused := logs.all("webhook: merged repair refused")
	if len(refused) != 1 || refused[0].attrs["evidence"] != "live_state" {
		t.Fatalf("refused lines = %+v, want one with evidence=live_state", refused)
	}
	if len(logs.all("webhook: surface merged repaired")) != 0 {
		t.Fatal("surface must not be repaired")
	}
}

func TestMergedRepairUsesLiveEvidence(t *testing.T) {
	srv, logs, _, _ := mergedSurfaceWithOpenEvent(t, PRStateOpen, nil)

	srv.syncPhaseParent(context.Background(), srv.endpoints["/review"], truthPR, openEnvelope(), PhaseQueued)

	repaired := logs.all("webhook: surface merged repaired")
	if len(repaired) != 1 {
		t.Fatalf("repairs = %d, want one", len(repaired))
	}
	if evidence := fmt.Sprint(repaired[0].attrs["evidence"]); !strings.HasPrefix(evidence, "live pr_state=") {
		t.Fatalf("evidence = %q, want live pr_state=…", evidence)
	}
}

func TestMergedRepairDeferredOnLookupError(t *testing.T) {
	srv, logs, edits, _ := mergedSurfaceWithOpenEvent(t, "", errors.New("gh down"))

	srv.syncPhaseParent(context.Background(), srv.endpoints["/review"], truthPR, openEnvelope(), PhaseQueued)

	if _, phase, _, _ := srv.tasks.SurfaceState(truthPR); phase != PhaseMerged {
		t.Fatalf("phase = %q, want merged kept", phase)
	}
	if calls := edits.take(); len(calls) != 0 {
		t.Fatalf("edits = %+v, want no write", calls)
	}
	if len(logs.all("webhook: merged repair deferred")) != 1 {
		t.Fatal("want one deferred line")
	}
}

func TestMergedRepairSkipsLookupWithoutContradiction(t *testing.T) {
	srv, _, _, lookups := mergedSurfaceWithOpenEvent(t, PRStateMerged, nil)

	srv.syncPhaseParent(context.Background(), srv.endpoints["/merged"], truthPR, mergeSurfaceEnvelope(), PhaseMerged)

	if *lookups != 0 {
		t.Fatalf("live state lookups = %d, want none without a contradiction", *lookups)
	}
}

func TestMergedWorkdirUsesRepoCheckout(t *testing.T) {
	turn := make(chan WebhookWorkContext, 1)
	exec := func(_ context.Context, _, _ string, _ string, workCtx *WebhookWorkContext) error {
		turn <- *workCtx
		return nil
	}
	srv, st, _, _ := newIsolationServer(t, exec)
	setPRState(srv, PRStateMerged, nil)
	for path, ep := range srv.endpoints {
		ep.Workspace = config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/srv/checkouts/widget", Mode: config.WorkspaceModeMutable}
		srv.endpoints[path] = ep
	}
	ctx := context.Background()
	ep := srv.endpoints["/merged"]
	deliveryID := "del-merged-workdir"
	seedProcessingDelivery(t, st, ep.Name, deliveryID, "pull_request")
	receipt, _ := st.WebhookDeliveryRepo().Get(ctx, ep.Name, deliveryID)
	item := dispatchItem{ep: ep, body: []byte(mergedBody(7)), deliveryID: deliveryID, eventType: "pull_request", payloadHash: deliveryID, prKey: "acme/widget#7"}
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, srv.verdicts, false, "", ep.CommentTrigger)

	srv.handleMergedDelivery(item, receipt, envelope, nil)

	select {
	case workCtx := <-turn:
		if workCtx.Worktree != "/srv/checkouts/widget" {
			t.Fatalf("merged lane workdir = %q, want the repo checkout", workCtx.Worktree)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the post-merge work never ran")
	}
}

func runMergeLane(t *testing.T, state string, err error, deliveryID string) (*store.SQLiteStore, *Server) {
	t.Helper()
	srv, st, _ := newSurfaceServer(t)
	setPRState(srv, state, err)
	ep := srv.endpoints["/merge"]
	ep.Admit = defaultCatchAllAdmit()
	srv.endpoints["/merge"] = ep
	body := []byte(prReviewBody(181, "feat/x", "owner", "approved", "c0ffee"))
	receiptID := seedProcessingDelivery(t, st, ep.Name, deliveryID, "pull_request_review")
	if execErr := srv.executeDelivery(ep, body, receiptID, deliveryID, "pull_request_review", 1, nil, func() bool { return false }, nil); execErr != nil {
		t.Fatalf("execute merge lane: %v", execErr)
	}
	return st, srv
}

func TestMergeTruthUnconfirmedMergeIsSkipped(t *testing.T) {
	st, _ := runMergeLane(t, PRStateOpen, nil, "del-merge-open")
	receipt, _ := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-merge", "del-merge-open")
	if receipt == nil || receipt.Status != store.WebhookStatusSkipped || !strings.Contains(receipt.ErrorSummary, "merge not confirmed: PR is open") {
		t.Fatalf("receipt = %+v, want skipped with merge not confirmed: PR is open", receipt)
	}
}

func TestMergeTruthLookupFailureIsSkipped(t *testing.T) {
	st, _ := runMergeLane(t, "", errors.New("gh down"), "del-merge-err")
	receipt, _ := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-merge", "del-merge-err")
	if receipt == nil || receipt.Status != store.WebhookStatusSkipped || !strings.Contains(receipt.ErrorSummary, "live state lookup failed") {
		t.Fatalf("receipt = %+v, want skipped with live state lookup failed", receipt)
	}
}

func TestMergeTruthConfirmedMergeCompletes(t *testing.T) {
	st, srv := runMergeLane(t, PRStateMerged, nil, "del-merge-ok")
	receipt, _ := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-merge", "del-merge-ok")
	if receipt == nil || receipt.Status != store.WebhookStatusCompleted {
		t.Fatalf("receipt = %+v, want completed", receipt)
	}
	if _, phase, _, _ := srv.prSurfaceState(truthPR); phase != PhaseMerged {
		t.Fatalf("phase = %q, want merged", phase)
	}
}

func TestNoisyLinesStayBelowInfo(t *testing.T) {
	var out strings.Builder
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	m := NewWorkspaceManager()
	m.logBindingMismatch(WorkspaceRequest{Lane: "fix", BindingWorktree: "/a"}, &WorkspaceLease{Path: "/b"})

	if strings.Contains(out.String(), "workspace binding mismatch") {
		t.Fatalf("INFO capture = %q, want the mismatch line at DEBUG", out.String())
	}
}
