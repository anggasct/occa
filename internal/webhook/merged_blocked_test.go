package webhook

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/store"
)

const mergedBlockedKey = "acme/widget#7"

func noopExec(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
	return nil
}

// seedBlockedRow leaves the PR the way a no-change fix hand-off does: an
// active session on the row, then blocked with an operator reason, escalated.
func seedBlockedRow(t *testing.T, srv *Server, st *store.SQLiteStore, reason string) {
	t.Helper()
	ctx := context.Background()
	repo := st.PRQueueRepo()
	if _, err := repo.EnsurePR(ctx, mergedBlockedKey, "", ""); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	if err := repo.SetActive(ctx, mergedBlockedKey, "fix", "ses-blocked-1", 1); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	if err := repo.SetRetryState(ctx, mergedBlockedKey, 2, 0); err != nil {
		t.Fatalf("SetRetryState: %v", err)
	}
	if err := repo.SetBlocked(ctx, mergedBlockedKey, reason); err != nil {
		t.Fatalf("SetBlocked: %v", err)
	}
	srv.markEscalated(mergedBlockedKey)
}

func postMerged(t *testing.T, url, deliveryID string) {
	t.Helper()
	if resp := post(t, url+"/merged?secret=s3cret", deliveryID, "pull_request", mergedBody(7)); resp.StatusCode != http.StatusOK {
		t.Fatalf("post merged: status %d", resp.StatusCode)
	}
}

func isEscalated(srv *Server, prKey string) bool {
	srv.escalationMu.Lock()
	defer srv.escalationMu.Unlock()
	return srv.escalated[prKey]
}

// AC-01, AC-02, AC-07: a confirmed merge drains the blocked row, forgets the
// escalation, and the next recovery no longer counts the PR as blocked.
func TestMergedDeliveryClosesBlockedRow(t *testing.T) {
	logs := captureServerLogs(t)
	srv, st, _, url := newIsolationServer(t, noopExec)
	setPRState(srv, PRStateMerged, nil)
	reason := "fix run made no change — operator action required"
	seedBlockedRow(t, srv, st, reason)

	postMerged(t, url, "del-merged-blocked")
	waitForDeliveryStatus(t, st, "github-widget-merged", "del-merged-blocked", store.WebhookStatusCompleted)

	run, err := st.PRQueueRepo().Get(context.Background(), mergedBlockedKey)
	if err != nil || run == nil {
		t.Fatalf("pr row = %+v %v", run, err)
	}
	if run.Status != store.PRStatusDrained || run.BlockedReason != "" || run.RetryCount != 0 || run.ActiveSession != "" || run.ActiveKind != "" {
		t.Fatalf("a merged PR must drain its blocked row: %+v", run)
	}
	receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-merged", "del-merged-blocked")
	if err != nil || receipt == nil || !strings.Contains(receipt.ErrorSummary, "merged — queue drained") {
		t.Fatalf("merged receipt = %+v %v", receipt, err)
	}
	closed := logs.all("webhook: merged PR closed its blocked row")
	if len(closed) != 1 {
		t.Fatalf("closed-row log lines = %d, want 1", len(closed))
	}
	if got := attrValue(closed[0], "blocked_reason"); got != reason {
		t.Fatalf("closed-row log blocked_reason = %q, want %q", got, reason)
	}
	if isEscalated(srv, mergedBlockedKey) {
		t.Fatal("a merged PR must forget its escalation")
	}
	if got := srv.RecoverPRQueue(); !strings.Contains(got, "blocked 0") {
		t.Fatalf("recovery after the merge = %q, want blocked 0", got)
	}
}

// AC-03: an unconfirmed merge leaves the blocked row exactly as it was.
func TestUnconfirmedMergeKeepsBlockedRow(t *testing.T) {
	logs := captureServerLogs(t)
	srv, st, _, url := newIsolationServer(t, noopExec)
	setPRState(srv, PRStateOpen, nil)
	reason := "fix exhausted after 2/2 retries (no work in lease) — operator action required"
	seedBlockedRow(t, srv, st, reason)

	postMerged(t, url, "del-merged-open")
	waitForDeliveryStatus(t, st, "github-widget-merged", "del-merged-open", store.WebhookStatusSkipped)

	run, err := st.PRQueueRepo().Get(context.Background(), mergedBlockedKey)
	if err != nil || run == nil {
		t.Fatalf("pr row = %+v %v", run, err)
	}
	if run.Status != store.PRStatusBlocked || run.BlockedReason != reason {
		t.Fatalf("an open PR must stay blocked: %+v", run)
	}
	if !isEscalated(srv, mergedBlockedKey) {
		t.Fatal("an open PR must keep its escalation")
	}
	if len(logs.all("webhook: merged PR closed its blocked row")) != 0 {
		t.Fatal("an unconfirmed merge must not report a closed row")
	}
}

// AC-04: an active row drains as before, with no closed-row line.
func TestMergedDeliveryActiveRowUnchanged(t *testing.T) {
	logs := captureServerLogs(t)
	srv, st, _, url := newIsolationServer(t, noopExec)
	setPRState(srv, PRStateMerged, nil)
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, mergedBlockedKey, "", ""); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	if err := st.PRQueueRepo().SetActive(ctx, mergedBlockedKey, "fix", "ses-active-1", 1); err != nil {
		t.Fatalf("SetActive: %v", err)
	}

	postMerged(t, url, "del-merged-active")
	waitForDeliveryStatus(t, st, "github-widget-merged", "del-merged-active", store.WebhookStatusCompleted)

	run, err := st.PRQueueRepo().Get(ctx, mergedBlockedKey)
	if err != nil || run == nil || run.Status != store.PRStatusDrained || run.ActiveSession != "" {
		t.Fatalf("an active row must drain on merge: %+v %v", run, err)
	}
	if len(logs.all("webhook: merged PR closed its blocked row")) != 0 {
		t.Fatal("an active row is not a blocked row")
	}
}

type failingUnblockRepo struct {
	store.PRQueueRepo
}

func (failingUnblockRepo) Unblock(context.Context, string) error {
	return errors.New("disk on fire")
}

// AC-05: a failed unblock is logged and the merge still completes.
func TestMergedDeliveryUnblockFailureStillCompletes(t *testing.T) {
	logs := captureServerLogs(t)
	srv, st, _, url := newIsolationServer(t, noopExec)
	setPRState(srv, PRStateMerged, nil)
	seedBlockedRow(t, srv, st, "fix run made no change — operator action required")
	srv.SetPRQueueRepo(failingUnblockRepo{st.PRQueueRepo()})

	postMerged(t, url, "del-merged-unblock-fail")
	waitForDeliveryStatus(t, st, "github-widget-merged", "del-merged-unblock-fail", store.WebhookStatusCompleted)
	waitForSurfacePhase(t, srv, mergedBlockedKey, PhaseMerged)

	warned := logs.all("webhook: merged unblock failed")
	if len(warned) != 1 || !strings.Contains(attrValue(warned[0], "error"), "disk on fire") {
		t.Fatalf("unblock failure log = %+v, want one line naming the error", warned)
	}
	if len(logs.all("webhook: merged PR closed its blocked row")) != 0 {
		t.Fatal("a failed unblock must not report a closed row")
	}
}
