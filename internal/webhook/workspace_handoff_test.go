package webhook

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func fixEndpointWithWorkspace() config.EndpointConfig {
	ep := prTestEndpoint("fix", "/fix", config.AutomationFull)
	ep.Workspace = config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/widget", Mode: config.WorkspaceModeMutable}
	return ep
}

type handoffCase struct {
	srv *Server
	st  *store.SQLiteStore
	url string
	rec *prRecorder
}

func newHandoffServer(t *testing.T, werr error, prState string) handoffCase {
	t.Helper()
	rec := newPRRecorder("thread-pr", nil)
	srv, st, url := newPRTestServer(t, rec, []config.EndpointConfig{fixEndpointWithWorkspace()})
	srv.workspaceRetryBackoff = nil
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{err: werr})
	srv.SetPRStateResolver(func(context.Context, string, string) (string, error) {
		if prState == "" {
			return "", fmt.Errorf("gh down")
		}
		return prState, nil
	})
	return handoffCase{srv: srv, st: st, url: url, rec: rec}
}

func (c handoffCase) post(t *testing.T, deliveryID string) {
	t.Helper()
	if resp := post(t, c.url+"/fix?secret=s3cret", deliveryID, "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "abc1234def5678")); resp.StatusCode != 200 {
		t.Fatalf("post status = %d", resp.StatusCode)
	}
}

func waitForBlocked(t *testing.T, st *store.SQLiteStore, prKey string) *store.PRRun {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if row, err := st.PRQueueRepo().Get(context.Background(), prKey); err == nil && row != nil && row.Status == store.PRStatusBlocked {
			return row
		}
		time.Sleep(10 * time.Millisecond)
	}
	row, _ := st.PRQueueRepo().Get(context.Background(), prKey)
	t.Fatalf("PR row = %+v, want blocked", row)
	return nil
}

// AC-01: a dirty worktree after the retry budget blocks the open PR.
func TestWorkspaceDirtyHandsOffAsBlocked(t *testing.T) {
	logs := captureServerLogs(t)
	c := newHandoffServer(t, fmt.Errorf("%w: worktree at /projects/widget/.worktree/x has uncommitted changes", ErrWorkspaceDirty), PRStateOpen)
	c.post(t, "del-dirty-1")
	waitForReceipt(t, c.st, store.WebhookStatusFailed)
	row := waitForBlocked(t, c.st, "acme/widget#7")
	if !strings.HasPrefix(row.BlockedReason, "fix could not get its worktree — operator action required: workspace dirty: worktree at") {
		t.Fatalf("blocked reason = %q", row.BlockedReason)
	}
	waitForLog(t, logs, "webhook: PR handed off, workspace not usable")
	if n := len(c.rec.order); n != 0 {
		t.Fatalf("agent turns = %d, want 0", n)
	}
}

// AC-02: a worktree conflict blocks on the first attempt.
func TestWorktreeConflictHandsOffAsBlocked(t *testing.T) {
	// F-007: captureServerLogs swaps the process-global logger, so this
	// test must observe its own hand-off WARN before ending; a WARN left
	// pending by the dispatcher bleeds into the next test's capture.
	logs := captureServerLogs(t)
	c := newHandoffServer(t, fmt.Errorf("%w: branch feat/x attached at /projects/widget/.worktree/y belongs to a different execution key", ErrWorktreeConflict), PRStateOpen)
	c.srv.workspaceRetryBackoff = []time.Duration{time.Hour}
	c.post(t, "del-conflict-1")
	row := waitForBlocked(t, c.st, "acme/widget#7")
	if !strings.Contains(row.BlockedReason, "worktree conflict: branch feat/x attached at") {
		t.Fatalf("blocked reason = %q", row.BlockedReason)
	}
	waitForLog(t, logs, "webhook: PR handed off, workspace not usable")
}

// AC-03: merged, closed or unknown PR state → no block.
func TestWorkspaceHandOffSkipsNonOpenPR(t *testing.T) {
	for _, state := range []string{PRStateMerged, PRStateClosed, ""} {
		t.Run("state="+state, func(t *testing.T) {
			logs := captureServerLogs(t)
			c := newHandoffServer(t, fmt.Errorf("%w: dirty", ErrWorkspaceDirty), state)
			c.post(t, "del-nonopen-"+state)
			waitForReceipt(t, c.st, store.WebhookStatusFailed)
			time.Sleep(50 * time.Millisecond)
			if row, _ := c.st.PRQueueRepo().Get(context.Background(), "acme/widget#7"); row != nil && row.Status == store.PRStatusBlocked {
				t.Fatalf("PR blocked for state %q", state)
			}
			if len(logs.all("webhook: PR handed off, workspace not usable")) != 0 {
				t.Fatal("no hand-off expected")
			}
		})
	}
}

// AC-04: an already blocked PR is not escalated twice.
func TestWorkspaceHandOffDoesNotReEscalate(t *testing.T) {
	logs := captureServerLogs(t)
	c := newHandoffServer(t, fmt.Errorf("%w: dirty", ErrWorkspaceDirty), PRStateOpen)
	row := &store.PRRun{PRKey: "acme/widget#7", Status: store.PRStatusBlocked, ActiveKind: "fix"}
	c.srv.handOffWorkspace(fixEndpointWithWorkspace(), "acme/widget#7", row, ErrWorkspaceDirty)
	if len(logs.all("webhook: PR handed off, workspace not usable")) != 0 {
		t.Fatal("an already blocked PR must not be handed off again")
	}
}

// AC-05: other workspace errors keep today's behaviour (no block).
func TestWorkspaceUnavailableDoesNotHandOff(t *testing.T) {
	c := newHandoffServer(t, fmt.Errorf("%w: resolver down", ErrWorkspaceUnavailable), PRStateOpen)
	c.post(t, "del-unavail-1")
	waitForReceipt(t, c.st, store.WebhookStatusFailed)
	time.Sleep(50 * time.Millisecond)
	if row, _ := c.st.PRQueueRepo().Get(context.Background(), "acme/widget#7"); row != nil && row.Status == store.PRStatusBlocked {
		t.Fatal("ErrWorkspaceUnavailable must not block the PR")
	}
}

// AC-07: the reason is clamped and redacts the endpoint secret.
func TestWorkspaceHandOffReasonBoundedAndRedacted(t *testing.T) {
	c := newHandoffServer(t, nil, PRStateOpen)
	ctx := context.Background()
	if _, err := c.st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-7", "root-7"); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	row, _ := c.st.PRQueueRepo().Get(ctx, "acme/widget#7")
	row.ActiveKind = "fix"
	ep := fixEndpointWithWorkspace()
	werr := fmt.Errorf("%w: s3cret %s", ErrWorkspaceDirty, strings.Repeat("z", 600))
	c.srv.handOffWorkspace(ep, "acme/widget#7", row, werr)
	got := waitForBlocked(t, c.st, "acme/widget#7")
	if strings.Contains(got.BlockedReason, "s3cret") {
		t.Fatalf("reason leaks the secret: %q", got.BlockedReason)
	}
	detail := strings.TrimPrefix(got.BlockedReason, "fix could not get its worktree — operator action required: ")
	if n := len([]rune(detail)); n > 301 {
		t.Fatalf("detail = %d runes, want <= 301", n)
	}
}

// AC-06: after the hand-off, operator retry resolves the fix kind and leaves
// the blocked state (buildRetryBody needs the live PR, which is not stubbed
// here, so the assertion stops at the kind the retry would re-run).
func TestWorkspaceHandOffIsResumableWithRetry(t *testing.T) {
	// F-007: wait for the hand-off WARN instead of a blind sleep so the
	// log lands in this test's capture window, never the next one's.
	logs := captureServerLogs(t)
	c := newHandoffServer(t, fmt.Errorf("%w: dirty", ErrWorkspaceDirty), PRStateOpen)
	c.post(t, "del-retry-1")
	waitForBlocked(t, c.st, "acme/widget#7")
	waitForLog(t, logs, "webhook: PR handed off, workspace not usable")
	row, _ := c.st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	if kind := c.srv.inferRetryKind(context.Background(), row); kind != "fix" {
		t.Fatalf("retry kind = %q, want fix", kind)
	}
	if _, handled, err := c.srv.operatorRetry(context.Background(), channel.IncomingMessage{}, "acme/widget#7"); err != nil || !handled {
		t.Fatalf("operatorRetry: handled=%v err=%v", handled, err)
	}
	if got, _ := c.st.PRQueueRepo().Get(context.Background(), "acme/widget#7"); got == nil || got.Status == store.PRStatusBlocked {
		t.Fatalf("PR row after retry = %+v, want unblocked", got)
	}
}
