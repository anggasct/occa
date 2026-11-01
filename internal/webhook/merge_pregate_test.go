package webhook

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func review(login, state, body, submitted, commit string) ghReconcileReview {
	r := ghReconcileReview{State: state, Body: body, SubmittedAt: submitted}
	r.Author.Login = login
	r.Commit.OID = commit
	return r
}

func TestHasApprovalCandidate(t *testing.T) {
	trust := map[string]bool{"kumasct": true}
	cases := []struct {
		name    string
		reviews []ghReconcileReview
		author  string
		want    bool
	}{
		{"formal approval", []ghReconcileReview{review("alice", "APPROVED", "", "2026-10-31T01:00:00Z", "aaa")}, "kumasct", true},
		{"approval then changes requested", []ghReconcileReview{
			review("alice", "APPROVED", "", "2026-10-31T01:00:00Z", "aaa"),
			review("alice", "CHANGES_REQUESTED", "", "2026-10-31T02:00:00Z", "aaa"),
		}, "kumasct", false},
		{"approval then dismissed", []ghReconcileReview{
			review("alice", "APPROVED", "", "2026-10-31T01:00:00Z", "aaa"),
			review("alice", "DISMISSED", "", "2026-10-31T02:00:00Z", "aaa"),
		}, "kumasct", false},
		{"untrusted commented approval body", []ghReconcileReview{review("mallory", "COMMENTED", "**Verdict:** APPROVED", "2026-10-31T01:00:00Z", "aaa")}, "mallory", false},
		{"trusted self-review approval", []ghReconcileReview{review("kumasct", "COMMENTED", "**Verdict:** APPROVED", "2026-10-31T01:00:00Z", "aaa")}, "kumasct", true},
		{"self-review request changes", []ghReconcileReview{review("kumasct", "COMMENTED", "**Verdict:** REQUEST_CHANGES", "2026-10-31T01:00:00Z", "aaa")}, "kumasct", false},
		{"approval on an older commit only", []ghReconcileReview{review("alice", "APPROVED", "", "2026-10-30T01:00:00Z", "old")}, "kumasct", true},
		{"plain comment is not a verdict", []ghReconcileReview{
			review("alice", "APPROVED", "", "2026-10-31T01:00:00Z", "aaa"),
			review("alice", "COMMENTED", "nit", "2026-10-31T03:00:00Z", "aaa"),
		}, "kumasct", true},
		{"no reviews", nil, "kumasct", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasApprovalCandidate(tc.reviews, tc.author, trust); got != tc.want {
				t.Fatalf("hasApprovalCandidate = %v, want %v", got, tc.want)
			}
		})
	}
}

type pregateCase struct {
	srv   *Server
	st    *store.SQLiteStore
	url   string
	rec   *prRecorder
	calls *int64
}

func newPregateServer(t *testing.T, facts ReconcileFacts, err error) pregateCase {
	t.Helper()
	rec := newPRRecorder("thread-pr", nil)
	srv, st, url := newPRTestServer(t, rec, []config.EndpointConfig{
		prTestEndpoint("merge", "/merge", config.AutomationFull),
	})
	var calls int64
	srv.SetReconcileFacts(func(context.Context, string, string) (ReconcileFacts, error) {
		atomic.AddInt64(&calls, 1)
		return facts, err
	})
	return pregateCase{srv: srv, st: st, url: url, rec: rec, calls: &calls}
}

func (c pregateCase) postReview(t *testing.T, deliveryID, state, commit string) {
	t.Helper()
	if resp := post(t, c.url+"/merge?secret=s3cret", deliveryID, "pull_request_review", prReviewBody(7, "feat/x", "owner", state, commit)); resp.StatusCode != 200 {
		t.Fatalf("post status = %d", resp.StatusCode)
	}
}

func (c pregateCase) execCalls() int {
	c.rec.mu.Lock()
	defer c.rec.mu.Unlock()
	return len(c.rec.order)
}

// AC-02: no approval candidate on the open PR → skipped, no turn.
func TestMergePregateSkipsWithoutApproval(t *testing.T) {
	logs := captureServerLogs(t)
	c := newPregateServer(t, ReconcileFacts{State: "OPEN", HeadSHA: "abc1234def5678", ApprovalCandidate: false}, nil)
	c.postReview(t, "del-pregate-1", "commented", "abc1234def5678")
	receipt := waitForReceipt(t, c.st, store.WebhookStatusSkipped)
	if !strings.Contains(receipt.ErrorSummary, "merge skipped: no valid approval on live PR") {
		t.Fatalf("summary = %q", receipt.ErrorSummary)
	}
	if n := c.execCalls(); n != 0 {
		t.Fatalf("merge turns = %d, want 0", n)
	}
	rec := logs.find(t, "webhook: merge refused, no approval on live PR")
	if rec.attrs["pr_key"] != "acme/widget#7" || rec.attrs["head"] != "abc1234def" {
		t.Fatalf("log attrs = %v", rec.attrs)
	}
}

// AC-03: an approval candidate exists → the turn runs as today.
func TestMergePregateRunsWithApprovalCandidate(t *testing.T) {
	c := newPregateServer(t, ReconcileFacts{State: "OPEN", ApprovalCandidate: true}, nil)
	c.postReview(t, "del-pregate-2", "commented", "abc1234def5678")
	waitForCalls(t, c.rec, 1)
}

// AC-04: lookup failure → the turn runs; the gate never fails closed.
func TestMergePregateLookupFailureFallsThrough(t *testing.T) {
	logs := captureServerLogs(t)
	c := newPregateServer(t, ReconcileFacts{}, errors.New("gh pr view: boom"))
	c.postReview(t, "del-pregate-3", "commented", "abc1234def5678")
	waitForCalls(t, c.rec, 1)
	logs.find(t, "webhook: merge pre-gate lookup failed")
}

// AC-05: a merged PR is left to the existing terminal path.
func TestMergePregateIgnoresNonOpenPR(t *testing.T) {
	logs := captureServerLogs(t)
	c := newPregateServer(t, ReconcileFacts{State: "MERGED", ApprovalCandidate: false}, nil)
	c.postReview(t, "del-pregate-4", "commented", "abc1234def5678")
	waitForCalls(t, c.rec, 1)
	if len(logs.all("webhook: merge refused, no approval on live PR")) != 0 {
		t.Fatal("pre-gate must not refuse a non-open PR")
	}
}

// AC-06: a recorded head-current block skips first; no live lookup.
func TestMergePregateRecordedBlockSkipsBeforeLookup(t *testing.T) {
	c := newPregateServer(t, ReconcileFacts{State: "OPEN", ApprovalCandidate: true}, nil)
	c.postReview(t, "del-pregate-5", "changes_requested", "abc1234def5678")
	receipt := waitForReceipt(t, c.st, store.WebhookStatusSkipped)
	if strings.Contains(receipt.ErrorSummary, "merge skipped: no valid approval on live PR") {
		t.Fatalf("summary = %q, want the recorded-verdict skip", receipt.ErrorSummary)
	}
	time.Sleep(50 * time.Millisecond)
	if n := atomic.LoadInt64(c.calls); n != 0 {
		t.Fatalf("live lookups = %d, want 0", n)
	}
}

// AC-07: other lanes never consult the pre-gate.
func TestMergePregateOnlyOnMergeLane(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	srv, _, url := newPRTestServer(t, rec, []config.EndpointConfig{
		prTestEndpoint("review", "/review", config.AutomationFull),
	})
	var calls int64
	srv.SetReconcileFacts(func(context.Context, string, string) (ReconcileFacts, error) {
		atomic.AddInt64(&calls, 1)
		return ReconcileFacts{State: "OPEN"}, nil
	})
	if resp := post(t, url+"/review?secret=s3cret", "del-pregate-6", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "abc1234def5678")); resp.StatusCode != 200 {
		t.Fatalf("post status = %d", resp.StatusCode)
	}
	waitForCalls(t, rec, 1)
	if n := atomic.LoadInt64(&calls); n != 0 {
		t.Fatalf("live lookups on review lane = %d, want 0", n)
	}
}
