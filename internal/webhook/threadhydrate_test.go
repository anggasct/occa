package webhook

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

// IMP-117 AC-01: a PR only the queue knows resolves after a restart — the
// thread, the task and the short callback id — and /cancel in its thread
// unblocks it.
func TestRestartHydratesPRThreads(t *testing.T) {
	srv, st, _ := operatorTestServer(t, func(string, string) bool { return true })
	ctx := context.Background()
	q := st.PRQueueRepo()
	if _, err := q.EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
		t.Fatal(err)
	}
	if err := q.UpdatePRMeta(ctx, "acme/widget#7", "feat/x", "https://github.com/acme/widget/pull/7"); err != nil {
		t.Fatal(err)
	}
	if err := q.SetBlocked(ctx, "acme/widget#7", "fix made no change — operator action required"); err != nil {
		t.Fatal(err)
	}
	// An older PR on the same branch: the newer row wins.
	if _, err := q.EnsurePR(ctx, "acme/widget#3", "thread-old", "root-old"); err != nil {
		t.Fatal(err)
	}
	if err := q.UpdatePRMeta(ctx, "acme/widget#3", "feat/x", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx, `UPDATE pr_queue SET updated_at = 1 WHERE pr_key = 'acme/widget#3'`); err != nil {
		t.Fatal(err)
	}

	if reply, _, _ := srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/cancel"), "cancel", ""); !strings.Contains(reply, "inside the PR thread") {
		t.Fatalf("before hydration the thread is unknown, reply = %q", reply)
	}
	if n := srv.hydrateTaskThreads(ctx); n != 1 {
		t.Fatalf("hydrated = %d, want 1 (newest row per branch)", n)
	}
	if prKey, ok := srv.tasks.PRKeyForThread("thread-pr"); !ok || prKey != "acme/widget#7" {
		t.Fatalf("PRKeyForThread = %q %v", prKey, ok)
	}
	if prKey, ok := srv.tasks.PRKeyForTask(TaskThreadKey("acme/widget", "feat/x")); !ok || prKey != "acme/widget#7" {
		t.Fatalf("PRKeyForTask = %q %v", prKey, ok)
	}
	if repo, branch, ok := srv.tasks.TaskForShortID(ShortTaskID("acme/widget", "feat/x")); !ok || repo != "acme/widget" || branch != "feat/x" {
		t.Fatalf("TaskForShortID = %q %q %v", repo, branch, ok)
	}
	if n := srv.hydrateTaskThreads(ctx); n != 0 {
		t.Fatalf("a second hydrate must keep known records, loaded %d", n)
	}
	reply, handled, err := srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/cancel"), "cancel", "")
	if err != nil || !handled {
		t.Fatalf("cancel = %q %v %v", reply, handled, err)
	}
	row, _ := q.Get(ctx, "acme/widget#7")
	if row == nil || row.Status == store.PRStatusBlocked {
		t.Fatalf("row after /cancel = %+v, want unblocked", row)
	}
}

type threadLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *threadLines) add(_ context.Context, _, _, text string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, text)
	return nil
}

func (l *threadLines) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

func blockedPRServer(t *testing.T, reason string) (*Server, *store.SQLiteStore, string, *threadLines) {
	t.Helper()
	rec := newPRRecorder("thread-pr", nil)
	review := prTestEndpoint("review", "/review", config.AutomationFull)
	review.CommentTrigger = []string{"please re-review"}
	srv, st, url := newPRTestServer(t, rec, []config.EndpointConfig{review})
	lines := &threadLines{}
	srv.SetNotifier(lines.add)
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetBlocked(ctx, "acme/widget#7", reason); err != nil {
		t.Fatal(err)
	}
	return srv, st, url, lines
}

const syncBody = `{"action":"synchronize","number":7,"pull_request":{"number":7,"state":"open","user":{"login":"owner"},"head":{"ref":"feat/x","sha":"42cbc7a53cdc1d0d849d1aba15669eb5ed48922c","repo":{"full_name":"acme/widget"}},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}},"repository":{"full_name":"acme/widget"}}`

func waitForLines(t *testing.T, lines *threadLines, n int) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := lines.all(); len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	return lines.all()
}

// IMP-117 AC-02: a push to a PR blocked by a run outcome unblocks it and says
// so once; a storm-cap block stays.
func TestNewHeadUnblocksBlockedPR(t *testing.T) {
	_, st, url, lines := blockedPRServer(t, "fix made no change — operator action required: nothing to do")
	if resp := post(t, url+"/review?secret=s3cret", "del-sync", "pull_request", syncBody); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: %d", resp.StatusCode)
	}
	got := waitForLines(t, lines, 1)
	unblocked := 0
	for _, line := range got {
		if strings.HasPrefix(line, "🔓 New commits on acme/widget#7 — unblocked (was: The fix run finished without changing anything).") {
			unblocked++
		}
	}
	if unblocked != 1 {
		t.Fatalf("thread lines = %q, want one unblock line", got)
	}
	if row, _ := st.PRQueueRepo().Get(context.Background(), "acme/widget#7"); row == nil || row.Status == store.PRStatusBlocked {
		t.Fatalf("row = %+v, want unblocked", row)
	}

	_, st2, url2, _ := blockedPRServer(t, "auto-fix storm cap — manual action (3 auto-fix runs in 24h)")
	if resp := post(t, url2+"/review?secret=s3cret", "del-sync-cap", "pull_request", syncBody); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st2, "github-widget-review", "del-sync-cap", store.WebhookStatusSkipped)
	if row, _ := st2.PRQueueRepo().Get(context.Background(), "acme/widget#7"); row == nil || row.Status != store.PRStatusBlocked {
		t.Fatalf("storm cap row = %+v, want still blocked", row)
	}
}

// IMP-117 AC-03: a re-review request on a blocked PR is skipped with one
// line in the thread saying why and what to press.
func TestBlockedPRAnswersReReviewRequest(t *testing.T) {
	_, st, url, lines := blockedPRServer(t, "fix made no change — operator action required: nothing to do")
	if resp := post(t, url+"/review?secret=s3cret", "del-ask", "issue_comment", string(issueCommentEventBodyForRepo(t, "acme/widget", 7, "please re-review", ""))); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-ask", store.WebhookStatusSkipped)
	got := waitForLines(t, lines, 1)
	if len(got) != 1 || !strings.HasPrefix(got[0], "⏸ acme/widget#7 is blocked — The fix run finished without changing anything. Use Retry or Cancel") {
		t.Fatalf("thread lines = %q", got)
	}
}
