package webhook

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

// IMP-112 AC-01: compact callbacks fit Telegram and resolve back to the task.
func TestShortTaskCallbacks(t *testing.T) {
	id := ShortTaskID("acme/widget", "feat/x")
	if !regexp.MustCompile(`^[0-9a-f]{10}$`).MatchString(id) {
		t.Fatalf("ShortTaskID = %q, want 10 hex", id)
	}
	long := strings.Repeat("b", 200)
	for _, action := range []string{TaskActionReview, TaskActionRetry, TaskActionCancel, TaskActionMerge} {
		cb := ShortTaskCallback(action, "anggasct/some-long-repository-name", long)
		if len(cb) > 64 {
			t.Fatalf("%s callback is %d bytes, Telegram allows 64", action, len(cb))
		}
		gotAction, gotID, ok := ParseShortTaskCallback(cb)
		if !ok || gotAction != action || gotID != ShortTaskID("anggasct/some-long-repository-name", long) {
			t.Fatalf("parse %q = %q %q %v", cb, gotAction, gotID, ok)
		}
	}
	if _, _, ok := ParseShortTaskCallback("ta:explode:" + id); ok {
		t.Fatal("an unknown action must not parse")
	}
	reg := NewTaskThreadRegistry()
	reg.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskPROpened)
	if repo, branch, ok := reg.TaskForShortID(id); !ok || repo != "acme/widget" || branch != "feat/x" {
		t.Fatalf("TaskForShortID = %q %q %v", repo, branch, ok)
	}
	if _, _, ok := reg.TaskForShortID("0000000000"); ok {
		t.Fatal("an unknown id must not resolve")
	}
}

// IMP-112 AC-04: every cause the code writes reads as plain copy.
func TestDescribeBlockCauses(t *testing.T) {
	cases := []struct {
		reason, headline, detail, next string
	}{
		{"fix made no change — operator action required: the finding asks for a vault edit", "The fix run finished without changing anything.", "Agent said: the finding asks for a vault edit", "press Merge"},
		{"review stopped on a denied permission — operator action required: external_directory [/home/ubuntu/.docker/*]", "The review run was refused access it asked for.", "Refused: external_directory [/home/ubuntu/.docker/*]", "allow it in the config"},
		{"fix could not get its worktree — operator action required: workspace dirty: /repo/.worktree/x", "The fix run could not get a clean checkout.", "workspace dirty: /repo/.worktree/x", "Clean or remove the worktree"},
		{"fix exhausted after 2/2 retries (no work in lease) — operator action required", "The fix run failed 2 times in a row: it changed nothing.", "", "press Retry"},
		{"review exhausted after 3/3 retries (provider stall) — operator action required", "The review run failed 3 times in a row: the model stopped responding.", "", "press Retry"},
		{"live PR state unresolvable — retry refused (fail-closed)", "occa could not read this PR's state from GitHub.", "", "gh is logged in"},
		{"auto-fix storm cap — manual action (3 auto-fix runs in 24h)", "Auto-fix ran 3 times in 24h and stopped.", "", "Fix it by hand"},
		{"interrupted by restart — re-run via /retry or re-trigger", "interrupted by restart — re-run via /retry or re-trigger", "", "Press Retry"},
	}
	for _, tc := range cases {
		got := describeBlock(tc.reason)
		if got.Headline != tc.headline || got.Detail != tc.detail || !strings.Contains(got.Next, tc.next) {
			t.Errorf("describeBlock(%q) = %+v", tc.reason, got)
		}
	}
	thread := formatBlockedCopy("acme/widget", "7", describeBlock(cases[0].reason), nil, false)
	if !strings.HasPrefix(thread, "🔴 acme/widget#7 needs you — blocked\n") || !strings.Contains(thread, "\nNext: ") || strings.Contains(thread, "Open the PR thread") {
		t.Fatalf("thread copy = %q", thread)
	}
	if ch := formatBlockedCopy("acme/widget", "7", describeBlock(cases[0].reason), nil, true); !strings.HasSuffix(ch, "\nOpen the PR thread to act.") {
		t.Fatalf("channel copy = %q", ch)
	}
}

type sentWithButtons struct {
	target  string
	text    string
	buttons []channel.Button
}

// IMP-112 AC-04: a no-change hand-off posts the plain thread line with
// Retry · Merge · Cancel, a channel copy without buttons, and keeps the
// stored reason.
func TestBlockedHandOffPostsButtonsAndPlainCopy(t *testing.T) {
	srv, st, _ := operatorTestServer(t, func(string, string) bool { return true })
	ctx := context.Background()
	rec := srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-pr", TaskPROpened)
	srv.tasks.LinkPRThread("acme/widget#7", rec)
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetActive(ctx, "acme/widget#7", "fix", "sess-1", 1); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var sent []sentWithButtons
	var channelTexts []string
	srv.SetSenderWithButtons(func(_ context.Context, _, target, text string, buttons []channel.Button) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, sentWithButtons{target: target, text: text, buttons: buttons})
		return "msg-1", nil
	})
	srv.SetNotifier(func(_ context.Context, _, target, text string) error {
		mu.Lock()
		defer mu.Unlock()
		if strings.Contains(text, "needs you") {
			channelTexts = append(channelTexts, text)
		}
		return nil
	})
	row, _ := st.PRQueueRepo().Get(ctx, "acme/widget#7")
	ep := srv.endpointsByName["github-widget-fix"]
	srv.handOffNoChange(ep, "acme/widget#7", row, &WebhookWorkContext{AgentReply: "the finding asks for a vault edit"})

	mu.Lock()
	defer mu.Unlock()
	var line *sentWithButtons
	for i := range sent {
		if strings.Contains(sent[i].text, "needs you — blocked") {
			line = &sent[i]
		}
	}
	if line == nil {
		t.Fatalf("no blocked thread line with buttons; sent = %+v", sent)
	}
	if !strings.Contains(line.text, "The fix run finished without changing anything.") || !strings.Contains(line.text, "Agent said: the finding asks for a vault edit") {
		t.Fatalf("thread line = %q", line.text)
	}
	var labels, values []string
	for _, b := range line.buttons {
		labels = append(labels, b.Label)
		values = append(values, b.Value)
	}
	if strings.Join(labels, ",") != "Retry,Merge,Cancel" {
		t.Fatalf("blocked buttons = %v", labels)
	}
	for i, action := range []string{TaskActionRetry, TaskActionMerge, TaskActionCancel} {
		if values[i] != ShortTaskCallback(action, "acme/widget", "feat/x") {
			t.Fatalf("button %d value = %q", i, values[i])
		}
	}
	if len(channelTexts) != 1 || !strings.Contains(channelTexts[0], "Open the PR thread to act.") {
		t.Fatalf("channel escalation = %v", channelTexts)
	}
	after, _ := st.PRQueueRepo().Get(ctx, "acme/widget#7")
	if after.Status != store.PRStatusBlocked || after.BlockedReason != "fix made no change — operator action required: the finding asks for a vault edit" {
		t.Fatalf("stored block = %+v", after)
	}
	if got := describeBlock(after.BlockedReason).statusAction(); !strings.HasSuffix(got, " — use the buttons below") {
		t.Fatalf("📌 action = %q", got)
	}
}

// IMP-112 AC-03: Retry and Cancel buttons act like /retry and /cancel and
// answer in the PR thread; a non-operator is refused.
func TestTaskActionRetryAndCancel(t *testing.T) {
	allowed := true
	srv, st, _ := operatorTestServer(t, func(string, string) bool { return allowed })
	ctx := context.Background()
	rec := srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-pr", TaskPROpened)
	srv.tasks.LinkPRThread("acme/widget#7", rec)
	var mu sync.Mutex
	var notes []string
	var targets []string
	srv.SetNotifier(func(_ context.Context, _, target, text string) error {
		mu.Lock()
		defer mu.Unlock()
		notes = append(notes, text)
		targets = append(targets, target)
		return nil
	})
	block := func() {
		t.Helper()
		if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
			t.Fatal(err)
		}
		if err := st.PRQueueRepo().SetActive(ctx, "acme/widget#7", "fix", "sess-9", 11); err != nil {
			t.Fatal(err)
		}
		if err := st.PRQueueRepo().SetBlocked(ctx, "acme/widget#7", "fix made no change — operator action required"); err != nil {
			t.Fatal(err)
		}
	}
	srv.retryBodyBuilder = func(ctx context.Context, repo, number, kind string) ([]byte, string, error) {
		return []byte(`{"action":"created","synthetic":"retry"}`), "issue_comment", nil
	}
	last := func() (string, string) {
		mu.Lock()
		defer mu.Unlock()
		if len(notes) == 0 {
			t.Fatal("the press posted nothing")
		}
		return notes[len(notes)-1], targets[len(targets)-1]
	}

	block()
	if err := srv.HandleTaskAction(ctx, operatorMsg("", ""), TaskActionRetry, "acme/widget", "feat/x"); err != nil {
		t.Fatal(err)
	}
	if text, target := last(); !strings.Contains(text, "Re-running fix") || target != "thread-pr" {
		t.Fatalf("retry reply = %q to %q", text, target)
	}
	if row, _ := st.PRQueueRepo().Get(ctx, "acme/widget#7"); row.Status == store.PRStatusBlocked {
		t.Fatalf("retry must lift the block: %+v", row)
	}

	waitForPRIdle(t, srv, "acme/widget#7")
	block()
	if err := srv.HandleTaskAction(ctx, operatorMsg("", ""), TaskActionCancel, "acme/widget", "feat/x"); err != nil {
		t.Fatal(err)
	}
	if row, _ := st.PRQueueRepo().Get(ctx, "acme/widget#7"); row.Status == store.PRStatusBlocked {
		t.Fatalf("cancel must lift the block: %+v", row)
	}

	allowed = false
	if err := srv.HandleTaskAction(ctx, operatorMsg("", ""), TaskActionRetry, "acme/widget", "feat/x"); err != nil {
		t.Fatal(err)
	}
	if text, _ := last(); text != "⛔ Not allowed." {
		t.Fatalf("non-operator reply = %q", text)
	}
}

func seedBlockedPR(t *testing.T, srv *Server, st *store.SQLiteStore, number int, branch, reason string) {
	t.Helper()
	ctx := context.Background()
	prKey := "acme/widget#" + strconv.Itoa(number)
	rec := srv.tasks.Upsert("acme/widget", branch, "root-"+branch, "thread-"+branch, TaskPROpened)
	srv.tasks.LinkPRThread(prKey, rec)
	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-"+branch, "root-"+branch); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetBlocked(ctx, prKey, reason); err != nil {
		t.Fatal(err)
	}
}

// IMP-112 AC-05: /inbox lists blocked PRs in plain words, with one button row
// for each of the first five.
func TestInbox(t *testing.T) {
	allowed := true
	srv, st, _ := operatorTestServer(t, func(string, string) bool { return allowed })
	ctx := context.Background()
	msg := operatorMsg("", "/inbox")

	text, buttons, err := srv.Inbox(ctx, msg)
	if err != nil || text != "📥 Nothing needs you right now." || len(buttons) != 0 {
		t.Fatalf("empty inbox = %q %v %v", text, buttons, err)
	}

	seedBlockedPR(t, srv, st, 1, "feat/a", "fix made no change — operator action required")
	seedBlockedPR(t, srv, st, 2, "feat/b", "review exhausted after 3/3 retries (model error) — operator action required")
	text, buttons, err = srv.Inbox(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"📥 Needs you — 2 PRs", "1. acme/widget#1 — The fix run finished without changing anything.", "2. acme/widget#2 — The review run failed 3 times in a row: the model returned an error."} {
		if !strings.Contains(text, want) {
			t.Fatalf("inbox %q must contain %q", text, want)
		}
	}
	rows := map[int][]string{}
	for _, b := range buttons {
		rows[b.Row] = append(rows[b.Row], b.Label)
		if b.URL == "" && !strings.HasPrefix(b.Value, "ta:") {
			t.Fatalf("inbox button %+v must be a compact callback", b)
		}
	}
	if strings.Join(rows[1], ",") != "Retry #1,Merge #1,Cancel #1" || strings.Join(rows[2], ",") != "Retry #2,Merge #2,Cancel #2" {
		t.Fatalf("inbox rows = %v", rows)
	}

	for n := 3; n <= 7; n++ {
		seedBlockedPR(t, srv, st, n, "feat/"+strconv.Itoa(n), "fix made no change — operator action required")
	}
	text, buttons, _ = srv.Inbox(ctx, msg)
	rowSet := map[int]bool{}
	for _, b := range buttons {
		rowSet[b.Row] = true
	}
	if len(rowSet) != 5 || !strings.Contains(text, "…and 2 more") || !strings.Contains(text, "7. acme/widget#7") {
		t.Fatalf("7 blocked → rows %v, text %q", rowSet, text)
	}

	allowed = false
	if text, _, _ := srv.Inbox(ctx, msg); text != "⛔ Not allowed." {
		t.Fatalf("non-operator inbox = %q", text)
	}
}

// IMP-112 AC-07: the digest counts the last 24h and names what waits.
func TestDigest(t *testing.T) {
	srv, st, _ := operatorTestServer(t, func(string, string) bool { return true })
	ctx := context.Background()
	repo := st.WebhookDeliveryRepo()
	seed := func(endpoint, id, prKey string, started bool, to store.WebhookStatus) {
		t.Helper()
		rid := seedProcessingDelivery(t, st, endpoint, id, "pull_request")
		if prKey != "" {
			if err := repo.SetPRKey(ctx, rid, prKey); err != nil {
				t.Fatal(err)
			}
		}
		if !started {
			if _, err := st.DB().ExecContext(ctx, `UPDATE webhook_delivery SET started_at = 0 WHERE id = ?`, rid); err != nil {
				t.Fatal(err)
			}
		}
		if ok, err := repo.Transition(ctx, rid, []store.WebhookStatus{store.WebhookStatusProcessing}, to, ""); err != nil || !ok {
			t.Fatalf("transition %s: %v %v", id, ok, err)
		}
	}
	seed("github-widget-merged", "m1", "acme/widget#5", false, store.WebhookStatusCompleted)
	seed("github-widget-merged", "m2", "acme/widget#6", false, store.WebhookStatusCompleted)
	seed("github-widget-review", "r1", "acme/widget#5", true, store.WebhookStatusCompleted)
	seed("github-widget-review", "r2", "acme/widget#6", true, store.WebhookStatusCompleted)
	seed("github-widget-fix", "f1", "acme/widget#6", true, store.WebhookStatusFailed)
	seed("github-widget-review", "s1", "acme/widget#6", false, store.WebhookStatusSkipped)
	seedBlockedPR(t, srv, st, 8, "feat/h", "fix made no change — operator action required")

	got, err := srv.buildDigest(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	want := "☀️ occa — last 24h\n✅ merged 2: acme/widget#5, acme/widget#6\n🔁 runs 3 (review 2 · fix 1) · failed 1\n📥 needs you 1: acme/widget#8 — /inbox"
	if got != want {
		t.Fatalf("digest =\n%s\nwant\n%s", got, want)
	}
	if later, _ := srv.buildDigest(ctx, time.Now().AddDate(0, 0, 2)); !strings.Contains(later, "✅ merged 0") || !strings.Contains(later, "🔁 runs 0 · failed 0") {
		t.Fatalf("digest outside the window = %q", later)
	}
	now, err := srv.DigestNow(ctx, operatorMsg("", "/digest"))
	if err != nil || now != want {
		t.Fatalf("/digest = %q %v", now, err)
	}
}

// IMP-112 AC-07: the digest fires at the next local HH:MM.
func TestDigestNextAt(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		t.Skip("no tz database")
	}
	cfg := config.DigestConfig{Hour: 8, Minute: 0, Location: loc}
	before := time.Date(2026, 11, 2, 7, 30, 0, 0, loc)
	if got := cfg.NextAt(before); !got.Equal(time.Date(2026, 11, 2, 8, 0, 0, 0, loc)) {
		t.Fatalf("NextAt(07:30) = %v", got)
	}
	at := time.Date(2026, 11, 2, 8, 0, 0, 0, loc)
	if got := cfg.NextAt(at); !got.Equal(time.Date(2026, 11, 3, 8, 0, 0, 0, loc)) {
		t.Fatalf("NextAt(08:00) = %v", got)
	}
}

// IMP-112 AC-08: the PR card counts runs and retries.
func TestPhaseParentBaseCountsRuns(t *testing.T) {
	srv, st, _ := operatorTestServer(t, nil)
	ctx := context.Background()
	repo := st.WebhookDeliveryRepo()
	if got := srv.phaseParentBase(ctx, "acme/widget", "7", "acme/widget#7", PhaseReview); got != "👀 acme/widget#7 — review" {
		t.Fatalf("no runs → %q", got)
	}
	for i, id := range []string{"a", "b", "c"} {
		rid := seedProcessingDelivery(t, st, "github-widget-review", "run-"+id, "pull_request")
		if err := repo.SetPRKey(ctx, rid, "acme/widget#7"); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if err := repo.BumpAttempt(ctx, rid); err != nil {
				t.Fatal(err)
			}
		}
	}
	got := srv.phaseParentBase(ctx, "acme/widget", "7", "acme/widget#7", PhaseReview)
	if got != "👀 acme/widget#7 — review · 3 runs · 1 retry" {
		t.Fatalf("parent base = %q", got)
	}
	if !phaseParentPattern.MatchString(got + " · ⏱️ 3m") {
		t.Fatalf("parent %q must stay one line", got)
	}
	if FormatRunCounters(1, 0) != " · 1 run" || FormatRunCounters(2, 3) != " · 2 runs · 3 retries" || FormatRunCounters(0, 4) != "" {
		t.Fatal("FormatRunCounters wording drifted")
	}
}
