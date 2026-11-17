package webhook

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

const actionTestPR = "acme/widget#7"

func seedActionWorkItem(t *testing.T, st *store.SQLiteStore) {
	t.Helper()
	if err := st.StageRepo().UpsertWorkItem(context.Background(), store.WorkItem{
		ID: actionTestPR, Repo: "acme/widget", Number: 7, Kind: store.WorkItemPullRequest,
	}); err != nil {
		t.Fatalf("upsert work item: %v", err)
	}
}

func seedActionRun(t *testing.T, st *store.SQLiteStore, stage store.Stage, end store.StageState, code store.BlockerCode) int64 {
	t.Helper()
	ctx := context.Background()
	id, err := st.StageRepo().StartRun(ctx, store.StageRunStart{
		WorkItemID: actionTestPR, Stage: stage, TriggeredBy: "delivery:github-widget-review/del-1",
	})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	switch {
	case code != "":
		if err := st.StageRepo().MarkBlocked(ctx, id, code, "blocked in test"); err != nil {
			t.Fatalf("block run: %v", err)
		}
	case end != "":
		if err := st.StageRepo().EndRun(ctx, id, end, "ended in test"); err != nil {
			t.Fatalf("end run: %v", err)
		}
	}
	return id
}

func listActions(t *testing.T, st *store.SQLiteStore) []store.ActionRequest {
	t.Helper()
	rows, err := st.StageRepo().ListActions(context.Background(), actionTestPR)
	if err != nil {
		t.Fatalf("list actions: %v", err)
	}
	return rows
}

// AC-08: every validation refusal — missing reason, bad confirm, illegal
// state, unknown action, unknown item — writes zero action_request rows.
func TestFireActionRefusalsWriteNoRows(t *testing.T) {
	srv, st, _ := operatorTestServer(t, func(_, _ string) bool { return true })
	seedActionWorkItem(t, st)
	seedActionRun(t, st, store.StageReview, store.StageStateFailed, "")

	cases := []struct {
		name string
		req  FireActionRequest
		code string
	}{
		{"unknown action", FireActionRequest{WorkItemID: actionTestPR, Action: "explode"}, refusalActionUnknown},
		{"unknown item", FireActionRequest{WorkItemID: "acme/widget#99", Action: ActionReReview}, refusalItemUnknown},
		{"skip no reason", FireActionRequest{WorkItemID: actionTestPR, Action: ActionSkipStage}, refusalReasonRequired},
		{"reject no reason", FireActionRequest{WorkItemID: actionTestPR, Action: ActionReject}, refusalReasonRequired},
		{"clean no reason", FireActionRequest{WorkItemID: actionTestPR, Action: ActionCleanWorkspace, Params: map[string]string{"mode": "discard"}}, refusalReasonRequired},
		{"merge no reason", FireActionRequest{WorkItemID: actionTestPR, Action: ActionMergeNow}, refusalReasonRequired},
		{"clean bad param", FireActionRequest{WorkItemID: actionTestPR, Action: ActionCleanWorkspace, Reason: "x", Params: map[string]string{"bogus": "1"}}, refusalParamsInvalid},
		{"unblock not blocked", FireActionRequest{WorkItemID: actionTestPR, Action: ActionUnblock}, refusalActionNotLegal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := srv.FireAction(context.Background(), "telegram:op", tc.req)
			if err != nil {
				t.Fatalf("FireAction: %v", err)
			}
			if out.Accepted || out.Code != tc.code {
				t.Fatalf("outcome = %+v, want refused %s", out, tc.code)
			}
			if out.Sentence == "" {
				t.Fatalf("refusal has no sentence: %+v", out)
			}
		})
	}
	if rows := listActions(t, st); len(rows) != 0 {
		t.Fatalf("validation refusals wrote %d action_request rows, want 0", len(rows))
	}
}

// merge_now is the only typed-confirmation action: the confirm token must be
// the work item id verbatim (AC from control.md §confirm).
func TestFireActionMergeConfirmGate(t *testing.T) {
	srv, st, _ := operatorTestServer(t, func(_, _ string) bool { return true })
	seedActionWorkItem(t, st)
	seedActionRun(t, st, store.StageMerge, store.StageStateBlocked, store.BlockerCIRed)

	out, err := srv.FireAction(context.Background(), "telegram:op", FireActionRequest{
		WorkItemID: actionTestPR, Action: ActionMergeNow, Reason: "CI is actually green", Confirm: "wrong",
	})
	if err != nil {
		t.Fatalf("FireAction: %v", err)
	}
	if out.Accepted || out.Code != refusalConfirmMismatch {
		t.Fatalf("outcome = %+v, want confirm_mismatch", out)
	}
	if rows := listActions(t, st); len(rows) != 0 {
		t.Fatalf("confirm refusal wrote %d rows, want 0", len(rows))
	}
}

// retry only gates on a reason once the manual-retry cap (3) is spent.
func TestFireActionRetryReasonCap(t *testing.T) {
	srv, st, _ := operatorTestServer(t, func(_, _ string) bool { return true })
	seedActionWorkItem(t, st)
	ctx := context.Background()
	for i := 0; i <= manualRetryReasonCap; i++ {
		id, err := st.StageRepo().StartRun(ctx, store.StageRunStart{
			WorkItemID: actionTestPR, Stage: store.StageReview, TriggeredBy: "manual", ManualTrigger: true,
		})
		if err != nil {
			t.Fatalf("manual run %d: %v", i, err)
		}
		if err := st.StageRepo().EndRun(ctx, id, store.StageStateFailed, "failed"); err != nil {
			t.Fatalf("end run %d: %v", i, err)
		}
	}
	out, err := srv.FireAction(ctx, "telegram:op", FireActionRequest{WorkItemID: actionTestPR, Action: ActionRetry})
	if err != nil {
		t.Fatalf("FireAction: %v", err)
	}
	if out.Accepted || out.Code != refusalReasonRequired {
		t.Fatalf("retry at cap = %+v, want reason_required", out)
	}
	if rows := listActions(t, st); len(rows) != 0 {
		t.Fatalf("cap refusal wrote %d rows, want 0", len(rows))
	}
}

// AC-09: the same idempotency key replays the recorded outcome — same
// action_id, no second row, and the effect never fires twice.
func TestFireActionIdempotentReplay(t *testing.T) {
	srv, st, _ := operatorTestServer(t, func(_, _ string) bool { return true })
	seedActionWorkItem(t, st)
	req := FireActionRequest{WorkItemID: actionTestPR, Action: ActionReReview, IdempotencyKey: "idem-1"}

	first, err := srv.FireAction(context.Background(), "telegram:op", req)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if !first.Accepted || first.ActionID == 0 || first.Replayed {
		t.Fatalf("first = %+v, want accepted with an id", first)
	}
	second, err := srv.FireAction(context.Background(), "telegram:op", req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if !second.Replayed || second.ActionID != first.ActionID || second.Sentence != first.Sentence {
		t.Fatalf("replay = %+v, want same outcome as %+v", second, first)
	}
	if rows := listActions(t, st); len(rows) != 1 {
		t.Fatalf("action rows = %d, want 1", len(rows))
	}
}

// AC-10: a retry fires a fresh attempt n+1 and leaves attempt n untouched.
func TestFireActionRetryCreatesNextAttempt(t *testing.T) {
	srv, st, _ := operatorTestServer(t, func(_, _ string) bool { return true })
	ctx := context.Background()
	seedActionWorkItem(t, st)
	run1 := seedActionRun(t, st, store.StageReview, store.StageStateFailed, "")
	if _, err := st.PRQueueRepo().EnsurePR(ctx, actionTestPR, "thread-pr", "root-1"); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	if err := st.PRQueueRepo().SetActive(ctx, actionTestPR, "review", "sess-1", 0); err != nil {
		t.Fatalf("set active: %v", err)
	}
	srv.retryBodyBuilder = func(_ context.Context, _, _, _ string) ([]byte, string, error) {
		return []byte(prReviewBody(7, "feat/x", "owner", "commented", "c0ffee-retry")), "pull_request_review", nil
	}
	before, err := st.StageRepo().ListRuns(ctx, actionTestPR)
	if err != nil || len(before) != 1 {
		t.Fatalf("runs before = %v %v", before, err)
	}

	out, err := srv.FireAction(ctx, "telegram:op", FireActionRequest{WorkItemID: actionTestPR, Action: ActionRetry})
	if err != nil {
		t.Fatalf("FireAction: %v", err)
	}
	if !out.Accepted {
		t.Fatalf("retry refused: %+v", out)
	}

	var attempt2 *store.StageRun
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runs, err := st.StageRepo().ListRuns(ctx, actionTestPR)
		if err != nil {
			t.Fatalf("list runs: %v", err)
		}
		for i := range runs {
			if runs[i].Attempt == 2 {
				attempt2 = &runs[i]
			}
		}
		if attempt2 != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if attempt2 == nil {
		t.Fatal("retry created no attempt-2 run")
	}
	after, _ := st.StageRepo().ListRuns(ctx, actionTestPR)
	var unchanged bool
	for _, r := range after {
		if r.ID == run1 && r.Attempt == 1 && r.State == before[0].State && r.EndedAt == before[0].EndedAt {
			unchanged = true
		}
	}
	if !unchanged {
		t.Fatalf("attempt-1 row mutated: %+v", after)
	}
}

// A PR card that exists in the queue but has no stage graph yet gets a work
// item adopted on first use instead of an item_unknown refusal.
func TestFireActionAdoptsQueueItem(t *testing.T) {
	srv, st, _ := operatorTestServer(t, func(_, _ string) bool { return true })
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, actionTestPR, "thread-pr", "root-1"); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	out, err := srv.FireAction(ctx, "telegram:op", FireActionRequest{WorkItemID: actionTestPR, Action: ActionReReview})
	if err != nil {
		t.Fatalf("FireAction: %v", err)
	}
	if !out.Accepted {
		t.Fatalf("re_review on a fresh PR card refused: %+v", out)
	}
	item, err := st.StageRepo().GetWorkItem(ctx, actionTestPR)
	if err != nil || item == nil {
		t.Fatalf("work item not adopted: %v %v", item, err)
	}
}

// Accepted actions post a one-line audit trail into the PR thread carrying
// the action, the actor and the quoted reason.
func TestFireActionThreadLine(t *testing.T) {
	srv, st, _ := operatorTestServer(t, func(_, _ string) bool { return true })
	var lines []string
	srv.SetNotifier(func(_ context.Context, _, _, text string) error {
		lines = append(lines, text)
		return nil
	})
	seedActionWorkItem(t, st)
	seedActionRun(t, st, store.StageFix, store.StageStateFailed, "")
	if _, err := st.PRQueueRepo().EnsurePR(context.Background(), actionTestPR, "thread-pr", "root-1"); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	out, err := srv.FireAction(context.Background(), "telegram:op", FireActionRequest{
		WorkItemID: actionTestPR, Action: ActionSkipStage, Reason: "gate is stale",
	})
	if err != nil {
		t.Fatalf("FireAction: %v", err)
	}
	if !out.Accepted {
		t.Fatalf("skip refused: %+v", out)
	}
	want := `⚙️ skip_stage · @telegram:op · reason: "gate is stale"`
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, l := range lines {
			if strings.Contains(l, want) {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("thread lines = %q, want one containing %q", lines, want)
}

// AC-16: every chat command and its API call share the same validation —
// identical accepted/refused outcome and identical sentence.
func TestChatActionParity(t *testing.T) {
	cases := []struct {
		cmd    string
		arg    string
		req    FireActionRequest
		seed   func(t *testing.T, st *store.SQLiteStore)
		action string
	}{
		{"re-review", "", FireActionRequest{Action: ActionReReview}, seedActionWorkItem, ActionReReview},
		{"retry", "", FireActionRequest{Action: ActionRetry}, seedActionWorkItem, ActionRetry},
		{"skip", "", FireActionRequest{Action: ActionSkipStage}, seedActionWorkItem, ActionSkipStage},
		{"reject", "", FireActionRequest{Action: ActionReject}, seedActionWorkItem, ActionReject},
		{"approve", "", FireActionRequest{Action: ActionApprove}, seedActionWorkItem, ActionApprove},
		{"unblock", "", FireActionRequest{Action: ActionUnblock}, seedActionWorkItem, ActionUnblock},
		{"cancel", "", FireActionRequest{Action: ActionStop}, seedActionWorkItem, ActionStop},
		{"recheck", "", FireActionRequest{Action: ActionRecheck}, seedActionWorkItem, ActionRecheck},
		{"rebind", "", FireActionRequest{Action: ActionRebind}, seedActionWorkItem, ActionRebind},
		{"fix-ci", "", FireActionRequest{Action: ActionFixCI}, seedActionWorkItem, ActionFixCI},
		{"fix-review", "", FireActionRequest{Action: ActionFixReview}, seedActionWorkItem, ActionFixReview},
		{"clean", "", FireActionRequest{Action: ActionCleanWorkspace}, seedActionWorkItem, ActionCleanWorkspace},
		{"merge-now", "", FireActionRequest{Action: ActionMergeNow}, seedActionWorkItem, ActionMergeNow},
		{"merge-now", "wrong-id good reason", FireActionRequest{Action: ActionMergeNow, Confirm: "wrong-id", Reason: "good reason"}, seedActionWorkItem, ActionMergeNow},
		{"retry-with", "openai/gpt-5", FireActionRequest{Action: ActionRetryWith, Params: map[string]string{"profile": "openai/gpt-5"}}, seedActionWorkItem, ActionRetryWith},
	}
	for _, tc := range cases {
		t.Run(tc.cmd+"/"+tc.arg, func(t *testing.T) {
			ctx := context.Background()

			srvChat, stChat, _ := operatorTestServer(t, func(_, _ string) bool { return true })
			tc.seed(t, stChat)
			rec := srvChat.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-pr", TaskClaimed)
			srvChat.tasks.LinkPRThread(actionTestPR, rec)
			reply, handled, err := srvChat.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/"+tc.cmd+" "+tc.arg), tc.cmd, tc.arg)
			if err != nil || !handled {
				t.Fatalf("chat /%s: handled=%v err=%v", tc.cmd, handled, err)
			}

			srvAPI, stAPI, _ := operatorTestServer(t, func(_, _ string) bool { return true })
			tc.seed(t, stAPI)
			req := tc.req
			req.WorkItemID = actionTestPR
			out, err := srvAPI.FireAction(ctx, "telegram:op", req)
			if err != nil {
				t.Fatalf("api %s: %v", tc.action, err)
			}

			if out.Accepted {
				if reply != "⚙️ "+out.Sentence {
					t.Fatalf("chat reply %q != accepted sentence %q", reply, out.Sentence)
				}
			} else if reply != "⚠️ "+out.Error() {
				t.Fatalf("chat reply %q != refusal %q", reply, out.Error())
			}

			if rows := listActions(t, stChat); len(rows) > 0 && rows[0].Action != tc.action {
				t.Fatalf("chat recorded action %q, want %q", rows[0].Action, tc.action)
			}
		})
	}
}

// AC-21 (event side): a GitHub review event for a head the operator just
// re-reviewed is still deduped against the recorded review key.
func TestOperatorReviewKeyDedupesGitHubEvent(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	reviewEp := prTestEndpoint("review", "/review", config.AutomationFull)
	_, st, url := newPRTestServer(t, rec, []config.EndpointConfig{reviewEp})
	ctx := context.Background()

	id := seedProcessingDelivery(t, st, reviewEp.Name, "action-re_review-1", "pull_request_review")
	if ok, err := st.WebhookDeliveryRepo().Transition(ctx, id, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusCompleted, ""); err != nil || !ok {
		t.Fatalf("complete seeded delivery: %v %v", err, ok)
	}
	if err := st.WebhookDeliveryRepo().SetReviewKey(ctx, id, actionTestPR+"@"+reviewButtonHead); err != nil {
		t.Fatalf("set review key: %v", err)
	}

	body := prReviewBody(7, "feat/x", "owner", "commented", reviewButtonHead)
	if resp := post(t, url+"/review?secret=s3cret", "del-gh-same-head", "pull_request_review", body); resp.StatusCode != 200 {
		t.Fatalf("post: %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-gh-same-head", store.WebhookStatusSkipped)
	receipt, _ := st.WebhookDeliveryRepo().Get(ctx, "github-widget-review", "del-gh-same-head")
	if receipt == nil || !strings.Contains(receipt.ErrorSummary, "duplicate review event") {
		t.Fatalf("event receipt = %+v, want a same-head duplicate skip", receipt)
	}
}
