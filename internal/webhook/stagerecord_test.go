package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

const stagePR = "acme/widget#7"

type stageRunWant struct {
	stage   store.Stage
	attempt int
	state   store.StageState
	code    store.BlockerCode
}

func stageRunsOf(t *testing.T, st *store.SQLiteStore, prKey string) []store.StageRun {
	t.Helper()
	runs, err := st.StageRepo().ListRuns(context.Background(), prKey)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	return runs
}

func assertStageRuns(t *testing.T, st *store.SQLiteStore, prKey string, want []stageRunWant) []store.StageRun {
	t.Helper()
	runs := stageRunsOf(t, st, prKey)
	var got []stageRunWant
	for _, r := range runs {
		got = append(got, stageRunWant{r.Stage, r.Attempt, r.State, r.BlockerCode})
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stage runs = %+v, want %+v", got, want)
	}
	return runs
}

type stageExec struct {
	mu    sync.Mutex
	calls int
	act   func(srv *Server, n int, workCtx *WebhookWorkContext) error
	srv   *Server
}

func (e *stageExec) exec(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
	e.mu.Lock()
	e.calls++
	n := e.calls
	e.mu.Unlock()
	if workCtx.ThreadID == "" {
		workCtx.ThreadID = "thread-pr"
	}
	workCtx.SessionID = fmt.Sprintf("ses-%d", n)
	return e.act(e.srv, n, workCtx)
}

func newStageServer(t *testing.T, act func(srv *Server, n int, workCtx *WebhookWorkContext) error, endpoints ...config.EndpointConfig) (*Server, *store.SQLiteStore, string) {
	t.Helper()
	srv, st, url := newPRTestServer(t, newPRRecorder("thread-pr", nil), endpoints)
	e := &stageExec{act: act, srv: srv}
	srv.executor = e.exec
	srv.SetStageRepo(st.StageRepo())
	srv.reviewPosted = func(context.Context, string, string, time.Time) (bool, error) { return true, nil }
	srv.retryBackoffs = map[failureClass][]time.Duration{
		failureModel:           {0, 0, 0},
		failureReviewNotPosted: {0},
	}
	return srv, st, url
}

func TestStageRunsFromLaneOutcomes(t *testing.T) {
	ok := func(*Server, int, *WebhookWorkContext) error { return nil }
	cases := []struct {
		name     string
		workflow string
		event    string
		review   string
		act      func(srv *Server, n int, workCtx *WebhookWorkContext) error
		posted   func(n int) bool
		setup    func(srv *Server)
		receipt  store.WebhookStatus
		want     []stageRunWant
	}{
		{
			name: "review completes", workflow: "review", event: "commented", act: ok,
			receipt: store.WebhookStatusCompleted,
			want:    []stageRunWant{{store.StageReview, 1, store.StageStateDone, ""}},
		},
		{
			name: "review not posted then retried", workflow: "review", event: "commented", act: ok,
			posted:  func(n int) bool { return n >= 2 },
			receipt: store.WebhookStatusCompleted,
			want: []stageRunWant{
				{store.StageReview, 1, store.StageStateFailed, ""},
				{store.StageReview, 2, store.StageStateDone, ""},
			},
		},
		{
			name: "review never posted", workflow: "review", event: "commented", act: ok,
			posted:  func(int) bool { return false },
			receipt: store.WebhookStatusFailed,
			want: []stageRunWant{
				{store.StageReview, 1, store.StageStateFailed, ""},
				{store.StageReview, 2, store.StageStateBlocked, store.BlockerRetriesExhausted},
			},
		},
		{
			name: "fix changes nothing", workflow: "fix", event: "changes_requested",
			act:     func(*Server, int, *WebhookWorkContext) error { return errNoWorkInLease },
			receipt: store.WebhookStatusFailed,
			want:    []stageRunWant{{store.StageFix, 1, store.StageStateBlocked, store.BlockerAgentNoCommit}},
		},
		{
			name: "fix model errors exhaust retries", workflow: "fix", event: "changes_requested",
			act:     func(*Server, int, *WebhookWorkContext) error { return relay.ErrWebhookAgentResponse },
			receipt: store.WebhookStatusFailed,
			want: []stageRunWant{
				{store.StageFix, 1, store.StageStateFailed, ""},
				{store.StageFix, 2, store.StageStateFailed, ""},
				{store.StageFix, 3, store.StageStateFailed, ""},
				{store.StageFix, 4, store.StageStateBlocked, store.BlockerRetriesExhausted},
			},
		},
		{
			name: "fix denied a permission", workflow: "fix", event: "changes_requested",
			act:     func(*Server, int, *WebhookWorkContext) error { return relay.ErrWebhookPermissionDenied },
			receipt: store.WebhookStatusFailed,
			want:    []stageRunWant{{store.StageFix, 1, store.StageStateBlocked, store.BlockerAuthDenied}},
		},
		{
			name: "fix hit a provider limit", workflow: "fix", event: "changes_requested",
			act: func(*Server, int, *WebhookWorkContext) error {
				return fmt.Errorf("%w: quota reached", relay.ErrWebhookProviderLimit)
			},
			receipt: store.WebhookStatusFailed,
			want:    []stageRunWant{{store.StageFix, 1, store.StageStateBlocked, store.BlockerProviderLimit}},
		},
		{
			name: "fix fails while the PR state is unreadable", workflow: "fix", event: "changes_requested",
			act:     func(*Server, int, *WebhookWorkContext) error { return relay.ErrWebhookAgentResponse },
			setup:   func(srv *Server) { setPRState(srv, "", errors.New("gh down")) },
			receipt: store.WebhookStatusFailed,
			want:    []stageRunWant{{store.StageFix, 1, store.StageStateBlocked, store.BlockerGitHubUnreachable}},
		},
		{
			name: "operator cancels a running fix", workflow: "fix", event: "changes_requested",
			act: func(srv *Server, _ int, _ *WebhookWorkContext) error {
				srv.dispatcherFor(stagePR).requestCancel()
				return relay.ErrWebhookAgentResponse
			},
			receipt: store.WebhookStatusSkipped,
			want:    []stageRunWant{{store.StageFix, 1, store.StageStateCancelled, ""}},
		},
		{
			name: "operator skips a running fix", workflow: "fix", event: "changes_requested",
			act: func(srv *Server, _ int, _ *WebhookWorkContext) error {
				srv.dispatcherFor(stagePR).requestSkip()
				return relay.ErrWebhookAgentResponse
			},
			receipt: store.WebhookStatusSkipped,
			want:    []stageRunWant{{store.StageFix, 1, store.StageStateSkipped, ""}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ep := prTestEndpoint(tc.workflow, "/"+tc.workflow, config.AutomationFull)
			srv, st, url := newStageServer(t, tc.act, ep)
			if tc.setup != nil {
				tc.setup(srv)
			}
			if tc.posted != nil {
				var mu sync.Mutex
				lookups := 0
				srv.reviewPosted = func(context.Context, string, string, time.Time) (bool, error) {
					mu.Lock()
					defer mu.Unlock()
					lookups++
					return tc.posted(lookups), nil
				}
			}
			body := prReviewBody(7, "feat/x", "owner", tc.event, "c0ffee1")
			if resp := post(t, url+"/"+tc.workflow+"?secret=s3cret", "del-stage", "pull_request_review", body); resp.StatusCode != http.StatusOK {
				t.Fatalf("post: status %d", resp.StatusCode)
			}
			waitForDeliveryStatus(t, st, ep.Name, "del-stage", tc.receipt)
			runs := assertStageRuns(t, st, stagePR, tc.want)
			for _, r := range runs {
				if r.TriggeredBy != "delivery:"+ep.Name+"/del-stage" || r.StartedAt == 0 || r.AgentSessionID == "" {
					t.Fatalf("run = %+v, want trigger, start and session recorded", r)
				}
				if r.State.Terminal() && r.EndedAt == 0 {
					t.Fatalf("terminal run without ended_at: %+v", r)
				}
			}
			last := runs[len(runs)-1]
			if last.State == store.StageStateBlocked {
				row, err := st.PRQueueRepo().Get(context.Background(), stagePR)
				if err != nil || row == nil || last.BlockerDetail != row.BlockedReason {
					t.Fatalf("blocker detail = %q, pr row = %+v %v", last.BlockerDetail, row, err)
				}
			}
			item, err := st.StageRepo().GetWorkItem(context.Background(), stagePR)
			if err != nil || item.State != store.WorkItemOpen || item.Repo != "acme/widget" || item.Number != 7 {
				t.Fatalf("work item = %+v, %v", item, err)
			}
		})
	}
}

func TestStageRunNotCreatedForSkipBeforeStart(t *testing.T) {
	ep := prTestEndpoint("fix", "/fix", config.AutomationReviewOnly)
	_, st, url := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, ep)
	if resp := post(t, url+"/fix?secret=s3cret", "del-early", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee1")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, ep.Name, "del-early", store.WebhookStatusSkipped)
	if runs := stageRunsOf(t, st, stagePR); len(runs) != 0 {
		t.Fatalf("a delivery skipped before it ran recorded %+v", runs)
	}
}

func TestStageRunsForMergeLane(t *testing.T) {
	cases := []struct {
		name     string
		refuse   string
		prState  string
		want     store.StageState
		item     store.WorkItemState
		detailIn string
	}{
		{name: "merge confirmed", prState: PRStateMerged, want: store.StageStateDone, item: store.WorkItemDone},
		{name: "merge refused by a gate", refuse: "no approval", prState: PRStateOpen, want: store.StageStateSkipped, item: store.WorkItemOpen, detailIn: "merge skipped: no approval"},
		{name: "merge not confirmed", prState: PRStateOpen, want: store.StageStateSkipped, item: store.WorkItemOpen, detailIn: "merge not confirmed: PR is open"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ep := prTestEndpoint("merge", "/merge", config.AutomationFull)
			srv, st, _ := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, ep)
			setPRState(srv, tc.prState, nil)
			if tc.refuse != "" {
				srv.codeMerge = func(context.Context, config.EndpointConfig, *WebhookWorkContext) (string, error) {
					return tc.refuse, nil
				}
			}
			id := seedProcessingDelivery(t, st, ep.Name, "del-merge", "pull_request_review")
			body := []byte(prReviewBody(7, "feat/x", "owner", "approved", "c0ffee1"))
			if err := srv.executeDelivery(ep, body, id, "del-merge", "pull_request_review", 1, nil, func() bool { return false }, nil); err != nil {
				t.Fatalf("execute merge: %v", err)
			}
			runs := assertStageRuns(t, st, stagePR, []stageRunWant{{store.StageMerge, 1, tc.want, ""}})
			if !strings.Contains(runs[0].BlockerDetail, tc.detailIn) {
				t.Fatalf("detail = %q, want it to contain %q", runs[0].BlockerDetail, tc.detailIn)
			}
			item, err := st.StageRepo().GetWorkItem(context.Background(), stagePR)
			if err != nil || item.State != tc.item {
				t.Fatalf("work item = %+v, %v; want %s", item, err, tc.item)
			}
		})
	}
}

func TestStageWorkItemClosedByMergedDelivery(t *testing.T) {
	cases := []struct {
		name    string
		prState string
		merged  bool
		receipt store.WebhookStatus
		want    store.WorkItemState
	}{
		{name: "merged", prState: PRStateMerged, merged: true, receipt: store.WebhookStatusCompleted, want: store.WorkItemDone},
		{name: "closed unmerged", prState: PRStateClosed, receipt: store.WebhookStatusSkipped, want: store.WorkItemAbandoned},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ep := prTestEndpoint("merged", "/merged", config.AutomationFull)
			srv, st, url := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, ep)
			if err := st.StageRepo().UpsertWorkItem(context.Background(), store.WorkItem{ID: stagePR, Repo: "acme/widget", Number: 7}); err != nil {
				t.Fatal(err)
			}
			setPRState(srv, tc.prState, nil)
			if resp := post(t, url+"/merged?secret=s3cret", "del-closed", "pull_request", prClosedBody(7, tc.merged)); resp.StatusCode != http.StatusOK {
				t.Fatalf("post: status %d", resp.StatusCode)
			}
			waitForDeliveryStatus(t, st, ep.Name, "del-closed", tc.receipt)
			item, err := st.StageRepo().GetWorkItem(context.Background(), stagePR)
			if err != nil || item.State != tc.want {
				t.Fatalf("work item = %+v, %v; want %s", item, err, tc.want)
			}
			if runs := stageRunsOf(t, st, stagePR); len(runs) != 0 {
				t.Fatalf("the merged hook recorded stage runs: %+v", runs)
			}
		})
	}
}

func TestStageRunBlockedByRestart(t *testing.T) {
	ep := prTestEndpoint("review", "/review", config.AutomationFull)
	srv, st, _ := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, ep)
	ctx := context.Background()
	repo := st.StageRepo()
	if err := repo.UpsertWorkItem(ctx, store.WorkItem{ID: stagePR, Repo: "acme/widget", Number: 7}); err != nil {
		t.Fatal(err)
	}
	interrupted := seedProcessingDelivery(t, st, ep.Name, "del-interrupted", "pull_request_review")
	seedProcessingDelivery(t, st, ep.Name, "del-live", "pull_request_review")
	if ok, err := st.WebhookDeliveryRepo().Transition(ctx, interrupted, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusFailed, interruptedByRestart); err != nil || !ok {
		t.Fatalf("fail receipt: %v %v", ok, err)
	}
	for _, del := range []string{"del-interrupted", "del-live"} {
		if _, err := repo.StartRun(ctx, store.StageRunStart{WorkItemID: stagePR, Stage: store.StageReview, TriggeredBy: "delivery:" + ep.Name + "/" + del}); err != nil {
			t.Fatal(err)
		}
	}
	srv.stages.recoverInterrupted(ctx, srv.deliveries)
	runs := assertStageRuns(t, st, stagePR, []stageRunWant{
		{store.StageReview, 1, store.StageStateBlocked, store.BlockerTimeout},
		{store.StageReview, 2, store.StageStateRunning, ""},
	})
	if runs[0].BlockerDetail != "app restarted" {
		t.Fatalf("detail = %q, want app restarted", runs[0].BlockerDetail)
	}
}

type failingStageRepo struct{ store.StageRepo }

var errStageDown = errors.New("stage store down")

func (failingStageRepo) UpsertWorkItem(context.Context, store.WorkItem) error { return errStageDown }
func (failingStageRepo) StartRun(context.Context, store.StageRunStart) (int64, error) {
	return 0, errStageDown
}
func (failingStageRepo) SetWorkItemState(context.Context, string, store.WorkItemState) error {
	return errStageDown
}

func TestStageRecordErrorLeavesDeliveryUnchanged(t *testing.T) {
	for _, recording := range []string{"off", "failing"} {
		t.Run(recording, func(t *testing.T) {
			logs := captureServerLogs(t)
			ep := prTestEndpoint("review", "/review", config.AutomationFull)
			srv, st, url := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, ep)
			srv.SetStageRepo(nil)
			if recording == "failing" {
				srv.SetStageRepo(failingStageRepo{})
			}
			if resp := post(t, url+"/review?secret=s3cret", "del-unchanged", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1")); resp.StatusCode != http.StatusOK {
				t.Fatalf("post: status %d", resp.StatusCode)
			}
			waitForDeliveryStatus(t, st, ep.Name, "del-unchanged", store.WebhookStatusCompleted)
			waitForPRQueueStatus(t, st, stagePR, store.PRStatusDrained)
			if got := len(logs.all("stage: record failed")); (recording == "failing") != (got > 0) {
				t.Fatalf("record failed logs = %d with recording %s", got, recording)
			}
			if len(logs.all("webhook: delivery completed")) != 1 {
				t.Fatal("the delivery did not complete exactly once")
			}
		})
	}
}

func TestBlockerCodeForReason(t *testing.T) {
	cases := []struct {
		reason string
		want   store.BlockerCode
	}{
		{escalateReason("review", failureReviewNotPosted, 1, 1), store.BlockerRetriesExhausted},
		{escalateReason("fix", failureModel, 3, 3), store.BlockerRetriesExhausted},
		{fmt.Sprintf("auto-fix storm cap — manual action (%d auto-fix runs in 24h)", autoFixStormMax), store.BlockerRetriesExhausted},
		{"fix made no change — operator action required", store.BlockerAgentNoCommit},
		{"fix made no change — operator action required: nothing to edit", store.BlockerAgentNoCommit},
		{"fix stopped on a denied permission — operator action required: read /etc", store.BlockerAuthDenied},
		{"review stopped on a provider limit — operator action required: quota", store.BlockerProviderLimit},
		{"fix could not get its worktree — operator action required: dirty tree", store.BlockerWorkspaceDirty},
		{"live PR state unresolvable — retry refused (fail-closed)", store.BlockerGitHubUnreachable},
		{"merge starved — manual action", store.BlockerMergeStarved},
		{interruptedByRestart, store.BlockerTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.reason, func(t *testing.T) {
			got, ok := blockerCodeFor(tc.reason)
			if !ok || got != tc.want {
				t.Fatalf("blockerCodeFor = %q, %v; want %q", got, ok, tc.want)
			}
		})
	}
	if got, ok := blockerCodeFor("something else"); ok || got != "" {
		t.Fatalf("unknown reason = %q, %v; want no code", got, ok)
	}
}
