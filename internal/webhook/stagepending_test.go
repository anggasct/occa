package webhook

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

func stageRunOf(t *testing.T, st *store.SQLiteStore, endpoint, deliveryID string) (store.StageRun, bool) {
	t.Helper()
	trigger := stageTriggerPrefix + stageRunKey(endpoint, deliveryID)
	var workItem string
	if err := st.DB().QueryRow(`SELECT work_item_id FROM stage_run WHERE triggered_by = ? LIMIT 1`, trigger).Scan(&workItem); err != nil {
		return store.StageRun{}, false
	}
	for _, run := range stageRunsOf(t, st, workItem) {
		if run.TriggeredBy == trigger {
			return run, true
		}
	}
	return store.StageRun{}, false
}

func waitForStageRun(t *testing.T, st *store.SQLiteStore, endpoint, deliveryID string, state store.StageState) store.StageRun {
	t.Helper()
	var run store.StageRun
	waitForCondition(t, fmt.Sprintf("stage run of %s %s", deliveryID, state), func() bool {
		got, ok := stageRunOf(t, st, endpoint, deliveryID)
		run = got
		return ok && got.State == state
	})
	return run
}

func waitForAdmissionDecided(t *testing.T, srv *Server, endpoint, deliveryID string) {
	t.Helper()
	key := stageRunKey(endpoint, deliveryID)
	waitForCondition(t, "admission of "+deliveryID+" decided", func() bool {
		srv.stages.mu.Lock()
		slot := srv.stages.queued[key]
		srv.stages.mu.Unlock()
		if slot == nil {
			return true
		}
		slot.mu.Lock()
		defer slot.mu.Unlock()
		return slot.done
	})
}

func parkPR(srv *Server, prKey string) *dispatcher {
	d := newDispatcher(srv, "pr:"+prKey)
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()
	return d
}

func enqueueStage(t *testing.T, srv *Server, ep config.EndpointConfig, deliveryID, eventType, body string) {
	t.Helper()
	item := dispatchItem{ep: ep, body: []byte(body), deliveryID: deliveryID, eventType: eventType, payloadHash: deliveryID}
	if _, ok, why := srv.enqueue(ep, ExtractExecutionKey(item.body), item); !ok {
		t.Fatalf("enqueue %s: %s", deliveryID, why)
	}
}

func prSyncBody(head string) string {
	return fmt.Sprintf(`{"action":"synchronize","pull_request":{"number":7,"state":"open","user":{"login":"owner"},"head":{"ref":"feat/x","sha":%q,"repo":{"full_name":"acme/widget"}},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}},"repository":{"full_name":"acme/widget"}}`, head)
}

func TestQueuedDeliveryRunsAsItsPendingAttempt(t *testing.T) {
	ep := prTestEndpoint("review", "/review", config.AutomationFull)
	srv, st, _ := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, ep)
	d := parkPR(srv, stagePR)
	enqueueStage(t, srv, ep, "del-queued", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1"))

	pending := waitForStageRun(t, st, ep.Name, "del-queued", store.StageStatePending)
	if pending.Stage != store.StageReview || pending.Attempt != 1 || pending.StartedAt != 0 {
		t.Fatalf("pending run = %+v", pending)
	}
	if item, err := st.StageRepo().GetWorkItem(context.Background(), stagePR); err != nil || item.State != store.WorkItemOpen {
		t.Fatalf("work item = %+v, %v", item, err)
	}

	srv.dispatchWG.Add(1)
	go d.run(srv.shutdownCtx)
	d.signal()
	waitForDeliveryStatus(t, st, ep.Name, "del-queued", store.WebhookStatusCompleted)
	runs := assertStageRuns(t, st, stagePR, []stageRunWant{{store.StageReview, 1, store.StageStateDone, ""}})
	if runs[0].ID != pending.ID || runs[0].StartedAt == 0 || runs[0].LastHeartbeatAt == 0 || runs[0].EndedAt == 0 {
		t.Fatalf("run = %+v, want the pending row %d started and ended", runs[0], pending.ID)
	}
}

func TestPendingRunEndsWhenANewerDeliveryReplacesIt(t *testing.T) {
	cases := []struct {
		name   string
		queue  func(t *testing.T, srv *Server, merge, review config.EndpointConfig)
		detail string
	}{
		{
			name: "coalesced by a newer state event",
			queue: func(t *testing.T, srv *Server, merge, _ config.EndpointConfig) {
				enqueueStage(t, srv, merge, "del-newer", "pull_request", prSyncBody("bead0002"))
			},
			detail: "coalesced by a newer state event",
		},
		{
			name: "evicted for a higher-priority delivery",
			queue: func(t *testing.T, srv *Server, _, review config.EndpointConfig) {
				srv.maxQueuedPerKey = 2
				srv.stateShare = 0.5
				enqueueStage(t, srv, review, "del-review-1", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1"))
				enqueueStage(t, srv, review, "del-review-2", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee2"))
			},
			detail: "evicted for a higher-priority delivery",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			merge := prTestEndpoint("merge", "/merge", config.AutomationFull)
			review := prTestEndpoint("review", "/review", config.AutomationFull)
			srv, st, _ := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, merge, review)
			parkPR(srv, stagePR)
			enqueueStage(t, srv, merge, "del-older", "pull_request", prSyncBody("bead0001"))
			tc.queue(t, srv, merge, review)

			run := waitForStageRun(t, st, merge.Name, "del-older", store.StageStateSkipped)
			if run.Stage != store.StageMerge || run.Attempt != 1 || run.BlockerDetail != tc.detail || run.StartedAt != 0 {
				t.Fatalf("replaced run = %+v, want merge attempt 1 skipped %q", run, tc.detail)
			}
		})
	}
}

func TestQueuedCopyNotAdmittedWritesNoPendingRun(t *testing.T) {
	cases := []struct {
		name string
		mode string
		body string
	}{
		{name: "author policy refuses the fix", mode: config.AutomationReviewOnly, body: prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee1")},
		{name: "no admit rule for the copy", mode: config.AutomationFull, body: prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fix := prTestEndpoint("fix", "/fix", tc.mode)
			fix.Admit = []config.AdmitRule{{Event: "pull_request_review", ReviewState: []string{"changes_requested"}}}
			srv, st, _ := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, fix)
			parkPR(srv, stagePR)
			enqueueStage(t, srv, fix, "del-copy", "pull_request_review", tc.body)
			waitForAdmissionDecided(t, srv, fix.Name, "del-copy")
			if runs := stageRunsOf(t, st, stagePR); len(runs) != 0 {
				t.Fatalf("a copy that would not be admitted recorded %+v", runs)
			}
		})
	}
}

func TestDuplicateReviewEndsItsPendingRun(t *testing.T) {
	ep := prTestEndpoint("review", "/review", config.AutomationFull)
	srv, st, _ := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, ep)
	enqueueStage(t, srv, ep, "del-first", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1"))
	waitForDeliveryStatus(t, st, ep.Name, "del-first", store.WebhookStatusCompleted)
	enqueueStage(t, srv, ep, "del-again", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1"))
	waitForDeliveryStatus(t, st, ep.Name, "del-again", store.WebhookStatusSkipped)

	run := waitForStageRun(t, st, ep.Name, "del-again", store.StageStateSkipped)
	if run.Attempt != 2 || run.StartedAt != 0 || !strings.HasPrefix(run.BlockerDetail, "duplicate review event") {
		t.Fatalf("duplicate run = %+v, want attempt 2 skipped as a duplicate review", run)
	}
	if first, _ := stageRunOf(t, st, ep.Name, "del-first"); first.State != store.StageStateDone || first.Attempt != 1 {
		t.Fatalf("first run = %+v", first)
	}
}

func TestQueuedRunEndsWhenTheQueueIsDrained(t *testing.T) {
	cases := []struct {
		name   string
		act    func(srv *Server, review, merged config.EndpointConfig, merge *atomic.Bool) error
		state  store.StageState
		detail string
	}{
		{
			name: "operator cancel",
			act: func(srv *Server, review, _ config.EndpointConfig, _ *atomic.Bool) error {
				queueBehind(srv, review, "del-behind", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1"))
				srv.dispatcherFor(stagePR).requestCancel()
				return relay.ErrWebhookAgentResponse
			},
			state:  store.StageStateCancelled,
			detail: "cancelled by operator",
		},
		{
			name: "PR merged",
			act: func(srv *Server, review, merged config.EndpointConfig, merge *atomic.Bool) error {
				queueBehind(srv, review, "del-behind", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1"))
				merge.Store(true)
				queueBehind(srv, merged, "del-merged", "pull_request", prClosedBody(7, true))
				return nil
			},
			state:  store.StageStateSkipped,
			detail: "PR merged",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fix := prTestEndpoint("fix", "/fix", config.AutomationFull)
			review := prTestEndpoint("review", "/review", config.AutomationFull)
			merged := prTestEndpoint("merged", "/merged", config.AutomationFull)
			var isMerged atomic.Bool
			var once atomic.Bool
			srv, st, _ := newStageServer(t, nil, fix, review, merged)
			srv.SetPRStateResolver(func(context.Context, string, string) (string, error) {
				if isMerged.Load() {
					return PRStateMerged, nil
				}
				return PRStateOpen, nil
			})
			srv.executor = func(_ context.Context, _, _, _ string, workCtx *WebhookWorkContext) error {
				workCtx.ThreadID = "thread-pr"
				if workCtx.Workflow != "fix" || once.Swap(true) {
					return nil
				}
				return tc.act(srv, review, merged, &isMerged)
			}
			enqueueStage(t, srv, fix, "del-fix", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee1"))

			run := waitForStageRun(t, st, review.Name, "del-behind", tc.state)
			if run.Stage != store.StageReview || run.BlockerDetail != tc.detail || run.StartedAt != 0 {
				t.Fatalf("drained run = %+v, want review %s %q", run, tc.state, tc.detail)
			}
		})
	}
}

func queueBehind(srv *Server, ep config.EndpointConfig, deliveryID, eventType, body string) {
	item := dispatchItem{ep: ep, body: []byte(body), deliveryID: deliveryID, eventType: eventType, payloadHash: deliveryID}
	srv.enqueue(ep, ExtractExecutionKey(item.body), item)
}

func TestPendingSweepClosesRunsOfSettledReceipts(t *testing.T) {
	cases := []struct {
		name    string
		receipt *store.WebhookDelivery
		live    bool
		want    store.StageState
		detail  string
	}{
		{name: "receipt completed", receipt: &store.WebhookDelivery{Status: store.WebhookStatusCompleted, ErrorSummary: "review posted"}, want: store.StageStateSkipped, detail: "review posted"},
		{name: "receipt failed without summary", receipt: &store.WebhookDelivery{Status: store.WebhookStatusFailed}, want: store.StageStateSkipped, detail: "failed"},
		{name: "receipt gone", want: store.StageStateSkipped, detail: receiptGoneDetail},
		{name: "receipt still received", receipt: &store.WebhookDelivery{Status: store.WebhookStatusReceived}, want: store.StageStatePending},
		{name: "settled receipt of a queued delivery", receipt: &store.WebhookDelivery{Status: store.WebhookStatusSkipped}, live: true, want: store.StageStatePending},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ep := prTestEndpoint("review", "/review", config.AutomationFull)
			srv, st, _ := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, ep)
			const deliveryID = "del-pending"
			if tc.receipt != nil {
				r := *tc.receipt
				r.Endpoint, r.DeliveryID, r.EventType, r.PayloadHash = ep.Name, deliveryID, "pull_request_review", deliveryID
				if _, err := st.WebhookDeliveryRepo().Create(ctx, r); err != nil {
					t.Fatal(err)
				}
			}
			if tc.live {
				addLiveDispatcher(srv, dispatchItem{ep: ep, deliveryID: deliveryID, prKey: stagePR}, false)
			}
			repo := st.StageRepo()
			if err := repo.UpsertWorkItem(ctx, store.WorkItem{ID: stagePR, Repo: "acme/widget", Number: 7}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.QueueRun(ctx, store.StageRunStart{WorkItemID: stagePR, Stage: store.StageReview, TriggeredBy: stageTriggerPrefix + stageRunKey(ep.Name, deliveryID)}); err != nil {
				t.Fatal(err)
			}

			srv.reconcileOnce(ctx)

			run, ok := stageRunOf(t, st, ep.Name, deliveryID)
			if !ok || run.State != tc.want || run.BlockerDetail != tc.detail {
				t.Fatalf("run = %+v, want %s %q", run, tc.want, tc.detail)
			}
		})
	}
}

func TestStageRunPendingTransitions(t *testing.T) {
	ctx := context.Background()
	ep := prTestEndpoint("review", "/review", config.AutomationFull)
	_, st, _ := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, ep)
	repo := st.StageRepo()
	if err := repo.UpsertWorkItem(ctx, store.WorkItem{ID: stagePR, Repo: "acme/widget", Number: 7}); err != nil {
		t.Fatal(err)
	}
	start := store.StageRunStart{WorkItemID: stagePR, Stage: store.StageReview, TriggeredBy: "delivery:review/del-1"}
	id, err := repo.QueueRun(ctx, start)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := repo.QueueRun(ctx, start); err != nil || again != id {
		t.Fatalf("second queue of the same trigger = %d, %v; want %d", again, err, id)
	}
	if err := repo.PromotePending(ctx, id, "/ws"); err != nil {
		t.Fatal(err)
	}
	if err := repo.PromotePending(ctx, id, "/ws"); !errors.Is(err, store.ErrStageRunNotPending) {
		t.Fatalf("second promote = %v, want not pending", err)
	}
	if err := repo.EndPending(ctx, id, store.StageStateSkipped, "late"); !errors.Is(err, store.ErrStageRunNotPending) {
		t.Fatalf("end of a running run = %v, want not pending", err)
	}
	if _, err := repo.BlockPending(ctx, store.StageRunBlock{WorkItemID: stagePR, Stage: store.StageReview, Code: store.BlockerTimeout}, id); !errors.Is(err, store.ErrStageRunNotPending) {
		t.Fatalf("pending block of a running run = %v, want not pending", err)
	}
	if _, err := repo.BlockPending(ctx, store.StageRunBlock{WorkItemID: stagePR, Stage: store.StageReview, Code: store.BlockerTimeout}, 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("pending block with nothing pending = %v, want not found", err)
	}
	if err := repo.EndRun(ctx, id, store.StageStateDone, ""); err != nil {
		t.Fatal(err)
	}
	if err := repo.EndPending(ctx, id, store.StageStateSkipped, "late"); !errors.Is(err, store.ErrTerminalAlreadySet) {
		t.Fatalf("end of a done run = %v, want terminal already set", err)
	}
	if err := repo.EndPending(ctx, id, store.StageStateDone, ""); !errors.Is(err, store.ErrStageStateInvalid) {
		t.Fatalf("end pending as done = %v, want invalid state", err)
	}
	if runs := assertStageRuns(t, st, stagePR, []stageRunWant{{store.StageReview, 1, store.StageStateDone, ""}}); runs[0].WorkspacePath != "/ws" {
		t.Fatalf("run = %+v, want the workspace recorded on promote", runs[0])
	}
}

func TestBlockBeforeStartReusesThePendingRun(t *testing.T) {
	const worktree = "fix could not get its worktree — operator action required: dirty"
	cases := []struct {
		name  string
		queue func(t *testing.T, srv *Server, eps map[string]config.EndpointConfig)
		block func(t *testing.T, srv *Server, st *store.SQLiteStore, eps map[string]config.EndpointConfig)
		want  []stageRunWant
		reuse bool
	}{
		{
			name: "merge starved with a queued merge",
			queue: func(t *testing.T, srv *Server, eps map[string]config.EndpointConfig) {
				enqueueStage(t, srv, eps["merge"], "del-pending", "pull_request", prSyncBody("bead0001"))
			},
			block: func(_ *testing.T, srv *Server, _ *store.SQLiteStore, _ map[string]config.EndpointConfig) {
				srv.prMirrorBlocked(stagePR, "merge", "merge starved — manual action")
			},
			want:  []stageRunWant{{store.StageMerge, 1, store.StageStateBlocked, store.BlockerMergeStarved}},
			reuse: true,
		},
		{
			name: "worktree failure with a queued fix",
			queue: func(t *testing.T, srv *Server, eps map[string]config.EndpointConfig) {
				enqueueStage(t, srv, eps["fix"], "del-pending", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee1"))
			},
			block: func(t *testing.T, srv *Server, st *store.SQLiteStore, eps map[string]config.EndpointConfig) {
				row, err := st.PRQueueRepo().Get(context.Background(), stagePR)
				if err != nil || row == nil {
					t.Fatalf("row = %+v, %v", row, err)
				}
				srv.escalateBlock(eps["fix"], stagePR, row, worktree, "fix run could not start", "dirty")
			},
			want:  []stageRunWant{{store.StageFix, 1, store.StageStateBlocked, store.BlockerWorkspaceDirty}},
			reuse: true,
		},
		{
			name: "pending run left by a previous process",
			queue: func(t *testing.T, srv *Server, eps map[string]config.EndpointConfig) {
				ctx := context.Background()
				if _, err := srv.prQueues.EnsurePR(ctx, stagePR, "", ""); err != nil {
					t.Fatal(err)
				}
				if err := srv.stages.ensureWorkItem(ctx, stagePR); err != nil {
					t.Fatal(err)
				}
				if _, err := srv.stages.repo.QueueRun(ctx, store.StageRunStart{WorkItemID: stagePR, Stage: store.StageMerge, TriggeredBy: stageTriggerPrefix + stageRunKey(eps["merge"].Name, "del-pending")}); err != nil {
					t.Fatal(err)
				}
			},
			block: func(_ *testing.T, srv *Server, _ *store.SQLiteStore, _ map[string]config.EndpointConfig) {
				srv.prMirrorBlocked(stagePR, "merge", "merge starved — manual action")
			},
			want:  []stageRunWant{{store.StageMerge, 1, store.StageStateBlocked, store.BlockerMergeStarved}},
			reuse: true,
		},
		{
			name: "pending run of another stage",
			queue: func(t *testing.T, srv *Server, eps map[string]config.EndpointConfig) {
				enqueueStage(t, srv, eps["review"], "del-pending", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1"))
			},
			block: func(_ *testing.T, srv *Server, _ *store.SQLiteStore, _ map[string]config.EndpointConfig) {
				srv.prMirrorBlocked(stagePR, "merge", "merge starved — manual action")
			},
			want: []stageRunWant{
				{store.StageReview, 1, store.StageStatePending, ""},
				{store.StageMerge, 1, store.StageStateBlocked, store.BlockerMergeStarved},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			eps := map[string]config.EndpointConfig{}
			var list []config.EndpointConfig
			for _, wf := range []string{"merge", "fix", "review"} {
				eps[wf] = prTestEndpoint(wf, "/"+wf, config.AutomationFull)
				list = append(list, eps[wf])
			}
			srv, st, _ := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, list...)
			d := parkPR(srv, stagePR)
			tc.queue(t, srv, eps)
			first := tc.want[0]
			var pendingEP string
			for _, ep := range eps {
				if s, _ := stageForWorkflow(ep.Workflow); s == first.stage {
					pendingEP = ep.Name
				}
			}
			pending := waitForStageRun(t, st, pendingEP, "del-pending", store.StageStatePending)

			tc.block(t, srv, st, eps)
			runs := assertStageRuns(t, st, stagePR, tc.want)
			if tc.reuse && (runs[0].ID != pending.ID || runs[0].StartedAt != 0 || runs[0].BlockerDetail == "") {
				t.Fatalf("blocked run = %+v, want pending run %d blocked in place", runs[0], pending.ID)
			}

			srv.drainPRQueue(d, stagePR, store.StageStateSkipped, "PR merged")
			srv.reconcileOnce(ctx)
			after := stageRunsOf(t, st, stagePR)
			for i, want := range tc.want {
				if want.state == store.StageStateBlocked && !reflect.DeepEqual(after[i], runs[i]) {
					t.Fatalf("blocked run changed after drain and sweep: %+v, was %+v", after[i], runs[i])
				}
			}
		})
	}
}
