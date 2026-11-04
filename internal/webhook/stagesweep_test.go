package webhook

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

type beatRecordingRepo struct {
	store.StageRepo
	mu    sync.Mutex
	beats []time.Time
}

func (r *beatRecordingRepo) Heartbeat(ctx context.Context, runID int64, at time.Time) error {
	r.mu.Lock()
	r.beats = append(r.beats, at)
	r.mu.Unlock()
	return r.StageRepo.Heartbeat(ctx, runID, at)
}

func (r *beatRecordingRepo) recorded() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.beats...)
}

func TestStageHeartbeatCoalescedPerSecond(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	cases := []struct {
		name   string
		events []time.Duration
		want   []time.Duration
	}{
		{name: "events a second apart", events: []time.Duration{0, time.Second, 2 * time.Second}, want: []time.Duration{0, time.Second, 2 * time.Second}},
		{name: "events 100ms apart", events: []time.Duration{0, 100 * time.Millisecond}, want: []time.Duration{0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var clock time.Time
			ep := prTestEndpoint("review", "/review", config.AutomationFull)
			srv, st, _ := newStageServer(t, func(_ *Server, _ int, workCtx *WebhookWorkContext) error {
				for _, at := range tc.events {
					clock = base.Add(at)
					workCtx.Heartbeat()
				}
				return nil
			}, ep)
			repo := &beatRecordingRepo{StageRepo: st.StageRepo()}
			srv.SetStageRepo(repo)
			srv.stages.now = func() time.Time { return clock }
			id := seedProcessingDelivery(t, st, ep.Name, "del-hb", "pull_request_review")
			body := []byte(prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1"))
			if err := srv.executeDelivery(ep, body, id, "del-hb", "pull_request_review", 1, nil, func() bool { return false }, func() {}); err != nil {
				t.Fatalf("execute review: %v", err)
			}
			var want []time.Time
			for _, at := range tc.want {
				want = append(want, base.Add(at))
			}
			if got := repo.recorded(); !reflect.DeepEqual(got, want) {
				t.Fatalf("heartbeat writes = %v, want %v", got, want)
			}
			runs := assertStageRuns(t, st, stagePR, []stageRunWant{{store.StageReview, 1, store.StageStateDone, ""}})
			if runs[0].LastHeartbeatAt != want[len(want)-1].Unix() {
				t.Fatalf("last_heartbeat_at = %d, want %d", runs[0].LastHeartbeatAt, want[len(want)-1].Unix())
			}
		})
	}
}

func TestStageRunDerivedStallUsesStallFreshness(t *testing.T) {
	ep := prTestEndpoint("review", "/review", config.AutomationFull)
	srv, st, _ := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, ep)
	srv.SetStallFreshness(90 * time.Second)
	ctx := context.Background()
	repo := st.StageRepo()
	if err := repo.UpsertWorkItem(ctx, store.WorkItem{ID: stagePR, Repo: "acme/widget", Number: 7}); err != nil {
		t.Fatal(err)
	}
	runID, err := repo.StartRun(ctx, store.StageRunStart{WorkItemID: stagePR, Stage: store.StageReview})
	if err != nil {
		t.Fatal(err)
	}
	beat := time.Unix(1_800_000_000, 0)
	if err := repo.Heartbeat(ctx, runID, beat); err != nil {
		t.Fatal(err)
	}
	run := stageRunsOf(t, st, stagePR)[0]
	cases := []struct {
		name string
		now  time.Time
		want bool
	}{
		{name: "within freshness", now: beat.Add(srv.stallFreshness), want: false},
		{name: "past freshness", now: beat.Add(srv.stallFreshness + time.Second), want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := run.Stalled(tc.now, srv.stallFreshness); got != tc.want {
				t.Fatalf("Stalled = %v, want %v", got, tc.want)
			}
			if stored := stageRunsOf(t, st, stagePR)[0].State; stored != store.StageStateRunning {
				t.Fatalf("stored state = %q, want running", stored)
			}
		})
	}
}

func TestStaleReceiptSweep(t *testing.T) {
	const deliveryID = "del-stale"
	cases := []struct {
		name   string
		fresh  bool
		setup  func(t *testing.T, srv *Server, st *store.SQLiteStore, ep config.EndpointConfig, receiptID int64)
		closed bool
	}{
		{name: "unowned stale receipt is closed", closed: true},
		{
			name: "active item of another endpoint does not own it", closed: true,
			setup: func(t *testing.T, srv *Server, _ *store.SQLiteStore, ep config.EndpointConfig, _ int64) {
				other := ep
				other.Name = "fix"
				addLiveDispatcher(srv, dispatchItem{ep: other, deliveryID: deliveryID, prKey: stagePR}, true)
			},
		},
		{
			name: "owned by a dispatcher active item",
			setup: func(t *testing.T, srv *Server, _ *store.SQLiteStore, ep config.EndpointConfig, _ int64) {
				addLiveDispatcher(srv, dispatchItem{ep: ep, deliveryID: deliveryID, prKey: stagePR}, true)
			},
		},
		{
			name: "owned by a queued item",
			setup: func(t *testing.T, srv *Server, _ *store.SQLiteStore, ep config.EndpointConfig, _ int64) {
				addLiveDispatcher(srv, dispatchItem{ep: ep, deliveryID: deliveryID, prKey: stagePR}, false)
			},
		},
		{
			name: "owned by the pr queue active delivery",
			setup: func(t *testing.T, _ *Server, st *store.SQLiteStore, _ config.EndpointConfig, receiptID int64) {
				ctx := context.Background()
				if _, err := st.PRQueueRepo().EnsurePR(ctx, stagePR, "", ""); err != nil {
					t.Fatal(err)
				}
				if err := st.PRQueueRepo().SetActive(ctx, stagePR, "review", "", receiptID); err != nil {
					t.Fatal(err)
				}
			},
		},
		{name: "younger than claim grace", fresh: true},
		{
			name: "already completed",
			setup: func(t *testing.T, _ *Server, st *store.SQLiteStore, _ config.EndpointConfig, receiptID int64) {
				if ok, err := st.WebhookDeliveryRepo().Transition(context.Background(), receiptID, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusCompleted, "done"); err != nil || !ok {
					t.Fatalf("complete receipt: %v %v", ok, err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureServerLogs(t)
			ctx := context.Background()
			ep := prTestEndpoint("review", "/review", config.AutomationFull)
			srv, st, _ := newStageServer(t, func(*Server, int, *WebhookWorkContext) error { return nil }, ep)
			receiptID := seedProcessingDelivery(t, st, ep.Name, deliveryID, "pull_request_review")
			if err := st.WebhookDeliveryRepo().SetPRKey(ctx, receiptID, stagePR); err != nil {
				t.Fatal(err)
			}
			repo := st.StageRepo()
			if err := repo.UpsertWorkItem(ctx, store.WorkItem{ID: stagePR, Repo: "acme/widget", Number: 7}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.StartRun(ctx, store.StageRunStart{WorkItemID: stagePR, Stage: store.StageReview, TriggeredBy: stageTriggerPrefix + stageRunKey(ep.Name, deliveryID)}); err != nil {
				t.Fatal(err)
			}
			if tc.setup != nil {
				tc.setup(t, srv, st, ep, receiptID)
			}
			if !tc.fresh {
				if _, err := st.DB().Exec(`UPDATE webhook_delivery SET updated_at = ? WHERE id = ?`, time.Now().Add(-2*srv.claimGrace).Unix(), receiptID); err != nil {
					t.Fatal(err)
				}
			}
			before, err := st.WebhookDeliveryRepo().GetByID(ctx, receiptID)
			if err != nil || before == nil {
				t.Fatalf("receipt: %+v %v", before, err)
			}

			srv.reconcileOnce(ctx)

			after, err := st.WebhookDeliveryRepo().GetByID(ctx, receiptID)
			if err != nil || after == nil {
				t.Fatalf("receipt: %+v %v", after, err)
			}
			closedLogs := logs.all("webhook: stale receipt closed")
			run := stageRunsOf(t, st, stagePR)[0]
			if !tc.closed {
				if after.Status != before.Status || after.ErrorSummary != before.ErrorSummary || after.UpdatedAt != before.UpdatedAt {
					t.Fatalf("receipt touched: %+v, was %+v", after, before)
				}
				if len(closedLogs) != 0 || run.State != store.StageStateRunning {
					t.Fatalf("closed logs = %d, run = %+v; want none and running", len(closedLogs), run)
				}
				return
			}
			if after.Status != store.WebhookStatusFailed || after.ErrorSummary != "abandoned: no live run after claim grace" {
				t.Fatalf("receipt = %+v, want failed abandoned", after)
			}
			if len(closedLogs) != 1 || closedLogs[0].attrs["id"] != receiptID || closedLogs[0].attrs["endpoint"] != ep.Name {
				t.Fatalf("closed logs = %+v, want one with id %d", closedLogs, receiptID)
			}
			if run.State != store.StageStateBlocked || run.BlockerCode != store.BlockerTimeout || run.BlockerDetail != "abandoned" {
				t.Fatalf("stage run = %+v, want blocked timeout abandoned", run)
			}
		})
	}
}

func addLiveDispatcher(srv *Server, item dispatchItem, active bool) {
	d := newDispatcher(srv, "pr:"+item.prKey)
	if active {
		d.setActive(item)
	} else {
		d.lanes[d.laneFor(item)].push(item)
	}
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()
}
