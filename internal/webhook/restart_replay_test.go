package webhook

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func restartReplayServer(t *testing.T) (*Server, *fakeExecutor, *store.SQLiteStore, config.EndpointConfig) {
	t.Helper()
	ep := config.EndpointConfig{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"}
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	srv.SetWebhookReplayRepo(st.WebhookReplayRepo())
	return srv, exec, st, srv.endpointsByName["github"]
}

func newTestServerFullWithStore(t *testing.T, endpoints []config.EndpointConfig, st *store.SQLiteStore) (*Server, *fakeExecutor, *store.SQLiteStore) {
	t.Helper()
	exec := &fakeExecutor{}
	srv := New(config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults(endpoints),
	}, exec.exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetStallFreshness(2 * time.Minute)
	return srv, exec, st
}

func shutdownItem(ep config.EndpointConfig, deliveryID string) dispatchItem {
	return dispatchItem{ep: ep, body: []byte(`{"action":"opened","id":"` + deliveryID + `"}`), deliveryID: deliveryID, eventType: "pull_request", prKey: "acme/app#7"}
}

func pendingReplays(t *testing.T, st *store.SQLiteStore) map[string]store.WebhookReplay {
	t.Helper()
	rows, err := st.WebhookReplayRepo().ListDue(context.Background(), time.Now().Add(48*time.Hour).Unix(), 50)
	if err != nil {
		t.Fatalf("list replays: %v", err)
	}
	out := map[string]store.WebhookReplay{}
	for _, row := range rows {
		out[row.DeliveryID] = row
	}
	return out
}

func TestRestartReplayRecordsActiveAndQueuedItems(t *testing.T) {
	srv, _, st, ep := restartReplayServer(t)
	active := shutdownItem(ep, "del-active")
	d := &dispatcher{server: srv, key: "pr:acme/app#7", lanes: makePRLanes(8), lastActivity: time.Now()}
	d.setActive(active)
	d.push(shutdownItem(ep, "del-queued-1"))
	d.push(shutdownItem(ep, "del-queued-2"))
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()

	srv.recordActiveReplays()
	d.retire("shutting down")
	srv.abandon(active, "shutting down")

	rows := pendingReplays(t, st)
	if len(rows) != 3 {
		t.Fatalf("pending replays = %d (%v), want 3", len(rows), rows)
	}
	for _, id := range []string{"del-active", "del-queued-1", "del-queued-2"} {
		row, ok := rows[id]
		if !ok {
			t.Fatalf("replay for %s missing", id)
		}
		if row.Reason != "interrupted by shutdown" || len(row.Body) == 0 {
			t.Fatalf("replay %s = reason %q body %d bytes, want interrupted by shutdown with body", id, row.Reason, len(row.Body))
		}
	}
}

func TestRestartReplayRearmsAnInterruptedReplay(t *testing.T) {
	srv, _, st, ep := restartReplayServer(t)
	ctx := context.Background()
	id, _, err := st.WebhookReplayRepo().Create(ctx, store.WebhookReplay{
		Endpoint: "github", DeliveryID: "del-replayed", EventType: "pull_request", Body: []byte(`{}`),
		Status: store.WebhookReplayReplayed, ExpiresAt: time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("create replay: %v", err)
	}
	item := shutdownItem(ep, "del-replayed")
	item.replayID = id

	srv.recordInterruptedReplay(item)

	if n, _ := st.WebhookReplayRepo().Count(ctx, store.WebhookReplayPending); n != 1 {
		t.Fatalf("pending replays = %d, want the existing row re-armed", n)
	}
	if n, _ := st.WebhookReplayRepo().Count(ctx, store.WebhookReplayReplayed); n != 0 {
		t.Fatalf("replayed rows = %d, want 0", n)
	}
}

func TestRestartReplayReopensFailedReceiptAfterRestart(t *testing.T) {
	srv, _, st, ep := restartReplayServer(t)
	ctx := context.Background()
	item := shutdownItem(ep, "del-restart")
	srv.abandon(item, "shutting down")
	receipt, err := st.WebhookDeliveryRepo().Get(ctx, "github", "del-restart")
	if err != nil || receipt == nil || receipt.Status != store.WebhookStatusFailed {
		t.Fatalf("receipt after shutdown = %+v, err %v, want failed", receipt, err)
	}

	restarted, exec, _ := newTestServerFullWithStore(t, []config.EndpointConfig{ep}, st)
	restarted.SetWebhookReplayRepo(st.WebhookReplayRepo())
	restarted.SetReplayChecker(func(context.Context, store.WebhookReplay) (replayDecision, error) {
		return replayGo, nil
	})
	if _, err := st.DB().ExecContext(ctx, `UPDATE webhook_replay SET next_attempt_at = 0`); err != nil {
		t.Fatalf("make replay due: %v", err)
	}
	restarted.replayPending(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && exec.callCount() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if exec.callCount() != 1 {
		t.Fatalf("executions after restart = %d, want 1", exec.callCount())
	}
}

func TestRestartReplayMergedLaneChecker(t *testing.T) {
	srv, _, _, _ := restartReplayServer(t)
	row := store.WebhookReplay{EventType: "pull_request", PRKey: "acme/app#7"}
	cases := []struct {
		state string
		err   error
		want  replayDecision
	}{
		{state: PRStateMerged, want: replayGo},
		{state: PRStateOpen, want: replaySuperseded},
		{state: PRStateClosed, want: replaySuperseded},
		{err: errors.New("gh down"), want: replayUndecidable},
	}
	for _, tc := range cases {
		srv.SetPRStateResolver(func(context.Context, string, string) (string, error) { return tc.state, tc.err })
		got, _ := srv.mergedReplayChecker(context.Background(), row)
		if got != tc.want {
			t.Fatalf("state %q err %v → %v, want %v", tc.state, tc.err, got, tc.want)
		}
	}
}

func TestRestartReplayClosesSupersededReceipt(t *testing.T) {
	srv, _, st, ep := restartReplayServer(t)
	ctx := context.Background()
	for _, id := range []string{"del-coalesced", "del-evicted"} {
		if _, err := st.WebhookDeliveryRepo().Create(ctx, store.WebhookDelivery{Endpoint: "github", DeliveryID: id, EventType: "check_suite", Status: store.WebhookStatusReceived}); err != nil {
			t.Fatalf("create receipt: %v", err)
		}
	}

	srv.closeSupersededReceipt(shutdownItem(ep, "del-coalesced"), "coalesced by a newer state event")
	srv.closeSupersededReceipt(shutdownItem(ep, "del-evicted"), "evicted for a higher-priority delivery")
	srv.closeSupersededReceipt(shutdownItem(ep, "del-missing"), "coalesced by a newer state event")

	for id, want := range map[string]string{
		"del-coalesced": "superseded: coalesced by a newer state event",
		"del-evicted":   "superseded: evicted for a higher-priority delivery",
	} {
		receipt, err := st.WebhookDeliveryRepo().Get(ctx, "github", id)
		if err != nil || receipt == nil {
			t.Fatalf("receipt %s: %v", id, err)
		}
		if receipt.Status != store.WebhookStatusSkipped || receipt.ErrorSummary != want {
			t.Fatalf("receipt %s = %s %q, want skipped %q", id, receipt.Status, receipt.ErrorSummary, want)
		}
	}
}

func TestRestartReplayWithoutReplayStore(t *testing.T) {
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	item := shutdownItem(srv.endpointsByName["github"], "del-no-replays")
	srv.recordInterruptedReplay(item)
	srv.abandon(item, "shutting down")
	receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), "github", "del-no-replays")
	if err != nil || receipt == nil || receipt.Status != store.WebhookStatusFailed {
		t.Fatalf("receipt = %+v, err %v, want failed", receipt, err)
	}
}
