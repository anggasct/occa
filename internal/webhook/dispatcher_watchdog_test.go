package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func stuckPayload() string {
	return `{"repository":{"full_name":"anggasct/occa"},"branch":"fix/watchdog"}`
}

func newWatchdogServer(t *testing.T, exec Executor) (*Server, *store.SQLiteStore) {
	t.Helper()
	cfg := config.WebhookConfig{
		Bind:    "127.0.0.1:0",
		Policy:  testPolicy(),
		Runtime: testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
		}),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	return srv, st
}

func waitForStatus(t *testing.T, st *store.SQLiteStore, endpoint, deliveryID string, want store.WebhookStatus) *store.WebhookDelivery {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), endpoint, deliveryID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if receipt != nil && receipt.Status == want {
			return receipt
		}
		time.Sleep(10 * time.Millisecond)
	}
	receipt, _ := st.WebhookDeliveryRepo().Get(context.Background(), endpoint, deliveryID)
	t.Fatalf("delivery %s never reached %s: %+v", deliveryID, want, receipt)
	return nil
}

func TestDispatcherStuckFlagReleasesKey(t *testing.T) {
	stuck := make(chan struct{})
	var mu sync.Mutex
	var cards []string
	releaseStuck := make(chan struct{})
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		if workCtx.DeliveryID == "delivery-stuck" {
			close(stuck)
			select {
			case <-releaseStuck:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}
		return nil
	}
	srv, st := newWatchdogServer(t, exec)
	srv.SetDispatcherStuckAfter(80 * time.Millisecond)
	srv.SetDispatcherWatchdogInterval(time.Hour)
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		mu.Lock()
		cards = append(cards, text)
		mu.Unlock()
		return nil
	})
	srv.TaskRegistry().Upsert("anggasct/occa", "fix/watchdog", "root-1", "thread-1", TaskDispatched)
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	url := ts.URL + "/github?secret=s3cret"
	if resp := post(t, url, "delivery-stuck", "pull_request", stuckPayload()); resp.StatusCode != http.StatusOK {
		t.Fatalf("stuck expected 200, got %d", resp.StatusCode)
	}
	select {
	case <-stuck:
	case <-time.After(5 * time.Second):
		t.Fatal("stuck delivery never started")
	}
	waitForStatus(t, st, "github", "delivery-stuck", store.WebhookStatusProcessing)
	if resp := post(t, url, "delivery-next", "pull_request", stuckPayload()); resp.StatusCode != http.StatusOK {
		t.Fatalf("next expected 200, got %d", resp.StatusCode)
	}
	if rec, _ := st.WebhookDeliveryRepo().Get(context.Background(), "github", "delivery-next"); rec != nil {
		t.Fatalf("queued delivery created a row while waiting: %+v", rec)
	}
	time.Sleep(200 * time.Millisecond)
	if n := srv.sweepStuckDispatchers(time.Now()); n != 1 {
		t.Fatalf("sweep handled %d dispatchers, want 1", n)
	}
	mu.Lock()
	joined := strings.Join(cards, "\n")
	mu.Unlock()
	if !strings.Contains(joined, "STUCK") || !strings.Contains(joined, "delivery-stuck") || !strings.Contains(joined, "fix/watchdog") {
		t.Fatalf("stuck alarm card missing repo/branch/delivery: %q", joined)
	}
	waitForStatus(t, st, "github", "delivery-next", store.WebhookStatusCompleted)
	close(releaseStuck)
	// The watchdog owns the run it declared stuck: the delivery ends in a
	// terminal receipt even when the agent turn returns afterwards.
	waitForStatus(t, st, "github", "delivery-stuck", store.WebhookStatusFailed)
}

func TestDispatcherPanicRebuildsWithoutOrphan(t *testing.T) {
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	}
	cfg := config.WebhookConfig{
		Bind:    "127.0.0.1:0",
		Policy:  testPolicy(),
		Runtime: testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{
			{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze", Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/tmp/repo", Mode: config.WorkspaceModeMutable}},
		}),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	calls := 0
	srv.SetWorkspaceResolver(workspaceResolverFunc(func(ctx context.Context, req WorkspaceRequest) (*WorkspaceLease, error) {
		calls++
		if calls == 1 {
			panic("boom-resolve")
		}
		return &WorkspaceLease{Path: "/tmp/repo/.worktree/branch", Mode: req.Mode}, nil
	}))
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	url := ts.URL + "/github?secret=s3cret"
	if resp := post(t, url, "delivery-panic", "pull_request", stuckPayload()); resp.StatusCode != http.StatusOK {
		t.Fatalf("panic delivery expected 200, got %d", resp.StatusCode)
	}
	if resp := post(t, url, "delivery-after", "pull_request", stuckPayload()); resp.StatusCode != http.StatusOK {
		t.Fatalf("follower expected 200, got %d", resp.StatusCode)
	}
	waitForStatus(t, st, "github", "delivery-panic", store.WebhookStatusFailed)
	receipt, _ := st.WebhookDeliveryRepo().Get(context.Background(), "github", "delivery-panic")
	if receipt == nil || !strings.Contains(strings.ToLower(receipt.ErrorSummary), "panic") {
		t.Fatalf("panic receipt missing panic summary: %+v", receipt)
	}
	waitForStatus(t, st, "github", "delivery-after", store.WebhookStatusCompleted)
	if n := srv.dispatcherCount(); n != 1 {
		t.Fatalf("dispatcher count = %d, want 1 live replacement", n)
	}
}

type workspaceResolverFunc func(ctx context.Context, req WorkspaceRequest) (*WorkspaceLease, error)

func (f workspaceResolverFunc) ResolveWorkspace(ctx context.Context, req WorkspaceRequest) (*WorkspaceLease, error) {
	return f(ctx, req)
}

func TestDispatcherQueuedDeliveryHasNoRow(t *testing.T) {
	release := make(chan struct{})
	started := make(chan string, 8)
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		started <- workCtx.DeliveryID
		if workCtx.DeliveryID == "delivery-first" {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	srv, st := newWatchdogServer(t, exec)
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	url := ts.URL + "/github?secret=s3cret"
	if resp := post(t, url, "delivery-first", "pull_request", stuckPayload()); resp.StatusCode != http.StatusOK {
		t.Fatalf("first expected 200, got %d", resp.StatusCode)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first never started heavy body")
	}
	if resp := post(t, url, "delivery-queued", "pull_request", stuckPayload()); resp.StatusCode != http.StatusOK {
		t.Fatalf("queued expected 200, got %d", resp.StatusCode)
	}
	time.Sleep(150 * time.Millisecond)
	if rec, _ := st.WebhookDeliveryRepo().Get(context.Background(), "github", "delivery-queued"); rec != nil {
		t.Fatalf("queued delivery created a row while waiting: %+v", rec)
	}
	select {
	case id := <-started:
		t.Fatalf("queued heavy body started while first blocked: %s", id)
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	waitForStatus(t, st, "github", "delivery-first", store.WebhookStatusCompleted)
	waitForStatus(t, st, "github", "delivery-queued", store.WebhookStatusCompleted)
}

func TestDispatcherQueueOnlyAlarmPreservesFIFO(t *testing.T) {
	var mu sync.Mutex
	var order []string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		mu.Lock()
		order = append(order, workCtx.DeliveryID)
		mu.Unlock()
		return nil
	}
	srv, _ := newWatchdogServer(t, exec)
	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	mkItem := func(id string) dispatchItem {
		return dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: id, eventType: "pull_request", payloadHash: "hash-" + id, lane: 3}
	}
	d := &dispatcher{server: srv, key: "key:fifo-alarm-test", lanes: makePRLanes(8), lastActivity: time.Now().Add(-10 * time.Minute)}
	for _, id := range []string{"fifo-1", "fifo-2", "fifo-3"} {
		d.lanes[3] <- mkItem(id)
	}
	snap := d.snapshot()
	if snap.queueLen != 3 || snap.hasActive {
		t.Fatalf("unexpected snapshot for queue-only alarm: %+v", snap)
	}
	srv.alarmStuckDispatcher(d, snap, time.Now(), time.Minute, livenessUnknown)
	var got []string
	for i := 0; i < 3; i++ {
		select {
		case it := <-d.lanes[3]:
			got = append(got, it.deliveryID)
		default:
			t.Fatalf("alarm consumed a queued item, got %v", got)
		}
	}
	want := []string{"fifo-1", "fifo-2", "fifo-3"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("queue order after alarm = %v, want %v", got, want)
	}
	for _, id := range want {
		d.lanes[3] <- mkItem(id)
	}
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()
	srv.alarmStuckDispatcher(d, d.snapshot(), time.Now(), time.Minute, livenessUnknown)
	srv.releaseStuckDispatcher(d)
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n == 3 {
			break
		}
		if time.Now().After(deadline) {
			mu.Lock()
			cur := append([]string(nil), order...)
			mu.Unlock()
			t.Fatalf("executor order incomplete: %v", cur)
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("executor start order = %v, want %v", order, want)
	}
}

func TestDispatcherRunPanicFailsActiveWithoutRequeue(t *testing.T) {
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	}
	srv, st := newWatchdogServer(t, exec)
	ctx := context.Background()
	repo := st.WebhookDeliveryRepo()
	if _, err := repo.Create(ctx, store.WebhookDelivery{Endpoint: "github", DeliveryID: "delivery-active", EventType: "pull_request", PayloadHash: "hash-active", Attempt: 1}); err != nil {
		t.Fatalf("create active receipt: %v", err)
	}
	receipt, _ := repo.Get(ctx, "github", "delivery-active")
	if ok, err := repo.Transition(ctx, receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusProcessing, ""); err != nil || !ok {
		t.Fatalf("claim active processing: ok=%v err=%v", ok, err)
	}
	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	active := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-active", eventType: "pull_request", payloadHash: "hash-active"}
	queued := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-queued", eventType: "pull_request", payloadHash: "hash-queued"}
	d := &dispatcher{server: srv, key: "key:panic-active-test", lanes: makePRLanes(8), lastActivity: time.Now()}
	d.lanes[3] <- queued
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()
	srv.rebuildAfterPanic(d, active, "boom-run")
	failed, _ := repo.Get(ctx, "github", "delivery-active")
	if failed == nil || failed.Status != store.WebhookStatusFailed {
		t.Fatalf("active receipt not failed: %+v", failed)
	}
	if !strings.Contains(strings.ToLower(failed.ErrorSummary), "panic") {
		t.Fatalf("active receipt missing panic summary: %+v", failed)
	}
	srv.dispatchMu.Lock()
	fresh := srv.dispatchers[d.key]
	srv.dispatchMu.Unlock()
	if fresh == nil {
		t.Fatal("expected live replacement dispatcher for queued item")
	}
	waitForStatus(t, st, "github", "delivery-queued", store.WebhookStatusCompleted)
	if rec, _ := repo.Get(ctx, "github", "delivery-active"); rec == nil || rec.Status != store.WebhookStatusFailed {
		t.Fatalf("active receipt changed after rebuild: %+v", rec)
	} else if rec.Attempt != 1 {
		t.Fatalf("active receipt re-queued and re-executed: %+v", rec)
	}
}

func TestDispatcherActivityRefreshesStamp(t *testing.T) {
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	}
	srv, _ := newWatchdogServer(t, exec)
	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	item := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-heartbeat", eventType: "pull_request", payloadHash: "hash-hb"}
	d := &dispatcher{server: srv, key: "key:heartbeat-stamp-test", lanes: makePRLanes(8)}
	d.setActive(item)
	before := d.snapshot().lastActive
	time.Sleep(20 * time.Millisecond)
	d.touch()
	after := d.snapshot().lastActive
	if !after.After(before) {
		t.Fatalf("touch did not refresh lastActivity: before=%v after=%v", before, after)
	}
	if got := d.snapshot().startedAt; got.IsZero() {
		t.Fatal("startedAt must be set after setActive")
	}

	var captured func()
	probe := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		captured = workCtx.Heartbeat
		if captured == nil {
			t.Error("executeDelivery did not expose Heartbeat on work context")
		} else {
			mark := d.snapshot().lastActive
			time.Sleep(20 * time.Millisecond)
			captured()
			if refreshed := d.snapshot().lastActive; !refreshed.After(mark) {
				t.Errorf("Heartbeat did not refresh dispatcher stamp")
			}
		}
		return nil
	}
	srv.executor = probe
	if err := srv.executeDelivery(ep, []byte(stuckPayload()), 0, "delivery-hb-probe", "pull_request", 1, nil, nil, d.touch); err != nil {
		t.Fatalf("executeDelivery: %v", err)
	}
	if captured == nil {
		t.Fatal("heartbeat was not captured by executor")
	}
}

func TestDispatcherSilentStillAlarms(t *testing.T) {
	var mu sync.Mutex
	var cards []string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		return nil
	}
	srv, _ := newWatchdogServer(t, exec)
	srv.SetDispatcherStuckAfter(5 * time.Minute)
	srv.SetDispatcherWatchdogInterval(time.Hour)
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		mu.Lock()
		cards = append(cards, text)
		mu.Unlock()
		return nil
	})
	srv.TaskRegistry().Upsert("anggasct/occa", "fix/watchdog", "root-1", "thread-1", TaskDispatched)
	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	item := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-frozen", eventType: "pull_request", payloadHash: "hash-frozen"}
	d := &dispatcher{server: srv, key: "key:silent-alarm-test", lanes: makePRLanes(8)}
	d.setActive(item)
	silentSince := time.Now().Add(-6 * time.Minute)
	d.mu.Lock()
	d.activeStartedAt = silentSince
	d.lastActivity = silentSince
	d.mu.Unlock()
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()
	if n := srv.sweepStuckDispatchers(time.Now()); n != 1 {
		t.Fatalf("frozen run sweep handled %d, want 1", n)
	}
	mu.Lock()
	joined := strings.Join(cards, "\n")
	mu.Unlock()
	if !strings.Contains(joined, "STUCK") || !strings.Contains(joined, "delivery-frozen") {
		t.Fatalf("silent run must still alarm LOUD, got %q", joined)
	}
}

func TestDispatcherCIWaitNoAlarm(t *testing.T) {
	var mu sync.Mutex
	var cards []string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		for i := 0; i < 28; i++ {
			if workCtx.Heartbeat != nil {
				workCtx.Heartbeat()
			}
			select {
			case <-time.After(time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	srv, _ := newWatchdogServer(t, exec)
	srv.SetDispatcherStuckAfter(5 * time.Minute)
	srv.SetDispatcherWatchdogInterval(time.Hour)
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		mu.Lock()
		cards = append(cards, text)
		mu.Unlock()
		return nil
	})
	srv.TaskRegistry().Upsert("anggasct/occa", "fix/watchdog", "root-1", "thread-1", TaskDispatched)
	ep := config.EndpointConfig{Name: "github", Platform: "telegram", ChannelID: "chat1", Admit: defaultCatchAllAdmit()}
	item := dispatchItem{ep: ep, body: []byte(stuckPayload()), deliveryID: "delivery-ci-wait", eventType: "pull_request", payloadHash: "hash-ci"}
	d := &dispatcher{server: srv, key: "key:ci-wait-test", lanes: makePRLanes(8)}
	d.setActive(item)
	d.mu.Lock()
	d.activeStartedAt = time.Now().Add(-11 * time.Minute)
	d.mu.Unlock()
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()
	if err := srv.executeDelivery(ep, []byte(stuckPayload()), 0, "delivery-ci-wait", "pull_request", 1, nil, nil, d.touch); err != nil {
		t.Fatalf("CI-wait execution: %v", err)
	}
	snap := d.snapshot()
	if time.Since(snap.lastActive) >= 5*time.Minute {
		t.Fatalf("CI poll loop must keep lastActivity fresh, age=%s", time.Since(snap.lastActive))
	}
	if n := srv.sweepStuckDispatchers(time.Now()); n != 0 {
		t.Fatalf("CI-waiting run past pickup threshold must not alarm, sweep=%d", n)
	}
	mu.Lock()
	joined := strings.Join(cards, "\n")
	mu.Unlock()
	if strings.Contains(joined, "STUCK") {
		t.Fatalf("CI-waiting run produced false-positive alarm: %q", joined)
	}
	if time.Since(snap.startedAt) < 5*time.Minute {
		t.Fatalf("test setup must keep pickup older than threshold, started=%s ago", time.Since(snap.startedAt))
	}
}
