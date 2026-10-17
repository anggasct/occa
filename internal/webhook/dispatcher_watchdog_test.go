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

func waitForAnyReceipt(t *testing.T, st *store.SQLiteStore, endpoint, deliveryID string) *store.WebhookDelivery {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), endpoint, deliveryID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if receipt != nil {
			return receipt
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("delivery %s never got a receipt row", deliveryID)
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
	waitForAnyReceipt(t, st, "github", "delivery-next")
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
	waitForStatus(t, st, "github", "delivery-stuck", store.WebhookStatusCompleted)
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

func TestDispatcherEnqueueReceiptBeforeHeavyBody(t *testing.T) {
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
	receipt := waitForAnyReceipt(t, st, "github", "delivery-queued")
	if receipt.Status != store.WebhookStatusReceived && receipt.Status != store.WebhookStatusProcessing {
		t.Fatalf("queued receipt status = %s, want received/processing", receipt.Status)
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
