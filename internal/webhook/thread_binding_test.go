package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func newThreadBindingServer(t *testing.T, exec Executor) (*Server, *store.SQLiteStore) {
	t.Helper()
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{prTestEndpoint("fix", "/fix", config.AutomationFull)}),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	srv.SetSessionStore(st.SessionRepo())
	srv.SetThreadStarter(func(context.Context, string, string, string, string) (string, error) {
		return "thread-new", nil
	})
	return srv, st
}

func TestRunUsesPersistedPRThreadAndWritesSessionBinding(t *testing.T) {
	var gotThread string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		gotThread = workCtx.ThreadID
		workCtx.ReportSession("ses-recovered")
		return nil
	}
	srv, st := newThreadBindingServer(t, exec)
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-saved", "root-saved"); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/fix?secret=s3cret", "del-binding", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "abc1234def5678"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	waitForReceiptDeliveryID(t, st, "del-binding", store.WebhookStatusCompleted)
	if gotThread != "thread-saved" {
		t.Fatalf("run thread = %q, want persisted thread-saved", gotThread)
	}
	channel, err := st.SessionRepo().ThreadChannel(ctx, "telegram", "thread-saved")
	if err != nil || channel != "chat1" {
		t.Fatalf("session binding = %q, %v; want chat1", channel, err)
	}
}

func TestRunWithoutResolvableThreadFailsLoudly(t *testing.T) {
	var calls int64
	var notices int64
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		return nil
	}
	srv, st := newThreadBindingServer(t, exec)
	srv.SetNotifier(func(_ context.Context, _, _, text string) error {
		if strings.Contains(text, "RUN REFUSED") {
			atomic.AddInt64(&notices, 1)
		}
		return nil
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/fix?secret=s3cret", "del-unbound", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "abc1234def5678"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceiptDeliveryID(t, st, "del-unbound", store.WebhookStatusFailed)
	if receipt.ErrorSummary != "run refused: PR thread binding unresolved (re-run /retry or re-trigger)" {
		t.Fatalf("unexpected failure summary: %q", receipt.ErrorSummary)
	}
	if atomic.LoadInt64(&calls) != 0 {
		t.Fatalf("unbound run executed %d times", calls)
	}
	if atomic.LoadInt64(&notices) != 1 {
		t.Fatalf("notices = %d, want exactly 1", notices)
	}
}
