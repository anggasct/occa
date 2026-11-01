package webhook

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

// failReceiptLikeWatchdog moves the delivery out of processing the way the
// stuck watchdog does, so the dispatcher no longer owns it.
func failReceiptLikeWatchdog(t *testing.T, srv *Server, endpoint, deliveryID string) {
	t.Helper()
	receipt, err := srv.deliveries.Get(context.Background(), endpoint, deliveryID)
	if err != nil || receipt == nil {
		t.Fatalf("receipt lookup: %v %v", receipt, err)
	}
	if _, err := srv.deliveries.Transition(context.Background(), receipt.ID,
		[]store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted, store.WebhookStatusProcessing},
		store.WebhookStatusFailed, "stuck — session aborted"); err != nil {
		t.Fatalf("transition: %v", err)
	}
}

func markAllDispatchersTakenOver(srv *Server) {
	srv.dispatchMu.Lock()
	defer srv.dispatchMu.Unlock()
	for _, d := range srv.dispatchers {
		d.markWatchdogTookOver()
	}
}

// AC-01: the watchdog settled the delivery while the turn ran; the incomplete
// turn must not be re-run beside the fresh owner.
func TestSelfHealDroppedWhenWatchdogTookOver(t *testing.T) {
	logs := captureServerLogs(t)
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	exec.errOnAttempt = func(attempt int) error {
		if attempt == 1 {
			failReceiptLikeWatchdog(t, srv, "github", "delivery-1")
			markAllDispatchersTakenOver(srv)
		}
		return fmt.Errorf("relay: %w: empty output buffer at terminal event", relay.ErrWebhookResponseIncomplete)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{"action":"opened"}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", resp.StatusCode)
	}
	waitForLog(t, logs, "webhook: self-heal dropped, run taken over by watchdog")
	if rec := logs.find(t, "webhook: self-heal dropped, run taken over by watchdog"); rec.attrs["delivery_id"] != "delivery-1" {
		t.Fatalf("delivery_id = %v", rec.attrs["delivery_id"])
	}
	time.Sleep(100 * time.Millisecond)
	if n := exec.callCount(); n != 1 {
		t.Fatalf("executor calls = %d, want 1 (no self-heal after watchdog takeover)", n)
	}
	if len(logs.all("webhook: self-heal retry bump failed")) > 0 {
		t.Fatal("bump must not be attempted after a watchdog takeover")
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusFailed)
	if receipt.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", receipt.Attempt)
	}
}

// AC-02: no watchdog, but the row already left processing; the bump fails
// and the delivery is not re-run.
func TestSelfHealStopsWhenBumpFails(t *testing.T) {
	logs := captureServerLogs(t)
	srv, exec, _ := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	exec.errOnAttempt = func(attempt int) error {
		if attempt == 1 {
			failReceiptLikeWatchdog(t, srv, "github", "delivery-1")
		}
		return fmt.Errorf("relay: %w: empty output buffer at terminal event", relay.ErrWebhookResponseIncomplete)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if resp := post(t, ts.URL+"/github?secret=s3cret", "delivery-1", "pull_request", `{"action":"opened"}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", resp.StatusCode)
	}
	waitForLog(t, logs, "webhook: self-heal retry bump failed")
	time.Sleep(100 * time.Millisecond)
	if n := exec.callCount(); n != 1 {
		t.Fatalf("executor calls = %d, want 1 (no re-run without a processing row)", n)
	}
}
