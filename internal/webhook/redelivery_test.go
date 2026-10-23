package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func TestRedeliveryReopensSkippedAndRejected(t *testing.T) {
	for _, status := range []store.WebhookStatus{store.WebhookStatusSkipped, store.WebhookStatusRejected} {
		t.Run(string(status), func(t *testing.T) {
			srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
				{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
			})
			mux := http.NewServeMux()
			mux.HandleFunc("/", srv.handleRequest)
			ts := httptest.NewServer(mux)
			defer ts.Close()

			created, err := st.WebhookDeliveryRepo().Create(context.Background(), store.WebhookDelivery{
				Endpoint:   "github",
				DeliveryID: "del-reopen",
				EventType:  "pull_request",
				Status:     status,
			})
			if err != nil || !created {
				t.Fatalf("create %s receipt: %v %v", status, created, err)
			}

			resp := post(t, ts.URL+"/github?secret=s3cret", "del-reopen", "pull_request", `{"action":"opened","number":42}`)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			receipt := waitForReceiptDeliveryID(t, st, "del-reopen", store.WebhookStatusCompleted)
			if receipt.Attempt != 2 {
				t.Fatalf("attempt = %d, want 2", receipt.Attempt)
			}
			if exec.callCount() != 1 {
				t.Fatalf("executor calls = %d, want 1", exec.callCount())
			}
		})
	}
}
