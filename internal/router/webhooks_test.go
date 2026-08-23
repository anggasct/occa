package router

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

func TestHandleWebhooksShowsStatusAndFailureSummary(t *testing.T) {
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	if _, err := st.WebhookDeliveryRepo().Create(context.Background(), store.WebhookDelivery{
		Endpoint:     "github-review",
		DeliveryID:   "delivery-1",
		EventType:    "pull_request",
		PayloadHash:  "hash",
		Status:       store.WebhookStatusFailed,
		ErrorSummary: "agent unreachable [redacted]",
	}); err != nil {
		t.Fatalf("create delivery: %v", err)
	}

	r := &Router{store: st}
	got, err := r.handleWebhooks(context.Background(), channel.IncomingMessage{}, "")
	if err != nil {
		t.Fatalf("handle webhooks: %v", err)
	}
	for _, want := range []string{"github-review", "pull_request", "failed", "agent unreachable [redacted]"} {
		if !strings.Contains(got, want) {
			t.Fatalf("diagnostics %q missing %q", got, want)
		}
	}
}
