package webhook

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func boolPtr(v bool) *bool {
	return &v
}

func silentEndpoint() config.EndpointConfig {
	return config.EndpointConfig{
		Name:       "github",
		Path:       "/github",
		Secret:     "secret",
		Platform:   "telegram",
		ChannelID:  "chat",
		Prompt:     "p",
		NotifySkip: boolPtr(false),
		Admit:      defaultCatchAllAdmit(),
	}
}

func TestSkippedWithOptOutStaysRecordedWithoutNotifier(t *testing.T) {
	ep := silentEndpoint()
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{ep})
	var mu sync.Mutex
	calls := 0
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return nil
	})
	id := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-optout")
	envelope := WebhookEnvelope{"repository": "o/r", "pr_number": "7", "event_type": "pull_request", "delivery_id": "del-optout"}
	srv.markSkipped(id, ep, envelope, "admitted no rule")
	mu.Lock()
	defer mu.Unlock()
	if calls != 0 {
		t.Fatalf("notifier calls = %d, want 0", calls)
	}
	receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-optout")
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v %v", receipt, err)
	}
	if receipt.Status != store.WebhookStatusSkipped {
		t.Fatalf("status = %s, want skipped", receipt.Status)
	}
	if receipt.ErrorSummary == "" {
		t.Fatal("skip reason missing from store record")
	}
}

func TestSkippedWithoutOptOutEmitsOneCard(t *testing.T) {
	ep := config.EndpointConfig{
		Name:      "github",
		Path:      "/github",
		Secret:    "secret",
		Platform:  "telegram",
		ChannelID: "chat",
		Prompt:    "p",
		Admit:     defaultCatchAllAdmit(),
	}
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{ep})
	var mu sync.Mutex
	var texts []string
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		mu.Lock()
		texts = append(texts, text)
		mu.Unlock()
		return nil
	})
	id := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-noisy")
	envelope := WebhookEnvelope{"repository": "o/r", "pr_number": "7", "event_type": "pull_request", "delivery_id": "del-noisy"}
	srv.markSkipped(id, ep, envelope, "admitted no rule")
	mu.Lock()
	defer mu.Unlock()
	if len(texts) != 1 {
		t.Fatalf("notifier calls = %d, want 1", len(texts))
	}
}

func TestSkipOptOutCoversAllFamilies(t *testing.T) {
	t.Run("admit reject", func(t *testing.T) {
		ep := silentEndpoint()
		ep.Admit = []config.AdmitRule{{Event: "ping"}}
		srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
		var mu sync.Mutex
		calls := 0
		srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return nil
		})
		id := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-admit")
		body := []byte(`{"action":"opened","repository":{"full_name":"o/r"},"pull_request":{"number":7}}`)
		if err := srv.executeDelivery(ep, body, id, "del-admit", "pull_request", 1, nil, nil, nil); err != nil {
			t.Fatalf("executeDelivery: %v", err)
		}
		if exec.callCount() != 0 {
			t.Fatalf("executor calls = %d, want 0", exec.callCount())
		}
		receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-admit")
		if err != nil || receipt == nil || receipt.Status != store.WebhookStatusSkipped {
			t.Fatalf("receipt = %+v err=%v, want skipped", receipt, err)
		}
		mu.Lock()
		defer mu.Unlock()
		if calls != 0 {
			t.Fatalf("notifier calls = %d, want 0", calls)
		}
	})

	t.Run("review duplicate", func(t *testing.T) {
		ep := silentEndpoint()
		ep.Admit = defaultCatchAllAdmit()
		srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
		var mu sync.Mutex
		calls := 0
		srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return nil
		})
		body := reviewEventBody(t, "changes_requested", "**Verdict:** REQUEST_CHANGES", "5932515abc123")
		id1 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-dup-1")
		if err := srv.executeDelivery(ep, body, id1, "del-dup-1", "pull_request_review", 1, nil, nil, nil); err != nil {
			t.Fatalf("first executeDelivery: %v", err)
		}
		if exec.callCount() != 1 {
			t.Fatalf("first delivery calls = %d, want 1", exec.callCount())
		}
		mu.Lock()
		calls = 0
		mu.Unlock()
		id2 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-dup-2")
		if err := srv.executeDelivery(ep, body, id2, "del-dup-2", "pull_request_review", 1, nil, nil, nil); err != nil {
			t.Fatalf("duplicate executeDelivery: %v", err)
		}
		receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-dup-2")
		if err != nil || receipt == nil || receipt.Status != store.WebhookStatusSkipped {
			t.Fatalf("duplicate receipt = %+v err=%v, want skipped", receipt, err)
		}
		mu.Lock()
		defer mu.Unlock()
		if calls != 0 {
			t.Fatalf("duplicate notifier calls = %d, want 0", calls)
		}
	})

	t.Run("trigger rate cap", func(t *testing.T) {
		ep := silentEndpoint()
		ep.Workflow = "review"
		ep.CommentTrigger = []string{"please re-review"}
		ep.Admit = []config.AdmitRule{
			{Event: "issue_comment", Actions: []string{"created"}, Require: []string{"comment_trigger", "pr_open"}},
		}
		srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
		var mu sync.Mutex
		calls := 0
		srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return nil
		})
		body := issueCommentEventBody(t, 7, "please re-review this PR")
		id := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-cap")
		if err := srv.executeDelivery(ep, body, id, "del-cap", "issue_comment", 1, nil, nil, nil); err != nil {
			t.Fatalf("executeDelivery: %v", err)
		}
		if exec.callCount() != 0 {
			t.Fatalf("executor calls = %d, want 0", exec.callCount())
		}
		receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-cap")
		if err != nil || receipt == nil || receipt.Status != store.WebhookStatusSkipped {
			t.Fatalf("receipt = %+v err=%v, want skipped", receipt, err)
		}
		mu.Lock()
		defer mu.Unlock()
		if calls != 0 {
			t.Fatalf("notifier calls = %d, want 0", calls)
		}
	})

	t.Run("configured event skip", func(t *testing.T) {
		ep := silentEndpoint()
		ep.SkipEvents = []string{"pull_request"}
		srv, _, st := newTestServerFull(t, []config.EndpointConfig{ep})
		var mu sync.Mutex
		calls := 0
		srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return nil
		})
		item := dispatchItem{
			ep:          ep,
			body:        []byte(`{"action":"opened"}`),
			deliveryID:  "del-configured",
			eventType:   "pull_request",
			payloadHash: "hash-configured-1",
		}
		if got := srv.beginExecution(item); got != nil {
			t.Fatalf("beginExecution = %+v, want nil for skipped delivery", got)
		}
		receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-configured")
		if err != nil || receipt == nil || receipt.Status != store.WebhookStatusSkipped {
			t.Fatalf("receipt = %+v err=%v, want skipped", receipt, err)
		}
		mu.Lock()
		defer mu.Unlock()
		if calls != 0 {
			t.Fatalf("notifier calls = %d, want 0", calls)
		}
	})
}

func TestTerminalNotificationsIgnoreOptOut(t *testing.T) {
	ep := silentEndpoint()
	srv, _, _ := newTestServerFull(t, []config.EndpointConfig{ep})
	for _, status := range []string{"COMPLETED", "FAILED"} {
		var mu sync.Mutex
		calls := 0
		srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return nil
		})
		srv.emitAudit(context.Background(), ep, WebhookEnvelope{"repository": "o/r", "event_type": "pull_request", "delivery_id": "del-1"}, status, "", "")
		mu.Lock()
		got := calls
		mu.Unlock()
		if got != 1 {
			t.Fatalf("%s notifier calls = %d, want 1", status, got)
		}
		time.Sleep(time.Millisecond)
	}
}
