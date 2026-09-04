package webhook

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

// postDelivery drives one delivery through the server's HTTP entrypoint and
// waits for its receipt to reach the given status, returning the receipt.
func postDelivery(t *testing.T, srv *Server, st *store.SQLiteStore, path, secret, deliveryID string) *store.WebhookDelivery {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	resp := post(t, ts.URL+path+"?secret="+secret, deliveryID, "pull_request", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", resp.StatusCode)
	}
	return waitForReceipt(t, st, store.WebhookStatusFailed)
}

func TestTimeoutSummaryClassifiesStallBeforeFirstToken(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	if err := st.ChannelRepo().UpsertModel(context.Background(), "telegram", "chat1", "opencode/muse-spark-1.3-contributor-free@xhigh"); err != nil {
		t.Fatalf("upsert channel model: %v", err)
	}

	// The stream opened and the prompt went out, but no token ever arrived
	// before the budget died: the model stalled before its first token.
	exec.err = context.DeadlineExceeded
	sentAt := time.Now().Add(-27 * time.Minute)
	exec.progress = func() relay.TurnProgress {
		return relay.TurnProgress{PromptSentAt: sentAt}
	}

	audit := make(chan string, 2)
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		audit <- text
		return nil
	})

	receipt := postDelivery(t, srv, st, "/github", "s3cret", "delivery-stall-1")

	want := "timed out after 30m0s — provider stall before first token (27m0s, model opencode/muse-spark-1.3-contributor-free@xhigh)"
	if !strings.Contains(receipt.ErrorSummary, want) {
		t.Fatalf("summary %q missing classified stall reason %q", receipt.ErrorSummary, want)
	}
	if strings.Count(receipt.ErrorSummary, "\n") != 0 {
		t.Fatalf("summary must stay single-line, got %q", receipt.ErrorSummary)
	}

	select {
	case summary := <-audit:
		if !strings.Contains(summary, "Reason: "+want) {
			t.Fatalf("audit Reason line missing classified reason: %q", summary)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expected FAILED audit notification")
	}
}

func TestTimeoutSummaryClassifiesMidTurnStall(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	if err := st.ChannelRepo().UpsertModel(context.Background(), "telegram", "chat1", "opencode/muse-spark@xhigh"); err != nil {
		t.Fatalf("upsert channel model: %v", err)
	}

	exec.err = context.DeadlineExceeded
	exec.progress = func() relay.TurnProgress {
		return relay.TurnProgress{
			PromptSentAt: time.Now().Add(-30 * time.Minute),
			FirstDeltaAt: time.Now().Add(-25 * time.Minute),
			LastDeltaAt:  time.Now().Add(-5 * time.Minute),
			DeltaCount:   42,
		}
	}

	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error { return nil })

	receipt := postDelivery(t, srv, st, "/github", "s3cret", "delivery-stall-2")

	want := "timed out after 30m0s — provider stall mid-turn — no output for 5m0s (model opencode/muse-spark@xhigh)"
	if !strings.Contains(receipt.ErrorSummary, want) {
		t.Fatalf("summary %q missing mid-turn stall reason %q", receipt.ErrorSummary, want)
	}
}

func TestTimeoutSummaryClassifiesLongGeneration(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	if err := st.ChannelRepo().UpsertModel(context.Background(), "telegram", "chat1", "opencode/muse-spark@xhigh"); err != nil {
		t.Fatalf("upsert channel model: %v", err)
	}

	exec.err = context.DeadlineExceeded
	exec.progress = func() relay.TurnProgress {
		return relay.TurnProgress{
			PromptSentAt: time.Now().Add(-30 * time.Minute),
			FirstDeltaAt: time.Now().Add(-29 * time.Minute),
			LastDeltaAt:  time.Now().Add(-10 * time.Second),
			DeltaCount:   9999,
		}
	}

	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error { return nil })

	receipt := postDelivery(t, srv, st, "/github", "s3cret", "delivery-longgen-1")

	want := "timed out after 30m0s — work exceeded 30m0s (long generation), model opencode/muse-spark@xhigh"
	if !strings.Contains(receipt.ErrorSummary, want) {
		t.Fatalf("summary %q missing long-generation reason %q", receipt.ErrorSummary, want)
	}
}

func TestTimeoutSummaryWithoutModelOmitsModelName(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})

	exec.err = context.DeadlineExceeded
	exec.progress = func() relay.TurnProgress {
		return relay.TurnProgress{PromptSentAt: time.Now().Add(-27 * time.Minute)}
	}

	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error { return nil })

	receipt := postDelivery(t, srv, st, "/github", "s3cret", "delivery-stall-3")

	want := "timed out after 30m0s — provider stall before first token (27m0s, model unknown)"
	if !strings.Contains(receipt.ErrorSummary, want) {
		t.Fatalf("summary %q missing unknown-model stall reason %q", receipt.ErrorSummary, want)
	}
}

// A non-timeout executor failure must keep its own summary — no timeout
// classification may leak into it.
func TestNonTimeoutFailureKeepsExistingSummary(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})

	exec.err = errors.New("agent unreachable: dial timeout")
	exec.progress = func() relay.TurnProgress {
		return relay.TurnProgress{PromptSentAt: time.Now().Add(-27 * time.Minute)}
	}

	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error { return nil })

	receipt := postDelivery(t, srv, st, "/github", "s3cret", "delivery-agenterr-1")

	if !strings.Contains(receipt.ErrorSummary, "agent unreachable: dial timeout") {
		t.Fatalf("summary %q must carry the raw executor error", receipt.ErrorSummary)
	}
	if strings.Contains(receipt.ErrorSummary, "provider stall") || strings.Contains(receipt.ErrorSummary, "long generation") {
		t.Fatalf("non-timeout failure must not be classified: %q", receipt.ErrorSummary)
	}
}

func TestTimeoutSummaryNilWorkContext(t *testing.T) {
	if got := timeoutSummary(30*time.Minute, nil); got != "timed out after 30m0s" {
		t.Fatalf("timeoutSummary(nil) = %q, want canned budget line", got)
	}
}
