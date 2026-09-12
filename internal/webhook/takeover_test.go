package webhook

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
)

func takeoverTestEndpoint() config.EndpointConfig {
	return config.EndpointConfig{
		Name:      "takeover-test",
		Platform:  "discord",
		ChannelID: "chan-1",
		Workflow:  "github_fix",
		ThreadID:  "thread-1",
		Prompt:    "do it",
	}
}

func TestFailDeliveryMarksTakeoverEligible(t *testing.T) {
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{takeoverTestEndpoint()})
	srv.SetSessionStore(st.SessionRepo())

	ep := takeoverTestEndpoint()
	envelope := WebhookEnvelope{"delivery_id": "del-1", "repository": "o/r", "pr_number": "7"}
	workCtx := &WebhookWorkContext{SessionID: "failed-sess", ThreadID: "thread-1", AgentPID: 4242}
	srv.failDelivery(ep, 0, "del-1", "pull_request_review", envelope, "agent exploded", workCtx)

	candidate, err := st.SessionRepo().TakeoverCandidate(context.Background(), "discord", "chan-1", "thread-1")
	if err != nil {
		t.Fatalf("TakeoverCandidate: %v", err)
	}
	if candidate == nil || candidate.SessionID != "failed-sess" || candidate.AgentPID != 4242 {
		t.Fatalf("candidate = %+v, want failed-sess/4242", candidate)
	}
	for _, want := range []string{"PR: #7", "FAILED", "del-1"} {
		if !strings.Contains(candidate.Seed, want) {
			t.Fatalf("seed missing %q: %q", want, candidate.Seed)
		}
	}
}

func TestFailDeliverySkipsMarkWithoutSessionOrThread(t *testing.T) {
	ep := takeoverTestEndpoint()
	envelope := WebhookEnvelope{"delivery_id": "del-1"}

	t.Run("no session", func(t *testing.T) {
		srv, _, st := newTestServerFull(t, []config.EndpointConfig{takeoverTestEndpoint()})
		srv.SetSessionStore(st.SessionRepo())
		srv.failDelivery(ep, 0, "del-1", "pull_request_review", envelope, "template render failed", &WebhookWorkContext{ThreadID: "thread-1"})
		if c, _ := st.SessionRepo().TakeoverCandidate(context.Background(), "discord", "chan-1", "thread-1"); c != nil {
			t.Fatalf("session-less failure must not mark: %+v", c)
		}
	})

	t.Run("no thread", func(t *testing.T) {
		srv, _, st := newTestServerFull(t, []config.EndpointConfig{takeoverTestEndpoint()})
		srv.SetSessionStore(st.SessionRepo())
		srv.failDelivery(ep, 0, "del-1", "pull_request_review", envelope, "boom", &WebhookWorkContext{SessionID: "failed-sess"})
		if c, _ := st.SessionRepo().TakeoverCandidate(context.Background(), "discord", "chan-1", ""); c != nil {
			t.Fatalf("thread-less failure must not mark: %+v", c)
		}
	})

	t.Run("no store", func(t *testing.T) {
		srv, _, _ := newTestServerFull(t, []config.EndpointConfig{takeoverTestEndpoint()})
		srv.failDelivery(ep, 0, "del-1", "pull_request_review", envelope, "boom", &WebhookWorkContext{SessionID: "failed-sess", ThreadID: "thread-1"})
	})
}

func TestExecuteDeliverySuccessClearsTakeover(t *testing.T) {
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{takeoverTestEndpoint()})
	srv.SetSessionStore(st.SessionRepo())
	srv.processingTimeout = time.Minute
	ctx := context.Background()

	if err := st.SessionRepo().MarkTakeoverEligible(ctx, "discord", "chan-1", "thread-1", "failed-old", 100, "seed"); err != nil {
		t.Fatalf("mark: %v", err)
	}

	ep := takeoverTestEndpoint()
	ep.Workflow = ""
	if err := srv.executeDelivery(ep, []byte(`{"x":1}`), 0, "del-9", "pull_request_review", 1, nil, nil); err != nil {
		t.Fatalf("executeDelivery: %v", err)
	}
	if c, _ := st.SessionRepo().TakeoverCandidate(ctx, "discord", "chan-1", "thread-1"); c != nil {
		t.Fatalf("completed delivery left a stale mark: %+v", c)
	}
}
