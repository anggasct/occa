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
		srv.failDelivery(ep, 0, "del-1", "pull_request_review", envelope, "session create failed", &WebhookWorkContext{ThreadID: "thread-1"})
		candidate, err := st.SessionRepo().TakeoverCandidate(context.Background(), "discord", "chan-1", "thread-1")
		if err != nil {
			t.Fatalf("TakeoverCandidate: %v", err)
		}
		if candidate == nil || candidate.SessionID != "" {
			t.Fatalf("session-less failure must leave a seed-only mark, got %+v", candidate)
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

func TestFailDeliveryPostsContinuationBanner(t *testing.T) {
	type bannerSend struct {
		platform  string
		channelID string
		text      string
	}

	ep := takeoverTestEndpoint()
	envelope := WebhookEnvelope{"delivery_id": "del-1", "repository": "o/r", "pr_number": "7"}

	t.Run("discord thread", func(t *testing.T) {
		srv, _, _ := newTestServerFull(t, []config.EndpointConfig{takeoverTestEndpoint()})
		var sends []bannerSend
		srv.notifier = func(_ context.Context, platform, channelID, text string) error {
			sends = append(sends, bannerSend{platform, channelID, text})
			return nil
		}
		srv.failDelivery(ep, 0, "del-1", "pull_request_review", envelope, "agent exploded", &WebhookWorkContext{SessionID: "failed-sess", ThreadID: "thread-1"})
		if len(sends) != 1 {
			t.Fatalf("banner sends = %d, want exactly 1", len(sends))
		}
		if sends[0].platform != "discord" || sends[0].channelID != "thread-1" {
			t.Fatalf("banner target = %+v, want discord/thread-1", sends[0])
		}
		for _, want := range []string{"del-1", "agent exploded", "Chat di thread ini"} {
			if !strings.Contains(sends[0].text, want) {
				t.Fatalf("banner missing %q: %q", want, sends[0].text)
			}
		}
	})

	t.Run("telegram topic", func(t *testing.T) {
		srv, _, _ := newTestServerFull(t, []config.EndpointConfig{takeoverTestEndpoint()})
		var sends []bannerSend
		srv.notifier = func(_ context.Context, platform, channelID, text string) error {
			sends = append(sends, bannerSend{platform, channelID, text})
			return nil
		}
		tgEp := takeoverTestEndpoint()
		tgEp.Platform = "telegram"
		tgEp.ChannelID = "chan-1"
		srv.failDelivery(tgEp, 0, "del-1", "pull_request_review", envelope, "agent exploded", &WebhookWorkContext{SessionID: "failed-sess", ThreadID: "topic-1"})
		if len(sends) != 1 || sends[0].channelID != "chan-1:topic-1" {
			t.Fatalf("banner sends = %+v, want one to chan-1:topic-1", sends)
		}
	})

	t.Run("no thread sends nothing", func(t *testing.T) {
		srv, _, _ := newTestServerFull(t, []config.EndpointConfig{takeoverTestEndpoint()})
		var sends []bannerSend
		srv.notifier = func(_ context.Context, platform, channelID, text string) error {
			sends = append(sends, bannerSend{platform, channelID, text})
			return nil
		}
		srv.failDelivery(ep, 0, "del-1", "pull_request_review", envelope, "boom", &WebhookWorkContext{SessionID: "failed-sess"})
		if len(sends) != 0 {
			t.Fatalf("thread-less failure posted %d banners, want 0", len(sends))
		}
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
