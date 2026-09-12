package webhook

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
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
		for _, want := range []string{"del-1", "agent exploded", "Continue in this thread"} {
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

func newTakeoverServer(t *testing.T, exec Executor, st *store.SQLiteStore) *Server {
	t.Helper()
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Endpoints: []config.EndpointConfig{takeoverTestEndpoint()},
	}
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetSessionStore(st.SessionRepo())
	srv.processingTimeout = time.Minute
	return srv
}

func TestExecuteDeliveryCompletedMarksTakeover(t *testing.T) {
	ctx := context.Background()

	t.Run("completed delivery marks its session", func(t *testing.T) {
		st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer func() { _ = st.Close() }()
		exec := func(context.Context, string, string, string, *WebhookWorkContext) error { return nil }
		srv := newTakeoverServer(t, exec, st)
		srv.executor = func(_ context.Context, _ string, _ string, _ string, workCtx *WebhookWorkContext) error {
			workCtx.SessionID = "completed-sess"
			workCtx.AgentPID = 4242
			return nil
		}
		ep := takeoverTestEndpoint()
		ep.Workflow = ""
		if err := srv.executeDelivery(ep, []byte(`{"x":1}`), 0, "del-9", "pull_request_review", 1, nil, nil); err != nil {
			t.Fatalf("executeDelivery: %v", err)
		}
		candidate, err := st.SessionRepo().TakeoverCandidate(ctx, "discord", "chan-1", "thread-1")
		if err != nil {
			t.Fatalf("TakeoverCandidate: %v", err)
		}
		if candidate == nil || candidate.SessionID != "completed-sess" || candidate.AgentPID != 4242 {
			t.Fatalf("candidate = %+v, want completed-sess/4242", candidate)
		}
		for _, want := range []string{"COMPLETED", "del-9"} {
			if !strings.Contains(candidate.Seed, want) {
				t.Fatalf("seed missing %q: %q", want, candidate.Seed)
			}
		}
	})

	t.Run("completed attempt replaces a stale failed mark", func(t *testing.T) {
		st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer func() { _ = st.Close() }()
		exec := func(context.Context, string, string, string, *WebhookWorkContext) error { return nil }
		srv := newTakeoverServer(t, exec, st)
		srv.executor = func(_ context.Context, _ string, _ string, _ string, workCtx *WebhookWorkContext) error {
			workCtx.SessionID = "retry-completed-sess"
			workCtx.AgentPID = 500
			return nil
		}
		if err := st.SessionRepo().MarkTakeoverEligible(ctx, "discord", "chan-1", "thread-1", "failed-old", 100, "seed-old"); err != nil {
			t.Fatalf("seed stale mark: %v", err)
		}

		ep := takeoverTestEndpoint()
		ep.Workflow = ""
		if err := srv.executeDelivery(ep, []byte(`{"x":1}`), 0, "del-9", "pull_request_review", 2, nil, nil); err != nil {
			t.Fatalf("executeDelivery: %v", err)
		}
		candidate, _ := st.SessionRepo().TakeoverCandidate(ctx, "discord", "chan-1", "thread-1")
		if candidate == nil || candidate.SessionID != "retry-completed-sess" {
			t.Fatalf("candidate = %+v, want the latest terminal attempt retry-completed-sess", candidate)
		}
	})
}

func TestFailDeliveryLinksRootCard(t *testing.T) {
	ep := takeoverTestEndpoint()
	envelope := WebhookEnvelope{"delivery_id": "del-1", "repository": "o/r", "pr_number": "7"}

	t.Run("discord thread", func(t *testing.T) {
		srv, _, st := newTestServerFull(t, []config.EndpointConfig{takeoverTestEndpoint()})
		srv.SetSessionStore(st.SessionRepo())
		workCtx := &WebhookWorkContext{SessionID: "failed-sess", ThreadID: "thread-1", RootMessageID: "root-1"}
		srv.failDelivery(ep, 0, "del-1", "pull_request_review", envelope, "agent exploded", workCtx)

		root, err := st.SessionRepo().ThreadRoot(context.Background(), "discord", "chan-1", "thread-1")
		if err != nil {
			t.Fatalf("ThreadRoot: %v", err)
		}
		if root == nil {
			t.Fatal("root card link missing after failed delivery")
		}
		if root.MessageID != "root-1" || root.Channel != "chan-1" {
			t.Fatalf("root target = %s/%s, want root-1/chan-1", root.MessageID, root.Channel)
		}
		for _, want := range []string{"⚠️ FAILED", "Reason: agent exploded", "Delivery: del-1", "➡️ Details in thread: <#thread-1>"} {
			if !strings.Contains(root.Card, want) {
				t.Fatalf("stored card missing %q: %q", want, root.Card)
			}
		}
	})

	t.Run("telegram topic target", func(t *testing.T) {
		srv, _, st := newTestServerFull(t, []config.EndpointConfig{takeoverTestEndpoint()})
		srv.SetSessionStore(st.SessionRepo())
		tgEp := takeoverTestEndpoint()
		tgEp.Platform = "telegram"
		workCtx := &WebhookWorkContext{SessionID: "failed-sess", ThreadID: "topic-1", RootMessageID: "root-1"}
		srv.failDelivery(tgEp, 0, "del-1", "pull_request_review", envelope, "boom", workCtx)

		root, err := st.SessionRepo().ThreadRoot(context.Background(), "telegram", "chan-1", "topic-1")
		if err != nil {
			t.Fatalf("ThreadRoot: %v", err)
		}
		if root == nil || root.Channel != "chan-1:topic-1" {
			t.Fatalf("root = %+v, want chan-1:topic-1 target", root)
		}
	})

	t.Run("no root message", func(t *testing.T) {
		srv, _, st := newTestServerFull(t, []config.EndpointConfig{takeoverTestEndpoint()})
		srv.SetSessionStore(st.SessionRepo())
		workCtx := &WebhookWorkContext{SessionID: "failed-sess", ThreadID: "thread-1"}
		srv.failDelivery(ep, 0, "del-1", "pull_request_review", envelope, "boom", workCtx)

		if root, _ := st.SessionRepo().ThreadRoot(context.Background(), "discord", "chan-1", "thread-1"); root != nil {
			t.Fatalf("rootless delivery linked a root card: %+v", root)
		}
	})
}

func TestExecuteDeliveryCompletedLinksRootCard(t *testing.T) {
	ctx := context.Background()
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	exec := func(context.Context, string, string, string, *WebhookWorkContext) error { return nil }
	srv := newTakeoverServer(t, exec, st)
	srv.executor = func(_ context.Context, _ string, _ string, _ string, workCtx *WebhookWorkContext) error {
		workCtx.SessionID = "completed-sess"
		workCtx.AgentPID = 4242
		workCtx.RootMessageID = "root-9"
		return nil
	}
	ep := takeoverTestEndpoint()
	ep.Workflow = ""
	if err := srv.executeDelivery(ep, []byte(`{"x":1}`), 0, "del-9", "pull_request_review", 1, nil, nil); err != nil {
		t.Fatalf("executeDelivery: %v", err)
	}

	root, err := st.SessionRepo().ThreadRoot(ctx, "discord", "chan-1", "thread-1")
	if err != nil {
		t.Fatalf("ThreadRoot: %v", err)
	}
	if root == nil {
		t.Fatal("completed delivery did not link its root card")
	}
	if root.MessageID != "root-9" || root.Channel != "chan-1" {
		t.Fatalf("root target = %s/%s, want root-9/chan-1", root.MessageID, root.Channel)
	}
	if !strings.Contains(root.Card, "✅ COMPLETED") || !strings.Contains(root.Card, "Delivery: del-9") {
		t.Fatalf("stored card missing terminal audit lines: %q", root.Card)
	}
	if strings.Contains(root.Card, "Follow-up:") {
		t.Fatalf("stored card must not carry a follow-up line: %q", root.Card)
	}
}

func TestCompletedTerminalCardCarriesHint(t *testing.T) {
	envelope := WebhookEnvelope{"delivery_id": "del-1", "repository": "o/r", "pr_number": "7"}

	completed := FormatTerminalCard(envelope, "github_fix", "COMPLETED", "", "thread-1", "discord", 90*time.Second)
	if strings.Count(completed, "Continue in this thread to keep full context.") != 1 {
		t.Fatalf("completed card must carry exactly one continuation hint: %q", completed)
	}

	withReason := FormatTerminalCard(envelope, "github_fix", "COMPLETED", "custom reason", "", "telegram", 0)
	if !strings.Contains(withReason, "Reason: custom reason") || strings.Count(withReason, "Continue in this thread") != 1 {
		t.Fatalf("completed card with reason malformed: %q", withReason)
	}

	failed := FormatTerminalCard(envelope, "github_fix", "FAILED", "boom", "thread-1", "discord", 30*time.Second)
	if strings.Contains(failed, "Continue in this thread to keep full context.") {
		t.Fatalf("failed card must not gain the hint line: %q", failed)
	}
	if !strings.Contains(failed, "Reason: boom") {
		t.Fatalf("failed card lost its reason: %q", failed)
	}
}
