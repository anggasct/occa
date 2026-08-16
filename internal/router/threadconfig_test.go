package router

import (
	"context"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

func newThreadTestRouter() (*Router, *fakeRelayClient) {
	r, client, _, overrides := newTestRouterWithAccess()
	overrides.overrides["discord:parent:user1"] = &store.UserOverride{
		ChannelID: "parent", Platform: "discord", UserID: "user1", Role: "admin",
	}
	return r, client
}

func ownedThreadMsg(threadID, text string, reply *fakeReplyCtx) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:        "discord",
		ChannelID:       "parent",
		ParentChannelID: "parent",
		ThreadID:        threadID,
		UserID:          "user1",
		Text:            text,
		IsThread:        true,
		IsMention:       true,
		ReplyCtx:        reply,
	}
}

func threadConfigsOf(r *Router) *fakeThreadConfigRepo {
	return r.store.(*fakeStore).ThreadConfigRepo().(*fakeThreadConfigRepo)
}

func channelOf(r *Router, platform, channelID string) *store.Channel {
	return r.store.(*fakeStore).channelRepo.channels[platform+":"+channelID]
}

func TestSnapshotAtSessionActivation(t *testing.T) {
	r, client := newThreadTestRouter()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["discord:parent"] = &store.Channel{
		ChannelID: "parent", Platform: "discord", Workdir: "/repo", Model: "openai/gpt-4o", ListenMode: "all",
	}

	if err := r.Route(context.Background(), ownedThreadMsg("thread-1", "hello", &fakeReplyCtx{})); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)

	tc := threadConfigsOf(r).configs["discord:parent:thread-1"]
	if tc == nil {
		t.Fatal("thread config not materialized at session activation")
	}
	if tc.Workdir != "/repo" || tc.Model != "openai/gpt-4o" || tc.ListenMode != "all" {
		t.Fatalf("snapshot = %+v, want channel effective values", tc)
	}
	if ch := channelOf(r, "discord", "parent"); ch == nil || ch.Workdir != "/repo" || ch.Model != "openai/gpt-4o" || ch.ListenMode != "all" {
		t.Fatalf("channel row must stay untouched, got %+v", ch)
	}
}

func TestEnsureThreadConfigIdempotent(t *testing.T) {
	r, _ := newThreadTestRouter()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["discord:parent"] = &store.Channel{
		ChannelID: "parent", Platform: "discord", Workdir: "/repo", ListenMode: "all",
	}
	threadConfigsOf(r).configs["discord:parent:thread-1"] = &store.ThreadConfig{
		Platform: "discord", ChannelID: "parent", ThreadID: "thread-1", Workdir: "/custom", ListenMode: "mention",
	}

	if err := r.Route(context.Background(), ownedThreadMsg("thread-1", "/dir", &fakeReplyCtx{})); err != nil {
		t.Fatalf("Route: %v", err)
	}
	tc := threadConfigsOf(r).configs["discord:parent:thread-1"]
	if tc.Workdir != "/custom" || tc.ListenMode != "mention" {
		t.Fatalf("existing thread row overwritten by ensureThreadConfig: %+v", tc)
	}
}

func TestEffectiveWorkdirIsolation(t *testing.T) {
	r, _ := newThreadTestRouter()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["discord:parent"] = &store.Channel{
		ChannelID: "parent", Platform: "discord", Workdir: "/channel-wd",
	}

	threadWithRow := ownedThreadMsg("thread-1", "", &fakeReplyCtx{})
	threadConfigsOf(r).configs["discord:parent:thread-1"] = &store.ThreadConfig{
		Platform: "discord", ChannelID: "parent", ThreadID: "thread-1", Workdir: "/thread-wd",
	}
	if got := r.effectiveWorkdir(context.Background(), threadWithRow); got != "/thread-wd" {
		t.Fatalf("thread workdir = %q, want /thread-wd", got)
	}

	st.channelRepo.channels["discord:parent"].Workdir = "/channel-wd-2"
	if got := r.effectiveWorkdir(context.Background(), threadWithRow); got != "/thread-wd" {
		t.Fatalf("thread workdir after channel change = %q, want /thread-wd", got)
	}

	threadNoRow := ownedThreadMsg("thread-2", "", &fakeReplyCtx{})
	if got := r.effectiveWorkdir(context.Background(), threadNoRow); got != "/channel-wd-2" {
		t.Fatalf("no-row thread workdir = %q, want channel /channel-wd-2", got)
	}

	threadConfigsOf(r).configs["discord:parent:thread-2"] = &store.ThreadConfig{
		Platform: "discord", ChannelID: "parent", ThreadID: "thread-2", Workdir: "",
	}
	if got := r.effectiveWorkdir(context.Background(), threadNoRow); got != "/default-workdir" {
		t.Fatalf("empty-row thread workdir = %q, want agent default", got)
	}
}

func TestEffectiveListenModeIsolation(t *testing.T) {
	r, _ := newThreadTestRouter()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["discord:parent"] = &store.Channel{
		ChannelID: "parent", Platform: "discord", ListenMode: "all",
	}

	threadWithRow := ownedThreadMsg("thread-1", "", &fakeReplyCtx{})
	threadConfigsOf(r).configs["discord:parent:thread-1"] = &store.ThreadConfig{
		Platform: "discord", ChannelID: "parent", ThreadID: "thread-1", ListenMode: "mention",
	}
	if got := r.effectiveListenMode(context.Background(), threadWithRow); got != "mention" {
		t.Fatalf("thread listen mode = %q, want mention", got)
	}

	st.channelRepo.channels["discord:parent"].ListenMode = "thread"
	if got := r.effectiveListenMode(context.Background(), threadWithRow); got != "mention" {
		t.Fatalf("thread listen mode after channel change = %q, want mention", got)
	}

	threadNoRow := ownedThreadMsg("thread-2", "", &fakeReplyCtx{})
	if got := r.effectiveListenMode(context.Background(), threadNoRow); got != "thread" {
		t.Fatalf("no-row thread listen mode = %q, want channel thread", got)
	}

	threadConfigsOf(r).configs["discord:parent:thread-2"] = &store.ThreadConfig{
		Platform: "discord", ChannelID: "parent", ThreadID: "thread-2", ListenMode: "",
	}
	if got := r.effectiveListenMode(context.Background(), threadNoRow); got != "mention" {
		t.Fatalf("empty-row thread listen mode = %q, want mention default", got)
	}
}

func TestSetDirInThreadIsThreadScoped(t *testing.T) {
	r, _ := newThreadTestRouter()
	dir := t.TempDir()

	if err := r.Route(context.Background(), ownedThreadMsg("thread-1", "/dir "+dir, &fakeReplyCtx{})); err != nil {
		t.Fatalf("Route: %v", err)
	}
	tc := threadConfigsOf(r).configs["discord:parent:thread-1"]
	if tc == nil || tc.Workdir != dir {
		t.Fatalf("thread workdir = %+v, want %q", tc, dir)
	}
	if ch := channelOf(r, "discord", "parent"); ch != nil && ch.Workdir != "" {
		t.Fatalf("parent channel workdir changed: %+v", ch)
	}
}

func TestBareModelInThreadWritesThreadConfig(t *testing.T) {
	r, client := newThreadTestRouter()
	client.providers = modelTestProviders()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["discord:parent"] = &store.Channel{
		ChannelID: "parent", Platform: "discord", Model: "zai-coding-plan/glm-5.2", ListenMode: "mention",
	}

	reply := &fakeReplyCtx{}
	if err := r.Route(context.Background(), ownedThreadMsg("thread-1", "/model openai/gpt-4o", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !strings.Contains(reply.sends[0], "Thread model set: openai/gpt-4o") {
		t.Fatalf("unexpected reply: %q", reply.sends[0])
	}
	tc := threadConfigsOf(r).configs["discord:parent:thread-1"]
	if tc == nil || tc.Model != "openai/gpt-4o" {
		t.Fatalf("thread model = %+v, want openai/gpt-4o", tc)
	}
	if ch := channelOf(r, "discord", "parent"); ch.Model != "zai-coding-plan/glm-5.2" {
		t.Fatalf("channel model changed: %+v", ch)
	}
	if o := st.overrideRepo.overrides["discord:parent:user1"]; o != nil && o.Model != "" {
		t.Fatalf("personal override changed: %+v", o)
	}
}

func TestBareModelInChannelAdminWritesChannel(t *testing.T) {
	t.Run("admin writes channel model", func(t *testing.T) {
		r, client, reply, overrides := newTestRouterWithAccess()
		client.providers = modelTestProviders()
		overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
			ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
		}
		st := r.store.(*fakeStore)
		st.channelRepo.channels["telegram:chat1"] = &store.Channel{
			ChannelID: "chat1", Platform: "telegram", ListenMode: "all", Workdir: "/repo",
		}

		if err := r.Route(context.Background(), msg("/model openai/gpt-4o", reply)); err != nil {
			t.Fatalf("Route: %v", err)
		}
		if !strings.Contains(reply.sends[0], "Channel model set: openai/gpt-4o") {
			t.Fatalf("unexpected response: %q", reply.sends[0])
		}
		ch := channelOf(r, "telegram", "chat1")
		if ch.Model != "openai/gpt-4o" || ch.ListenMode != "all" || ch.Workdir != "/repo" {
			t.Fatalf("unexpected channel after model set: %+v", ch)
		}
	})

	t.Run("non-admin writes personal override", func(t *testing.T) {
		r, client, reply, overrides := newTestRouterWithAccess()
		client.providers = modelTestProviders()
		overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
			ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
		}

		if err := r.Route(context.Background(), msg("/model openai/gpt-4o", reply)); err != nil {
			t.Fatalf("Route: %v", err)
		}
		if !strings.Contains(reply.sends[0], "Personal model set: openai/gpt-4o") {
			t.Fatalf("unexpected response: %q", reply.sends[0])
		}
		o := overrides.overrides["telegram:chat1:user1"]
		if o.Model != "openai/gpt-4o" {
			t.Fatalf("non-admin personal model = %q, want openai/gpt-4o", o.Model)
		}
		if ch := channelOf(r, "telegram", "chat1"); ch != nil && ch.Model != "" {
			t.Fatalf("non-admin changed channel model: %+v", ch)
		}
	})
}

func TestModelDefaultClearsCurrentLocation(t *testing.T) {
	t.Run("thread", func(t *testing.T) {
		r, _ := newThreadTestRouter()
		threadConfigsOf(r).configs["discord:parent:thread-1"] = &store.ThreadConfig{
			Platform: "discord", ChannelID: "parent", ThreadID: "thread-1", Model: "openai/gpt-4o",
		}
		reply := &fakeReplyCtx{}
		if err := r.Route(context.Background(), ownedThreadMsg("thread-1", "/model default", reply)); err != nil {
			t.Fatalf("Route: %v", err)
		}
		if !strings.Contains(reply.sends[0], "Thread model cleared") {
			t.Fatalf("unexpected response: %q", reply.sends[0])
		}
		tc := threadConfigsOf(r).configs["discord:parent:thread-1"]
		if tc.Model != "" {
			t.Fatalf("thread model after default = %q, want empty", tc.Model)
		}
	})

	t.Run("channel admin", func(t *testing.T) {
		r, _, reply, overrides := newTestRouterWithAccess()
		overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
			ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
		}
		r.store.(*fakeStore).channelRepo.channels["telegram:chat1"] = &store.Channel{
			ChannelID: "chat1", Platform: "telegram", Model: "openai/gpt-4o", ListenMode: "mention",
		}
		if err := r.Route(context.Background(), msg("/model default", reply)); err != nil {
			t.Fatalf("Route: %v", err)
		}
		if !strings.Contains(reply.sends[0], "Channel model cleared") {
			t.Fatalf("unexpected response: %q", reply.sends[0])
		}
		ch := channelOf(r, "telegram", "chat1")
		if ch.Model != "" {
			t.Fatalf("channel model after default = %q, want empty", ch.Model)
		}
	})

	t.Run("personal non-admin", func(t *testing.T) {
		r, _, reply, overrides := newTestRouterWithAccess()
		overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
			ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow", Model: "openai/gpt-4o",
		}
		if err := r.Route(context.Background(), msg("/model default", reply)); err != nil {
			t.Fatalf("Route: %v", err)
		}
		if !strings.Contains(reply.sends[0], "Personal model cleared") {
			t.Fatalf("unexpected response: %q", reply.sends[0])
		}
		o := overrides.overrides["telegram:chat1:user1"]
		if o.Model != "" {
			t.Fatalf("personal model after default = %q, want empty", o.Model)
		}
	})
}

func TestOldSessionAndChannelKeywordsRejected(t *testing.T) {
	for _, tt := range []struct {
		name string
		text string
	}{
		{name: "channel ref", text: "/model channel openai/gpt-4o"},
		{name: "session ref", text: "/model session openai/gpt-4o"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, client := newThreadTestRouter()
			client.providers = modelTestProviders()
			reply := &fakeReplyCtx{}
			if err := r.Route(context.Background(), ownedThreadMsg("thread-1", tt.text, reply)); err != nil {
				t.Fatalf("Route: %v", err)
			}
			if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "Usage: /model") {
				t.Fatalf("expected usage error, got: %v", reply.sends)
			}
			if tc := threadConfigsOf(r).configs["discord:parent:thread-1"]; tc != nil && tc.Model != "" {
				t.Fatalf("keyword form wrote a thread model: %+v", tc)
			}
		})
	}
}

func TestEffectiveModelIsolation(t *testing.T) {
	r, client := newThreadTestRouter()
	client.providers = modelTestProviders()
	ctx := context.Background()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["discord:parent"] = &store.Channel{
		ChannelID: "parent", Platform: "discord", Model: "anthropic/claude-3", ListenMode: "mention",
	}

	t.Run("thread row exists beats channel", func(t *testing.T) {
		threadConfigsOf(r).configs["discord:parent:thread-1"] = &store.ThreadConfig{
			Platform: "discord", ChannelID: "parent", ThreadID: "thread-1", Model: "openai/gpt-4o",
		}
		model, err := r.effectiveModel(ctx, ownedThreadMsg("thread-1", "", &fakeReplyCtx{}))
		if err != nil {
			t.Fatalf("effectiveModel: %v", err)
		}
		if model == nil || model.ProviderID != "openai" || model.ID != "gpt-4o" {
			t.Fatalf("thread model = %+v, want openai/gpt-4o", model)
		}
	})

	t.Run("empty thread row means agent default", func(t *testing.T) {
		threadConfigsOf(r).configs["discord:parent:thread-2"] = &store.ThreadConfig{
			Platform: "discord", ChannelID: "parent", ThreadID: "thread-2", Model: "",
		}
		model, err := r.effectiveModel(ctx, ownedThreadMsg("thread-2", "", &fakeReplyCtx{}))
		if err != nil {
			t.Fatalf("effectiveModel: %v", err)
		}
		if model != nil {
			t.Fatalf("expected agent default, got %+v", model)
		}
	})

	t.Run("no row follows channel", func(t *testing.T) {
		model, err := r.effectiveModel(ctx, ownedThreadMsg("thread-3", "", &fakeReplyCtx{}))
		if err != nil {
			t.Fatalf("effectiveModel: %v", err)
		}
		if model == nil || model.ProviderID != "anthropic" || model.ID != "claude-3" {
			t.Fatalf("no-row thread model = %+v, want anthropic/claude-3", model)
		}
	})

	t.Run("personal beats thread", func(t *testing.T) {
		threadConfigsOf(r).configs["discord:parent:thread-1"] = &store.ThreadConfig{
			Platform: "discord", ChannelID: "parent", ThreadID: "thread-1", Model: "openai/gpt-4o",
		}
		st.overrideRepo.overrides["discord:parent:user1"] = &store.UserOverride{
			ChannelID: "parent", Platform: "discord", UserID: "user1", Role: "admin", Model: "zai-coding-plan/glm-5.2",
		}
		model, err := r.effectiveModel(ctx, ownedThreadMsg("thread-1", "", &fakeReplyCtx{}))
		if err != nil {
			t.Fatalf("effectiveModel: %v", err)
		}
		if model == nil || model.ProviderID != "zai-coding-plan" || model.ID != "glm-5.2" {
			t.Fatalf("personal model should beat thread, got %+v", model)
		}
	})
}

func TestChannelModelChangeDoesNotAffectExistingThreads(t *testing.T) {
	r, client := newThreadTestRouter()
	client.providers = modelTestProviders()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["discord:parent"] = &store.Channel{
		ChannelID: "parent", Platform: "discord", Model: "openai/gpt-4o", ListenMode: "mention",
	}

	reply := &fakeReplyCtx{}
	if err := r.Route(context.Background(), ownedThreadMsg("thread-1", "/model", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !strings.Contains(reply.sends[0], "openai/gpt-4o") {
		t.Fatalf("thread view before channel change = %q", reply.sends[0])
	}

	st.channelRepo.channels["discord:parent"].Model = "anthropic/claude-3"
	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), ownedThreadMsg("thread-1", "/model", reply2)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !strings.Contains(reply2.sends[0], "openai/gpt-4o") {
		t.Fatalf("existing thread model changed with the channel: %q", reply2.sends[0])
	}
	if strings.Contains(reply2.sends[0], "anthropic/claude-3") {
		t.Fatalf("existing thread picked up the new channel model: %q", reply2.sends[0])
	}
}

func TestNewThreadAfterChannelChangeInheritsNewValue(t *testing.T) {
	r, client := newThreadTestRouter()
	client.providers = modelTestProviders()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["discord:parent"] = &store.Channel{
		ChannelID: "parent", Platform: "discord", Model: "openai/gpt-4o", ListenMode: "mention",
	}

	if err := r.Route(context.Background(), ownedThreadMsg("thread-1", "/model", &fakeReplyCtx{})); err != nil {
		t.Fatalf("Route thread-1: %v", err)
	}
	st.channelRepo.channels["discord:parent"].Model = "anthropic/claude-3"

	reply := &fakeReplyCtx{}
	if err := r.Route(context.Background(), ownedThreadMsg("thread-2", "/model", reply)); err != nil {
		t.Fatalf("Route thread-2: %v", err)
	}
	if !strings.Contains(reply.sends[0], "anthropic/claude-3") {
		t.Fatalf("new thread did not inherit the new channel model: %q", reply.sends[0])
	}
	tc := threadConfigsOf(r).configs["discord:parent:thread-2"]
	if tc == nil || tc.Model != "anthropic/claude-3" {
		t.Fatalf("thread-2 snapshot = %+v, want anthropic/claude-3", tc)
	}
}

func TestModelBrowserSetInThreadWritesThreadConfig(t *testing.T) {
	r, client := newThreadTestRouter()
	client.providers = browseProviders()
	threadConfigsOf(r).configs["discord:parent:thread-1"] = &store.ThreadConfig{
		Platform: "discord", ChannelID: "parent", ThreadID: "thread-1",
	}
	token, err := r.modelBrowser.register(modelBrowseAction{kind: "set", providerID: "openai", modelID: "gpt-4o"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	reply := newBrowseReplyCtx()
	cb := ownedThreadMsg("thread-1", "", reply.fakeReplyCtx)
	cb.IsCallback = true
	cb.CallbackData = "model:" + token
	cb.CallbackRef = fakeRef{id: "1"}
	cb.ReplyCtx = reply
	if err := r.Route(context.Background(), cb); err != nil {
		t.Fatalf("Route callback: %v", err)
	}
	text, _ := reply.editSnapshot()
	if text != "✅ Thread model set: openai/gpt-4o" {
		t.Fatalf("text = %q", text)
	}
	tc := threadConfigsOf(r).configs["discord:parent:thread-1"]
	if tc.Model != "openai/gpt-4o" {
		t.Fatalf("thread model = %q, want openai/gpt-4o", tc.Model)
	}
}

func telegramTopicMsg(chatID, threadID, text string, reply *fakeReplyCtx) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:  "telegram",
		ChannelID: chatID,
		ThreadID:  threadID,
		UserID:    "user1",
		Text:      text,
		IsThread:  true,
		IsMention: true,
		ReplyCtx:  reply,
	}
}

// TestSameThreadIDDifferentChatsIsolated proves Telegram forum topic ids are
// scoped to their parent chat: two chats that happen to use the same bare
// topic id (555) must not share a workdir/model/listen snapshot.
func TestSameThreadIDDifferentChatsIsolated(t *testing.T) {
	r, client := newThreadTestRouter()
	client.providers = modelTestProviders()
	ctx := context.Background()
	st := r.store.(*fakeStore)
	st.overrideRepo.overrides["telegram:chat-a:user1"] = &store.UserOverride{
		ChannelID: "chat-a", Platform: "telegram", UserID: "user1", Role: "admin",
	}
	st.overrideRepo.overrides["telegram:chat-b:user1"] = &store.UserOverride{
		ChannelID: "chat-b", Platform: "telegram", UserID: "user1", Role: "admin",
	}
	st.channelRepo.channels["telegram:chat-a"] = &store.Channel{
		ChannelID: "chat-a", Platform: "telegram", Model: "anthropic/claude-3", ListenMode: "all",
	}
	st.channelRepo.channels["telegram:chat-b"] = &store.Channel{
		ChannelID: "chat-b", Platform: "telegram", Model: "zai-coding-plan/glm-5.2", ListenMode: "mention",
	}

	if err := r.Route(ctx, telegramTopicMsg("chat-a", "555", "/model openai/gpt-4o", &fakeReplyCtx{})); err != nil {
		t.Fatalf("Route chat-a: %v", err)
	}
	tcA := threadConfigsOf(r).configs["telegram:chat-a:555"]
	if tcA == nil || tcA.Model != "openai/gpt-4o" {
		t.Fatalf("chat-a topic row = %+v, want openai/gpt-4o", tcA)
	}
	if tcB := threadConfigsOf(r).configs["telegram:chat-b:555"]; tcB != nil {
		t.Fatalf("chat-b topic row must stay untouched, got %+v", tcB)
	}

	modelA, err := r.effectiveModel(ctx, telegramTopicMsg("chat-a", "555", "", &fakeReplyCtx{}))
	if err != nil {
		t.Fatalf("effectiveModel chat-a: %v", err)
	}
	if modelA == nil || modelA.ProviderID != "openai" || modelA.ID != "gpt-4o" {
		t.Fatalf("chat-a effective model = %+v, want openai/gpt-4o", modelA)
	}
	modelB, err := r.effectiveModel(ctx, telegramTopicMsg("chat-b", "555", "", &fakeReplyCtx{}))
	if err != nil {
		t.Fatalf("effectiveModel chat-b: %v", err)
	}
	if modelB == nil || modelB.ProviderID != "zai-coding-plan" || modelB.ID != "glm-5.2" {
		t.Fatalf("chat-b effective model = %+v, want zai-coding-plan/glm-5.2", modelB)
	}
}

// TestNonThreadResolutionSkipsThreadConfig proves the no-thread-config-read
// boundary: plain (non-thread) workdir/listen/model resolution and set
// commands must never touch the thread-config repository.
func TestNonThreadResolutionSkipsThreadConfig(t *testing.T) {
	r, client, _ := newTestRouter()
	client.providers = modelTestProviders()
	ctx := context.Background()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", Workdir: "/repo", Model: "openai/gpt-4o", ListenMode: "all",
	}
	plain := msg("hello", &fakeReplyCtx{})
	tc := threadConfigsOf(r)

	if got := r.effectiveWorkdir(ctx, plain); got != "/repo" {
		t.Fatalf("plain workdir = %q, want /repo", got)
	}
	if got := r.effectiveListenMode(ctx, plain); got != "all" {
		t.Fatalf("plain listen mode = %q, want all", got)
	}
	if _, err := r.effectiveModel(ctx, plain); err != nil {
		t.Fatalf("effectiveModel: %v", err)
	}
	if tc.getCalls != 0 {
		t.Fatalf("non-thread resolution made %d thread-config reads, want 0", tc.getCalls)
	}

	if err := r.Route(ctx, plain); err != nil {
		t.Fatalf("Route plain passthrough: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if tc.getCalls != 0 {
		t.Fatalf("non-thread passthrough made %d thread-config reads, want 0", tc.getCalls)
	}
	if tc.writeCalls != 0 {
		t.Fatalf("non-thread passthrough made %d thread-config writes, want 0", tc.writeCalls)
	}

	dir := t.TempDir()
	for _, tt := range []struct{ text string }{
		{text: "/model openai/gpt-4o"},
		{text: "/channel all"},
		{text: "/dir " + dir},
	} {
		if err := r.Route(ctx, msg(tt.text, &fakeReplyCtx{})); err != nil {
			t.Fatalf("Route %q: %v", tt.text, err)
		}
	}
	if tc.getCalls != 0 {
		t.Fatalf("non-thread set commands made %d thread-config reads, want 0", tc.getCalls)
	}
	if tc.writeCalls != 0 {
		t.Fatalf("non-thread set commands made %d thread-config writes, want 0", tc.writeCalls)
	}
	if len(tc.configs) != 0 {
		t.Fatalf("non-thread activity materialized thread-config rows: %+v", tc.configs)
	}
}
