package router

import (
	"context"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

func TestModelResolutionUsesExplicitLocationBeforeLowerScopes(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*fakeStore)
		msg    channel.IncomingMessage
		source modelSource
		model  string
	}{
		{
			name: "thread setting wins over every lower scope",
			setup: func(st *fakeStore) {
				st.channelRepo.channels["discord:parent"] = &store.Channel{Platform: "discord", ChannelID: "parent", Model: "anthropic/claude-3"}
				st.overrideRepo.overrides["discord:parent:user1"] = &store.UserOverride{Platform: "discord", ChannelID: "parent", UserID: "user1", Model: "openai/gpt-4o"}
				st.threadConfigs = newFakeThreadConfigRepo(st.channelRepo)
				st.threadConfigs.configs["discord:parent:thread-1"] = &store.ThreadConfig{Platform: "discord", ChannelID: "parent", ThreadID: "thread-1", Model: "zai-coding-plan/glm-5.2"}
			},
			msg:    ownedThreadMsg("thread-1", "hello", &fakeReplyCtx{}),
			source: modelSourceThread,
			model:  "zai-coding-plan/glm-5.2",
		},
		{
			name: "channel setting wins over personal and legacy session",
			setup: func(st *fakeStore) {
				st.channelRepo.channels["telegram:chat1"] = &store.Channel{Platform: "telegram", ChannelID: "chat1", Model: "anthropic/claude-3"}
				st.overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{Platform: "telegram", ChannelID: "chat1", UserID: "user1", Model: "openai/gpt-4o"}
				st.sessionRepo.activeID = "sess-legacy"
				st.sessionRepo.models = map[string]string{"sess-legacy": "zai-coding-plan/glm-5.2"}
			},
			msg:    msgFrom("user1", "hello", &fakeReplyCtx{}),
			source: modelSourceChannel,
			model:  "anthropic/claude-3",
		},
		{
			name: "personal override wins over legacy session",
			setup: func(st *fakeStore) {
				st.overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{Platform: "telegram", ChannelID: "chat1", UserID: "user1", Model: "openai/gpt-4o"}
				st.sessionRepo.activeID = "sess-legacy"
				st.sessionRepo.models = map[string]string{"sess-legacy": "zai-coding-plan/glm-5.2"}
			},
			msg:    msgFrom("user1", "hello", &fakeReplyCtx{}),
			source: modelSourcePersonal,
			model:  "openai/gpt-4o",
		},
		{
			name: "legacy session wins over agent default",
			setup: func(st *fakeStore) {
				st.sessionRepo.activeID = "sess-legacy"
				st.sessionRepo.models = map[string]string{"sess-legacy": "zai-coding-plan/glm-5.2"}
			},
			msg:    msgFrom("user1", "hello", &fakeReplyCtx{}),
			source: modelSourceSession,
			model:  "zai-coding-plan/glm-5.2",
		},
		{
			name:   "agent default has an explicit source",
			msg:    msgFrom("user1", "hello", &fakeReplyCtx{}),
			source: modelSourceDefault,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _, _, _ := newTestRouterWithAccess()
			st := r.store.(*fakeStore)
			if tt.setup != nil {
				tt.setup(st)
			}
			resolution, err := r.resolveModel(context.Background(), tt.msg)
			if err != nil {
				t.Fatalf("resolveModel: %v", err)
			}
			if resolution.source != tt.source {
				t.Fatalf("source = %q, want %q", resolution.source, tt.source)
			}
			if tt.model == "" {
				if resolution.model != nil {
					t.Fatalf("model = %+v, want agent default", resolution.model)
				}
				return
			}
			if resolution.model == nil || formatModelRef(*resolution.model) != tt.model {
				t.Fatalf("model = %+v, want %q", resolution.model, tt.model)
			}
		})
	}
}

func TestModelDefaultAllowsLowerScopeWithoutMutatingIt(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["telegram:chat1"] = &store.Channel{Platform: "telegram", ChannelID: "chat1", Model: "anthropic/claude-3", ListenMode: "mention"}
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{Platform: "telegram", ChannelID: "chat1", UserID: "user1", Role: "admin", Model: "openai/gpt-4o"}

	if err := r.Route(context.Background(), msg("/model default", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !strings.Contains(reply.sends[0], "Channel model cleared") {
		t.Fatalf("response = %q", reply.sends[0])
	}
	if overrides.overrides["telegram:chat1:user1"].Model != "openai/gpt-4o" {
		t.Fatal("default cleared the personal override instead of only the channel location")
	}
	resolution, err := r.resolveModel(context.Background(), msg("hello", &fakeReplyCtx{}))
	if err != nil {
		t.Fatalf("resolveModel: %v", err)
	}
	if resolution.source != modelSourcePersonal || formatModelRef(*resolution.model) != "openai/gpt-4o" {
		t.Fatalf("resolution after default = %+v, want personal override", resolution)
	}
}

func TestIgnoredListenMessageDoesNotAcquireResponseSlot(t *testing.T) {
	r, client, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{Platform: "telegram", ChannelID: "chat1", UserID: "user1", Role: "allow"}
	r.store.(*fakeStore).channelRepo.channels["telegram:chat1"] = &store.Channel{Platform: "telegram", ChannelID: "chat1", ListenMode: "mention"}

	ordinary := msg("ordinary message", reply)
	ordinary.IsMention = false
	if err := r.Route(context.Background(), ordinary); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.sendCalls != 0 || len(reply.sends) != 0 {
		t.Fatalf("ignored message touched relay/reply: sends=%d replies=%v", client.sendCalls, reply.sends)
	}
	r.responses.mu.Lock()
	active, queued := len(r.responses.active), len(r.responses.queues)
	r.responses.mu.Unlock()
	if active != 0 || queued != 0 {
		t.Fatalf("ignored message created response state: active=%d queued=%d", active, queued)
	}
}

func TestThreadListenModeRejectsUnownedDiscordThreadWithoutMention(t *testing.T) {
	r, _, _, overrides := newTestRouterWithAccess()
	overrides.overrides["discord:thread-1:user1"] = &store.UserOverride{Platform: "discord", ChannelID: "thread-1", UserID: "user1", Role: "allow"}
	r.store.(*fakeStore).channelRepo.channels["discord:parent"] = &store.Channel{Platform: "discord", ChannelID: "parent", ListenMode: "thread"}

	msg := channel.IncomingMessage{
		Platform:        "discord",
		ChannelID:       "thread-1",
		ParentChannelID: "parent",
		ThreadID:        "thread-1",
		UserID:          "user1",
		IsThread:        true,
	}
	allowed, mode, err := r.listenModeDecision(context.Background(), msg)
	if err != nil {
		t.Fatalf("listenModeDecision: %v", err)
	}
	if allowed || mode != "thread" {
		t.Fatalf("unowned thread decision = allowed=%v mode=%q, want false/thread", allowed, mode)
	}
}

func TestListenModeViewShowsLocationModeAndNextAction(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["discord:parent:user1"] = &store.UserOverride{Platform: "discord", ChannelID: "parent", UserID: "user1", Role: "admin"}
	st := r.store.(*fakeStore)
	st.channelRepo.channels["discord:parent"] = &store.Channel{Platform: "discord", ChannelID: "parent", ListenMode: "mention"}

	thread := ownedThreadMsg("thread-1", "/channel", reply)
	if err := r.Route(context.Background(), thread); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "Listen mode: this thread · mention") || !strings.Contains(reply.sends[0], "parent-channel policy remains isolated") {
		t.Fatalf("thread view = %v", reply.sends)
	}

	statusReply := &fakeReplyCtx{}
	if err := r.Route(context.Background(), ownedThreadMsg("thread-1", "/status", statusReply)); err != nil {
		t.Fatalf("Route status: %v", err)
	}
	if len(statusReply.sends) != 1 || !strings.Contains(statusReply.sends[0], "Listen mode: this thread · mention") || !strings.Contains(statusReply.sends[0], "parent-channel policy remains isolated") {
		t.Fatalf("status view = %v", statusReply.sends)
	}
}

func TestDiscordThreadConfigScopeUsesParentForMessageAndInteractionShapes(t *testing.T) {
	r, _, _, overrides := newTestRouterWithAccess()
	overrides.overrides["discord:parent:user1"] = &store.UserOverride{Platform: "discord", ChannelID: "parent", UserID: "user1", Role: "allow"}
	st := r.store.(*fakeStore)
	st.channelRepo.channels["discord:parent"] = &store.Channel{Platform: "discord", ChannelID: "parent", Model: "openai/gpt-4o", ListenMode: "all"}

	shapes := []channel.IncomingMessage{
		{Platform: "discord", ChannelID: "thread", ParentChannelID: "parent", ThreadID: "thread", UserID: "user1", IsThread: true},
		{Platform: "discord", ChannelID: "parent", ParentChannelID: "parent", ThreadID: "thread", UserID: "user1", IsThread: true},
	}
	for i, msg := range shapes {
		resolution, err := r.resolveModel(context.Background(), msg)
		if err != nil {
			t.Fatalf("shape %d resolveModel: %v", i, err)
		}
		if resolution.source != modelSourceChannel || formatModelRef(*resolution.model) != "openai/gpt-4o" {
			t.Fatalf("shape %d model resolution = %+v", i, resolution)
		}
		mode, err := r.effectiveListenMode(context.Background(), msg)
		if err != nil || mode != "all" {
			t.Fatalf("shape %d listen mode = %q, err=%v", i, mode, err)
		}
	}
}
