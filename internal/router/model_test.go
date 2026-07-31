package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

func modelTestProviders() relay.Providers {
	return relay.Providers{All: []relay.Provider{
		{ID: "openai", Models: map[string]json.RawMessage{"gpt-4o": nil}},
		{ID: "anthropic", Models: map[string]json.RawMessage{"claude-3": nil}},
	}}
}

func TestModelViewUsesPersonalThenChannelThenDefault(t *testing.T) {
	r, client, reply, overrides := newTestRouterWithAccess()
	client.providers = modelTestProviders()
	st := r.store.(*fakeStore)
	st.channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", Model: "anthropic/claude-3", ListenMode: "mention",
	}
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin", Model: "openai/gpt-4o",
	}

	if err := r.Route(context.Background(), msg("/occa:model", reply)); err != nil {
		t.Fatalf("Route personal model view: %v", err)
	}
	if !strings.Contains(reply.sends[0], "openai/gpt-4o") {
		t.Fatalf("expected personal model, got %q", reply.sends[0])
	}

	o := overrides.overrides["telegram:chat1:user1"]
	o.Model = ""
	o.Role = "allow"
	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msg("/occa:model", reply2)); err != nil {
		t.Fatalf("Route channel model view: %v", err)
	}
	if !strings.Contains(reply2.sends[0], "anthropic/claude-3") {
		t.Fatalf("expected channel model, got %q", reply2.sends[0])
	}

	delete(st.channelRepo.channels, "telegram:chat1")
	reply3 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msg("/occa:model", reply3)); err != nil {
		t.Fatalf("Route default model view: %v", err)
	}
	if !strings.Contains(reply3.sends[0], "agent default") {
		t.Fatalf("expected agent default, got %q", reply3.sends[0])
	}
}

func TestModelSetPersonalOverride(t *testing.T) {
	r, client, reply, overrides := newTestRouterWithAccess()
	client.providers = modelTestProviders()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	if err := r.Route(context.Background(), msg("/occa:model openai/gpt-4o", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	o := overrides.overrides["telegram:chat1:user1"]
	if o.Model != "openai/gpt-4o" {
		t.Fatalf("model = %q, want openai/gpt-4o", o.Model)
	}
	if !strings.Contains(reply.sends[0], "Personal model set") {
		t.Fatalf("unexpected response: %q", reply.sends[0])
	}
}

func TestModelCommandRequiresAllowedUser(t *testing.T) {
	roles := []struct {
		name string
		role string
	}{
		{name: "unknown"},
		{name: "denied", role: "deny"},
	}
	actions := []struct {
		name string
		text string
	}{
		{name: "view", text: "/occa:model"},
		{name: "set personal", text: "/occa:model openai/gpt-4o"},
	}

	for _, role := range roles {
		for _, action := range actions {
			t.Run(role.name+" "+action.name, func(t *testing.T) {
				r, client, reply, overrides := newTestRouterWithAccess()
				client.providers = modelTestProviders()
				if role.role != "" {
					overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
						ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: role.role,
					}
				}

				if err := r.Route(context.Background(), msg(action.text, reply)); err != nil {
					t.Fatalf("Route: %v", err)
				}
				if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "Access denied") {
					t.Fatalf("unexpected response: %v", reply.sends)
				}
				if client.providerCalls != 0 {
					t.Fatalf("provider calls = %d, want 0", client.providerCalls)
				}
				if stored := overrides.overrides["telegram:chat1:user1"]; stored != nil && stored.Model != "" {
					t.Fatalf("model was persisted for denied user: %+v", stored)
				}
			})
		}
	}
}

func TestModelSetChannelDefaultRequiresAdmin(t *testing.T) {
	r, client, reply, overrides := newTestRouterWithAccess()
	client.providers = modelTestProviders()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
	}

	if err := r.Route(context.Background(), msg("/occa:model channel openai/gpt-4o", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !strings.Contains(reply.sends[0], "Admin access required") {
		t.Fatalf("unexpected response: %q", reply.sends[0])
	}
	if ch := r.store.(*fakeStore).channelRepo.channels["telegram:chat1"]; ch != nil {
		t.Fatalf("non-admin changed channel model: %+v", ch)
	}
}

func TestModelChannelUsageRequiresModelID(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	if err := r.Route(context.Background(), msg("/occa:model channel", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !strings.Contains(reply.sends[0], "Usage: /occa:model channel") {
		t.Fatalf("unexpected response: %q", reply.sends[0])
	}
}

func TestModelSetChannelDefaultPreservesChannelSettings(t *testing.T) {
	r, client, reply, overrides := newTestRouterWithAccess()
	client.providers = modelTestProviders()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}
	st := r.store.(*fakeStore)
	st.channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", ListenMode: "all", Workdir: "/repo",
	}

	if err := r.Route(context.Background(), msg("/occa:model channel openai/gpt-4o", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	ch := st.channelRepo.channels["telegram:chat1"]
	if ch.Model != "openai/gpt-4o" || ch.ListenMode != "all" || ch.Workdir != "/repo" {
		t.Fatalf("unexpected channel after model set: %+v", ch)
	}
}

func TestModelValidationRejectsMalformedAndUnknownValues(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want string
	}{
		{name: "malformed", arg: "openai", want: "invalid model"},
		{name: "unknown provider", arg: "missing/gpt-4o", want: "unknown provider"},
		{name: "unknown model", arg: "openai/missing", want: "unknown model"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, client, reply, overrides := newTestRouterWithAccess()
			client.providers = modelTestProviders()
			overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
				ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
			}

			if err := r.Route(context.Background(), msg("/occa:model "+tt.arg, reply)); err != nil {
				t.Fatalf("Route: %v", err)
			}
			if !strings.Contains(reply.sends[0], tt.want) {
				t.Fatalf("response = %q, want %q", reply.sends[0], tt.want)
			}
			if overrides.overrides["telegram:chat1:user1"].Model != "" {
				t.Fatal("invalid model was persisted")
			}
		})
	}
}

func TestModelProviderErrorsUseSafeReplies(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "unreachable", err: fmt.Errorf("private backend detail: %w", relay.ErrUnreachable), want: "Agent unreachable"},
		{name: "unexpected", err: errors.New("private backend detail"), want: "Model provider list unavailable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, client, reply, overrides := newTestRouterWithAccess()
			client.providersErr = tt.err
			overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
				ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
			}

			if err := r.Route(context.Background(), msg("/occa:model openai/gpt-4o", reply)); err != nil {
				t.Fatalf("Route: %v", err)
			}
			if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], tt.want) {
				t.Fatalf("response = %v, want %q", reply.sends, tt.want)
			}
			if strings.Contains(reply.sends[0], "private backend detail") {
				t.Fatalf("raw error leaked to chat: %q", reply.sends[0])
			}
		})
	}
}

func TestModelInstanceErrorUsesSafeReply(t *testing.T) {
	r, client, reply, overrides := newTestRouterWithAccess()
	r.instances.(*fakeInstanceProvider).err = errors.New("private instance detail")
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
	}

	if err := r.Route(context.Background(), msg("/occa:model openai/gpt-4o", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "Agent unreachable") {
		t.Fatalf("unexpected response: %v", reply.sends)
	}
	if strings.Contains(reply.sends[0], "private instance detail") {
		t.Fatalf("raw error leaked to chat: %q", reply.sends[0])
	}
	if client.providerCalls != 0 {
		t.Fatalf("provider calls = %d, want 0", client.providerCalls)
	}
}

func TestModelCommandHidesRepositoryError(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
	}
	r.store.(*fakeStore).channelRepo.getErr = errors.New("private database detail")

	if err := r.Route(context.Background(), msg("/occa:model", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "Command failed") {
		t.Fatalf("unexpected response: %v", reply.sends)
	}
	if strings.Contains(reply.sends[0], "private database detail") {
		t.Fatalf("raw error leaked to chat: %q", reply.sends[0])
	}
}

func TestModelValidationPreservesInternalCause(t *testing.T) {
	t.Run("provider", func(t *testing.T) {
		r, client, _, _ := newTestRouterWithAccess()
		cause := errors.New("provider decode failed")
		client.providersErr = cause

		err := r.validateModel(context.Background(), msg("", &fakeReplyCtx{}), relay.ModelRef{ProviderID: "openai", ID: "gpt-4o"})
		if !errors.Is(err, cause) {
			t.Fatalf("error does not preserve cause: %v", err)
		}
	})

	t.Run("instance", func(t *testing.T) {
		r, _, _, _ := newTestRouterWithAccess()
		cause := errors.New("instance spawn failed")
		r.instances.(*fakeInstanceProvider).err = cause

		err := r.validateModel(context.Background(), msg("", &fakeReplyCtx{}), relay.ModelRef{ProviderID: "openai", ID: "gpt-4o"})
		if !errors.Is(err, cause) {
			t.Fatalf("error does not preserve cause: %v", err)
		}
	})
}

func TestMessagesResolvePersonalModelsPerUser(t *testing.T) {
	r, client, _, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin", Model: "openai/gpt-4o",
	}
	overrides.overrides["telegram:chat1:user2"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user2", Role: "allow", Model: "anthropic/claude-3",
	}

	if err := r.Route(context.Background(), msgFrom("user1", "first", &fakeReplyCtx{})); err != nil {
		t.Fatalf("Route user1: %v", err)
	}
	if client.lastModel == nil || client.lastModel.ProviderID != "openai" {
		t.Fatalf("user1 model = %+v", client.lastModel)
	}

	if err := r.Route(context.Background(), msgFrom("user2", "second", &fakeReplyCtx{})); err != nil {
		t.Fatalf("Route user2: %v", err)
	}
	if client.lastModel == nil || client.lastModel.ProviderID != "anthropic" {
		t.Fatalf("user2 model = %+v", client.lastModel)
	}
}

func TestMessageUsesChannelModelWithoutPersonalOverride(t *testing.T) {
	r, client, _, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}
	r.store.(*fakeStore).channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", Model: "openai/gpt-4o", ListenMode: "mention",
	}

	if err := r.Route(context.Background(), msg("hello", &fakeReplyCtx{})); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.lastModel == nil || client.lastModel.ProviderID != "openai" || client.lastModel.ID != "gpt-4o" {
		t.Fatalf("expected channel model, got %+v", client.lastModel)
	}
}

func TestThreadModelUsesParentChannelScope(t *testing.T) {
	r, client, reply, overrides := newTestRouterWithAccess()
	client.providers = modelTestProviders()
	st := r.store.(*fakeStore)
	overrides.overrides["discord:thread:user1"] = &store.UserOverride{
		ChannelID: "thread", Platform: "discord", UserID: "user1", Role: "admin",
	}
	st.channelRepo.channels["discord:parent"] = &store.Channel{
		ChannelID: "parent", Platform: "discord", Model: "openai/gpt-4o", ListenMode: "mention",
	}

	threadMsg := channel.IncomingMessage{
		Platform:        "discord",
		ChannelID:       "thread",
		ParentChannelID: "parent",
		UserID:          "user1",
		Text:            "/occa:model",
		IsThread:        true,
		ReplyCtx:        reply,
	}
	if err := r.Route(context.Background(), threadMsg); err != nil {
		t.Fatalf("Route view: %v", err)
	}
	if !strings.Contains(reply.sends[0], "openai/gpt-4o") {
		t.Fatalf("expected parent model, got %q", reply.sends[0])
	}

	threadMsg.Text = "/occa:model channel anthropic/claude-3"
	if err := r.Route(context.Background(), threadMsg); err != nil {
		t.Fatalf("Route set: %v", err)
	}
	if got := st.channelRepo.channels["discord:parent"].Model; got != "anthropic/claude-3" {
		t.Fatalf("parent model = %q, want anthropic/claude-3", got)
	}
	if st.channelRepo.channels["discord:thread"] != nil {
		t.Fatal("thread-local model row should not be created")
	}
}

func TestMessageWithoutModelUsesAgentDefault(t *testing.T) {
	r, client, _, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	if err := r.Route(context.Background(), msg("hello", &fakeReplyCtx{})); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.lastModel != nil {
		t.Fatalf("expected agent default to omit model, got %+v", client.lastModel)
	}
}

func TestMessageRejectsInvalidStoredModel(t *testing.T) {
	r, client, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin", Model: "invalid",
	}

	if err := r.Route(context.Background(), msg("hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.sendCalls != 0 {
		t.Fatalf("message was forwarded with invalid stored model: %q", client.lastMsg)
	}
	if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "Message not sent") {
		t.Fatalf("unexpected response: %v", reply.sends)
	}
}

func TestMessageRejectsModelRepositoryFailure(t *testing.T) {
	r, client, reply, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
	}
	r.store.(*fakeStore).channelRepo.getErr = errors.New("database unavailable")

	if err := r.Route(context.Background(), msg("hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if client.sendCalls != 0 {
		t.Fatalf("message was forwarded after repository failure: %q", client.lastMsg)
	}
	if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "Message not sent") {
		t.Fatalf("unexpected response: %v", reply.sends)
	}
}

func TestUnresolvedChannelScopeDoesNotReadWriteOrSendModel(t *testing.T) {
	r, client, commandReply, overrides := newTestRouterWithAccess()
	client.providers = modelTestProviders()
	overrides.overrides["discord:thread:user1"] = &store.UserOverride{
		ChannelID: "thread", Platform: "discord", UserID: "user1", Role: "admin",
	}
	command := channel.IncomingMessage{
		Platform:               "discord",
		ChannelID:              "thread",
		ChannelScopeUnresolved: true,
		UserID:                 "user1",
		Text:                   "/occa:model channel openai/gpt-4o",
		IsMention:              true,
		ReplyCtx:               commandReply,
	}

	if err := r.Route(context.Background(), command); err != nil {
		t.Fatalf("Route command: %v", err)
	}
	if len(commandReply.sends) != 1 || !strings.Contains(commandReply.sends[0], "Channel information unavailable") {
		t.Fatalf("unexpected command response: %v", commandReply.sends)
	}
	if client.providerCalls != 0 {
		t.Fatalf("provider calls = %d, want 0", client.providerCalls)
	}
	if len(r.store.(*fakeStore).channelRepo.channels) != 0 {
		t.Fatalf("model row created with unresolved scope: %+v", r.store.(*fakeStore).channelRepo.channels)
	}

	messageReply := &fakeReplyCtx{}
	command.Text = "hello"
	command.ReplyCtx = messageReply
	if err := r.Route(context.Background(), command); err != nil {
		t.Fatalf("Route message: %v", err)
	}
	if client.sendCalls != 0 {
		t.Fatal("message was sent with unresolved scope")
	}
	if len(messageReply.sends) != 1 || !strings.Contains(messageReply.sends[0], "Message not sent") {
		t.Fatalf("unexpected message response: %v", messageReply.sends)
	}
}
