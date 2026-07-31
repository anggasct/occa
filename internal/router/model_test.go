package router

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

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

	delete(overrides.overrides, "telegram:chat1:user1")
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
