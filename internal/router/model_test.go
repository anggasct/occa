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
		{
			ID: "zai-coding-plan",
			Models: map[string]json.RawMessage{
				"glm-5.2": json.RawMessage(`{"variants":{"high":{"reasoningEffort":"high"},"max":{"reasoningEffort":"high"},"low":{"reasoningEffort":"low"}}}`),
				"no-var":  json.RawMessage(`{"name":"no-var"}`),
			},
		},
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

	if err := r.Route(context.Background(), msg("/model", reply)); err != nil {
		t.Fatalf("Route personal model view: %v", err)
	}
	if !strings.Contains(reply.sends[0], "openai/gpt-4o") {
		t.Fatalf("expected personal model, got %q", reply.sends[0])
	}

	o := overrides.overrides["telegram:chat1:user1"]
	o.Model = ""
	o.Role = "allow"
	reply2 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msg("/model", reply2)); err != nil {
		t.Fatalf("Route channel model view: %v", err)
	}
	if !strings.Contains(reply2.sends[0], "anthropic/claude-3") {
		t.Fatalf("expected channel model, got %q", reply2.sends[0])
	}

	delete(st.channelRepo.channels, "telegram:chat1")
	reply3 := &fakeReplyCtx{}
	if err := r.Route(context.Background(), msg("/model", reply3)); err != nil {
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
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
	}

	if err := r.Route(context.Background(), msg("/model openai/gpt-4o", reply)); err != nil {
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
		{name: "view", text: "/model"},
		{name: "set personal", text: "/model openai/gpt-4o"},
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

func TestOldModelKeywordsRejected(t *testing.T) {
	for _, tt := range []struct {
		name string
		text string
	}{
		{name: "channel with ref", text: "/model channel openai/gpt-4o"},
		{name: "channel alone", text: "/model channel"},
		{name: "session with ref", text: "/model session openai/gpt-4o"},
		{name: "session default", text: "/model session default"},
		{name: "session alone", text: "/model session"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, client, reply, overrides := newTestRouterWithAccess()
			client.providers = modelTestProviders()
			overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
				ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
			}

			if err := r.Route(context.Background(), msg(tt.text, reply)); err != nil {
				t.Fatalf("Route: %v", err)
			}
			if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "Usage: /model") {
				t.Fatalf("expected usage error, got: %v", reply.sends)
			}
			st := r.store.(*fakeStore)
			if ch := st.channelRepo.channels["telegram:chat1"]; ch != nil && ch.Model != "" {
				t.Fatalf("keyword form wrote a channel model: %+v", ch)
			}
			if o := overrides.overrides["telegram:chat1:user1"]; o != nil && o.Model != "" {
				t.Fatalf("keyword form wrote a personal model: %+v", o)
			}
			if len(st.ThreadConfigRepo().(*fakeThreadConfigRepo).configs) != 0 {
				t.Fatalf("keyword form wrote a thread config: %+v", st.ThreadConfigRepo().(*fakeThreadConfigRepo).configs)
			}
		})
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

			if err := r.Route(context.Background(), msg("/model "+tt.arg, reply)); err != nil {
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

			if err := r.Route(context.Background(), msg("/model openai/gpt-4o", reply)); err != nil {
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

	if err := r.Route(context.Background(), msg("/model openai/gpt-4o", reply)); err != nil {
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

	if err := r.Route(context.Background(), msg("/model", reply)); err != nil {
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
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.lastModel == nil || client.lastModel.ProviderID != "openai" {
		t.Fatalf("user1 model = %+v", client.lastModel)
	}

	if err := r.Route(context.Background(), msgFrom("user2", "second", &fakeReplyCtx{})); err != nil {
		t.Fatalf("Route user2: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
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
	waitForDispatch(t, client)
	waitForResponse(t, r)
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
		Text:            "/model",
		IsThread:        true,
		ReplyCtx:        reply,
	}
	if err := r.Route(context.Background(), threadMsg); err != nil {
		t.Fatalf("Route view: %v", err)
	}
	if !strings.Contains(reply.sends[0], "openai/gpt-4o") {
		t.Fatalf("expected parent model, got %q", reply.sends[0])
	}

	threadMsg.Text = "/model anthropic/claude-3"
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
	waitForDispatch(t, client)
	waitForResponse(t, r)
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
		Text:                   "/model openai/gpt-4o",
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

func TestParseModelRefVariant(t *testing.T) {
	t.Run("with variant", func(t *testing.T) {
		ref, err := parseModelRef("zai-coding-plan/glm-5.2@max")
		if err != nil {
			t.Fatalf("parseModelRef: %v", err)
		}
		want := relay.ModelRef{ProviderID: "zai-coding-plan", ID: "glm-5.2", Variant: "max"}
		if ref != want {
			t.Fatalf("got %+v, want %+v", ref, want)
		}
	})

	t.Run("without variant", func(t *testing.T) {
		ref, err := parseModelRef("zai-coding-plan/glm-5.2")
		if err != nil {
			t.Fatalf("parseModelRef: %v", err)
		}
		want := relay.ModelRef{ProviderID: "zai-coding-plan", ID: "glm-5.2", Variant: ""}
		if ref != want {
			t.Fatalf("got %+v, want %+v", ref, want)
		}
	})

	t.Run("empty variant error", func(t *testing.T) {
		_, err := parseModelRef("a/b@")
		if err == nil {
			t.Fatal("expected error for a/b@, got nil")
		}
		if !strings.Contains(err.Error(), "invalid variant") {
			t.Fatalf("expected error to contain 'invalid variant', got %q", err.Error())
		}
	})
}

func TestFormatModelRefVariant(t *testing.T) {
	t.Run("with variant", func(t *testing.T) {
		ref := relay.ModelRef{ProviderID: "zai-coding-plan", ID: "glm-5.2", Variant: "max"}
		if got := formatModelRef(ref); got != "zai-coding-plan/glm-5.2@max" {
			t.Fatalf("got %q, want zai-coding-plan/glm-5.2@max", got)
		}
	})

	t.Run("without variant", func(t *testing.T) {
		ref := relay.ModelRef{ProviderID: "zai-coding-plan", ID: "glm-5.2"}
		if got := formatModelRef(ref); got != "zai-coding-plan/glm-5.2" {
			t.Fatalf("got %q, want zai-coding-plan/glm-5.2", got)
		}
	})
}

func TestValidateModelVariant(t *testing.T) {
	r, client, _, _ := newTestRouterWithAccess()
	client.providers = relay.Providers{All: []relay.Provider{
		{
			ID: "zai-coding-plan",
			Models: map[string]json.RawMessage{
				"glm-5.2":  json.RawMessage(`{"variants":{"high":{},"max":{},"low":{}}}`),
				"no-var":   json.RawMessage(`{"name":"no-var"}`),
				"bad-json": json.RawMessage(`invalid`),
			},
		},
	}}

	t.Run("variant exists", func(t *testing.T) {
		ref := relay.ModelRef{ProviderID: "zai-coding-plan", ID: "glm-5.2", Variant: "max"}
		if err := r.validateModel(context.Background(), msg("", &fakeReplyCtx{}), ref); err != nil {
			t.Fatalf("validateModel: %v", err)
		}
	})

	t.Run("variant missing from variants map", func(t *testing.T) {
		ref := relay.ModelRef{ProviderID: "zai-coding-plan", ID: "glm-5.2", Variant: "max_unknown"}
		err := r.validateModel(context.Background(), msg("", &fakeReplyCtx{}), ref)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "unknown variant: max_unknown for zai-coding-plan/glm-5.2") {
			t.Fatalf("unexpected error message: %q", err.Error())
		}
	})

	t.Run("variants field missing", func(t *testing.T) {
		ref := relay.ModelRef{ProviderID: "zai-coding-plan", ID: "no-var", Variant: "any"}
		if err := r.validateModel(context.Background(), msg("", &fakeReplyCtx{}), ref); err != nil {
			t.Fatalf("expected pass for missing variants field, got %v", err)
		}
	})

	t.Run("variants field unparseable", func(t *testing.T) {
		ref := relay.ModelRef{ProviderID: "zai-coding-plan", ID: "bad-json", Variant: "any"}
		if err := r.validateModel(context.Background(), msg("", &fakeReplyCtx{}), ref); err != nil {
			t.Fatalf("expected pass for unparseable variants field, got %v", err)
		}
	})
}

func TestVariantsCommand(t *testing.T) {
	t.Run("no active model", func(t *testing.T) {
		r, client, reply, overrides := newTestRouterWithAccess()
		client.providers = modelTestProviders()
		overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
			ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
		}

		if err := r.Route(context.Background(), msg("/variants", reply)); err != nil {
			t.Fatalf("Route: %v", err)
		}
		if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "No active model. Usage: /variants [provider/model-id]") {
			t.Fatalf("unexpected reply: %v", reply.sends)
		}
	})

	t.Run("no arg resolves current effective model", func(t *testing.T) {
		r, client, _, overrides := newTestRouterWithAccess()
		client.providers = modelTestProviders()
		overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
			ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow", Model: "zai-coding-plan/glm-5.2",
		}
		reply := newBrowseReplyCtx()

		m := msg("/variants", reply.fakeReplyCtx)
		m.ReplyCtx = reply
		if err := r.Route(context.Background(), m); err != nil {
			t.Fatalf("Route: %v", err)
		}

		buttons := reply.sendSnapshot()
		labels := labelsOf(buttons)
		want := []string{"Set @high", "Set @low", "Set @max", "⬅️ Close"}
		if len(labels) != len(want) {
			t.Fatalf("buttons = %v, want %v", labels, want)
		}
		for i := range want {
			if labels[i] != want[i] {
				t.Fatalf("buttons[%d] = %q, want %q", i, labels[i], want[i])
			}
		}
		if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "⚙️ Variants: zai-coding-plan/glm-5.2") {
			t.Fatalf("unexpected text: %v", reply.sends)
		}
		if !strings.Contains(reply.sends[0], "[max]   Reasoning effort: high") {
			t.Fatalf("text missing formatted variant details: %q", reply.sends[0])
		}
	})

	t.Run("with arg lists variants", func(t *testing.T) {
		r, client, _, overrides := newTestRouterWithAccess()
		client.providers = modelTestProviders()
		overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
			ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
		}
		reply := newBrowseReplyCtx()

		m := msg("/variants zai-coding-plan/glm-5.2", reply.fakeReplyCtx)
		m.ReplyCtx = reply
		if err := r.Route(context.Background(), m); err != nil {
			t.Fatalf("Route: %v", err)
		}

		buttons := reply.sendSnapshot()
		labels := labelsOf(buttons)
		want := []string{"Set @high", "Set @low", "Set @max", "⬅️ Close"}
		if len(labels) != len(want) {
			t.Fatalf("buttons = %v, want %v", labels, want)
		}
		for i := range want {
			if labels[i] != want[i] {
				t.Fatalf("buttons[%d] = %q, want %q", i, labels[i], want[i])
			}
		}
	})

	t.Run("model without variants", func(t *testing.T) {
		r, client, _, overrides := newTestRouterWithAccess()
		client.providers = modelTestProviders()
		overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
			ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
		}

		bReply := newBrowseReplyCtx()
		m := msg("/variants zai-coding-plan/no-var", bReply.fakeReplyCtx)
		m.ReplyCtx = bReply
		if err := r.Route(context.Background(), m); err != nil {
			t.Fatalf("Route: %v", err)
		}

		if len(bReply.sends) == 0 || !strings.Contains(bReply.sends[0], "No variants for zai-coding-plan/no-var") {
			t.Fatalf("unexpected reply: %v", bReply.sends)
		}
	})

	t.Run("set variant button callback sets personal override", func(t *testing.T) {
		r, client, _, overrideRepo := newTestRouterWithAccess()
		client.providers = modelTestProviders()
		overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{
			ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
		}
		reply := newBrowseReplyCtx()

		m := msg("/variants zai-coding-plan/glm-5.2", reply.fakeReplyCtx)
		m.ReplyCtx = reply
		if err := r.Route(context.Background(), m); err != nil {
			t.Fatalf("Route: %v", err)
		}

		buttons := reply.sendSnapshot()
		setToken := buttonValue(buttons, "Set @max")
		if setToken == "" {
			t.Fatalf("missing Set @max button in %v", labelsOf(buttons))
		}

		setReply := newBrowseReplyCtx()
		if err := r.Route(context.Background(), callbackMsg("user1", setToken, setReply)); err != nil {
			t.Fatalf("Route callback: %v", err)
		}

		text, _ := setReply.editSnapshot()
		if text != "✅ Personal model set: zai-coding-plan/glm-5.2@max" {
			t.Fatalf("text = %q, want ✅ Personal model set: zai-coding-plan/glm-5.2@max", text)
		}

		o := overrideRepo.overrides["telegram:chat1:user1"]
		if o == nil || o.Model != "zai-coding-plan/glm-5.2@max" {
			t.Fatalf("user1 stored model = %+v, want zai-coding-plan/glm-5.2@max", o)
		}
	})
}

func TestModelPrecedenceSessionPersonalChannelDefault(t *testing.T) {
	tests := []struct {
		name          string
		sessionModel  string
		personalModel string
		channelModel  string
		wantProvider  string
		wantModel     string
	}{
		{name: "session only", sessionModel: "openai/gpt-4o", wantProvider: "openai", wantModel: "gpt-4o"},
		{name: "session beats personal", sessionModel: "openai/gpt-4o", personalModel: "anthropic/claude-3", wantProvider: "openai", wantModel: "gpt-4o"},
		{name: "session beats channel", sessionModel: "openai/gpt-4o", channelModel: "zai-coding-plan/glm-5.2", wantProvider: "openai", wantModel: "gpt-4o"},
		{name: "session beats all", sessionModel: "openai/gpt-4o", personalModel: "anthropic/claude-3", channelModel: "zai-coding-plan/glm-5.2", wantProvider: "openai", wantModel: "gpt-4o"},
		{name: "personal only", personalModel: "anthropic/claude-3", wantProvider: "anthropic", wantModel: "claude-3"},
		{name: "personal beats channel", personalModel: "anthropic/claude-3", channelModel: "zai-coding-plan/glm-5.2", wantProvider: "anthropic", wantModel: "claude-3"},
		{name: "channel only", channelModel: "zai-coding-plan/glm-5.2", wantProvider: "zai-coding-plan", wantModel: "glm-5.2"},
		{name: "agent default"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, client, _, overrides := newTestRouterWithAccess()
			client.providers = modelTestProviders()
			ctx := context.Background()

			if tt.sessionModel != "" {
				if err := r.store.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
					t.Fatalf("SetActive: %v", err)
				}
				if err := r.store.SessionRepo().SetModel(ctx, "telegram", "chat1", "", "user1", tt.sessionModel); err != nil {
					t.Fatalf("SetModel: %v", err)
				}
			}
			overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
				ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin", Model: tt.personalModel,
			}
			if tt.channelModel != "" {
				r.store.(*fakeStore).channelRepo.channels["telegram:chat1"] = &store.Channel{
					ChannelID: "chat1", Platform: "telegram", Model: tt.channelModel, ListenMode: "mention",
				}
			}

			if err := r.Route(ctx, msgFrom("user1", "hello", &fakeReplyCtx{})); err != nil {
				t.Fatalf("Route: %v", err)
			}
			waitForDispatch(t, client)
			waitForResponse(t, r)

			if tt.wantProvider == "" {
				if client.lastModel != nil {
					t.Fatalf("expected agent default, got %+v", client.lastModel)
				}
				return
			}
			if client.lastModel == nil || client.lastModel.ProviderID != tt.wantProvider || client.lastModel.ID != tt.wantModel {
				t.Fatalf("model = %+v, want %s/%s", client.lastModel, tt.wantProvider, tt.wantModel)
			}
		})
	}
}

func TestSessionModelThreadIsolation(t *testing.T) {
	r, client, _, overrides := newTestRouterWithAccess()
	client.providers = modelTestProviders()
	ctx := context.Background()
	st := r.store.(*fakeStore)

	if err := r.store.SessionRepo().SetActive(ctx, "discord", "thread-a", "thread-a", "", "sess-a", 100); err != nil {
		t.Fatalf("SetActive thread-a: %v", err)
	}
	if err := r.store.SessionRepo().SetModel(ctx, "discord", "thread-a", "thread-a", "", "openai/gpt-4o"); err != nil {
		t.Fatalf("SetModel thread-a: %v", err)
	}
	if err := r.store.SessionRepo().SetActive(ctx, "discord", "thread-b", "thread-b", "", "sess-b", 100); err != nil {
		t.Fatalf("SetActive thread-b: %v", err)
	}
	if err := r.store.SessionRepo().SetModel(ctx, "discord", "thread-b", "thread-b", "", "anthropic/claude-3"); err != nil {
		t.Fatalf("SetModel thread-b: %v", err)
	}
	st.channelRepo.channels["discord:parent"] = &store.Channel{
		ChannelID: "parent", Platform: "discord", Model: "zai-coding-plan/glm-5.2", ListenMode: "mention",
	}
	overrides.overrides["discord:parent:user1"] = &store.UserOverride{
		ChannelID: "parent", Platform: "discord", UserID: "user1", Role: "admin", Model: "zai-coding-plan/glm-5.2@low",
	}
	overrides.overrides["discord:thread-a:user1"] = &store.UserOverride{
		ChannelID: "thread-a", Platform: "discord", UserID: "user1", Role: "allow",
	}
	overrides.overrides["discord:thread-b:user1"] = &store.UserOverride{
		ChannelID: "thread-b", Platform: "discord", UserID: "user1", Role: "allow",
	}

	threadMsg := func(threadID, text string) {
		m := channel.IncomingMessage{
			Platform:        "discord",
			ChannelID:       threadID,
			ParentChannelID: "parent",
			ThreadID:        threadID,
			UserID:          "user1",
			Text:            text,
			IsThread:        true,
			IsMention:       true,
			ReplyCtx:        &fakeReplyCtx{},
		}
		if err := r.Route(ctx, m); err != nil {
			t.Fatalf("Route %s: %v", threadID, err)
		}
		waitForDispatch(t, client)
		waitForResponse(t, r)
	}

	threadMsg("thread-a", "hello a")
	if client.lastModel == nil || client.lastModel.ProviderID != "openai" || client.lastModel.ID != "gpt-4o" {
		t.Fatalf("thread-a model = %+v, want openai/gpt-4o", client.lastModel)
	}

	threadMsg("thread-b", "hello b")
	if client.lastModel == nil || client.lastModel.ProviderID != "anthropic" || client.lastModel.ID != "claude-3" {
		t.Fatalf("thread-b model = %+v, want anthropic/claude-3", client.lastModel)
	}
}

func TestSessionSwitcherRestoresModel(t *testing.T) {
	r, client, _, overrides := newTestRouterWithAccess()
	client.providers = modelTestProviders()
	ctx := context.Background()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin",
	}

	if err := r.store.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatalf("SetActive sess-1: %v", err)
	}
	if err := r.store.SessionRepo().SetModel(ctx, "telegram", "chat1", "", "user1", "openai/gpt-4o"); err != nil {
		t.Fatalf("SetModel sess-1: %v", err)
	}
	if err := r.store.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "user1", "sess-2", 100); err != nil {
		t.Fatalf("SetActive sess-2: %v", err)
	}

	if err := r.Route(ctx, msgFrom("user1", "hello", &fakeReplyCtx{})); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.lastModel != nil {
		t.Fatalf("sess-2 model = %+v, want agent default", client.lastModel)
	}

	if err := r.store.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatalf("SetActive back to sess-1: %v", err)
	}
	if err := r.Route(ctx, msgFrom("user1", "hello again", &fakeReplyCtx{})); err != nil {
		t.Fatalf("Route after switch: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)
	if client.lastModel == nil || client.lastModel.ProviderID != "openai" || client.lastModel.ID != "gpt-4o" {
		t.Fatalf("restored model = %+v, want openai/gpt-4o", client.lastModel)
	}
}

func TestModelViewIncludesSessionTier(t *testing.T) {
	r, client, reply, overrides := newTestRouterWithAccess()
	client.providers = modelTestProviders()
	ctx := context.Background()
	if err := r.store.SessionRepo().SetActive(ctx, "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	if err := r.store.SessionRepo().SetModel(ctx, "telegram", "chat1", "", "user1", "openai/gpt-4o"); err != nil {
		t.Fatalf("SetModel: %v", err)
	}
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin", Model: "anthropic/claude-3",
	}
	r.store.(*fakeStore).channelRepo.channels["telegram:chat1"] = &store.Channel{
		ChannelID: "chat1", Platform: "telegram", Model: "zai-coding-plan/glm-5.2", ListenMode: "mention",
	}

	if err := r.Route(ctx, msg("/model", reply)); err != nil {
		t.Fatalf("Route view: %v", err)
	}
	if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "openai/gpt-4o") {
		t.Fatalf("expected session model in view, got %v", reply.sends)
	}
}
