package router

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

func TestParseModelSearch(t *testing.T) {
	tests := []struct {
		name     string
		parts    []string
		provider string
		query    string
		search   bool
		usage    bool
	}{
		{name: "shorthand", parts: []string{"openrouter", "claude", "sonnet"}, provider: "openrouter", query: "claude sonnet", search: true},
		{name: "explicit", parts: []string{"search", "openrouter", "gemini"}, provider: "openrouter", query: "gemini", search: true},
		{name: "explicit missing query", parts: []string{"search", "openrouter"}, usage: true},
		{name: "exact model", parts: []string{"openrouter/model"}},
		{name: "malformed exact model", parts: []string{"openrouter/model", "extra"}},
		{name: "channel subcommand", parts: []string{"channel", "openrouter/model"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, query, search, usage := parseModelSearch(tt.parts)
			if provider != tt.provider || query != tt.query || search != tt.search || usage != tt.usage {
				t.Fatalf("parseModelSearch = %q, %q, %v, %v", provider, query, search, usage)
			}
		})
	}
}

func TestSearchModelIDsRanksMatches(t *testing.T) {
	models := map[string]json.RawMessage{
		"qwen":          nil,
		"qwen-z":        nil,
		"qwen-a":        nil,
		"vendor/qwen-2": nil,
		"old-qwen":      nil,
		"QWEN-legacy":   nil,
		"other":         nil,
	}

	got := SearchModelIDs(models, "QwEn")
	want := []string{"qwen", "QWEN-legacy", "qwen-a", "qwen-z", "vendor/qwen-2", "old-qwen"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("SearchModelIDs = %v, want %v", got, want)
	}
}

func searchProviders() map[string]json.RawMessage {
	models := make(map[string]json.RawMessage)
	for i := 0; i < 12; i++ {
		models["qwen-"+string(rune('a'+i))] = json.RawMessage(`{}`)
	}
	models["claude-sonnet"] = json.RawMessage(`{"variants":{"high":{}}}`)
	models["other-model"] = json.RawMessage(`{}`)
	return models
}

func relayProvidersWithSearchModels() relay.Providers {
	return relay.Providers{
		All: []relay.Provider{
			{ID: "openrouter", Models: searchProviders()},
			{ID: "anthropic", Models: map[string]json.RawMessage{"claude-3": json.RawMessage(`{}`)}},
		},
		Connected: []string{"openrouter"},
	}
}

func browseCommand(text string, reply *browseReplyCtx) channel.IncomingMessage {
	message := msg(text, reply.fakeReplyCtx)
	message.ReplyCtx = reply
	return message
}

func TestModelSearchFiltersAndPaginatesResults(t *testing.T) {
	r, client, _, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{Platform: "telegram", ChannelID: "chat1", UserID: "user1", Role: "allow"}
	client.providers = relayProvidersWithSearchModels()
	reply := newBrowseReplyCtx()

	if err := r.Route(context.Background(), browseCommand("/model openrouter qwen", reply)); err != nil {
		t.Fatalf("Route search: %v", err)
	}
	if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "Found 12 models · Page 1/2") {
		t.Fatalf("search view = %v", reply.sends)
	}
	buttons := reply.sendSnapshot()
	if len(buttons) != 13 {
		t.Fatalf("search page 1 buttons = %d (%v), want 13", len(buttons), labelsOf(buttons))
	}
	if buttonValue(buttons, "Next ▶️") == "" || buttonValue(buttons, "◀️ Prev") != "" {
		t.Fatalf("search page 1 navigation = %v", labelsOf(buttons))
	}
	for _, label := range labelsOf(buttons) {
		if label == "other-model" || label == "claude-sonnet" {
			t.Fatalf("unmatched model in search page: %q", label)
		}
	}

	nextReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", buttonValue(buttons, "Next ▶️"), nextReply)); err != nil {
		t.Fatalf("Route search next: %v", err)
	}
	text, nextButtons := nextReply.editSnapshot()
	if !strings.Contains(text, "Found 12 models · Page 2/2") {
		t.Fatalf("search page 2 text = %q", text)
	}
	if buttonValue(nextButtons, "◀️ Prev") == "" || buttonValue(nextButtons, "Next ▶️") != "" {
		t.Fatalf("search page 2 navigation = %v", labelsOf(nextButtons))
	}
}

func TestModelSearchRejectsDisconnectedProviderWithoutMutation(t *testing.T) {
	r, client, _, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{Platform: "telegram", ChannelID: "chat1", UserID: "user1", Role: "allow"}
	providers := relayProvidersWithSearchModels()
	providers.Connected = []string{"anthropic"}
	client.providers = providers
	reply := newBrowseReplyCtx()

	if err := r.Route(context.Background(), browseCommand("/model openrouter qwen", reply)); err != nil {
		t.Fatalf("Route search: %v", err)
	}
	if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "unknown or not connected") {
		t.Fatalf("disconnected provider view = %v", reply.sends)
	}
	if buttonValue(reply.sendSnapshot(), "openrouter") != "" {
		t.Fatalf("disconnected provider appeared in browser: %v", labelsOf(reply.sendSnapshot()))
	}
	if stored := overrides.overrides["telegram:chat1:user1"]; stored.Model != "" {
		t.Fatalf("search mutated model = %q", stored.Model)
	}
}

func TestModelSearchNoMatchesShowsEmptyViewWithoutMutation(t *testing.T) {
	r, client, _, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{Platform: "telegram", ChannelID: "chat1", UserID: "user1", Role: "allow"}
	client.providers = relayProvidersWithSearchModels()
	reply := newBrowseReplyCtx()

	if err := r.Route(context.Background(), browseCommand("/model openrouter nonexistent", reply)); err != nil {
		t.Fatalf("Route search: %v", err)
	}
	if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "no matching models") {
		t.Fatalf("empty search view = %v", reply.sends)
	}
	if buttonValue(reply.sendSnapshot(), "⬅️ Providers") == "" {
		t.Fatalf("empty search view missing provider navigation: %v", labelsOf(reply.sendSnapshot()))
	}
	if stored := overrides.overrides["telegram:chat1:user1"]; stored.Model != "" {
		t.Fatalf("empty search mutated model = %q", stored.Model)
	}
}

func TestModelSearchCallbackRevalidatesStaleModel(t *testing.T) {
	r, client, _, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{Platform: "telegram", ChannelID: "chat1", UserID: "user1", Role: "allow"}
	client.providers = relayProvidersWithSearchModels()
	reply := newBrowseReplyCtx()

	if err := r.Route(context.Background(), browseCommand("/model search openrouter claude", reply)); err != nil {
		t.Fatalf("Route search: %v", err)
	}
	modelToken := buttonValue(reply.sendSnapshot(), "claude-sonnet")
	if modelToken == "" {
		t.Fatalf("missing search result: %v", labelsOf(reply.sendSnapshot()))
	}
	client.providers.All[0].Models = map[string]json.RawMessage{"other-model": json.RawMessage(`{}`)}

	callbackReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", modelToken, callbackReply)); err != nil {
		t.Fatalf("Route stale callback: %v", err)
	}
	text, buttons := callbackReply.editSnapshot()
	if !strings.Contains(text, "Model unavailable") || len(buttons) == 0 {
		t.Fatalf("stale callback view = %q %v", text, labelsOf(buttons))
	}
	if stored := overrides.overrides["telegram:chat1:user1"]; stored.Model != "" {
		t.Fatalf("stale callback mutated model = %q", stored.Model)
	}
}

func TestModelSearchVariantSelectionUsesExistingSetPath(t *testing.T) {
	r, client, _, overrides := newTestRouterWithAccess()
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{Platform: "telegram", ChannelID: "chat1", UserID: "user1", Role: "admin"}
	client.providers = relayProvidersWithSearchModels()
	reply := newBrowseReplyCtx()

	if err := r.Route(context.Background(), browseCommand("/model openrouter claude", reply)); err != nil {
		t.Fatalf("Route search: %v", err)
	}
	modelToken := buttonValue(reply.sendSnapshot(), "claude-sonnet")
	variantsReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", modelToken, variantsReply)); err != nil {
		t.Fatalf("Route variants: %v", err)
	}
	_, variantButtons := variantsReply.editSnapshot()
	variantToken := buttonValue(variantButtons, "Set @high")
	if variantToken == "" {
		t.Fatalf("variant buttons = %v", labelsOf(variantButtons))
	}
	setReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", variantToken, setReply)); err != nil {
		t.Fatalf("Route variant set: %v", err)
	}
	setText, _ := setReply.editSnapshot()
	if !strings.Contains(setText, "openrouter/claude-sonnet@high") {
		t.Fatalf("variant set reply = %q", setText)
	}
	if got := r.store.(*fakeStore).channelRepo.channels["telegram:chat1"].Model; got != "openrouter/claude-sonnet@high" {
		t.Fatalf("stored model = %q", got)
	}
}
