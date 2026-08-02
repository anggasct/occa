package router

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

type browseReplyCtx struct {
	*fakeReplyCtx
	mu              sync.Mutex
	lastSendButtons []channel.Button
	lastEditText    string
	lastEditButtons []channel.Button
	editCount       int
}

func newBrowseReplyCtx() *browseReplyCtx {
	return &browseReplyCtx{fakeReplyCtx: &fakeReplyCtx{}}
}

func (b *browseReplyCtx) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastSendButtons = append([]channel.Button(nil), buttons...)
	return b.fakeReplyCtx.SendWithButtons(text, buttons)
}

func (b *browseReplyCtx) EditWithButtons(ref channel.MessageRef, text string, buttons []channel.Button) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastEditText = text
	b.lastEditButtons = append([]channel.Button(nil), buttons...)
	b.editCount++
	return b.fakeReplyCtx.EditWithButtons(ref, text, buttons)
}

func (b *browseReplyCtx) sendSnapshot() []channel.Button {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]channel.Button(nil), b.lastSendButtons...)
}

func (b *browseReplyCtx) editSnapshot() (string, []channel.Button) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastEditText, append([]channel.Button(nil), b.lastEditButtons...)
}

func buttonValue(buttons []channel.Button, label string) string {
	for _, b := range buttons {
		if b.Label == label {
			return b.Value
		}
	}
	return ""
}

func labelsOf(buttons []channel.Button) []string {
	out := make([]string, 0, len(buttons))
	for _, b := range buttons {
		out = append(out, b.Label)
	}
	return out
}

func browseProviders() relay.Providers {
	models := func(ids ...string) map[string]json.RawMessage {
		m := make(map[string]json.RawMessage, len(ids))
		for _, id := range ids {
			m[id] = json.RawMessage(`{}`)
		}
		return m
	}
	return relay.Providers{All: []relay.Provider{
		{ID: "anthropic", Models: models("claude-3", "claude-4")},
		{ID: "openai", Models: models("gpt-4o", "gpt-4-turbo")},
		{ID: "zai", Models: models("glm-5")},
	}}
}

func callbackMsg(userID, data string, reply *browseReplyCtx) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       userID,
		IsCallback:   true,
		CallbackData: data,
		CallbackRef:  fakeRef{id: "1"},
		IsMention:    true,
		ReplyCtx:     reply,
	}
}

func TestModelBrowserOpenShowsProviders(t *testing.T) {
	r, client, _ := newTestRouter()
	client.providers = browseProviders()
	reply := newBrowseReplyCtx()

	m := msg("/occa:model", reply.fakeReplyCtx)
	m.ReplyCtx = reply
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}

	buttons := reply.sendSnapshot()
	labels := labelsOf(buttons)
	want := []string{"anthropic", "openai", "zai", "✖️ Close"}
	if len(labels) != len(want) {
		t.Fatalf("buttons = %v, want %v", labels, want)
	}
	for i := range want {
		if labels[i] != want[i] {
			t.Fatalf("buttons = %v, want %v", labels, want)
		}
	}
	if reply.lastSendButtons == nil {
		t.Fatal("expected buttoned send")
	}
}

func TestModelBrowserProviderToModels(t *testing.T) {
	r, client, _ := newTestRouter()
	client.providers = browseProviders()
	reply := newBrowseReplyCtx()

	m := msg("/occa:model", reply.fakeReplyCtx)
	m.ReplyCtx = reply
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}

	data := buttonValue(reply.sendSnapshot(), "openai")
	if data == "" {
		t.Fatal("missing provider button")
	}
	editReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", data, editReply)); err != nil {
		t.Fatalf("Route callback: %v", err)
	}

	text, buttons := editReply.editSnapshot()
	if text != "Provider: openai — select model:" {
		t.Fatalf("text = %q", text)
	}
	labels := labelsOf(buttons)
	want := []string{"gpt-4-turbo", "gpt-4o", "⬅️ Providers", "✖️ Close"}
	if len(labels) != len(want) {
		t.Fatalf("buttons = %v, want %v", labels, want)
	}
	for i := range want {
		if labels[i] != want[i] {
			t.Fatalf("buttons = %v, want %v", labels, want)
		}
	}
}

func TestModelBrowserSetPersonalForCaller(t *testing.T) {
	r, client, _, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin"}
	overrideRepo.overrides["telegram:chat1:user2"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user2", Role: "allow"}
	client.providers = browseProviders()
	reply := newBrowseReplyCtx()

	m := msg("/occa:model", reply.fakeReplyCtx)
	m.ReplyCtx = reply
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	provToken := buttonValue(reply.sendSnapshot(), "openai")
	modelsReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user2", provToken, modelsReply)); err != nil {
		t.Fatalf("Route models: %v", err)
	}
	_, modelButtons := modelsReply.editSnapshot()
	setToken := buttonValue(modelButtons, "gpt-4o")

	setReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user2", setToken, setReply)); err != nil {
		t.Fatalf("Route set: %v", err)
	}
	text, buttons := setReply.editSnapshot()
	if text != "✅ Personal model set: openai/gpt-4o" {
		t.Fatalf("text = %q", text)
	}
	if len(buttons) != 0 {
		t.Fatalf("buttons = %v, want removed", buttons)
	}
	o := overrideRepo.overrides["telegram:chat1:user2"]
	if o == nil || o.Model != "openai/gpt-4o" {
		t.Fatalf("user2 model = %+v, want openai/gpt-4o", o)
	}
	if u1 := overrideRepo.overrides["telegram:chat1:user1"]; u1 != nil && u1.Model != "" {
		t.Fatalf("user1 model must stay untouched, got %+v", u1)
	}
}

func TestModelBrowserInvalidModelKeepsButtons(t *testing.T) {
	r, client, _ := newTestRouter()
	client.providers = browseProviders()
	token, err := r.modelBrowser.register(modelBrowseAction{kind: "set", page: 0, providerID: "openai", modelID: "nope"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	reply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", "model:"+token, reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	text, buttons := reply.editSnapshot()
	if text == "" || len(buttons) == 0 {
		t.Fatalf("expected error view with buttons kept, got %q %v", text, buttons)
	}
}

func TestModelBrowserPagination(t *testing.T) {
	providers := browseProviders()
	for i := 0; i < 15; i++ {
		providers.All = append(providers.All, relay.Provider{ID: "provider-" + string(rune('a'+i)), Models: map[string]json.RawMessage{"m": json.RawMessage(`{}`)}})
	}
	r, client, _ := newTestRouter()
	client.providers = providers
	reply := newBrowseReplyCtx()

	m := msg("/occa:model", reply.fakeReplyCtx)
	m.ReplyCtx = reply
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	page0 := reply.sendSnapshot()
	if len(page0) != 12 { // 10 providers + Next + Close
		t.Fatalf("page 0 buttons = %d, want 12 (%v)", len(page0), labelsOf(page0))
	}
	if len(page0) > 13 {
		t.Fatalf("page 0 exceeds button cap: %d", len(page0))
	}
	next := buttonValue(page0, "Next ▶️")
	page1Reply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", next, page1Reply)); err != nil {
		t.Fatalf("Route next: %v", err)
	}
	_, page1 := page1Reply.editSnapshot()
	if len(page1) != 10 { // 8 providers (18 total) + Prev + Close
		t.Fatalf("page 1 buttons = %d, want 10 (%v)", len(page1), labelsOf(page1))
	}
	if buttonValue(page1, "◀️ Prev") == "" {
		t.Fatalf("page 1 missing prev: %v", labelsOf(page1))
	}
	if buttonValue(page1, "Next ▶️") != "" {
		t.Fatalf("page 1 must not have next: %v", labelsOf(page1))
	}
}

func TestModelBrowserCloseAndStale(t *testing.T) {
	r, client, _ := newTestRouter()
	client.providers = browseProviders()
	reply := newBrowseReplyCtx()

	m := msg("/occa:model", reply.fakeReplyCtx)
	m.ReplyCtx = reply
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	closeToken := buttonValue(reply.sendSnapshot(), "✖️ Close")
	closeReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", closeToken, closeReply)); err != nil {
		t.Fatalf("Route close: %v", err)
	}
	text, buttons := closeReply.editSnapshot()
	if len(buttons) != 0 {
		t.Fatalf("close must remove buttons, got %v", labelsOf(buttons))
	}
	if text != "🤖 Model: agent default\n\nSelect provider:" {
		t.Fatalf("close must keep the page text, got %q", text)
	}

	staleReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", "model:not-a-real-token", staleReply)); err != nil {
		t.Fatalf("Route stale: %v", err)
	}
	text, buttons = staleReply.editSnapshot()
	if len(buttons) != 0 {
		t.Fatalf("stale must remove buttons, got %v", labelsOf(buttons))
	}
	if text == "" {
		t.Fatal("stale must render the current-model view")
	}
}

func TestModelBrowserBackToProviders(t *testing.T) {
	r, client, _ := newTestRouter()
	client.providers = browseProviders()
	reply := newBrowseReplyCtx()

	m := msg("/occa:model", reply.fakeReplyCtx)
	m.ReplyCtx = reply
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}
	provToken := buttonValue(reply.sendSnapshot(), "zai")
	modelsReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", provToken, modelsReply)); err != nil {
		t.Fatalf("Route models: %v", err)
	}
	_, modelButtons := modelsReply.editSnapshot()
	back := buttonValue(modelButtons, "⬅️ Providers")
	backReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", back, backReply)); err != nil {
		t.Fatalf("Route back: %v", err)
	}
	text, buttons := backReply.editSnapshot()
	if text != "🤖 Model: agent default\n\nSelect provider:" {
		t.Fatalf("back text = %q", text)
	}
	if buttonValue(buttons, "openai") == "" {
		t.Fatalf("back must show providers, got %v", labelsOf(buttons))
	}
}
