package router

import (
	"context"
	"encoding/json"
	"strings"
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
	p := relay.Providers{All: []relay.Provider{
		{ID: "anthropic", Models: models("claude-3", "claude-4")},
		{ID: "openai", Models: models("gpt-4o", "gpt-4-turbo")},
		{ID: "zai", Models: models("glm-5")},
	}}
	p.All[2].Models["glm-5.2"] = json.RawMessage(`{"variants":{"high":{"reasoningEffort":"high"},"max":{"reasoningEffort":"high"},"low":{"reasoningEffort":"low"}}}`)
	return p
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
	if text != "✅ Personal model set: openai/gpt-4o\nScope: personal override" {
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

func TestModelBrowserRoundTripsSlashContainingModelID(t *testing.T) {
	r, client, _, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "allow",
	}
	providers := browseProviders()
	providers.All = append(providers.All, relay.Provider{
		ID:     "openrouter",
		Models: map[string]json.RawMessage{"stealth/ox-alpha": json.RawMessage(`{}`)},
	})
	client.providers = providers

	openReply := newBrowseReplyCtx()
	m := msg("/model", openReply.fakeReplyCtx)
	m.ReplyCtx = openReply
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route open browser: %v", err)
	}

	providerToken := buttonValue(openReply.sendSnapshot(), "openrouter")
	if providerToken == "" {
		t.Fatal("missing openrouter provider button")
	}
	modelsReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", providerToken, modelsReply)); err != nil {
		t.Fatalf("Route provider callback: %v", err)
	}
	_, modelButtons := modelsReply.editSnapshot()
	modelToken := buttonValue(modelButtons, "stealth/ox-alpha")
	if modelToken == "" {
		t.Fatalf("missing slash-containing model button: %v", labelsOf(modelButtons))
	}

	setReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", modelToken, setReply)); err != nil {
		t.Fatalf("Route model callback: %v", err)
	}
	text, buttons := setReply.editSnapshot()
	if text != "✅ Personal model set: openrouter/stealth/ox-alpha\nScope: personal override" {
		t.Fatalf("set text = %q", text)
	}
	if len(buttons) != 0 {
		t.Fatalf("set buttons = %v, want removed", buttons)
	}
	if got := overrideRepo.overrides["telegram:chat1:user1"].Model; got != "openrouter/stealth/ox-alpha" {
		t.Fatalf("stored model = %q, want openrouter/stealth/ox-alpha", got)
	}

	resolution, err := r.resolveModel(context.Background(), msg("hello", &fakeReplyCtx{}))
	if err != nil {
		t.Fatalf("resolveModel: %v", err)
	}
	if resolution.model == nil || formatModelRef(*resolution.model) != "openrouter/stealth/ox-alpha" {
		t.Fatalf("resolved model = %+v", resolution.model)
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
	if text != "🤖 Model: agent default\nSource: agent default\n\nSelect provider:" {
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
	if text != "🤖 Model: agent default\nSource: agent default\n\nSelect provider:" {
		t.Fatalf("back text = %q", text)
	}
	if buttonValue(buttons, "openai") == "" {
		t.Fatalf("back must show providers, got %v", labelsOf(buttons))
	}
}

// TestModelBrowserShowsOnlyConnectedProviders: when the agent reports a
// connected list, the browser shows only those providers — the rest of the
// catalog is unusable (no credentials).
func TestModelBrowserShowsOnlyConnectedProviders(t *testing.T) {
	providers := browseProviders()
	providers.Connected = []string{"openai"}
	r, client, _ := newTestRouter()
	client.providers = providers
	reply := newBrowseReplyCtx()

	m := msg("/occa:model", reply.fakeReplyCtx)
	m.ReplyCtx = reply
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}

	labels := labelsOf(reply.sendSnapshot())
	want := []string{"openai", "✖️ Close"}
	if len(labels) != len(want) {
		t.Fatalf("buttons = %v, want %v", labels, want)
	}
	for i := range want {
		if labels[i] != want[i] {
			t.Fatalf("buttons = %v, want %v", labels, want)
		}
	}
}

// TestModelBrowserRowLayout: item buttons pair up in two columns (Row i/2+1)
// on Telegram; nav buttons share one row regardless of position.
func TestModelBrowserRowLayout(t *testing.T) {
	providers := browseProviders()
	r, client, _ := newTestRouter()
	client.providers = providers
	reply := newBrowseReplyCtx()

	m := msg("/occa:model", reply.fakeReplyCtx)
	m.ReplyCtx = reply
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}

	buttons := reply.sendSnapshot()
	itemIdx := 0
	for _, b := range buttons {
		if modelNavLabel(b.Label) {
			if b.Row != modelBrowserNavRow {
				t.Fatalf("nav button %q row = %d, want %d", b.Label, b.Row, modelBrowserNavRow)
			}
			continue
		}
		if want := itemIdx/2 + 1; b.Row != want {
			t.Fatalf("item %q row = %d, want %d", b.Label, b.Row, want)
		}
		itemIdx++
	}
}

// TestModelBrowserRowLayoutDiscord: on Discord a full page packs 5 items per
// row, so a 10-item page stays at 2 item rows + 1 nav row = 3 action rows
// (Discord caps messages at 5 action rows).
func TestModelBrowserRowLayoutDiscord(t *testing.T) {
	providers := browseProviders()
	for i := 0; i < 10; i++ {
		providers.All = append(providers.All, relay.Provider{ID: "provider-" + string(rune('a'+i)), Models: map[string]json.RawMessage{"m": json.RawMessage(`{}`)}})
	}
	r, client, _, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["discord:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "discord", UserID: "user1", Role: "admin"}
	client.providers = providers
	reply := newBrowseReplyCtx()

	m := channel.IncomingMessage{
		Platform:  "discord",
		ChannelID: "chat1",
		UserID:    "user1",
		Text:      "/occa:model",
		IsMention: true,
		ReplyCtx:  reply,
	}
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}

	buttons := reply.sendSnapshot()
	rows := map[int]int{}
	itemIdx := 0
	for _, b := range buttons {
		rows[b.Row]++
		if modelNavLabel(b.Label) {
			continue
		}
		if want := itemIdx/5 + 1; b.Row != want {
			t.Fatalf("item %q row = %d, want %d", b.Label, b.Row, want)
		}
		itemIdx++
	}
	if len(rows) > 5 {
		t.Fatalf("discord action rows = %d, want <= 5 (%v)", len(rows), rows)
	}
	if rows[modelBrowserNavRow] > 5 {
		t.Fatalf("nav row has %d buttons, want <= 5", rows[modelBrowserNavRow])
	}
}

func modelNavLabel(label string) bool {
	switch label {
	case "◀️ Prev", "Next ▶️", "✖️ Close", "⬅️ Close", "⬅️ Providers", "⬅️ Models":
		return true
	}
	return false
}

func TestModelBrowserVariantSelection(t *testing.T) {
	r, client, _, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{ChannelID: "chat1", Platform: "telegram", UserID: "user1", Role: "admin"}
	client.providers = browseProviders()
	reply := newBrowseReplyCtx()

	m := msg("/model", reply.fakeReplyCtx)
	m.ReplyCtx = reply
	if err := r.Route(context.Background(), m); err != nil {
		t.Fatalf("Route: %v", err)
	}

	zaiToken := buttonValue(reply.sendSnapshot(), "zai")
	if zaiToken == "" {
		t.Fatal("missing zai provider button")
	}

	modelsReply := newBrowseReplyCtx()
	if err := r.Route(context.Background(), callbackMsg("user1", zaiToken, modelsReply)); err != nil {
		t.Fatalf("Route models: %v", err)
	}
	_, modelButtons := modelsReply.editSnapshot()

	t.Run("model with variants renders variants view and sets variant", func(t *testing.T) {
		glm52Token := buttonValue(modelButtons, "glm-5.2")
		if glm52Token == "" {
			t.Fatalf("missing glm-5.2 button in %v", labelsOf(modelButtons))
		}

		variantsReply := newBrowseReplyCtx()
		if err := r.Route(context.Background(), callbackMsg("user1", glm52Token, variantsReply)); err != nil {
			t.Fatalf("Route glm-5.2: %v", err)
		}

		text, variantButtons := variantsReply.editSnapshot()
		if !strings.Contains(text, "⚙️ Variants: zai/glm-5.2") {
			t.Fatalf("text = %q, want variants header", text)
		}

		labels := labelsOf(variantButtons)
		want := []string{"Set @high", "Set @low", "Set @max", "⬅️ Models", "⬅️ Close"}
		if len(labels) != len(want) {
			t.Fatalf("variant buttons = %v, want %v", labels, want)
		}

		// Test back to models button
		backToken := buttonValue(variantButtons, "⬅️ Models")
		backReply := newBrowseReplyCtx()
		if err := r.Route(context.Background(), callbackMsg("user1", backToken, backReply)); err != nil {
			t.Fatalf("Route back: %v", err)
		}
		backText, _ := backReply.editSnapshot()
		if backText != "Provider: zai — select model:" {
			t.Fatalf("back text = %q, want models view", backText)
		}

		// Now set variant
		setHighToken := buttonValue(variantButtons, "Set @high")
		setReply := newBrowseReplyCtx()
		if err := r.Route(context.Background(), callbackMsg("user1", setHighToken, setReply)); err != nil {
			t.Fatalf("Route set high: %v", err)
		}

		setText, _ := setReply.editSnapshot()
		if setText != "✅ Channel model set: zai/glm-5.2@high\nScope: this channel" {
			t.Fatalf("setText = %q", setText)
		}
		if ch := r.store.(*fakeStore).channelRepo.channels["telegram:chat1"]; ch == nil || ch.Model != "zai/glm-5.2@high" {
			t.Fatalf("channel stored model = %+v, want zai/glm-5.2@high", ch)
		}
	})

	t.Run("model without variants sets model directly", func(t *testing.T) {
		glm5Token := buttonValue(modelButtons, "glm-5")
		if glm5Token == "" {
			t.Fatalf("missing glm-5 button in %v", labelsOf(modelButtons))
		}

		setReply := newBrowseReplyCtx()
		if err := r.Route(context.Background(), callbackMsg("user1", glm5Token, setReply)); err != nil {
			t.Fatalf("Route glm-5 set: %v", err)
		}

		setText, _ := setReply.editSnapshot()
		if setText != "✅ Channel model set: zai/glm-5\nScope: this channel" {
			t.Fatalf("setText = %q", setText)
		}
		if ch := r.store.(*fakeStore).channelRepo.channels["telegram:chat1"]; ch == nil || ch.Model != "zai/glm-5" {
			t.Fatalf("channel stored model = %+v, want zai/glm-5", ch)
		}
	})
}
