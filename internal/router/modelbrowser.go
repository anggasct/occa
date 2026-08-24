package router

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
)

const (
	modelBrowserNavRow  = 100
	modelCallbackPrefix = "model:"
	modelBrowserCap     = 1000
	modelBrowserTTL     = 30 * time.Minute
	modelBrowserPage    = 10
)

// errReplied signals a command handler that already sent its own reply
// through the reply context; the command dispatcher must not reply again.
var errReplied = errors.New("command already replied")

type modelBrowseAction struct {
	kind       string // "providers" | "models" | "set" | "close" | "variants"
	pageKind   string // view being closed ("providers" | "models" | "variants"); set on close actions
	page       int
	providerID string
	query      string
	modelID    string
	variant    string
	createdAt  time.Time
}

// modelBrowserBroker maps short callback tokens to browse actions so
// callback payloads stay far below the platforms' data limits (Telegram
// 64 bytes / Discord 100 chars). Entries are TTL-capped like the permission
// broker; a stale token simply renders the fallback view.
type modelBrowserBroker struct {
	mu     sync.Mutex
	tokens map[string]modelBrowseAction
}

func newModelBrowserBroker() *modelBrowserBroker {
	return &modelBrowserBroker{tokens: make(map[string]modelBrowseAction)}
}

func (b *modelBrowserBroker) register(action modelBrowseAction) (string, error) {
	action.createdAt = time.Now()
	buf := make([]byte, 9)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(buf)

	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.tokens) >= modelBrowserCap {
		var oldest string
		var oldestAt time.Time
		for t, a := range b.tokens {
			if oldest == "" || a.createdAt.Before(oldestAt) {
				oldest, oldestAt = t, a.createdAt
			}
		}
		delete(b.tokens, oldest)
	}
	b.tokens[token] = action
	return token, nil
}

func (b *modelBrowserBroker) lookup(token string) (modelBrowseAction, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	action, ok := b.tokens[token]
	if !ok {
		return modelBrowseAction{}, false
	}
	if time.Since(action.createdAt) > modelBrowserTTL {
		delete(b.tokens, token)
		return modelBrowseAction{}, false
	}
	return action, true
}

// openModelBrowser starts the interactive picker. Falls back silently to the
// static view (returns nil, no errReplied) when the agent is unreachable.
func (r *Router) openModelBrowser(ctx context.Context, msg channel.IncomingMessage) error {
	view, err := r.viewModel(ctx, msg)
	if err != nil {
		return err
	}
	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return nil
	}
	defer inst.End()
	providers, err := inst.Client().Providers(ctx)
	if err != nil {
		return nil
	}
	text, buttons, err := r.modelProvidersView(msg.Platform, providers, 0, view, false)
	if err != nil {
		return err
	}
	if _, err := msg.ReplyCtx.SendWithButtons(text, buttons); err != nil {
		return err
	}
	return errReplied
}

func (r *Router) openModelSearch(ctx context.Context, msg channel.IncomingMessage, providerID, query string) error {
	providers, err := r.modelBrowserProviders(ctx, msg)
	if err != nil {
		return safeReplyError("Agent unreachable", fmt.Errorf("model search: list providers: %w", err))
	}
	if _, ok := searchProviderByID(providers, providerID); !ok {
		prefix := fmt.Sprintf("⚠️ Provider %q is unknown or not connected.", providerID)
		text, buttons, viewErr := r.modelProvidersView(msg.Platform, providers, 0, prefix, false)
		if viewErr != nil {
			return viewErr
		}
		if _, sendErr := msg.ReplyCtx.SendWithButtons(text, buttons); sendErr != nil {
			return sendErr
		}
		return errReplied
	}

	text, buttons, err := r.modelModelsViewWithQuery(msg.Platform, providers, providerID, 0, false, "", query)
	if err != nil {
		return err
	}
	if _, err := msg.ReplyCtx.SendWithButtons(text, buttons); err != nil {
		return err
	}
	return errReplied
}

func (r *Router) handleModelCallback(ctx context.Context, msg channel.IncomingMessage) error {
	if msg.CallbackRef == nil || msg.ReplyCtx == nil {
		return nil
	}
	token := strings.TrimPrefix(msg.CallbackData, modelCallbackPrefix)
	action, ok := r.modelBrowser.lookup(token)
	if !ok {
		slog.Debug("model browser: stale callback", "platform", msg.Platform, "channel_id", msg.ChannelID)
		return r.modelBrowserFallback(ctx, msg)
	}
	switch action.kind {
	case "providers":
		return r.modelBrowserRenderProviders(ctx, msg, action.page)
	case "models":
		return r.modelBrowserRenderModels(ctx, msg, action.providerID, action.page, action.query)
	case "variants":
		return r.modelBrowserRenderVariants(ctx, msg, action.providerID, action.modelID, action.page, action.query)
	case "set":
		return r.modelBrowserSet(ctx, msg, action)
	case "close":
		return r.modelBrowserClose(ctx, msg, action)
	}
	return nil
}

// modelBrowserFallback replaces the stale message's buttons with the static
// current-model view.
func (r *Router) modelBrowserFallback(ctx context.Context, msg channel.IncomingMessage) error {
	text, err := r.viewModel(ctx, msg)
	if err != nil {
		text = "🤖 Model: agent default"
	}
	return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, nil)
}

func (r *Router) modelBrowserRenderProviders(ctx context.Context, msg channel.IncomingMessage, page int) error {
	providers, err := r.modelBrowserProviders(ctx, msg)
	if err != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Agent unreachable", nil)
	}
	view, err := r.viewModel(ctx, msg)
	if err != nil {
		view = "🤖 Model: agent default"
	}
	text, buttons, err := r.modelProvidersView(msg.Platform, providers, page, view, false)
	if err != nil {
		return err
	}
	return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
}

func (r *Router) modelBrowserRenderModels(ctx context.Context, msg channel.IncomingMessage, providerID string, page int, query string) error {
	providers, err := r.modelBrowserProviders(ctx, msg)
	if err != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Agent unreachable", nil)
	}
	text, buttons, err := r.modelModelsViewWithQuery(msg.Platform, providers, providerID, page, false, "", query)
	if err != nil {
		return err
	}
	return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
}

func (r *Router) modelBrowserRenderVariants(ctx context.Context, msg channel.IncomingMessage, providerID, modelID string, page int, query string) error {
	providers, err := r.modelBrowserProviders(ctx, msg)
	if err != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Agent unreachable", nil)
	}
	if query != "" && !searchModelAvailable(providers, providerID, modelID, query) {
		text, buttons, viewErr := r.modelModelsViewWithQuery(msg.Platform, providers, providerID, page, false, "⚠️ Model unavailable. Try again.\n", query)
		if viewErr != nil {
			return viewErr
		}
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
	}
	text, buttons, err := r.modelVariantsViewWithQuery(msg.Platform, providers, providerID, modelID, page, false, true, "", query)
	if err != nil {
		return err
	}
	return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
}

func (r *Router) modelBrowserSet(ctx context.Context, msg channel.IncomingMessage, action modelBrowseAction) error {
	providers, pErr := r.modelBrowserProviders(ctx, msg)
	if pErr != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Agent unreachable", nil)
	}
	if action.query != "" && !searchModelAvailable(providers, action.providerID, action.modelID, action.query) {
		text, buttons, vErr := r.modelModelsViewWithQuery(msg.Platform, providers, action.providerID, action.page, false, "⚠️ Model unavailable. Try again.\n", action.query)
		if vErr != nil {
			return vErr
		}
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
	}
	if action.variant == "" {
		if variants, ok := providers.Variants(action.providerID, action.modelID); ok && len(variants) > 0 {
			text, buttons, vErr := r.modelVariantsViewWithQuery(msg.Platform, providers, action.providerID, action.modelID, action.page, false, true, "", action.query)
			if vErr != nil {
				return vErr
			}
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
		}
	}

	ref := relay.ModelRef{ProviderID: action.providerID, ID: action.modelID, Variant: action.variant}
	if err := r.validateModel(ctx, msg, ref); err != nil {
		var replyErr *replyError
		message := "⚠️ Model unavailable. Try again."
		if errors.As(err, &replyErr) {
			message = "⚠️ " + replyErr.message
		}
		text, buttons, vErr := r.modelModelsViewWithQuery(msg.Platform, providers, action.providerID, action.page, false, message+"\n", action.query)
		if vErr != nil {
			return vErr
		}
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
	}

	if isOwnedThreadMessage(msg) {
		if err := r.store.ThreadConfigRepo().UpsertModel(ctx, msg.Platform, threadScopeChannelID(msg), msg.ThreadID, formatModelRef(ref)); err != nil {
			return err
		}
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "✅ Thread model set: "+formatModelRef(ref)+"\nScope: this thread", nil)
	}
	modelChannelID, err := modelScopeChannelID(msg)
	if err != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Channel information unavailable. Please try again.", nil)
	}
	if r.isAdmin(ctx, msg) {
		if err := r.store.ChannelRepo().UpsertModel(ctx, msg.Platform, modelChannelID, formatModelRef(ref)); err != nil {
			return err
		}
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "✅ Channel model set: "+formatModelRef(ref)+"\nScope: this channel", nil)
	}
	if err := r.store.OverrideRepo().UpsertModel(ctx, msg.Platform, modelChannelID, msg.UserID, formatModelRef(ref)); err != nil {
		return err
	}
	slog.Info("model browser: personal model set", "platform", msg.Platform, "channel_id", msg.ChannelID, "user_id", msg.UserID, "model", formatModelRef(ref))
	return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "✅ Personal model set: "+formatModelRef(ref)+"\nScope: personal override", nil)
}

// modelBrowserClose removes the buttons while preserving the current page's
// text: the close action carries the page kind it was created on.
func (r *Router) modelBrowserClose(ctx context.Context, msg channel.IncomingMessage, action modelBrowseAction) error {
	view, err := r.viewModel(ctx, msg)
	if err != nil {
		view = "🤖 Model: agent default"
	}
	providers, pErr := r.modelBrowserProviders(ctx, msg)
	if pErr != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, view, nil)
	}
	switch action.pageKind {
	case "variants":
		text, _, vErr := r.modelVariantsViewWithQuery(msg.Platform, providers, action.providerID, action.modelID, action.page, true, false, "", action.query)
		if vErr == nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, nil)
		}
	case "models":
		text, _, vErr := r.modelModelsViewWithQuery(msg.Platform, providers, action.providerID, action.page, true, "", action.query)
		if vErr == nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, nil)
		}
	case "providers":
		text, _, vErr := r.modelProvidersView(msg.Platform, providers, action.page, view, true)
		if vErr == nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, nil)
		}
	}
	return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, view, nil)
}

func (r *Router) modelBrowserProviders(ctx context.Context, msg channel.IncomingMessage) (relay.Providers, error) {
	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return relay.Providers{}, err
	}
	defer inst.End()
	return inst.Client().Providers(ctx)
}

// modelItemsPerRow keeps the browser within each platform's button-row
// limits: Telegram renders 2 columns, Discord packs 5 per row so a full
// 10-item page stays at 2 item rows + 1 nav row (Discord caps messages at
// 5 action rows).
func modelItemsPerRow(platform string) int {
	if platform == "discord" {
		return 5
	}
	return 2
}

// modelProvidersView renders one provider page. With textOnly the buttons
// (and their tokens) are omitted.
func (r *Router) modelProvidersView(platform string, providers relay.Providers, page int, current string, textOnly bool) (string, []channel.Button, error) {
	ids := providerIDs(providers)
	start, end := modelPageBounds(len(ids), page)
	text := current + "\n\nSelect provider:"
	if len(ids) == 0 {
		return text + "\n(no providers available)", nil, nil
	}

	var buttons []channel.Button
	if !textOnly {
		for i, id := range ids[start:end] {
			token, err := r.modelBrowser.register(modelBrowseAction{kind: "models", providerID: id})
			if err != nil {
				return "", nil, err
			}
			buttons = append(buttons, channel.Button{Label: id, Value: modelCallbackPrefix + token, Row: i/modelItemsPerRow(platform) + 1})
		}
	}
	buttons = append(buttons, r.modelNavButtons("providers", "", page, modelTotalPages(len(ids)), textOnly)...)
	return text, buttons, nil
}

func (r *Router) modelModelsView(platform string, providers relay.Providers, providerID string, page int, textOnly bool, prefix string) (string, []channel.Button, error) {
	return r.modelModelsViewWithQuery(platform, providers, providerID, page, textOnly, prefix, "")
}

func (r *Router) modelModelsViewWithQuery(platform string, providers relay.Providers, providerID string, page int, textOnly bool, prefix, query string) (string, []channel.Button, error) {
	p, ok := providerByID(providers, providerID)
	if !ok {
		if query != "" {
			return r.modelSearchUnavailableView(platform, providers, providerID, query, prefix)
		}
		return "", nil, fmt.Errorf("model browser: unknown provider %q", providerID)
	}
	ids := sortedModelIDs(p.Models)
	if query != "" {
		if _, connected := searchProviderByID(providers, providerID); !connected {
			return r.modelSearchUnavailableView(platform, providers, providerID, query, prefix)
		}
		ids = SearchModelIDs(p.Models, query)
	}
	pages := modelTotalPages(len(ids))
	if page < 0 {
		page = 0
	}
	if page >= pages {
		page = pages - 1
	}
	start, end := modelPageBounds(len(ids), page)
	text := prefix + fmt.Sprintf("Provider: %s — select model:", providerID)
	if query != "" {
		text = prefix + fmt.Sprintf("Provider: %s — search `%s`\nFound %d models · Page %d/%d", providerID, query, len(ids), page+1, pages)
	}
	if len(ids) == 0 {
		if query == "" {
			return text + "\n(no models available)", nil, nil
		}
		buttons, err := r.modelSearchNavigation(providerID, query, page, pages, textOnly)
		if err != nil {
			return "", nil, err
		}
		return text + "\n(no matching models)", buttons, nil
	}

	var buttons []channel.Button
	if !textOnly {
		for i, id := range ids[start:end] {
			token, err := r.modelBrowser.register(modelBrowseAction{kind: "set", page: page, providerID: providerID, query: query, modelID: id})
			if err != nil {
				return "", nil, err
			}
			buttons = append(buttons, channel.Button{Label: id, Value: modelCallbackPrefix + token, Row: i/modelItemsPerRow(platform) + 1})
		}
		if query == "" {
			back, err := r.modelBrowser.register(modelBrowseAction{kind: "providers"})
			if err != nil {
				return "", nil, err
			}
			buttons = append(buttons, channel.Button{Label: "⬅️ Providers", Value: modelCallbackPrefix + back, Row: modelBrowserNavRow})
		}
	}
	if query == "" {
		buttons = append(buttons, r.modelNavButtons("models", providerID, page, pages, textOnly)...)
	} else {
		searchButtons, err := r.modelSearchNavigation(providerID, query, page, pages, textOnly)
		if err != nil {
			return "", nil, err
		}
		buttons = append(buttons, searchButtons...)
	}
	return text, buttons, nil
}

func (r *Router) modelSearchNavigation(providerID, query string, page, pages int, textOnly bool) ([]channel.Button, error) {
	if textOnly {
		return nil, nil
	}
	providersToken, err := r.modelBrowser.register(modelBrowseAction{kind: "providers"})
	if err != nil {
		return nil, err
	}
	buttons := []channel.Button{{Label: "⬅️ Providers", Value: modelCallbackPrefix + providersToken, Row: modelBrowserNavRow}}
	buttons = append(buttons, r.modelNavButtonsWithQuery("models", providerID, page, pages, textOnly, query)...)
	return buttons, nil
}

func (r *Router) modelNavButtons(kind, providerID string, page, pages int, textOnly bool) []channel.Button {
	return r.modelNavButtonsWithQuery(kind, providerID, page, pages, textOnly, "")
}

func (r *Router) modelNavButtonsWithQuery(kind, providerID string, page, pages int, textOnly bool, query string) []channel.Button {
	if textOnly {
		return nil
	}
	var buttons []channel.Button
	if page > 0 {
		token, err := r.modelBrowser.register(modelBrowseAction{kind: kind, page: page - 1, providerID: providerID, query: query})
		if err == nil {
			buttons = append(buttons, channel.Button{Label: "◀️ Prev", Value: modelCallbackPrefix + token, Row: modelBrowserNavRow})
		}
	}
	if page < pages-1 {
		token, err := r.modelBrowser.register(modelBrowseAction{kind: kind, page: page + 1, providerID: providerID, query: query})
		if err == nil {
			buttons = append(buttons, channel.Button{Label: "Next ▶️", Value: modelCallbackPrefix + token, Row: modelBrowserNavRow})
		}
	}
	closeToken, err := r.modelBrowser.register(modelBrowseAction{kind: "close", pageKind: kind, page: page, providerID: providerID, query: query})
	if err == nil {
		buttons = append(buttons, channel.Button{Label: "✖️ Close", Value: modelCallbackPrefix + closeToken, Row: modelBrowserNavRow})
	}
	return buttons
}

func modelPageBounds(total, page int) (int, int) {
	if total <= 0 {
		return 0, 0
	}
	start := page * modelBrowserPage
	if start > total {
		start = total
	}
	end := start + modelBrowserPage
	if end > total {
		end = total
	}
	return start, end
}

func modelTotalPages(total int) int {
	if total <= 0 {
		return 1
	}
	return (total + modelBrowserPage - 1) / modelBrowserPage
}

// providerIDs returns the browsable provider ids: the connected ones when
// the agent reports them, otherwise the full catalog (backends without a
// connected list still show everything).
func providerIDs(providers relay.Providers) []string {
	if len(providers.Connected) > 0 {
		ids := append([]string(nil), providers.Connected...)
		sort.Strings(ids)
		return ids
	}
	ids := make([]string, 0, len(providers.All))
	for _, p := range providers.All {
		ids = append(ids, p.ID)
	}
	sort.Strings(ids)
	return ids
}

func providerByID(providers relay.Providers, id string) (relay.Provider, bool) {
	for _, p := range providers.All {
		if p.ID == id {
			return p, true
		}
	}
	return relay.Provider{}, false
}

func sortedModelIDs(models map[string]json.RawMessage) []string {
	ids := make([]string, 0, len(models))
	for id := range models {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (r *Router) modelVariantsView(platform string, providers relay.Providers, providerID, modelID string, page int, textOnly bool, fromBrowser bool, prefix string) (string, []channel.Button, error) {
	return r.modelVariantsViewWithQuery(platform, providers, providerID, modelID, page, textOnly, fromBrowser, prefix, "")
}

func (r *Router) modelVariantsViewWithQuery(platform string, providers relay.Providers, providerID, modelID string, page int, textOnly bool, fromBrowser bool, prefix, query string) (string, []channel.Button, error) {
	variants, ok := providers.Variants(providerID, modelID)
	if !ok || len(variants) == 0 {
		text := prefix + fmt.Sprintf("No variants for %s/%s", providerID, modelID)
		var buttons []channel.Button
		if !textOnly {
			if fromBrowser {
				back, err := r.modelBrowser.register(modelBrowseAction{kind: "models", providerID: providerID, page: page, query: query})
				if err != nil {
					return "", nil, err
				}
				buttons = append(buttons, channel.Button{Label: "⬅️ Models", Value: modelCallbackPrefix + back, Row: modelBrowserNavRow})
			}
			closeToken, err := r.modelBrowser.register(modelBrowseAction{kind: "close", pageKind: "variants", providerID: providerID, modelID: modelID, page: page, query: query})
			if err != nil {
				return "", nil, err
			}
			buttons = append(buttons, channel.Button{Label: "⬅️ Close", Value: modelCallbackPrefix + closeToken, Row: modelBrowserNavRow})
		}
		return text, buttons, nil
	}

	names := make([]string, 0, len(variants))
	for name := range variants {
		names = append(names, name)
	}
	sort.Strings(names)

	type variantConfig struct {
		ReasoningEffort      string `json:"reasoningEffort"`
		ReasoningEffortSnake string `json:"reasoning_effort"`
		Description          string `json:"description"`
	}

	maxBracketLen := 0
	for _, name := range names {
		bracketLen := len(name) + 2
		if bracketLen > maxBracketLen {
			maxBracketLen = bracketLen
		}
	}

	var sb strings.Builder
	if prefix != "" {
		sb.WriteString(prefix)
	}
	fmt.Fprintf(&sb, "⚙️ Variants: %s/%s\n", providerID, modelID)

	for _, name := range names {
		var vc variantConfig
		if raw, ok := variants[name]; ok && len(raw) > 0 {
			_ = json.Unmarshal(raw, &vc)
		}
		effort := vc.ReasoningEffort
		if effort == "" {
			effort = vc.ReasoningEffortSnake
		}
		detail := ""
		if effort != "" {
			detail = "Reasoning effort: " + effort
		} else if vc.Description != "" {
			detail = vc.Description
		}

		bracket := fmt.Sprintf("[%s]", name)
		if detail != "" {
			fmt.Fprintf(&sb, "%-*s%s\n", maxBracketLen+2, bracket, detail)
		} else {
			fmt.Fprintf(&sb, "%s\n", bracket)
		}
	}
	text := strings.TrimRight(sb.String(), "\n")

	var buttons []channel.Button
	if !textOnly {
		for i, name := range names {
			token, err := r.modelBrowser.register(modelBrowseAction{
				kind:       "set",
				page:       page,
				providerID: providerID,
				query:      query,
				modelID:    modelID,
				variant:    name,
			})
			if err != nil {
				return "", nil, err
			}
			buttons = append(buttons, channel.Button{
				Label: fmt.Sprintf("Set @%s", name),
				Value: modelCallbackPrefix + token,
				Row:   i/modelItemsPerRow(platform) + 1,
			})
		}

		if fromBrowser {
			back, err := r.modelBrowser.register(modelBrowseAction{kind: "models", providerID: providerID, page: page, query: query})
			if err != nil {
				return "", nil, err
			}
			buttons = append(buttons, channel.Button{Label: "⬅️ Models", Value: modelCallbackPrefix + back, Row: modelBrowserNavRow})
		}
		closeToken, err := r.modelBrowser.register(modelBrowseAction{
			kind:       "close",
			pageKind:   "variants",
			providerID: providerID,
			modelID:    modelID,
			page:       page,
			query:      query,
		})
		if err != nil {
			return "", nil, err
		}
		buttons = append(buttons, channel.Button{Label: "⬅️ Close", Value: modelCallbackPrefix + closeToken, Row: modelBrowserNavRow})
	}

	return text, buttons, nil
}
