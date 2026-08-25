package router

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
)

var errChannelScopeUnresolved = errors.New("channel scope unresolved")

type modelSource string

const (
	modelSourceThread   modelSource = "thread setting"
	modelSourceChannel  modelSource = "channel setting"
	modelSourcePersonal modelSource = "personal override"
	modelSourceSession  modelSource = "legacy session setting"
	modelSourceDefault  modelSource = "agent default"
)

type modelResolution struct {
	model  *relay.ModelRef
	source modelSource
}

func (r *Router) handleModel(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	parts := strings.Fields(args)
	if len(parts) == 0 {
		if err := r.openModelBrowser(ctx, msg); err != nil {
			if errors.Is(err, errReplied) {
				return "", errReplied
			}
			return "", err
		}
		return r.viewModel(ctx, msg)
	}

	if providerID, query, search, usage := parseModelSearch(parts); search {
		if err := r.openModelSearch(ctx, msg, providerID, query); err != nil {
			if errors.Is(err, errReplied) {
				return "", errReplied
			}
			return "", err
		}
		return "", errReplied
	} else if usage {
		return "Usage: /model <provider> <query> | /model search <provider> <query>", nil
	}

	if parts[0] == "default" {
		if len(parts) != 1 {
			return "Usage: /model [provider/model-id[@variant]] | default", nil
		}
		return r.clearModel(ctx, msg)
	}

	if parts[0] == "channel" || parts[0] == "session" {
		return "Usage: /model [provider/model-id[@variant]] | default", nil
	}
	if len(parts) != 1 {
		return "Usage: /model [provider/model-id[@variant]] | default", nil
	}
	ref, err := parseModelRef(parts[0])
	if err != nil {
		return "", err
	}
	if isOwnedThreadMessage(msg) {
		if err := r.validateModel(ctx, msg, ref); err != nil {
			return "", err
		}
		if err := r.store.ThreadConfigRepo().UpsertModel(ctx, msg.Platform, threadScopeChannelID(msg), msg.ThreadID, formatModelRef(ref)); err != nil {
			return "", fmt.Errorf("model: set thread: %w", err)
		}
		return fmt.Sprintf("✅ Thread model set: %s\nScope: this thread", formatModelRef(ref)), nil
	}
	modelChannelID, err := modelScopeChannelID(msg)
	if err != nil {
		return "", safeReplyError("Channel information unavailable. Please try again.", err)
	}
	if err := r.validateModel(ctx, msg, ref); err != nil {
		return "", err
	}
	if r.isAdmin(ctx, msg) {
		if err := r.store.ChannelRepo().UpsertModel(ctx, msg.Platform, modelChannelID, formatModelRef(ref)); err != nil {
			return "", fmt.Errorf("model: set channel: %w", err)
		}
		return fmt.Sprintf("✅ Channel model set: %s\nScope: this channel", formatModelRef(ref)), nil
	}
	if err := r.store.OverrideRepo().UpsertModel(ctx, msg.Platform, modelChannelID, msg.UserID, formatModelRef(ref)); err != nil {
		return "", fmt.Errorf("model: set personal: %w", err)
	}
	return fmt.Sprintf("✅ Personal model set: %s\nScope: personal override", formatModelRef(ref)), nil
}

func parseModelSearch(parts []string) (providerID, query string, search, usage bool) {
	if len(parts) == 0 {
		return "", "", false, false
	}
	if parts[0] == "search" {
		if len(parts) < 3 {
			return "", "", false, true
		}
		return parts[1], strings.Join(parts[2:], " "), true, false
	}
	if len(parts) < 2 || strings.Contains(parts[0], "/") || parts[0] == "default" || parts[0] == "channel" || parts[0] == "session" {
		return "", "", false, false
	}
	return parts[0], strings.Join(parts[1:], " "), true, false
}

func (r *Router) clearModel(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	if isOwnedThreadMessage(msg) {
		if err := r.store.ThreadConfigRepo().UpsertModel(ctx, msg.Platform, threadScopeChannelID(msg), msg.ThreadID, ""); err != nil {
			return "", fmt.Errorf("model: clear thread: %w", err)
		}
		return "✅ Thread model cleared.", nil
	}
	modelChannelID, err := modelScopeChannelID(msg)
	if err != nil {
		return "", safeReplyError("Channel information unavailable. Please try again.", err)
	}
	if r.isAdmin(ctx, msg) {
		if err := r.store.ChannelRepo().UpsertModel(ctx, msg.Platform, modelChannelID, ""); err != nil {
			return "", fmt.Errorf("model: clear channel: %w", err)
		}
		return "✅ Channel model cleared.", nil
	}
	if err := r.store.OverrideRepo().UpsertModel(ctx, msg.Platform, modelChannelID, msg.UserID, ""); err != nil {
		return "", fmt.Errorf("model: clear personal: %w", err)
	}
	return "✅ Personal model cleared.", nil
}

func (r *Router) viewModel(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	resolution, err := r.resolveModel(ctx, msg)
	if err != nil {
		return "", err
	}
	if resolution.model == nil {
		return "🤖 Model: agent default\nSource: agent default", nil
	}
	return fmt.Sprintf("🤖 Model: %s\nSource: %s", formatModelRef(*resolution.model), resolution.source), nil
}

func (r *Router) effectiveModel(ctx context.Context, msg channel.IncomingMessage) (*relay.ModelRef, error) {
	resolution, err := r.resolveModel(ctx, msg)
	if err != nil {
		return nil, err
	}
	return resolution.model, nil
}

func (r *Router) resolveModel(ctx context.Context, msg channel.IncomingMessage) (modelResolution, error) {
	modelChannelID, err := modelScopeChannelID(msg)
	if err != nil {
		return modelResolution{}, safeReplyError("Channel information unavailable. Please try again.", err)
	}

	thread, err := r.threadRow(ctx, msg)
	if err != nil {
		return modelResolution{}, fmt.Errorf("model: get thread: %w", err)
	}
	if thread != nil && thread.Model != "" {
		return r.parseStoredModel(thread.Model, modelSourceThread)
	}

	channelConfig, err := r.store.ChannelRepo().Get(ctx, msg.Platform, modelChannelID)
	if err != nil {
		return modelResolution{}, fmt.Errorf("model: get channel: %w", err)
	}
	if channelConfig != nil && channelConfig.Model != "" {
		return r.parseStoredModel(channelConfig.Model, modelSourceChannel)
	}

	override, err := r.store.OverrideRepo().Get(ctx, msg.Platform, modelChannelID, msg.UserID)
	if err != nil {
		return modelResolution{}, fmt.Errorf("model: get personal override: %w", err)
	}
	if override != nil && override.Model != "" {
		return r.parseStoredModel(override.Model, modelSourcePersonal)
	}

	threadID, userID := conversationKey(msg)
	sessionModel, err := r.store.SessionRepo().ActiveModel(ctx, msg.Platform, msg.ChannelID, threadID, userID)
	if err != nil {
		return modelResolution{}, fmt.Errorf("model: get session model: %w", err)
	}
	if sessionModel != "" {
		return r.parseStoredModel(sessionModel, modelSourceSession)
	}
	return modelResolution{source: modelSourceDefault}, nil
}

func (r *Router) parseStoredModel(model string, source modelSource) (modelResolution, error) {
	ref, err := parseModelRef(model)
	if err != nil {
		return modelResolution{}, fmt.Errorf("model: invalid stored model %q: %w", model, err)
	}
	return modelResolution{model: &ref, source: source}, nil
}

func modelScopeChannelID(msg channel.IncomingMessage) (string, error) {
	if msg.ChannelScopeUnresolved {
		return "", fmt.Errorf("model: %w", errChannelScopeUnresolved)
	}
	if msg.ParentChannelID != "" {
		return msg.ParentChannelID, nil
	}
	return msg.ChannelID, nil
}

func (r *Router) modelForMessage(ctx context.Context, msg channel.IncomingMessage) (*relay.ModelRef, error) {
	return r.effectiveModel(ctx, msg)
}

func (r *Router) validateModel(ctx context.Context, msg channel.IncomingMessage, ref relay.ModelRef) error {
	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return safeReplyError("Agent unreachable", fmt.Errorf("model: resolve agent instance: %w", err))
	}
	defer inst.End()

	providers, err := inst.Client().Providers(ctx)
	if err != nil {
		cause := fmt.Errorf("model: list providers: %w", err)
		if errors.Is(err, relay.ErrUnreachable) || errors.Is(err, relay.ErrTimeout) || errors.Is(err, relay.ErrNotFound) {
			return safeReplyError("Agent unreachable", cause)
		}
		return safeReplyError("Model provider list unavailable. Please try again.", cause)
	}
	if !providers.HasProvider(ref.ProviderID) {
		return safeReplyError(fmt.Sprintf("unknown provider: %s", ref.ProviderID), nil)
	}
	if !providers.HasModel(ref) {
		return safeReplyError(fmt.Sprintf("unknown model: %s", formatModelRef(ref)), nil)
	}
	if ref.Variant != "" && !providers.HasVariant(ref) {
		return safeReplyError(fmt.Sprintf("unknown variant: %s for %s/%s", ref.Variant, ref.ProviderID, ref.ID), nil)
	}
	return nil
}

func parseModelRef(value string) (relay.ModelRef, error) {
	parts := strings.SplitN(value, "/", 2)
	if len(parts) != 2 {
		return relay.ModelRef{}, safeReplyError(fmt.Sprintf("invalid model %q; use provider/model-id[@variant]", value), nil)
	}
	providerID, modelIDPart := parts[0], parts[1]
	if providerID == "" || modelIDPart == "" {
		return relay.ModelRef{}, safeReplyError(fmt.Sprintf("invalid model %q; use provider/model-id[@variant]", value), nil)
	}

	var variant string
	if modelID, v, hasAt := strings.Cut(modelIDPart, "@"); hasAt {
		if v == "" {
			return relay.ModelRef{}, safeReplyError("invalid variant", nil)
		}
		if modelID == "" {
			return relay.ModelRef{}, safeReplyError(fmt.Sprintf("invalid model %q; use provider/model-id[@variant]", value), nil)
		}
		modelIDPart = modelID
		variant = v
	}

	return relay.ModelRef{ProviderID: providerID, ID: modelIDPart, Variant: variant}, nil
}

func formatModelRef(ref relay.ModelRef) string {
	if ref.Variant != "" {
		return ref.ProviderID + "/" + ref.ID + "@" + ref.Variant
	}
	return ref.ProviderID + "/" + ref.ID
}

func (r *Router) handleVariants(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	parts := strings.Fields(args)
	var providerID, modelID string
	if len(parts) == 0 {
		modelRef, err := r.effectiveModel(ctx, msg)
		if err != nil {
			return "", err
		}
		if modelRef == nil {
			return "No active model. Usage: /variants [provider/model-id]", nil
		}
		providerID = modelRef.ProviderID
		modelID = modelRef.ID
	} else if len(parts) == 1 {
		ref, err := parseModelRef(parts[0])
		if err != nil {
			return "", err
		}
		providerID = ref.ProviderID
		modelID = ref.ID
	} else {
		return "Usage: /variants [provider/model-id]", nil
	}

	baseRef := relay.ModelRef{ProviderID: providerID, ID: modelID}
	if err := r.validateModel(ctx, msg, baseRef); err != nil {
		return "", err
	}

	providers, err := r.modelBrowserProviders(ctx, msg)
	if err != nil {
		return "", safeReplyError("Agent unreachable", fmt.Errorf("variants: list providers: %w", err))
	}

	text, buttons, err := r.modelVariantsView(msg.Platform, providers, providerID, modelID, 0, false, false, "")
	if err != nil {
		return "", err
	}

	if len(buttons) > 0 {
		if _, err := msg.ReplyCtx.SendWithButtons(text, buttons); err != nil {
			return "", err
		}
		return "", errReplied
	}
	return text, nil
}
