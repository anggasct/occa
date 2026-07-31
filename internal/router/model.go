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

func (r *Router) handleModel(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	if err := r.authorize(ctx, msg); err != nil {
		if errors.Is(err, ErrDenied) {
			return "", safeReplyError("Access denied. Ask an admin to /occa:allow you.", nil)
		}
		return "", safeReplyError("Unable to verify access. Please try again.", fmt.Errorf("model: authorize: %w", err))
	}

	parts := strings.Fields(args)
	if len(parts) == 0 {
		return r.viewModel(ctx, msg)
	}

	if parts[0] == "channel" {
		if len(parts) != 2 {
			return "Usage: /occa:model channel <provider>/<model-id>", nil
		}
		if !r.isAdmin(ctx, msg) {
			return "⚠️ Admin access required.", nil
		}
		ref, err := parseModelRef(parts[1])
		if err != nil {
			return "", err
		}
		modelChannelID, err := modelScopeChannelID(msg)
		if err != nil {
			return "", safeReplyError("Channel information unavailable. Please try again.", err)
		}
		if err := r.validateModel(ctx, msg, ref); err != nil {
			return "", err
		}

		if err := r.store.ChannelRepo().UpsertModel(ctx, msg.Platform, modelChannelID, formatModelRef(ref)); err != nil {
			return "", fmt.Errorf("model: set channel: %w", err)
		}
		return fmt.Sprintf("✅ Channel model set: %s", formatModelRef(ref)), nil
	}

	if len(parts) != 1 {
		return "Usage: /occa:model [channel] [provider/model-id]", nil
	}
	ref, err := parseModelRef(parts[0])
	if err != nil {
		return "", err
	}
	modelChannelID, err := modelScopeChannelID(msg)
	if err != nil {
		return "", safeReplyError("Channel information unavailable. Please try again.", err)
	}
	if err := r.validateModel(ctx, msg, ref); err != nil {
		return "", err
	}
	if err := r.store.OverrideRepo().UpsertModel(ctx, msg.Platform, modelChannelID, msg.UserID, formatModelRef(ref)); err != nil {
		return "", fmt.Errorf("model: set personal: %w", err)
	}
	return fmt.Sprintf("✅ Personal model set: %s", formatModelRef(ref)), nil
}

func (r *Router) viewModel(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	model, err := r.effectiveModel(ctx, msg)
	if err != nil {
		return "", err
	}
	if model == nil {
		return "🤖 Model: agent default", nil
	}
	return fmt.Sprintf("🤖 Model: %s", formatModelRef(*model)), nil
}

func (r *Router) effectiveModel(ctx context.Context, msg channel.IncomingMessage) (*relay.ModelRef, error) {
	modelChannelID, err := modelScopeChannelID(msg)
	if err != nil {
		return nil, safeReplyError("Channel information unavailable. Please try again.", err)
	}
	o, err := r.store.OverrideRepo().Get(ctx, msg.Platform, modelChannelID, msg.UserID)
	if err != nil {
		return nil, fmt.Errorf("model: get personal override: %w", err)
	}
	var model string
	if o != nil && o.Model != "" {
		model = o.Model
	} else {
		ch, err := r.store.ChannelRepo().Get(ctx, msg.Platform, modelChannelID)
		if err != nil {
			return nil, fmt.Errorf("model: get channel: %w", err)
		}
		if ch != nil {
			model = ch.Model
		}
	}
	if model == "" {
		return nil, nil
	}
	ref, err := parseModelRef(model)
	if err != nil {
		return nil, fmt.Errorf("model: invalid stored model %q: %w", model, err)
	}
	return &ref, nil
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
	return nil
}

func parseModelRef(value string) (relay.ModelRef, error) {
	if strings.Count(value, "/") != 1 {
		return relay.ModelRef{}, safeReplyError(fmt.Sprintf("invalid model %q; use provider/model-id", value), nil)
	}
	parts := strings.SplitN(value, "/", 2)
	if parts[0] == "" || parts[1] == "" {
		return relay.ModelRef{}, safeReplyError(fmt.Sprintf("invalid model %q; use provider/model-id", value), nil)
	}
	return relay.ModelRef{ProviderID: parts[0], ID: parts[1]}, nil
}

func formatModelRef(ref relay.ModelRef) string {
	return ref.ProviderID + "/" + ref.ID
}
