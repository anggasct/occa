package router

import (
	"context"
	"fmt"
	"strings"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

func (r *Router) handleModel(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
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
		if err := r.validateModel(ctx, msg, ref); err != nil {
			return "", err
		}

		ch, err := r.store.ChannelRepo().Get(ctx, msg.Platform, msg.ChannelID)
		if err != nil {
			return "", fmt.Errorf("model: get channel: %w", err)
		}
		if ch == nil {
			ch = &store.Channel{ChannelID: msg.ChannelID, Platform: msg.Platform, ListenMode: "mention"}
		}
		ch.Model = formatModelRef(ref)
		if err := r.store.ChannelRepo().Upsert(ctx, ch); err != nil {
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
	if err := r.validateModel(ctx, msg, ref); err != nil {
		return "", err
	}
	if err := r.store.OverrideRepo().UpsertModel(ctx, msg.Platform, msg.ChannelID, msg.UserID, formatModelRef(ref)); err != nil {
		return "", fmt.Errorf("model: set personal: %w", err)
	}
	return fmt.Sprintf("✅ Personal model set: %s", formatModelRef(ref)), nil
}

func (r *Router) viewModel(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	model, err := r.effectiveModel(ctx, msg)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("🤖 Model: %s", model), nil
}

func (r *Router) effectiveModel(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	o, err := r.store.OverrideRepo().Get(ctx, msg.Platform, msg.ChannelID, msg.UserID)
	if err != nil {
		return "", fmt.Errorf("model: get personal override: %w", err)
	}
	if o != nil && o.Model != "" {
		return o.Model, nil
	}

	ch, err := r.store.ChannelRepo().Get(ctx, msg.Platform, msg.ChannelID)
	if err != nil {
		return "", fmt.Errorf("model: get channel: %w", err)
	}
	if ch != nil && ch.Model != "" {
		return ch.Model, nil
	}
	return "agent default", nil
}

func (r *Router) modelForMessage(ctx context.Context, msg channel.IncomingMessage) (*relay.ModelRef, error) {
	model, err := r.effectiveModel(ctx, msg)
	if err != nil {
		return nil, err
	}
	if model == "agent default" {
		return nil, nil
	}
	ref, err := parseModelRef(model)
	if err != nil {
		return nil, fmt.Errorf("model: invalid stored model %q: %w", model, err)
	}
	return &ref, nil
}

func (r *Router) validateModel(ctx context.Context, msg channel.IncomingMessage, ref relay.ModelRef) error {
	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return relay.ErrUnreachable
	}
	defer inst.End()

	providers, err := inst.Client().Providers(ctx)
	if err != nil {
		return fmt.Errorf("provider lookup: %w", err)
	}
	if !providers.HasProvider(ref.ProviderID) {
		return fmt.Errorf("unknown provider: %s", ref.ProviderID)
	}
	if !providers.HasModel(ref) {
		return fmt.Errorf("unknown model: %s", formatModelRef(ref))
	}
	return nil
}

func parseModelRef(value string) (relay.ModelRef, error) {
	if strings.Count(value, "/") != 1 {
		return relay.ModelRef{}, fmt.Errorf("invalid model %q; use provider/model-id", value)
	}
	parts := strings.SplitN(value, "/", 2)
	if parts[0] == "" || parts[1] == "" {
		return relay.ModelRef{}, fmt.Errorf("invalid model %q; use provider/model-id", value)
	}
	return relay.ModelRef{ProviderID: parts[0], ID: parts[1]}, nil
}

func formatModelRef(ref relay.ModelRef) string {
	return ref.ProviderID + "/" + ref.ID
}
