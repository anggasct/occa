package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
)

const (
	modelSourceEndpoint = "endpoint"
	modelSourceChannel  = "channel"
	modelSourceFallback = "fallback"
)

var (
	errInvalidEndpointModel = errors.New("invalid endpoint model")
	errInvalidChannelModel  = errors.New("invalid channel model")
)

func (s *Server) laneModel(ep config.EndpointConfig) (*relay.ModelRef, string, error) {
	if model := strings.TrimSpace(ep.Model); model != "" {
		ref, err := relay.ParseModelRef(model)
		if err != nil {
			return nil, "", errInvalidEndpointModel
		}
		return &ref, modelSourceEndpoint, nil
	}
	if s.channels == nil {
		return nil, modelSourceFallback, nil
	}
	ch, err := s.channels.Get(context.Background(), ep.Platform, ep.ChannelID)
	if err != nil {
		return nil, "", fmt.Errorf("channel configuration error: %w", err)
	}
	if ch == nil || strings.TrimSpace(ch.Model) == "" {
		return nil, modelSourceFallback, nil
	}
	ref, err := relay.ParseModelRef(strings.TrimSpace(ch.Model))
	if err != nil {
		return nil, "", errInvalidChannelModel
	}
	return &ref, modelSourceChannel, nil
}

func setLaneModel(workCtx *WebhookWorkContext, envelope WebhookEnvelope, ref *relay.ModelRef, source string) {
	workCtx.Model = ref
	workCtx.ModelSource = source
	if envelope == nil {
		return
	}
	if ref != nil {
		envelope["model"] = relay.FormatModelRef(*ref)
	} else {
		envelope["model"] = "agent/session default"
	}
	envelope["model_source"] = source
}
