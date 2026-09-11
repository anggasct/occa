package webhook

import (
	"context"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/render"
)

type WebhookSendFunc func(ctx context.Context, channelID, text string) (string, error)
type WebhookEditFunc func(ctx context.Context, channelID, messageID, text string) error
type WebhookTypingFunc func(ctx context.Context, channelID string) error

type WebhookSink struct {
	channelID string
	platform  string
	limit     int
	sendFn    WebhookSendFunc
	editFn    WebhookEditFunc
	typingFn  WebhookTypingFunc
}

type WebhookSinkOption func(*WebhookSink)

func WithSinkLimit(limit int) WebhookSinkOption {
	return func(s *WebhookSink) {
		if limit > 0 {
			s.limit = limit
		}
	}
}

func WithSinkTyping(fn WebhookTypingFunc) WebhookSinkOption {
	return func(s *WebhookSink) {
		s.typingFn = fn
	}
}

func NewWebhookSink(channelID, platform string, sendFn WebhookSendFunc, editFn WebhookEditFunc, opts ...WebhookSinkOption) *WebhookSink {
	limit := render.TelegramLimit
	if platform == "discord" {
		limit = render.DiscordLimit
	}
	s := &WebhookSink{
		channelID: channelID,
		platform:  platform,
		limit:     limit,
		sendFn:    sendFn,
		editFn:    editFn,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *WebhookSink) Send(ctx context.Context, text string) (relay.EditHandle, error) {
	clamped := render.Clamp(text, s.limit)
	msgID, err := s.sendFn(ctx, s.channelID, clamped)
	if err != nil {
		return nil, err
	}
	return &webhookEditHandle{sink: s, msgID: msgID}, nil
}

func (s *WebhookSink) SendWithButtons(ctx context.Context, text string, buttons []channel.Button) (relay.EditHandle, error) {
	return s.Send(ctx, text)
}

func (s *WebhookSink) SendTyping(ctx context.Context) error {
	if s.typingFn != nil {
		return s.typingFn(ctx, s.channelID)
	}
	return nil
}

func (s *WebhookSink) HandleFromRef(ref channel.MessageRef) relay.EditHandle {
	if ref == nil {
		return nil
	}
	return &webhookEditHandle{sink: s, msgID: ref.ID()}
}

type webhookEditHandle struct {
	sink  *WebhookSink
	msgID string
}

func (h *webhookEditHandle) Ref() channel.MessageRef {
	return webhookMsgRef(h.msgID)
}

func (h *webhookEditHandle) Edit(ctx context.Context, text string) error {
	if h.sink.editFn == nil {
		return nil
	}
	clamped := render.Clamp(text, h.sink.limit)
	return h.sink.editFn(ctx, h.sink.channelID, h.msgID, clamped)
}

func (h *webhookEditHandle) EditWithButtons(ctx context.Context, text string, buttons []channel.Button) error {
	return h.Edit(ctx, text)
}

type webhookMsgRef string

func (r webhookMsgRef) ID() string {
	return string(r)
}
