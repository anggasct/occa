package relay

import (
	"context"

	"github.com/anggasct/occa/internal/channel"
)

type Sink interface {
	Send(ctx context.Context, text string) (EditHandle, error)
	SendWithButtons(ctx context.Context, text string, buttons []channel.Button) (EditHandle, error)
	SendTyping(ctx context.Context) error
}

type EditHandle interface {
	Ref() channel.MessageRef
	Edit(ctx context.Context, text string) error
	EditWithButtons(ctx context.Context, text string, buttons []channel.Button) error
}

type RefHandleProvider interface {
	HandleFromRef(ref channel.MessageRef) EditHandle
}

type channelSink struct {
	reply channel.ReplyContext
}

func NewChannelSink(reply channel.ReplyContext) Sink {
	if reply == nil {
		return nil
	}
	return &channelSink{reply: reply}
}

func (s *channelSink) Send(ctx context.Context, text string) (EditHandle, error) {
	ref, err := s.reply.Send(text)
	if err != nil {
		return nil, err
	}
	return &channelEditHandle{reply: s.reply, ref: ref}, nil
}

func (s *channelSink) SendWithButtons(ctx context.Context, text string, buttons []channel.Button) (EditHandle, error) {
	ref, err := s.reply.SendWithButtons(text, buttons)
	if err != nil {
		return nil, err
	}
	return &channelEditHandle{reply: s.reply, ref: ref}, nil
}

func (s *channelSink) SendTyping(ctx context.Context) error {
	return s.reply.SendTyping()
}

func (s *channelSink) HandleFromRef(ref channel.MessageRef) EditHandle {
	if ref == nil || s.reply == nil {
		return nil
	}
	return &channelEditHandle{reply: s.reply, ref: ref}
}

type channelEditHandle struct {
	reply channel.ReplyContext
	ref   channel.MessageRef
}

func (h *channelEditHandle) Ref() channel.MessageRef {
	return h.ref
}

func (h *channelEditHandle) Edit(ctx context.Context, text string) error {
	return h.reply.Edit(h.ref, text)
}

func (h *channelEditHandle) EditWithButtons(ctx context.Context, text string, buttons []channel.Button) error {
	return h.reply.EditWithButtons(h.ref, text, buttons)
}
