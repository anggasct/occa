package relay

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

type mockSink struct {
	sent        []string
	typingCalls int
	handles     []*mockEditHandle
}

func (m *mockSink) Send(ctx context.Context, text string) (EditHandle, error) {
	m.sent = append(m.sent, text)
	h := &mockEditHandle{ref: fakeRef{id: text}}
	m.handles = append(m.handles, h)
	return h, nil
}

func (m *mockSink) SendWithButtons(ctx context.Context, text string, buttons []channel.Button) (EditHandle, error) {
	return m.Send(ctx, text)
}

func (m *mockSink) SendTyping(ctx context.Context) error {
	m.typingCalls++
	return nil
}

type mockEditHandle struct {
	ref   channel.MessageRef
	edits []string
}

func (h *mockEditHandle) Ref() channel.MessageRef {
	return h.ref
}

func (h *mockEditHandle) Edit(ctx context.Context, text string) error {
	h.edits = append(h.edits, text)
	return nil
}

func (h *mockEditHandle) EditWithButtons(ctx context.Context, text string, buttons []channel.Button) error {
	return h.Edit(ctx, text)
}

func TestStreamerWithCustomSink(t *testing.T) {
	sink := &mockSink{}
	s := NewStreamerWithSink(sink, render.New(), render.Telegram)

	events := make(chan Event, 2)
	events <- Event{Type: EventDelta, Delta: "hello sink"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if len(sink.sent) == 0 {
		t.Fatal("expected message sent to mock sink")
	}
	if !strings.Contains(sink.sent[0], "hello sink") {
		t.Fatalf("sent message = %q, want hello sink", sink.sent[0])
	}
}

func TestStreamerSourceHasNoDirectReplyAccess(t *testing.T) {
	content, err := os.ReadFile("streamer.go")
	if err != nil {
		t.Fatalf("read streamer.go: %v", err)
	}
	src := string(content)
	if strings.Contains(src, "s.reply.") {
		t.Fatal("streamer.go must not call s.reply directly; all output must go through Sink")
	}
	for _, line := range strings.Split(src, "\n") {
		if strings.Contains(line, "reply.") && !strings.Contains(line, "NewStreamer") {
			t.Fatalf("unexpected reply call in streamer.go: %s", line)
		}
	}
}
