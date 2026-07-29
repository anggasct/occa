package relay

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

type fakeReplyContext struct {
	mu    sync.Mutex
	sends []string
	edits []string
	ref   channel.MessageRef
}

type fakeRef struct{ id string }

func (f fakeRef) ID() string { return f.id }

func (f *fakeReplyContext) SendTyping() error { return nil }

func (f *fakeReplyContext) Send(text string) (channel.MessageRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sends = append(f.sends, text)
	f.ref = fakeRef{id: "msg-1"}
	return f.ref, nil
}

func (f *fakeReplyContext) Edit(ref channel.MessageRef, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.edits = append(f.edits, text)
	return nil
}

func (f *fakeReplyContext) lastOutput() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.edits) > 0 {
		return f.edits[len(f.edits)-1]
	}
	if len(f.sends) > 0 {
		return f.sends[len(f.sends)-1]
	}
	return ""
}

func TestStreamerFinalEdit(t *testing.T) {
	reply := &fakeReplyContext{}
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)

	events := make(chan Event, 10)
	events <- Event{Type: "delta", Delta: "Hello "}
	events <- Event{Type: "delta", Delta: "world"}
	events <- Event{Type: "done"}
	close(events)

	ctx := context.Background()
	err := s.Run(ctx, events)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	out := reply.lastOutput()
	if out != "Hello world" {
		t.Fatalf("expected final output 'Hello world', got %q", out)
	}
}

func TestStreamerUnchangedBufferSkipsEdit(t *testing.T) {
	reply := &fakeReplyContext{}
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)

	events := make(chan Event, 10)
	events <- Event{Type: "delta", Delta: "same"}
	events <- Event{Type: "done"}
	close(events)

	err := s.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	totalEdits := len(reply.edits)
	reply.mu.Unlock()

	if totalEdits > 1 {
		t.Fatalf("expected at most 1 edit for unchanged buffer, got %d", totalEdits)
	}
}

func TestStreamerChannelClosed(t *testing.T) {
	reply := &fakeReplyContext{}
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)

	events := make(chan Event)
	close(events)

	err := s.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestStreamerContextCancel(t *testing.T) {
	reply := &fakeReplyContext{}
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)

	events := make(chan Event)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := s.Run(ctx, events)
	if err == nil {
		t.Fatal("expected context cancel error")
	}
}

func TestStreamerErrorEvent(t *testing.T) {
	reply := &fakeReplyContext{}
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)

	events := make(chan Event, 5)
	events <- Event{Type: "delta", Delta: "partial"}
	events <- Event{Type: "error", Delta: "something broke"}
	close(events)

	err := s.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()
	found := false
	for _, s := range reply.sends {
		if s == "⚠️ Agent error: something broke" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected error message in sends, got: %v", reply.sends)
	}
}
