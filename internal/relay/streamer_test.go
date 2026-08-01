package relay

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

type fakeReplyContext struct {
	mu       sync.Mutex
	sends    []string
	edits    map[string][]string
	refCount int
}

type fakeRef struct{ id string }

func (f fakeRef) ID() string { return f.id }

func newFakeReplyContext() *fakeReplyContext {
	return &fakeReplyContext{edits: make(map[string][]string)}
}

func (f *fakeReplyContext) SendTyping() error { return nil }

func (f *fakeReplyContext) Send(text string) (channel.MessageRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sends = append(f.sends, text)
	f.refCount++
	return fakeRef{id: fmt.Sprintf("msg-%d", f.refCount)}, nil
}

func (f *fakeReplyContext) Edit(ref channel.MessageRef, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.edits[ref.ID()] = append(f.edits[ref.ID()], text)
	return nil
}

func (f *fakeReplyContext) EditWithButtons(ref channel.MessageRef, text string, buttons []channel.Button) error {
	return f.Edit(ref, text)
}

func (f *fakeReplyContext) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sends = append(f.sends, text)
	f.refCount++
	return fakeRef{id: fmt.Sprintf("msg-%d", f.refCount)}, nil
}

func (f *fakeReplyContext) lastOutput() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id := f.refCount; id >= 1; id-- {
		key := fmt.Sprintf("msg-%d", id)
		if e := f.edits[key]; len(e) > 0 {
			return e[len(e)-1]
		}
	}
	if len(f.sends) > 0 {
		return f.sends[len(f.sends)-1]
	}
	return ""
}

func (f *fakeReplyContext) finalMessages() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var result []string
	for i := 1; i <= f.refCount; i++ {
		key := fmt.Sprintf("msg-%d", i)
		if e := f.edits[key]; len(e) > 0 {
			result = append(result, e[len(e)-1])
		} else if i <= len(f.sends) {
			result = append(result, f.sends[i-1])
		}
	}
	return result
}

func (f *fakeReplyContext) editCountFor(refID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.edits[refID])
}

func TestStreamerFinalEdit(t *testing.T) {
	reply := newFakeReplyContext()
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
	reply := newFakeReplyContext()
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
	totalEdits := len(reply.edits["msg-1"])
	reply.mu.Unlock()

	if totalEdits > 1 {
		t.Fatalf("expected at most 1 edit for unchanged buffer, got %d", totalEdits)
	}
}

func TestStreamerChannelClosed(t *testing.T) {
	reply := newFakeReplyContext()
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)

	events := make(chan Event)
	close(events)

	err := s.Run(context.Background(), events)
	if !errors.Is(err, ErrIncompleteStream) {
		t.Fatalf("Run error = %v, want ErrIncompleteStream", err)
	}
	if got := reply.lastOutput(); got != incompleteStreamMessage {
		t.Fatalf("incomplete notice = %q, want %q", got, incompleteStreamMessage)
	}
}

func TestStreamerContextCancel(t *testing.T) {
	reply := newFakeReplyContext()
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
	reply := newFakeReplyContext()
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)

	events := make(chan Event, 5)
	events <- Event{Type: "delta", Delta: "partial"}
	events <- Event{Type: "error", Delta: "something broke"}
	close(events)

	err := s.Run(context.Background(), events)
	if !errors.Is(err, ErrStreamFailed) {
		t.Fatalf("Run error = %v, want ErrStreamFailed", err)
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

func TestStreamerMultiMessageOverflow(t *testing.T) {
	reply := newFakeReplyContext()
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)

	paras := make([]string, 60)
	for i := range paras {
		paras[i] = fmt.Sprintf("paragraph-%d %s", i, strings.Repeat("word ", 30))
	}
	longContent := strings.Join(paras, "\n\n")

	events := make(chan Event, 10)
	events <- Event{Type: "delta", Delta: longContent}
	events <- Event{Type: "done"}
	close(events)

	err := s.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := reply.finalMessages()
	if len(msgs) < 2 {
		t.Fatalf("expected multiple messages, got %d", len(msgs))
	}

	for i, msg := range msgs {
		if len(msg) > render.TelegramLimit {
			t.Fatalf("message %d exceeds limit: %d > %d", i, len(msg), render.TelegramLimit)
		}
	}

	for i := 1; i < len(msgs); i++ {
		if !strings.HasPrefix(msgs[i], continuationMarker) {
			t.Fatalf("message %d missing continuation marker, got prefix: %q", i, msgs[i][:min(40, len(msgs[i]))])
		}
	}

	var reconstructed strings.Builder
	for i, msg := range msgs {
		if i > 0 {
			reconstructed.WriteString(strings.TrimPrefix(msg, continuationMarker))
		} else {
			reconstructed.WriteString(msg)
		}
	}
	full := reconstructed.String()
	if !strings.Contains(full, "paragraph-0") {
		t.Fatalf("reconstructed content missing first paragraph")
	}
	if !strings.Contains(full, fmt.Sprintf("paragraph-%d", len(paras)-1)) {
		t.Fatalf("reconstructed content missing last paragraph")
	}
}

func TestStreamerSingleMessageNoMarker(t *testing.T) {
	reply := newFakeReplyContext()
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)

	events := make(chan Event, 10)
	events <- Event{Type: "delta", Delta: "Short response"}
	events <- Event{Type: "done"}
	close(events)

	err := s.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := reply.finalMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 message, got %d", len(msgs))
	}
	if strings.Contains(msgs[0], continuationMarker) {
		t.Fatalf("single message should not have continuation marker")
	}
	if msgs[0] != "Short response" {
		t.Fatalf("expected 'Short response', got %q", msgs[0])
	}
}

func TestStreamerOnlyLastChunkEdited(t *testing.T) {
	reply := newFakeReplyContext()
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)

	paras := make([]string, 60)
	for i := range paras {
		paras[i] = strings.Repeat("word ", 30)
	}
	longContent := strings.Join(paras, "\n\n")

	events := make(chan Event, 10)
	events <- Event{Type: "delta", Delta: longContent[:len(longContent)/2]}
	events <- Event{Type: "delta", Delta: longContent[len(longContent)/2:]}
	events <- Event{Type: "done"}
	close(events)

	err := s.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := reply.finalMessages()
	if len(msgs) < 2 {
		t.Skip("content did not overflow, cannot test sealed-chunk no-edit")
	}

	for i := 1; i < len(msgs)-1; i++ {
		refID := fmt.Sprintf("msg-%d", i)
		if reply.editCountFor(refID) > 0 {
			t.Fatalf("sealed message %d was edited %d times", i, reply.editCountFor(refID))
		}
	}
}

func TestStreamerFinalEditReconciles(t *testing.T) {
	reply := newFakeReplyContext()
	renderer := render.New()
	s := NewStreamer(reply, renderer, render.Telegram)

	paras := make([]string, 60)
	for i := range paras {
		paras[i] = strings.Repeat("word ", 30)
	}
	longContent := strings.Join(paras, "\n\n")

	events := make(chan Event, 10)
	events <- Event{Type: "delta", Delta: longContent}
	events <- Event{Type: "done"}
	close(events)

	err := s.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := reply.finalMessages()
	if len(msgs) < 2 {
		t.Fatalf("expected multiple messages for final content, got %d", len(msgs))
	}

	expectedChunks, _ := renderer.Render(longContent, render.Telegram)
	if len(msgs) != len(expectedChunks) {
		t.Fatalf("final message count %d != expected chunk count %d", len(msgs), len(expectedChunks))
	}
}
