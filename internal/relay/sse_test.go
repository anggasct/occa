package relay

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReadSSELargeLineDelivered(t *testing.T) {
	big := strings.Repeat("x", 1024*1024)
	ch := make(chan Event, 8)
	go func() {
		_ = readSSE(context.Background(), strings.NewReader("event: message.part.delta\ndata: "+big+"\n\n"), ch)
	}()

	ev := <-ch
	if ev.Type != "delta" || ev.Delta != big {
		t.Fatalf("large line mangled: type=%s len=%d", ev.Type, len(ev.Delta))
	}
}

func TestReadSSELineOverLimitIsTypedError(t *testing.T) {
	huge := strings.Repeat("y", maxEventLineBytes+1)
	ch := make(chan Event, 8)
	done := make(chan error, 1)
	go func() { done <- readSSE(context.Background(), strings.NewReader("data: "+huge+"\n\n"), ch) }()

	err := <-done
	if err == nil {
		t.Fatal("expected a read error for an over-limit line")
	}
	ev := <-ch
	if ev.Type != "stream_error" || ev.Err == nil {
		t.Fatalf("expected terminal stream_error event, got %+v", ev)
	}
}

type failingReader struct {
	sent  bool
	mu    sync.Mutex
	after chan struct{}
}

func (r *failingReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.sent {
		r.sent = true
		return copy(p, "event: message.part.delta\ndata: partial\n\n"), nil
	}
	if r.after != nil {
		<-r.after
	}
	return 0, errors.New("connection reset")
}

func TestReadSSEMidStreamFailureDeliversPriorEvents(t *testing.T) {
	ch := make(chan Event, 8)
	reader := &failingReader{}
	done := make(chan error, 1)
	go func() { done <- readSSE(context.Background(), reader, ch) }()

	ev := <-ch
	if ev.Type != "delta" || ev.Delta != "partial" {
		t.Fatalf("prior event missing: %+v", ev)
	}
	err := <-done
	if err == nil || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("expected the transport error, got %v", err)
	}
	ev = <-ch
	if ev.Type != "stream_error" || ev.Err == nil {
		t.Fatalf("expected terminal stream_error event, got %+v", ev)
	}
}

func TestReadSSECleanEOFNoError(t *testing.T) {
	ch := make(chan Event, 8)
	if err := readSSE(context.Background(), strings.NewReader("event: done\n\n"), ch); err != nil {
		t.Fatalf("clean EOF must not error: %v", err)
	}
	if ev := <-ch; ev.Type != "done" {
		t.Fatalf("expected done event, got %+v", ev)
	}
}

func TestReadSSECancelIsNotReadFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan Event, 8)
	pr, pw := io.Pipe()

	done := make(chan error, 1)
	go func() { done <- readSSE(ctx, pr, ch) }()

	cancel()
	_ = pw.Close() // unblock the pending read

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readSSE did not stop promptly on cancel")
	}
	select {
	case ev := <-ch:
		if ev.Type == "stream_error" {
			t.Fatal("cancel must not be reported as a stream error")
		}
	default:
	}
}

func TestEventsLargeLineThroughHTTPServer(t *testing.T) {
	big := strings.Repeat("z", 1024*1024)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message.part.delta\ndata: "+big+"\n\n")
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL)
	events, err := client.Events(context.Background(), "sess-1")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	ev, ok := <-events
	if !ok {
		t.Fatal("events channel closed without delivering")
	}
	if ev.Delta != big {
		t.Fatalf("1MB event not delivered intact: %d bytes", len(ev.Delta))
	}
}
