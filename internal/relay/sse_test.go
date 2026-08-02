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
	huge := strings.Repeat("y", MaxEventLineBytes+1)
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

func TestParseJSONEvents(t *testing.T) {
	textPartUpdated := `{"type":"message.part.updated","properties":{"part":{"id":"prt-1","type":"text"}}}`
	cases := []struct {
		name  string
		setup []string // prior events fed to the same decoder, e.g. the part.updated that announces a part's type
		data  string
		want  Event
		ok    bool
	}{
		{"text delta", []string{textPartUpdated}, `{"type":"message.part.delta","properties":{"partID":"prt-1","field":"text","delta":"Hi"}}`, Event{Type: "delta", Delta: "Hi"}, true},
		{"delta for unannounced part skipped", nil, `{"type":"message.part.delta","properties":{"partID":"prt-1","field":"text","delta":"Hi"}}`, Event{}, false},
		{"idle is done", nil, `{"type":"session.idle","properties":{}}`, Event{Type: "done"}, true},
		{"heartbeat skipped", nil, `{"type":"server.heartbeat","properties":{}}`, Event{}, false},
		{"session status skipped", nil, `{"type":"session.status","properties":{"status":{"type":"busy"}}}`, Event{}, false},
		{"error surfaced", nil, `{"type":"session.error","properties":{}}`, Event{Type: "error", Delta: `{"type":"session.error","properties":{}}`}, true},
	}
	for _, c := range cases {
		decoder := newEventDecoder()
		for _, setup := range c.setup {
			parseSSEEvent(decoder, "", setup)
		}
		got, ok := parseSSEEvent(decoder, "", c.data)
		if ok != c.ok || got.Type != c.want.Type || got.Delta != c.want.Delta {
			t.Fatalf("%s: got (%+v, %v), want (%+v, %v)", c.name, got, ok, c.want, c.ok)
		}
	}
}

// TestReasoningPartDeltaNeverSurfacesAsText reproduces a live-server bug:
// a ReasoningPart's own content field is also named "text" (same as
// TextPart), so its message.part.delta events carry field:"text" too.
// Filtering on field name alone let reasoning content leak into the actual
// chat reply. The decoder must track each part's announced type and only
// forward deltas for parts explicitly typed "text".
func TestReasoningPartDeltaNeverSurfacesAsText(t *testing.T) {
	decoder := newEventDecoder()

	reasoningUpdated := `{"type":"message.part.updated","properties":{"part":{"id":"prt-reasoning","type":"reasoning"}}}`
	reasoningDelta := `{"type":"message.part.delta","properties":{"partID":"prt-reasoning","field":"text","delta":"thinking out loud"}}`
	textUpdated := `{"type":"message.part.updated","properties":{"part":{"id":"prt-text","type":"text"}}}`
	textDelta := `{"type":"message.part.delta","properties":{"partID":"prt-text","field":"text","delta":"the actual reply"}}`

	for _, data := range []string{reasoningUpdated, reasoningDelta} {
		if _, ok := parseSSEEvent(decoder, "", data); ok {
			t.Fatalf("reasoning-part event must never surface, got event for %q", data)
		}
	}

	parseSSEEvent(decoder, "", textUpdated)
	got, ok := parseSSEEvent(decoder, "", textDelta)
	if !ok || got.Type != "delta" || got.Delta != "the actual reply" {
		t.Fatalf("expected text-part delta to pass through, got (%+v, %v)", got, ok)
	}
}

func TestParseLegacyEventsStillWork(t *testing.T) {
	ev, ok := parseSSEEvent(newEventDecoder(), "message.part.delta", "hello")
	if !ok || ev.Type != "delta" || ev.Delta != "hello" {
		t.Fatalf("legacy delta: %+v %v", ev, ok)
	}
	ev, ok = parseSSEEvent(newEventDecoder(), "done", "")
	if !ok || ev.Type != "done" {
		t.Fatalf("legacy done: %+v %v", ev, ok)
	}
}
