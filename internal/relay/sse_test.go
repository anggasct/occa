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

// TestDecoderPartTransitions: part-type transitions emit segment/tool events
// and non-text deltas never surface as reply text.
func TestDecoderPartTransitions(t *testing.T) {
	updated := func(id, kind string) string {
		return `{"type":"message.part.updated","properties":{"part":{"id":"` + id + `","type":"` + kind + `"}}}`
	}
	delta := func(partID, text string) string {
		return `{"type":"message.part.delta","properties":{"partID":"` + partID + `","field":"text","delta":"` + text + `"}}`
	}

	cases := []struct {
		name     string
		sequence []string
		want     []Event
	}{
		{
			name:     "text to tool emits tool event",
			sequence: []string{updated("p1", "text"), updated("p2", "tool")},
			want:     []Event{{Type: EventTool}},
		},
		{
			name:     "text to reasoning emits segment",
			sequence: []string{updated("p1", "text"), updated("p2", "reasoning")},
			want:     []Event{{Type: EventSegment}},
		},
		{
			name:     "reasoning to text emits segment",
			sequence: []string{updated("p1", "reasoning"), updated("p2", "text")},
			want:     []Event{{Type: EventSegment}},
		},
		{
			name:     "tool to text emits segment after the tool event",
			sequence: []string{updated("p1", "tool"), updated("p2", "text")},
			want:     []Event{{Type: EventTool}, {Type: EventSegment}},
		},
		{
			name:     "adjacent tool parts each emit a tool event",
			sequence: []string{updated("p1", "tool"), updated("p2", "tool")},
			want:     []Event{{Type: EventTool}, {Type: EventTool}},
		},
		{
			name:     "same kind emits nothing",
			sequence: []string{updated("p1", "text"), updated("p2", "text")},
			want:     nil,
		},
		{
			name:     "first part emits nothing",
			sequence: []string{updated("p1", "text")},
			want:     nil,
		},
		{
			name:     "container part ignored for boundaries",
			sequence: []string{updated("p0", "message"), updated("p1", "text")},
			want:     nil,
		},
		{
			name: "full tool round trip",
			sequence: []string{
				updated("p1", "text"), delta("p1", "before"),
				updated("p2", "tool"), delta("p2", "internal tool progress"),
				updated("p3", "text"), delta("p3", "after"),
			},
			want: []Event{
				{Type: EventDelta, Delta: "before"},
				{Type: EventTool},
				{Type: EventSegment},
				{Type: EventDelta, Delta: "after"},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			decoder := newEventDecoder()
			var got []Event
			for _, data := range c.sequence {
				if ev, ok := parseSSEEvent(decoder, "", data); ok {
					got = append(got, ev)
				}
			}
			if len(got) != len(c.want) {
				t.Fatalf("events = %+v, want %+v", got, c.want)
			}
			for i := range got {
				if got[i].Type != c.want[i].Type || got[i].Delta != c.want[i].Delta {
					t.Fatalf("event %d = %+v, want %+v (all: %+v)", i, got[i], c.want[i], got)
				}
			}
			reply := ""
			for _, data := range c.sequence {
				if ev, ok := parseSSEEvent(decoder, "", data); ok && ev.Type == EventDelta {
					reply += ev.Delta
				}
			}
			if strings.Contains(reply, "internal tool progress") || strings.Contains(reply, "thinking out loud") {
				t.Fatalf("non-text delta leaked into reply: %q", reply)
			}
		})
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

// TestDecoderToolPartCarriesName: a tool part's name reaches the event so
// the notice can show which tool ran.
func TestDecoderToolPartCarriesName(t *testing.T) {
	decoder := newEventDecoder()
	updated := `{"type":"message.part.updated","properties":{"part":{"id":"prt-t","type":"tool","tool":"bash"}}}`
	ev, ok := parseSSEEvent(decoder, "", updated)
	if !ok || ev.Type != EventTool || ev.Delta != "bash" {
		t.Fatalf("tool event = (%+v, %v), want EventTool with Delta bash", ev, ok)
	}

	noName := `{"type":"message.part.updated","properties":{"part":{"id":"prt-t2","type":"tool"}}}`
	ev2, ok2 := parseSSEEvent(decoder, "", noName)
	if !ok2 || ev2.Type != EventTool || ev2.Delta != "" {
		t.Fatalf("nameless tool event = (%+v, %v), want empty Delta", ev2, ok2)
	}
}

// TestDecodeEmptyTextDeltaDropped: empty text deltas produce no event so the
// streamer never tries to send an empty message.
func TestDecodeEmptyTextDeltaDropped(t *testing.T) {
	d := newEventDecoder()
	d.parseJSON(`{"type":"message.part.updated","properties":{"part":{"id":"p1","type":"text"}}}`)
	ev, ok := d.parseJSON(`{"type":"message.part.delta","properties":{"field":"text","partID":"p1","delta":""}}`)
	if ok {
		t.Fatalf("empty delta produced event %+v", ev)
	}
}

// TestDecodeQuestionAsked parses a question.asked event with options.
func TestDecodeQuestionAsked(t *testing.T) {
	ev, ok := parseSSEEvent(newEventDecoder(), "question.asked", `{"id":"evt_1","type":"question.asked","properties":{"id":"que_1","sessionID":"ses_1","questions":[{"question":"Pilih?","header":"H","options":[{"label":"A","description":"opsi a"},{"label":"B"}]}],"tool":{"messageID":"msg_1","callID":"call_1"}}}`)
	if !ok || ev.Type != "question_asked" || ev.Question == nil {
		t.Fatalf("question event not parsed: %+v", ev)
	}
	q := ev.Question
	if q.ID != "que_1" || q.SessionID != "ses_1" || len(q.Questions) != 1 {
		t.Fatalf("question meta wrong: %+v", q)
	}
	info := q.Questions[0]
	if info.Question != "Pilih?" || info.Header != "H" || len(info.Options) != 2 {
		t.Fatalf("question info wrong: %+v", info)
	}
	if info.Options[0].Label != "A" || info.Options[0].Description != "opsi a" || info.Options[1].Label != "B" {
		t.Fatalf("question options wrong: %+v", info.Options)
	}
}

// TestDecodeQuestionAskedViaPayload parses a question.asked event that
// arrives with the type inside the JSON payload (no SSE event: line).
func TestDecodeQuestionAskedViaPayload(t *testing.T) {
	ev, ok := parseSSEEvent(newEventDecoder(), "", `{"type":"question.asked","properties":{"id":"que_2","sessionID":"ses_1","questions":[{"question":"Berapa?","header":"H","options":[{"label":"A"}]}],"tool":{"messageID":"m","callID":"c"}}}`)
	if !ok || ev.Type != "question_asked" || ev.Question == nil {
		t.Fatalf("payload question event not parsed: %+v", ev)
	}
	if ev.Question.ID != "que_2" || len(ev.Question.Questions) != 1 || ev.Question.Questions[0].Options[0].Label != "A" {
		t.Fatalf("question payload wrong: %+v", ev.Question)
	}
}

func TestDecoderToolContextExtraction(t *testing.T) {
	updated := func(partID, tool, stateInput string) string {
		if stateInput != "" {
			return `{"type":"message.part.updated","properties":{"part":{"id":"` + partID + `","type":"tool","tool":"` + tool + `","state":{"input":` + stateInput + `}}}}`
		}
		return `{"type":"message.part.updated","properties":{"part":{"id":"` + partID + `","type":"tool","tool":"` + tool + `"}}}`
	}

	cases := []struct {
		name        string
		payload     string
		wantTool    string
		wantContext string
	}{
		{
			name:        "filePath prioritized",
			payload:     updated("p1", "read", `{"filePath":"internal/relay/sse.go","command":"ls","path":"internal"}`),
			wantTool:    "read",
			wantContext: "internal/relay/sse.go",
		},
		{
			name:        "command used when filePath absent",
			payload:     updated("p2", "bash", `{"command":"go test ./...","path":"internal"}`),
			wantTool:    "bash",
			wantContext: "go test ./...",
		},
		{
			name:        "path used when filePath and command absent",
			payload:     updated("p3", "grep", `{"path":"internal/relay"}`),
			wantTool:    "grep",
			wantContext: "internal/relay",
		},
		{
			name:        "empty filePath falls back to command",
			payload:     updated("p4", "bash", `{"filePath":"","command":"git status"}`),
			wantTool:    "bash",
			wantContext: "git status",
		},
		{
			name:        "non-string value for priority key skipped",
			payload:     updated("p5", "list", `{"filePath":123,"path":"pkg/relay"}`),
			wantTool:    "list",
			wantContext: "pkg/relay",
		},
		{
			name:        "empty input object yields empty context",
			payload:     updated("p6", "read", `{}`),
			wantTool:    "read",
			wantContext: "",
		},
		{
			name:        "unknown keys only yields empty context",
			payload:     updated("p7", "custom", `{"foo":"bar"}`),
			wantTool:    "custom",
			wantContext: "",
		},
		{
			name:        "missing state input yields empty context",
			payload:     updated("p8", "read", ""),
			wantTool:    "read",
			wantContext: "",
		},
		{
			name:        "malformed input yields empty context defensively",
			payload:     updated("p9", "read", `"not an object"`),
			wantTool:    "read",
			wantContext: "",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			decoder := newEventDecoder()
			ev, ok := parseSSEEvent(decoder, "", c.payload)
			if !ok {
				t.Fatalf("expected event, got none")
			}
			if ev.Type != EventTool {
				t.Fatalf("event type = %q, want %q", ev.Type, EventTool)
			}
			if ev.Delta != c.wantTool {
				t.Fatalf("tool delta = %q, want %q", ev.Delta, c.wantTool)
			}
			if ev.ToolContext != c.wantContext {
				t.Fatalf("ToolContext = %q, want %q", ev.ToolContext, c.wantContext)
			}
		})
	}
}

func TestDecoderToolSamePartDedupe(t *testing.T) {
	// opencode streams several message.part.updated events per tool part
	// (pending → running → completed). The first event may arrive without
	// state.input; when the input arrives later for the same part, the decoder
	// must emit exactly one follow-up tool notice marked ToolSamePart so the
	// streamer updates the existing bubble instead of starting a new one.
	updated := func(partID, tool, stateInput string) string {
		if stateInput != "" {
			return `{"type":"message.part.updated","properties":{"part":{"id":"` + partID + `","type":"tool","tool":"` + tool + `","state":{"input":` + stateInput + `}}}}`
		}
		return `{"type":"message.part.updated","properties":{"part":{"id":"` + partID + `","type":"tool","tool":"` + tool + `"}}}`
	}

	decoder := newEventDecoder()

	// 1st event for part p1: pending, no input yet → bubble "⚙️ bash".
	ev, ok := parseSSEEvent(decoder, "", updated("p1", "bash", ""))
	if !ok || ev.Type != EventTool {
		t.Fatalf("first event: got ok=%v type=%v, want tool event", ok, ev.Type)
	}
	if ev.ToolSamePart {
		t.Fatalf("first event must not be same-part")
	}

	// 2nd event for p1: running, command arrived → update-in-place signal.
	ev, ok = parseSSEEvent(decoder, "", updated("p1", "bash", `{"command":"go test ./..."}`))
	if !ok || ev.Type != EventTool {
		t.Fatalf("second event: got ok=%v type=%v, want tool event", ok, ev.Type)
	}
	if !ev.ToolSamePart {
		t.Fatalf("second event must be same-part update")
	}
	if ev.ToolContext != "go test ./..." {
		t.Fatalf("ToolContext = %q, want %q", ev.ToolContext, "go test ./...")
	}

	// 3rd event for p1: completed with identical input → dedupe, no event.
	if ev, ok = parseSSEEvent(decoder, "", updated("p1", "bash", `{"command":"go test ./..."}`)); ok {
		t.Fatalf("duplicate identical event for same part must be dropped, got type=%v", ev.Type)
	}

	// A different part with the same tool must be a fresh (non-same-part) event.
	ev, ok = parseSSEEvent(decoder, "", updated("p2", "bash", `{"command":"go vet ./..."}`))
	if !ok || ev.Type != EventTool {
		t.Fatalf("new part event: got ok=%v type=%v, want tool event", ok, ev.Type)
	}
	if ev.ToolSamePart {
		t.Fatalf("new part must not be same-part update")
	}
}

func TestNonToolEventsNeverLeakToolContext(t *testing.T) {
	decoder := newEventDecoder()
	parseSSEEvent(decoder, "", `{"type":"message.part.updated","properties":{"part":{"id":"p1","type":"text"}}}`)

	deltaEv, ok := parseSSEEvent(decoder, "", `{"type":"message.part.delta","properties":{"partID":"p1","field":"text","delta":"hi"}}`)
	if !ok || deltaEv.ToolContext != "" {
		t.Fatalf("delta event ToolContext = %q, want empty", deltaEv.ToolContext)
	}

	doneEv, ok := parseSSEEvent(decoder, "", `{"type":"session.idle","properties":{}}`)
	if !ok || doneEv.ToolContext != "" {
		t.Fatalf("done event ToolContext = %q, want empty", doneEv.ToolContext)
	}
}
