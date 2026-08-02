package relay

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"
)

// MaxEventLineBytes bounds a single stream line: 1 MB of data payload plus
// line overhead. Agent output that embeds file content routinely exceeds
// bufio's 64 KB default.
const MaxEventLineBytes = 1024*1024 + 64*1024

// readSSE reads events until clean EOF, a read failure, or ctx cancellation.
// A failure that is not a clean EOF is emitted as a terminal stream_error
// event so callers can tell it apart from a clean close, and returned.
func readSSE(ctx context.Context, r io.Reader, ch chan<- Event) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), MaxEventLineBytes+1)
	decoder := newEventDecoder()
	var eventType, data string
	var hasFields bool

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Text()

		if line == "" {
			if hasFields {
				if event, ok := parseSSEEvent(decoder, eventType, data); ok {
					select {
					case ch <- event:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				eventType = ""
				data = ""
				hasFields = false
			}
			continue
		}

		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			hasFields = true
		} else if strings.HasPrefix(line, "data:") {
			chunk := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data != "" {
				data += "\n"
			}
			data += chunk
			hasFields = true
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := scanner.Err(); err != nil {
		select {
		case ch <- Event{Type: "stream_error", Err: err}:
		case <-ctx.Done():
		}
		return err
	}
	return nil
}

func parseSSEEvent(decoder *eventDecoder, eventType, data string) (Event, bool) {
	if eventType == "" {
		return decoder.parseJSON(data)
	}
	switch {
	case strings.Contains(eventType, "permission.asked") || strings.Contains(eventType, "permission"):
		return parsePermissionEvent(data), true
	case strings.Contains(eventType, "delta") || strings.Contains(eventType, "message.part.delta"):
		return Event{Type: "delta", Delta: data}, true
	case strings.Contains(eventType, "done") || strings.Contains(eventType, "complete"):
		return Event{Type: "done"}, true
	case strings.Contains(eventType, "error"):
		return Event{Type: "error", Delta: data}, true
	default:
		return Event{Type: "delta", Delta: data}, true
	}
}

// eventDecoder tracks each message part's type (as announced by
// message.part.updated) so message.part.delta events can be attributed to
// the right part. This is required because a part's own content field is
// always named "text" regardless of the part's type — a ReasoningPart's
// text and a TextPart's text both stream as field:"text" deltas, so field
// name alone cannot distinguish reasoning (internal) from text (the actual
// reply) content.
type eventDecoder struct {
	partKind   map[string]string
	activeKind string
}

func newEventDecoder() *eventDecoder {
	return &eventDecoder{partKind: make(map[string]string)}
}

// isStreamKind reports whether a part type participates in stream-boundary
// detection. Container/bookkeeping part types are skipped so they never
// trigger a spurious segment at stream start.
func isStreamKind(kind string) bool {
	switch kind {
	case "text", "tool", "reasoning":
		return true
	}
	return false
}

// parseJSON handles the current agent event stream, where the event type
// lives inside the JSON payload rather than in an SSE event: line.
// Bookkeeping events (heartbeats, session status) are skipped; text deltas
// from text-typed parts and the idle transition that marks completion are
// mapped to relay events. Part-type transitions emit segment (text finalized,
// next delta starts a new message) and tool parts emit a tool notice event.
func (d *eventDecoder) parseJSON(data string) (Event, bool) {
	var ev struct {
		Type       string `json:"type"`
		Properties struct {
			Field  string `json:"field"`
			Delta  string `json:"delta"`
			PartID string `json:"partID"`
			Part   struct {
				ID   string `json:"id"`
				Type string `json:"type"`
				Tool string `json:"tool"`
			} `json:"part"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		return Event{Type: "delta", Delta: data}, true
	}
	switch {
	case ev.Type == "message.part.updated":
		kind := ev.Properties.Part.Type
		if ev.Properties.Part.ID != "" {
			d.partKind[ev.Properties.Part.ID] = kind
		}
		if !isStreamKind(kind) {
			return Event{}, false
		}
		prev := d.activeKind
		d.activeKind = kind
		switch {
		case kind == "tool":
			return Event{Type: EventTool, Delta: ev.Properties.Part.Tool}, true
		case prev == "" || kind == prev:
			return Event{}, false
		case prev == "text" || kind == "text":
			return Event{Type: EventSegment}, true
		default:
			return Event{}, false
		}
	case ev.Type == "message.part.delta" && ev.Properties.Field == "text" && d.partKind[ev.Properties.PartID] == "text":
		return Event{Type: EventDelta, Delta: ev.Properties.Delta}, true
	case ev.Type == "session.idle":
		return Event{Type: EventDone}, true
	case strings.Contains(ev.Type, "error"):
		return Event{Type: EventError, Delta: data}, true
	default:
		return Event{}, false
	}
}

func parsePermissionEvent(data string) Event {
	var payload struct {
		Properties struct {
			ID         string `json:"id"`
			SessionID  string `json:"sessionID"`
			Permission string `json:"permission"`
			Tool       string `json:"tool"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return Event{Type: "delta", Delta: data}
	}
	return Event{
		Type: "permission_asked",
		Permission: &PermissionRequest{
			ID:         payload.Properties.ID,
			SessionID:  payload.Properties.SessionID,
			Permission: payload.Properties.Permission,
			Tool:       payload.Properties.Tool,
		},
	}
}
