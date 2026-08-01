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
				event := parseSSEEvent(eventType, data)
				select {
				case ch <- event:
				case <-ctx.Done():
					return ctx.Err()
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

func parseSSEEvent(eventType, data string) Event {
	switch {
	case strings.Contains(eventType, "permission.asked") || strings.Contains(eventType, "permission"):
		return parsePermissionEvent(data)
	case strings.Contains(eventType, "delta") || strings.Contains(eventType, "message.part.delta"):
		return Event{Type: "delta", Delta: data}
	case strings.Contains(eventType, "done") || strings.Contains(eventType, "complete"):
		return Event{Type: "done"}
	case strings.Contains(eventType, "error"):
		return Event{Type: "error", Delta: data}
	default:
		return Event{Type: "delta", Delta: data}
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
