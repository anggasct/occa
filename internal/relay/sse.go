package relay

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"
)

func readSSE(ctx context.Context, r io.Reader, ch chan<- Event) {
	scanner := bufio.NewScanner(r)
	var eventType, data string
	var hasFields bool

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := scanner.Text()

		if line == "" {
			if hasFields {
				event := parseSSEEvent(eventType, data)
				select {
				case ch <- event:
				case <-ctx.Done():
					return
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
