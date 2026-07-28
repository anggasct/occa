package relay

import (
	"bufio"
	"context"
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
				ch <- parseSSEEvent(eventType, data)
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
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			hasFields = true
		}
	}
}

func parseSSEEvent(eventType, data string) Event {
	switch {
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
