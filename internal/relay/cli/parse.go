package cli

import (
	"encoding/json"
	"strings"

	"github.com/anggasct/occa/internal/relay"
)

// parser turns stream-json stdout lines into relay events and captures the
// tool's own session id from the terminal result line.
type parser struct {
	realID string
}

func (p *parser) parseLine(line []byte) *relay.Event {
	var obj map[string]any
	if err := json.Unmarshal(line, &obj); err != nil {
		return nil
	}

	switch obj["type"] {
	case "stream_event":
		return p.parseStreamEvent(obj)
	case "assistant":
		return p.parseAssistant(obj)
	case "result":
		if id, ok := obj["session_id"].(string); ok && id != "" {
			p.realID = id
		}
		if subtype, _ := obj["subtype"].(string); subtype != "" && subtype != "success" {
			return &relay.Event{Type: "error", Delta: "cli: run finished with subtype " + subtype}
		}
		return &relay.Event{Type: "done"}
	case "error":
		return &relay.Event{Type: "error", Delta: stringify(obj["error"])}
	}
	return nil
}

func (p *parser) parseStreamEvent(obj map[string]any) *relay.Event {
	ev, ok := obj["event"].(map[string]any)
	if !ok {
		return nil
	}
	switch ev["type"] {
	case "content_block_delta":
		if delta, ok := ev["delta"].(map[string]any); ok {
			if text, ok := delta["text"].(string); ok && text != "" {
				return &relay.Event{Type: "delta", Delta: text}
			}
		}
	}
	return nil
}

func (p *parser) parseAssistant(obj map[string]any) *relay.Event {
	msg, ok := obj["message"].(map[string]any)
	if !ok {
		return nil
	}
	text := textFromContent(msg["content"])
	if text == "" {
		return nil
	}
	return &relay.Event{Type: "delta", Delta: text}
}

// textFromContent joins an assistant message's content, which is either a
// string or a list of typed blocks.
func textFromContent(content any) string {
	switch c := content.(type) {
	case string:
		return c
	case []any:
		var out strings.Builder
		for _, block := range c {
			if m, ok := block.(map[string]any); ok {
				if text, ok := m["text"].(string); ok {
					out.WriteString(text)
				}
			}
		}
		return out.String()
	}
	return ""
}

func stringify(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(data)
	}
}
