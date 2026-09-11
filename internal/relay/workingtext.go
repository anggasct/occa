package relay

import (
	"fmt"
	"strings"
	"time"
)

// WorkingTextParams carries the full progress-card state needed to render
// the canonical live progress text. It mirrors the streamer's workingState
// so both interactive turns and webhook turns render one card language.
type WorkingTextParams struct {
	Step              int
	Tool              string
	ToolContext       string
	ToolCount         int
	ToolStart         time.Time
	ReasoningActive   bool
	ReasoningStart    time.Time
	ReasoningDuration time.Duration
	Now               time.Time
}

// FormatWorkingText renders the canonical progress card text shared by the
// interactive streamer and the webhook progress ticker.
func FormatWorkingText(p WorkingTextParams) string {
	now := p.Now
	if now.IsZero() {
		now = time.Now()
	}
	if p.Step == 0 {
		if p.ReasoningActive {
			var elapsed time.Duration
			if !p.ReasoningStart.IsZero() {
				elapsed = now.Sub(p.ReasoningStart)
			}
			dur := formatDuration(elapsed)
			if dur != "" {
				return fmt.Sprintf("🧠 Thinking… (%s)", dur)
			}
			return "🧠 Thinking…"
		}
		if td := formatDuration(p.ReasoningDuration); td != "" {
			return "🧠 Thought for " + td
		}
		if p.ReasoningDuration > 0 || !p.ReasoningStart.IsZero() {
			return "🧠 Thought"
		}
		return ""
	}

	if p.ReasoningActive {
		var elapsed time.Duration
		if !p.ReasoningStart.IsZero() {
			elapsed = now.Sub(p.ReasoningStart)
		}
		dur := formatDuration(elapsed)
		toolPart := formatToolLabel(p.Tool, p.ToolContext, p.ToolCount)
		cleanTool := strings.TrimPrefix(toolPart, "⚙️ ")
		if dur != "" {
			return fmt.Sprintf("🧠 Thinking… (%s) · [Step %d] %s", dur, p.Step, cleanTool)
		}
		return fmt.Sprintf("🧠 Thinking… · [Step %d] %s", p.Step, cleanTool)
	}

	toolPart := formatToolLabel(p.Tool, p.ToolContext, p.ToolCount)
	cleanTool := strings.TrimPrefix(toolPart, "⚙️ ")
	step := p.Step
	if step <= 0 {
		step = 1
	}
	var elapsed time.Duration
	if !p.ToolStart.IsZero() {
		elapsed = now.Sub(p.ToolStart)
	}
	dur := formatDuration(elapsed)

	thoughtSuffix := ""
	if td := formatDuration(p.ReasoningDuration); td != "" {
		thoughtSuffix = " · 🧠 Thought for " + td
	}

	if dur != "" {
		return fmt.Sprintf("⚙️ [Step %d] %s (%s)%s", step, cleanTool, dur, thoughtSuffix)
	}
	return fmt.Sprintf("⚙️ [Step %d] %s%s", step, cleanTool, thoughtSuffix)
}
