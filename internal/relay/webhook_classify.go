package relay

import (
	"fmt"
	"time"
)

// StallFreshness is how recent the last stream delta must be for a timed-out
// turn to count as "work still progressing" rather than a provider stall. Two
// minutes of silence while streaming means the provider stopped producing.
const StallFreshness = 2 * time.Minute

// TurnProgress records how far a webhook turn got before it ended. It is the
// evidence the timeout classifier reads: whether the prompt was sent, whether
// any output token ever arrived, and how stale the stream was at abort.
type TurnProgress struct {
	PromptSentAt time.Time
	FirstDeltaAt time.Time
	LastDeltaAt  time.Time
	DeltaCount   int
}

// ClassifyTimeoutFailure turns observed turn progress into the reason suffix
// appended to the timeout summary. Zero deltas means the model never produced
// a token (provider stall before first token); a stale last delta means the
// stream went silent mid-turn; fresh deltas mean work was still arriving and
// the budget was simply outgrown.
func ClassifyTimeoutFailure(progress TurnProgress, budget time.Duration, model string) string {
	classification := "work exceeded " + budget.String() + " (long generation)"
	if model != "" {
		classification += ", model " + model
	}

	switch {
	case progress.DeltaCount == 0:
		return fmt.Sprintf("provider stall before first token (%s, model %s)", elapsedSince(progress.PromptSentAt), modelString(model))
	case time.Since(progress.LastDeltaAt) >= StallFreshness:
		return fmt.Sprintf("provider stall mid-turn — no output for %s (model %s)", shortDuration(time.Since(progress.LastDeltaAt)), modelString(model))
	default:
		return classification
	}
}

func elapsedSince(t time.Time) string {
	if t.IsZero() {
		return "unknown"
	}
	return shortDuration(time.Since(t))
}

func modelString(model string) string {
	if model == "" {
		return "unknown"
	}
	return model
}

// shortDuration renders a duration in minutes+seconds, dropping the zero
// minute component so the reason line stays compact ("4m32s", "27s").
func shortDuration(d time.Duration) string {
	if d < time.Minute {
		return d.Round(time.Second).String()
	}
	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60
	return fmt.Sprintf("%dm%ds", minutes, seconds)
}
