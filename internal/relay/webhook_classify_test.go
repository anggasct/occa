package relay

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestClassifyTimeoutFailure(t *testing.T) {
	tests := []struct {
		name      string
		progress  TurnProgress
		budget    time.Duration
		model     string
		wantSub   []string
		wantExact string
	}{
		{
			name:      "no delta stall before first token",
			progress:  TurnProgress{PromptSentAt: time.Now().Add(-27 * time.Minute)},
			budget:    30 * time.Minute,
			model:     "opencode/muse-spark-1.3-contributor-free@xhigh",
			wantSub:   []string{"provider stall before first token", "27m", "opencode/muse-spark-1.3-contributor-free@xhigh"},
			wantExact: "provider stall before first token (27m0s, model opencode/muse-spark-1.3-contributor-free@xhigh)",
		},
		{
			name:      "no delta and no prompt timestamp falls back to unknown",
			progress:  TurnProgress{},
			budget:    30 * time.Minute,
			model:     "",
			wantSub:   []string{"provider stall before first token", "unknown", "model unknown"},
			wantExact: "provider stall before first token (unknown, model unknown)",
		},
		{
			name: "stale last delta is mid-turn stall",
			progress: TurnProgress{
				PromptSentAt: time.Now().Add(-30 * time.Minute),
				FirstDeltaAt: time.Now().Add(-25 * time.Minute),
				LastDeltaAt:  time.Now().Add(-5 * time.Minute),
				DeltaCount:   42,
			},
			budget:    30 * time.Minute,
			model:     "opencode/muse-spark@xhigh",
			wantSub:   []string{"provider stall mid-turn", "5m0s", "opencode/muse-spark@xhigh"},
			wantExact: "provider stall mid-turn — no output for 5m0s (model opencode/muse-spark@xhigh)",
		},
		{
			name: "recent delta activity is long generation",
			progress: TurnProgress{
				PromptSentAt: time.Now().Add(-30 * time.Minute),
				FirstDeltaAt: time.Now().Add(-29 * time.Minute),
				LastDeltaAt:  time.Now().Add(-10 * time.Second),
				DeltaCount:   9999,
			},
			budget:    30 * time.Minute,
			model:     "opencode/big-model",
			wantSub:   []string{"work exceeded 30m0s", "long generation", "opencode/big-model"},
			wantExact: "work exceeded 30m0s (long generation), model opencode/big-model",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyTimeoutFailure(tt.progress, tt.budget, tt.model)
			if got != tt.wantExact {
				t.Fatalf("ClassifyTimeoutFailure() = %q, want %q", got, tt.wantExact)
			}
			if strings.Count(got, "\n") != 0 {
				t.Fatalf("classification must stay single-line, got %q", got)
			}
			for _, sub := range tt.wantSub {
				if !strings.Contains(got, sub) {
					t.Fatalf("classification %q missing %q", got, sub)
				}
			}
		})
	}
}

func TestWebhookTurnRecordsProgress(t *testing.T) {
	client := newWebhookTurnClient(eventsWith(
		Event{Type: "delta", Delta: "hello "},
		Event{Type: "delta", Delta: "world"},
		Event{Type: "done"},
	))

	before := time.Now()
	res, err := runWebhookTurn(t, client, context.Background())
	after := time.Now()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	p := res.Progress
	if p.DeltaCount != 2 {
		t.Fatalf("delta count = %d, want 2", p.DeltaCount)
	}
	if p.FirstDeltaAt.Before(before) || p.FirstDeltaAt.After(after) {
		t.Fatalf("first delta %v outside [%v, %v]", p.FirstDeltaAt, before, after)
	}
	if p.LastDeltaAt.Before(p.FirstDeltaAt) {
		t.Fatalf("last delta %v before first delta %v", p.LastDeltaAt, p.FirstDeltaAt)
	}
	if p.PromptSentAt.Before(before) || p.PromptSentAt.After(p.FirstDeltaAt) {
		t.Fatalf("prompt sent %v must fall between %v and first delta %v", p.PromptSentAt, before, p.FirstDeltaAt)
	}
}

func TestWebhookTurnTimeoutProgressShowsZeroDeltas(t *testing.T) {
	client := newWebhookTurnClient(emptyEvents())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	res, err := runWebhookTurn(t, client, ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if res.Progress.DeltaCount != 0 || res.Progress.FirstDeltaAt != (time.Time{}) {
		t.Fatalf("timed-out turn with no events must report zero progress: %+v", res.Progress)
	}
	if res.Progress.PromptSentAt.IsZero() {
		t.Fatal("prompt was sent, PromptSentAt must be set")
	}
}

func TestShortDuration(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want string
	}{
		{27 * time.Second, "27s"},
		{time.Minute, "1m0s"},
		{27 * time.Minute, "27m0s"},
		{4*time.Minute + 32*time.Second, "4m32s"},
		{2 * time.Hour, "120m0s"},
	}
	for _, tt := range tests {
		if got := shortDuration(tt.in); got != tt.want {
			t.Fatalf("shortDuration(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
