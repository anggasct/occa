package webhook

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/anggasct/occa/internal/relay"
)

func testEnvelope() WebhookEnvelope {
	return WebhookEnvelope{
		"repository":  "org/repo",
		"pr_number":   "42",
		"head_branch": "feat/test",
		"base_branch": "main",
		"delivery_id": "del-1",
	}
}

func collectEdits() (func(ctx context.Context, channelID, messageID, text string) error, *sync.Mutex, *[]string) {
	var mu sync.Mutex
	var edits []string
	editFn := func(ctx context.Context, channelID, messageID, text string) error {
		mu.Lock()
		defer mu.Unlock()
		edits = append(edits, text)
		return nil
	}
	return editFn, &mu, &edits
}

func lastEdit(mu *sync.Mutex, edits *[]string) string {
	mu.Lock()
	defer mu.Unlock()
	if len(*edits) == 0 {
		return ""
	}
	return (*edits)[len(*edits)-1]
}

// FormatProgressStatus must delegate to the shared streamer helper: identical
// inputs render identical strings.
func TestFormatProgressStatusMatchesSharedHelper(t *testing.T) {
	got := FormatProgressStatus(1, "bash", "echo hi", 65*time.Second)
	want := relay.FormatWorkingText(relay.WorkingTextParams{
		Step:        1,
		Tool:        "bash",
		ToolContext: "echo hi",
		ToolCount:   1,
		ToolStart:   time.Now().Add(-65 * time.Second),
		Now:         time.Now(),
	})
	if got != want {
		t.Fatalf("FormatProgressStatus = %q, shared helper = %q", got, want)
	}
	if got != "⚙️ [Step 1] bash: echo hi (1m 5s)" {
		t.Fatalf("FormatProgressStatus = %q, want golden %q", got, "⚙️ [Step 1] bash: echo hi (1m 5s)")
	}
}

// A scripted SSE sequence (reasoning → tool) must drive the webhook card
// through Thinking… into the Thought-for suffix state.
func TestProgressCardUpdaterReasoningSequence(t *testing.T) {
	editFn, mu, edits := collectEdits()
	updater := NewProgressCardUpdater("chan-1", "msg-1", testEnvelope(), "github_reviewer", "", "discord", editFn)
	defer updater.Stop()
	updater.SetMinInterval(0)

	now := time.Unix(100, 0)
	updater.SetNowFunc(func() time.Time { return now })

	updater.OnEvent(relay.Event{Type: relay.EventReasoning})
	if edit := lastEdit(mu, edits); !strings.Contains(edit, "Status: 🧠 Thinking…") {
		t.Fatalf("reasoning edit missing Thinking card: %q", edit)
	}

	now = time.Unix(112, 0)
	updater.OnEvent(relay.Event{Type: relay.EventTool, Delta: "bash"})
	edit := lastEdit(mu, edits)
	if !strings.Contains(edit, "Status: ⚙️ [Step 1] bash · 🧠 Thought for 12s") {
		t.Fatalf("tool edit missing Thought-for suffix: %q", edit)
	}
}

// Without reasoning events the card must render exactly the old tool-only
// text with no reasoning artifacts.
func TestProgressCardUpdaterNoReasoningMatchesToolOnlyText(t *testing.T) {
	editFn, mu, edits := collectEdits()
	updater := NewProgressCardUpdater("chan-1", "msg-1", testEnvelope(), "github_reviewer", "", "discord", editFn)
	defer updater.Stop()
	updater.SetMinInterval(0)

	now := time.Unix(200, 0)
	updater.SetNowFunc(func() time.Time { return now })
	updater.OnTool("bash", "echo 1", false)

	now = time.Unix(205, 0)
	updater.scheduleLocked(now)

	edit := lastEdit(mu, edits)
	if strings.Contains(edit, "🧠") {
		t.Fatalf("no-reasoning edit must not contain reasoning text: %q", edit)
	}
	if !strings.Contains(edit, "Status: ⚙️ [Step 1] bash: echo 1 (5s)") {
		t.Fatalf("no-reasoning edit missing old tool-only text: %q", edit)
	}
	if len(*edits) == 0 {
		t.Fatal("expected at least 1 edit")
	}
}

// Repeated same-tool executions consolidate onto one step with ×N,
// following the streamer semantics.
func TestProgressCardUpdaterSameToolConsolidates(t *testing.T) {
	editFn, mu, edits := collectEdits()
	updater := NewProgressCardUpdater("chan-1", "msg-1", testEnvelope(), "github_reviewer", "", "discord", editFn)
	defer updater.Stop()
	updater.SetMinInterval(0)

	now := time.Unix(300, 0)
	updater.SetNowFunc(func() time.Time { return now })
	updater.OnTool("bash", "a", false)

	now = time.Unix(302, 0)
	updater.OnTool("bash", "b", false)

	edit := lastEdit(mu, edits)
	if !strings.Contains(edit, "Status: ⚙️ [Step 1] bash ×2: b (2s)") {
		t.Fatalf("repeated tool missing consolidated ×2 text: %q", edit)
	}
}

// An oversized card must be clamped to the platform limit with a visible
// truncation marker instead of hitting a platform 400. Tool context is
// pre-capped at 40 runes by the shared formatter, so overflow here comes
// from an unbounded envelope field.
func TestProgressCardUpdaterClampsOversizedCard(t *testing.T) {
	editFn, mu, edits := collectEdits()
	envelope := testEnvelope()
	envelope["head_branch"] = strings.Repeat("x", 5000)
	updater := NewProgressCardUpdater("chan-1", "msg-1", envelope, "github_reviewer", "", "discord", editFn)
	defer updater.Stop()
	updater.SetMinInterval(0)

	updater.OnTool("bash", strings.Repeat("y", 5000), false)

	edit := lastEdit(mu, edits)
	if edit == "" {
		t.Fatal("expected an edit")
	}
	if n := utf8.RuneCountInString(edit); n > 2000 {
		t.Fatalf("clamped edit exceeds discord limit: %d runes", n)
	}
	if !strings.Contains(edit, "…") {
		t.Fatalf("clamped edit missing truncation marker: %q", edit[len(edit)-100:])
	}
}
