package webhook

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProgressCardUpdaterDebounceAndLastTextGuard(t *testing.T) {
	var mu sync.Mutex
	var edits []string

	envelope := WebhookEnvelope{
		"repository":  "org/repo",
		"pr_number":   "42",
		"head_branch": "feat/test",
		"base_branch": "main",
		"delivery_id": "del-1",
	}

	editFn := func(ctx context.Context, channelID, messageID, text string) error {
		mu.Lock()
		defer mu.Unlock()
		edits = append(edits, text)
		return nil
	}

	updater := NewProgressCardUpdater("chan-1", "msg-1", envelope, "github_reviewer", "", editFn)
	defer updater.Stop()

	// Configure small interval for test speed
	interval := 50 * time.Millisecond
	updater.SetMinInterval(interval)

	// First event should edit immediately
	updater.OnTool("bash", "echo 1", false)

	mu.Lock()
	if len(edits) != 1 {
		mu.Unlock()
		t.Fatalf("expected 1 edit immediately, got %d", len(edits))
	}
	firstEdit := edits[0]
	mu.Unlock()

	if !strings.Contains(firstEdit, "[Step 1]") || !strings.Contains(firstEdit, "bash: echo 1") {
		t.Errorf("first edit missing step or tool info: %s", firstEdit)
	}

	// Rapidly fire multiple tool events within the debounce window
	updater.OnTool("bash", "echo 2", false)
	updater.OnTool("read", "file.go", false)

	// In the debounce window, edits count should still be 1
	mu.Lock()
	countMid := len(edits)
	mu.Unlock()
	if countMid != 1 {
		t.Errorf("expected still 1 edit during debounce window, got %d", countMid)
	}

	// Wait for debounce window to elapse
	time.Sleep(interval * 2)

	mu.Lock()
	if len(edits) != 2 {
		mu.Unlock()
		t.Fatalf("expected exactly 2 edits after debounce window, got %d", len(edits))
	}
	secondEdit := edits[1]
	mu.Unlock()

	// Second edit should reflect the latest tool (Step 3: read: file.go)
	if !strings.Contains(secondEdit, "[Step 3]") || !strings.Contains(secondEdit, "read: file.go") {
		t.Errorf("second edit missing latest tool info: %s", secondEdit)
	}

	// Identical text edit guard test:
	// Calling schedule with identical status should NOT trigger an edit
	updater.mu.Lock()
	lastText := updater.lastText
	updater.mu.Unlock()

	updater.scheduleLocked(time.Now())
	time.Sleep(interval)

	mu.Lock()
	if len(edits) != 2 {
		mu.Unlock()
		t.Errorf("identical text was not suppressed by lastText guard, edits count = %d", len(edits))
	}
	mu.Unlock()

	if updater.lastText != lastText {
		t.Errorf("lastText changed unexpectedly")
	}
}

func TestProgressCardUpdaterSamePartContextUpdate(t *testing.T) {
	var mu sync.Mutex
	var edits []string

	envelope := WebhookEnvelope{
		"repository":  "org/repo",
		"pr_number":   "42",
		"delivery_id": "del-1",
	}

	editFn := func(ctx context.Context, channelID, messageID, text string) error {
		mu.Lock()
		defer mu.Unlock()
		edits = append(edits, text)
		return nil
	}

	updater := NewProgressCardUpdater("chan-1", "msg-1", envelope, "github_reviewer", "", editFn)
	defer updater.Stop()

	interval := 30 * time.Millisecond
	updater.SetMinInterval(interval)

	// Step 1: tool with no initial context
	updater.OnTool("bash", "", false)

	time.Sleep(interval + 10*time.Millisecond)

	// Tool same part updates context
	updater.OnTool("bash", "git status", true)

	time.Sleep(interval + 20*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(edits) < 2 {
		t.Fatalf("expected at least 2 edits, got %d", len(edits))
	}
	lastEdit := edits[len(edits)-1]
	if !strings.Contains(lastEdit, "bash: git status") {
		t.Errorf("expected context to be updated to 'git status', got %s", lastEdit)
	}
}
