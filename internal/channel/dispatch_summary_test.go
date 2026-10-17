package channel

import "testing"

func TestParseDispatchSummary(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		wantRepo   string
		wantBranch string
		wantTask   string
		wantOK     bool
	}{
		{
			name:       "api two-line summary",
			text:       "⏳ Dispatched acme/widget#feat/x · task-1792210842511548349-1\nSpec: specs/feat-x.md · Verify: go test ./...",
			wantRepo:   "acme/widget",
			wantBranch: "feat/x",
			wantTask:   "task-1792210842511548349-1",
			wantOK:     true,
		},
		{
			name:       "callback one-line summary",
			text:       "⏳ Dispatched acme/widget#feat/y · task-123",
			wantRepo:   "acme/widget",
			wantBranch: "feat/y",
			wantTask:   "task-123",
			wantOK:     true,
		},
		{
			name:       "custom icon",
			text:       "🚀 Dispatched acme/widget#feat/z · task-456\nSpec: specs/a.md",
			wantRepo:   "acme/widget",
			wantBranch: "feat/z",
			wantTask:   "task-456",
			wantOK:     true,
		},
		{
			name:       "host-prefixed summary",
			text:       "⏳ Dispatched github.com/acme/widget#feat/hosted · task-1792217637155287504-1\nSpec: specs/feat-x.md",
			wantRepo:   "github.com/acme/widget",
			wantBranch: "feat/hosted",
			wantTask:   "task-1792217637155287504-1",
			wantOK:     true,
		},
		{
			name:   "ordinary self message",
			text:   "echo",
			wantOK: false,
		},
		{
			name:   "missing task prefix",
			text:   "⏳ Dispatched acme/widget#feat/x · 123",
			wantOK: false,
		},
		{
			name:   "missing branch separator",
			text:   "⏳ Dispatched acme/widget · task-123",
			wantOK: false,
		},
		{
			name:   "repo without owner",
			text:   "⏳ Dispatched widget#feat/x · task-123",
			wantOK: false,
		},
		{
			name:   "unrelated dispatched word",
			text:   "Dispatched without head",
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, branch, taskID, ok := ParseDispatchSummary(tt.text)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if repo != tt.wantRepo || branch != tt.wantBranch || taskID != tt.wantTask {
				t.Fatalf("got %q %q %q, want %q %q %q", repo, branch, taskID, tt.wantRepo, tt.wantBranch, tt.wantTask)
			}
		})
	}
}
