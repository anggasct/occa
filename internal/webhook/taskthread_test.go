package webhook

import (
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
)

func TestStatusForWorkflow(t *testing.T) {
	cases := map[string]TaskStatus{
		"review": TaskClaimed,
		"fix":    TaskPROpened,
		"merge":  TaskPROpened,
		"merged": TaskMerged,
		"custom": TaskEvent,
		"":       TaskDispatched,
		"Review": TaskClaimed,
		"nope":   TaskDispatched,
		"MERGED": TaskMerged,
	}
	for workflow, want := range cases {
		if got := StatusForWorkflow(workflow); got != want {
			t.Errorf("StatusForWorkflow(%q) = %q, want %q", workflow, got, want)
		}
	}
}

func TestTaskStatusForAudit(t *testing.T) {
	if got := TaskStatusForAudit("FAILED", "merged"); got != TaskFailed {
		t.Errorf("FAILED audit = %q, want failed", got)
	}
	if got := TaskStatusForAudit("failed", "review"); got != TaskFailed {
		t.Errorf("failed audit = %q, want failed", got)
	}
	if got := TaskStatusForAudit("COMPLETED", "merged"); got != TaskMerged {
		t.Errorf("COMPLETED merged audit = %q, want merged", got)
	}
	if got := TaskStatusForAudit("SKIP", "review"); got != TaskClaimed {
		t.Errorf("SKIP review audit = %q, want claimed", got)
	}
}

func TestTaskIcon(t *testing.T) {
	if got := TaskIcon(TaskDispatched, nil); got != "⏳" {
		t.Errorf("dispatched icon = %q, want ⏳", got)
	}
	overrides := map[string]string{"dispatched": "🧪"}
	if got := TaskIcon(TaskDispatched, overrides); got != "🧪" {
		t.Errorf("override icon = %q, want 🧪", got)
	}
	if got := TaskIcon(TaskMerged, overrides); got != "✅" {
		t.Errorf("non-overridden icon = %q, want ✅", got)
	}
	if got := TaskIcon(TaskStatus("bogus"), nil); got != "📨" {
		t.Errorf("unknown status icon = %q, want 📨", got)
	}
}

func TestTaskColor(t *testing.T) {
	if got := TaskColor(TaskMerged, nil); got != 0x9B59B6 {
		t.Errorf("merged color = %#x, want purple", got)
	}
	if got := TaskColor(TaskMerged, map[string]int{"merged": 0x123456}); got != 0x123456 {
		t.Errorf("override color = %#x, want 0x123456", got)
	}
	if got := TaskColor(TaskMerged, map[string]int{"merged": 0x1FFFFFF}); got != 0x9B59B6 {
		t.Errorf("out-of-range override must fall back, got %#x", got)
	}
}

func TestTaskThreadRegistry(t *testing.T) {
	reg := NewTaskThreadRegistry()
	if _, ok := reg.Lookup("acme/widget", "feat/x"); ok {
		t.Fatal("empty registry must miss")
	}
	if _, ok := reg.Lookup("", "feat/x"); ok {
		t.Fatal("empty repo must miss")
	}
	reg.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskDispatched)
	reg.Upsert("acme/widget", "feat/y", "root-2", "thread-2", TaskClaimed)
	rec, ok := reg.Lookup("acme/widget", "feat/x")
	if !ok || rec.ThreadID != "thread-1" || rec.RootMessageID != "root-1" || rec.Status != TaskDispatched {
		t.Fatalf("lookup = %+v, %v; want thread-1/root-1/dispatched", rec, ok)
	}
	updated := reg.Upsert("ACME/widget", "feat/x", "", "", TaskMerged)
	if updated.ThreadID != "thread-1" || updated.RootMessageID != "root-1" {
		t.Errorf("empty IDs must preserve existing: %+v", updated)
	}
	if updated.Status != TaskMerged {
		t.Errorf("status = %q, want merged", updated.Status)
	}
	other, _ := reg.Lookup("acme/widget", "feat/y")
	if other.Status != TaskClaimed {
		t.Errorf("branches must not mix: %+v", other)
	}
	if n := len(reg.Snapshot()); n != 2 {
		t.Errorf("snapshot size = %d, want 2", n)
	}
}

func TestTaskRepoBranch(t *testing.T) {
	key := WebhookExecutionKey{Repository: "acme/widget", Branch: "feat/x"}
	repo, branch := TaskRepoBranch(key, WebhookEnvelope{})
	if repo != "acme/widget" || branch != "feat/x" {
		t.Errorf("got %q %q", repo, branch)
	}
	envelope := WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/y"}
	repo, branch = TaskRepoBranch(WebhookExecutionKey{}, envelope)
	if repo != "acme/widget" || branch != "feat/y" {
		t.Errorf("envelope fallback got %q %q", repo, branch)
	}
}

func TestFormatTaskParent(t *testing.T) {
	got := FormatTaskParent("acme/widget", "feat/x", "42", TaskMerged, nil)
	for _, want := range []string{"✅", "acme/widget#feat/x", "PR #42", "merged"} {
		if !strings.Contains(got, want) {
			t.Errorf("parent %q must contain %q", got, want)
		}
	}
	override := FormatTaskParent("acme/widget", "feat/x", "", TaskDispatched, map[string]string{"dispatched": "🧪"})
	if !strings.HasPrefix(override, "🧪") {
		t.Errorf("override parent = %q, want 🧪 prefix", override)
	}
}

func TestTaskCardHeader(t *testing.T) {
	if got := TaskCardHeader(WebhookEnvelope{}, "review", TaskClaimed, nil); got != "" {
		t.Errorf("header without branch = %q, want empty", got)
	}
	envelope := WebhookEnvelope{
		"repository":  "acme/widget",
		"head_branch": "feat/x",
		"pr_number":   "42",
		"pr_url":      "https://example.com/acme/widget/pull/42",
	}
	header := TaskCardHeader(envelope, "review", TaskClaimed, nil)
	for _, want := range []string{"👀", "acme/widget#feat/x", "[Open PR](https://example.com/acme/widget/pull/42)"} {
		if !strings.Contains(header, want) {
			t.Errorf("header %q must contain %q", header, want)
		}
	}
	if got := WithTaskHeader("body", envelope, "review", TaskClaimed, nil); !strings.HasPrefix(got, "👀") || !strings.HasSuffix(got, "body") {
		t.Errorf("wrapped card = %q", got)
	}
	if got := WithTaskHeader("body", WebhookEnvelope{}, "review", TaskClaimed, nil); got != "body" {
		t.Errorf("empty header must leave card alone, got %q", got)
	}
}

func TestMissingTaskWorkflows(t *testing.T) {
	mk := func(workflow string) config.EndpointConfig {
		return config.EndpointConfig{
			Name:       "ep-" + workflow,
			Workflow:   workflow,
			Repository: "acme/widget",
		}
	}
	full := []config.EndpointConfig{mk("review"), mk("fix"), mk("merge"), mk("merged")}
	if missing := MissingTaskWorkflows(full, "acme/widget"); len(missing) != 0 {
		t.Errorf("full set missing = %v", missing)
	}
	partial := []config.EndpointConfig{mk("review"), mk("fix")}
	missing := MissingTaskWorkflows(partial, "ACME/widget")
	if len(missing) != 2 || missing[0] != "merge" || missing[1] != "merged" {
		t.Errorf("partial missing = %v, want [merge merged]", missing)
	}
	other := []config.EndpointConfig{mk("review"), mk("fix"), mk("merge"), mk("merged")}
	other[0].Repository = "acme/other"
	if missing := MissingTaskWorkflows(other, "acme/widget"); len(missing) != 1 || missing[0] != "review" {
		t.Errorf("cross-repo missing = %v, want [review]", missing)
	}
}

func TestFallbackDue(t *testing.T) {
	now := time.Now()
	if FallbackDue(time.Time{}, now, 10*time.Minute) {
		t.Error("zero event time must never be due")
	}
	if FallbackDue(now, now, 0) {
		t.Error("zero interval must never be due")
	}
	if FallbackDue(now.Add(-11*time.Minute), now, 10*time.Minute) != true {
		t.Error("stale event must be due")
	}
	if FallbackDue(now.Add(-9*time.Minute), now, 10*time.Minute) {
		t.Error("fresh event must not be due")
	}
}

func TestFormatFallbackFinding(t *testing.T) {
	empty := FormatFallbackFinding("acme/widget", "feat/x", BranchPRResult{})
	if !strings.Contains(empty, "no open PR") || !strings.Contains(empty, "acme/widget#feat/x") {
		t.Errorf("empty finding = %q", empty)
	}
	found := FormatFallbackFinding("acme/widget", "feat/x", BranchPRResult{Found: true, Number: "42", URL: "https://example.com/p/42", State: "OPEN"})
	for _, want := range []string{"PR #42", "OPEN", "[Open PR](https://example.com/p/42)"} {
		if !strings.Contains(found, want) {
			t.Errorf("found finding %q must contain %q", found, want)
		}
	}
}

func TestParseBranchPRList(t *testing.T) {
	res, err := parseBranchPRList([]byte(`[]`))
	if err != nil || res.Found {
		t.Errorf("empty list = %+v, %v; want not found", res, err)
	}
	res, err = parseBranchPRList([]byte(`[{"number":42,"url":"https://example.com/p/42","state":"OPEN"}]`))
	if err != nil || !res.Found || res.Number != "42" || res.URL != "https://example.com/p/42" || res.State != "OPEN" {
		t.Errorf("row = %+v, %v", res, err)
	}
	if _, err := parseBranchPRList([]byte(`{broken`)); err == nil {
		t.Error("invalid JSON must error")
	}
}
