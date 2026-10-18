package webhook

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestResolveTaskThreadCanonicalBothForms(t *testing.T) {
	reg := NewTaskThreadRegistry()
	reg.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskDispatched)
	callers := []string{"dispatch", "claim", "review", "pr-fix", "merge", "merged", "redispatch", "watchdog", "executor", "audit", "fallback-poll", "delivery-fallback", "dispatch-fallback"}
	forms := []struct{ repo, branch string }{
		{"acme/widget", "feat/x"},
		{"github.com/acme/widget", "feat/x"},
		{"GitHub.com/Acme/Widget", "feat/x"},
		{"https://github.com/acme/widget", "feat/x"},
		{"acme/widget", "Feat/X"},
		{"acme/widget", "  FEAT/x  "},
		{"github.com/acme/widget", "FEAT/X"},
	}
	for _, caller := range callers {
		var want string
		for i, f := range forms {
			rec, ok := reg.ResolveTaskThread(f.repo, f.branch, caller)
			if !ok {
				t.Fatalf("caller %q form %+v miss, want hit", caller, f)
			}
			if rec.ThreadID != "thread-1" || rec.RootMessageID != "root-1" {
				t.Fatalf("caller %q form %+v = %+v, want root-1/thread-1", caller, f, rec)
			}
			key := TaskThreadKey(f.repo, f.branch)
			if key != "acme/widget#feat/x" {
				t.Fatalf("caller %q form %+v key = %q, want acme/widget#feat/x", caller, f, key)
			}
			if i == 0 {
				want = rec.ThreadID
			} else if rec.ThreadID != want {
				t.Fatalf("caller %q form %+v thread = %q, want %q", caller, f, rec.ThreadID, want)
			}
		}
	}
}

func TestResolveForDeliveryEnrichedAndDirectIdentical(t *testing.T) {
	reg := NewTaskThreadRegistry()
	reg.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskDispatched)
	direct := WebhookExecutionKey{Repository: "acme/widget", HeadRepository: "acme/widget", Branch: "feat/x"}
	enriched := WebhookExecutionKey{Repository: "acme/widget", HeadRepository: "acme/widget", Branch: "feat/x"}
	envDirect := WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/x"}
	envEnriched := WebhookEnvelope{"repository": "acme/widget", "head_branch": "feat/x"}
	if TaskThreadKeyForDelivery(direct, envDirect) != TaskThreadKeyForDelivery(enriched, envEnriched) {
		t.Fatal("enriched and direct canonical keys must match")
	}
	for _, caller := range []string{"review", "pr-fix", "fix", "merge", "merged"} {
		_, _, recDirect, okDirect := reg.ResolveForDelivery(direct, envDirect, caller)
		_, _, recEnriched, okEnriched := reg.ResolveForDelivery(enriched, envEnriched, caller)
		if !okDirect || !okEnriched {
			t.Fatalf("caller %q miss direct=%v enriched=%v", caller, okDirect, okEnriched)
		}
		if recDirect.ThreadID != recEnriched.ThreadID || recDirect.ThreadID != "thread-1" {
			t.Fatalf("caller %q direct=%+v enriched=%+v, want thread-1", caller, recDirect, recEnriched)
		}
	}
	longKey := WebhookExecutionKey{Repository: "github.com/Acme/Widget", HeadRepository: "github.com/Acme/Widget", Branch: "Feat/X"}
	_, _, recLong, okLong := reg.ResolveForDelivery(longKey, WebhookEnvelope{}, "pr-fix")
	if !okLong || recLong.ThreadID != "thread-1" {
		t.Fatalf("long host key miss: %+v %v", recLong, okLong)
	}
}

func TestResolveTaskThreadMissLogsInfo(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(prev)
	reg := NewTaskThreadRegistry()
	reg.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskDispatched)
	_, ok := reg.ResolveTaskThread("acme/widget", "feat/missing", "pr-fix")
	if ok {
		t.Fatal("missing branch must miss")
	}
	out := buf.String()
	if !strings.Contains(out, "task thread resolve") {
		t.Fatalf("miss must log resolve line, got %q", out)
	}
	if !strings.Contains(out, "acme/widget#feat/missing") {
		t.Fatalf("miss must carry canonical key, got %q", out)
	}
	if !strings.Contains(out, "pr-fix") {
		t.Fatalf("miss must carry caller, got %q", out)
	}
	buf.Reset()
	_, ok = reg.ResolveTaskThread("acme/widget", "feat/x", "claim")
	if !ok {
		t.Fatal("existing must hit")
	}
	out = buf.String()
	if !strings.Contains(out, "acme/widget#feat/x") || !strings.Contains(out, "claim") || !strings.Contains(out, "thread-1") {
		t.Fatalf("hit must carry canonical key, caller and thread, got %q", out)
	}
}

func TestUpsertThenPollNeverBindsStale(t *testing.T) {
	reg := NewTaskThreadRegistry()
	reg.Upsert("acme/widget", "feat/x", "root-old", "thread-old", TaskDispatched)
	freshRoot := "root-new"
	reg.Upsert("acme/widget", "feat/x", freshRoot, "", TaskDispatched)
	rec, ok := reg.ResolveTaskThread("acme/widget", "feat/x", "claim")
	if !ok {
		t.Fatal("reserved record must exist")
	}
	if rec.ThreadID == "thread-old" {
		t.Fatalf("fast poll bound stale thread-old after new root %q, got %+v", freshRoot, rec)
	}
	if rec.ThreadID != "" {
		t.Fatalf("reserved record must have empty thread, got %+v", rec)
	}
	if rec.RootMessageID != freshRoot {
		t.Fatalf("reserved root = %q, want %q", rec.RootMessageID, freshRoot)
	}
	reg.Upsert("acme/widget", "feat/x", freshRoot, "thread-new", TaskDispatched)
	rec, ok = reg.ResolveTaskThread("acme/widget", "feat/x", "claim")
	if !ok || rec.ThreadID != "thread-new" {
		t.Fatalf("after fresh thread, poll = %+v %v, want thread-new", rec, ok)
	}
}
