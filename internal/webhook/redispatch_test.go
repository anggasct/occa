package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

func redispatchBody() string {
	return `{"repo":"acme/widget","branch":"feat/x","spec_path":"specs/task.md","scope":"redispatch feat/x"}`
}

func TestUpsertPreservesSpecPath(t *testing.T) {
	reg := NewTaskThreadRegistry()
	reg.UpsertWithSpec("acme/widget", "feat/x", "root-1", "thread-1", TaskDispatched, "specs/orig.md")
	rec, ok := reg.Lookup("acme/widget", "feat/x")
	if !ok || rec.SpecPath != "specs/orig.md" {
		t.Fatalf("stored spec = %+v %v, want specs/orig.md", rec, ok)
	}
	updated := reg.Upsert("acme/widget", "feat/x", "", "", TaskClaimed)
	if updated.SpecPath != "specs/orig.md" {
		t.Errorf("empty upsert must preserve spec, got %q", updated.SpecPath)
	}
	if updated.RootMessageID != "root-1" || updated.ThreadID != "thread-1" {
		t.Errorf("empty upsert must preserve ids: %+v", updated)
	}
	overwritten := reg.UpsertWithSpec("acme/widget", "feat/x", "", "", TaskDispatched, "specs/new.md")
	if overwritten.SpecPath != "specs/new.md" {
		t.Errorf("explicit spec must overwrite, got %q", overwritten.SpecPath)
	}
}

func TestRedispatchMergedRefusesNoParentThread(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.tasks.UpsertWithSpec("acme/widget", "feat/x", "root-1", "thread-1", TaskMerged, "specs/orig.md")
	srv.SetBranchChecker(func(ctx context.Context, repo, branch string) (BranchPRResult, error) {
		t.Error("merged record must refuse before PR check")
		return BranchPRResult{}, nil
	})
	srv.SetBranchExistsChecker(func(ctx context.Context, repo, branch string) (bool, error) {
		t.Error("merged record must refuse before branch check")
		return true, nil
	})
	var mu sync.Mutex
	var senderCalls, notifierCalls, editCalls int
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) {
		mu.Lock()
		senderCalls++
		mu.Unlock()
		return "parent-new", nil
	})
	srv.SetNotifier(func(_ context.Context, _, _, _ string) error {
		mu.Lock()
		notifierCalls++
		mu.Unlock()
		return nil
	})
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error {
		mu.Lock()
		editCalls++
		mu.Unlock()
		return nil
	})
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) {
		mu.Lock()
		senderCalls++
		mu.Unlock()
		return "thread-new", nil
	})
	rec := postDispatch(t, srv, "dispatch-token", redispatchBody())
	if rec.Code != http.StatusConflict {
		t.Fatalf("merged redispatch status = %d, want 409: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(strings.ToLower(rec.Body.String()), "merged") {
		t.Errorf("refusal must name merged: %s", rec.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if senderCalls != 0 || notifierCalls != 0 || editCalls != 0 {
		t.Errorf("refusal must spawn nothing: sender=%d notifier=%d edit=%d", senderCalls, notifierCalls, editCalls)
	}
	if got := srv.dispatchTasks.Count(); got != 0 {
		t.Errorf("refusal must not create dispatch task, count=%d", got)
	}
	if after, _ := srv.tasks.Lookup("acme/widget", "feat/x"); after.Status != TaskMerged || after.RootMessageID != "root-1" || after.ThreadID != "thread-1" {
		t.Errorf("refusal must leave record untouched: %+v", after)
	}
}

func TestRedispatchOpenReusesSameParentThreadWithOriginalSpec(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.tasks.UpsertWithSpec("acme/widget", "feat/x", "root-1", "thread-1", TaskClaimed, "specs/orig.md")
	srv.SetBranchChecker(func(_ context.Context, _, _ string) (BranchPRResult, error) {
		return BranchPRResult{Found: true, Number: "42", URL: "https://github.com/acme/widget/pull/42", State: "OPEN"}, nil
	})
	srv.SetBranchExistsChecker(func(_ context.Context, _, _ string) (bool, error) {
		return true, nil
	})
	var mu sync.Mutex
	var senderCalls, threadCalls int
	var editTargets []string
	var editTexts []string
	var notifyTargets []string
	var notifyTexts []string
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) {
		mu.Lock()
		senderCalls++
		mu.Unlock()
		return "parent-new", nil
	})
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) {
		mu.Lock()
		threadCalls++
		mu.Unlock()
		return "thread-new", nil
	})
	srv.SetEditor(func(_ context.Context, _, _, messageID, text string) error {
		mu.Lock()
		editTargets = append(editTargets, messageID)
		editTexts = append(editTexts, text)
		mu.Unlock()
		return nil
	})
	srv.SetNotifier(func(_ context.Context, _, target, text string) error {
		mu.Lock()
		notifyTargets = append(notifyTargets, target)
		notifyTexts = append(notifyTexts, text)
		mu.Unlock()
		return nil
	})
	srv.SetFallbackPollAfter(time.Hour)
	rec := postDispatch(t, srv, "dispatch-token", redispatchBody())
	if rec.Code != http.StatusCreated {
		t.Fatalf("open retry status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	task, ok := srv.dispatchTasks.Get(payload["task_id"])
	if !ok {
		t.Fatalf("new task %q not stored", payload["task_id"])
	}
	if task.SpecPath != "specs/orig.md" {
		t.Errorf("retry spec = %q, want original specs/orig.md", task.SpecPath)
	}
	if strings.Contains(task.SpecPath, "specs/task.md") {
		t.Errorf("retry must not use placeholder: %q", task.SpecPath)
	}
	mu.Lock()
	defer mu.Unlock()
	if senderCalls != 0 {
		t.Errorf("retry must not post new parent, sender=%d", senderCalls)
	}
	if threadCalls != 0 {
		t.Errorf("retry must not open new thread, threadStarter=%d", threadCalls)
	}
	if len(editTexts) != 1 {
		t.Fatalf("parent edits = %d, want exactly 1 re-edit of existing parent", len(editTexts))
	}
	if editTargets[0] != "root-1" {
		t.Errorf("parent re-edit target = %q, want existing root-1", editTargets[0])
	}
	if !strings.Contains(editTexts[0], "Dispatched") || !strings.Contains(editTexts[0], task.ID) {
		t.Errorf("parent re-edit must be dispatched with new task id: %q", editTexts[0])
	}
	if !strings.Contains(editTexts[0], "specs/orig.md") {
		t.Errorf("parent re-edit must carry original spec: %q", editTexts[0])
	}
	if len(notifyTexts) != 1 {
		t.Fatalf("thread posts = %d, want exactly 1 separator in same thread", len(notifyTexts))
	}
	if notifyTargets[0] != "thread-1" {
		t.Errorf("retry thread target = %q, want existing thread-1", notifyTargets[0])
	}
	if !strings.Contains(notifyTexts[0], "---") || !strings.Contains(strings.ToLower(notifyTexts[0]), "re-dispatch") {
		t.Errorf("thread post must be retry separator: %q", notifyTexts[0])
	}
	if !strings.Contains(notifyTexts[0], "specs/orig.md") {
		t.Errorf("separator must carry original spec: %q", notifyTexts[0])
	}
	if strings.Contains(notifyTexts[0], "specs/task.md") {
		t.Errorf("separator must not contain placeholder: %q", notifyTexts[0])
	}
	after, ok := srv.tasks.Lookup("acme/widget", "feat/x")
	if !ok {
		t.Fatal("record must still exist after retry")
	}
	if after.RootMessageID != "root-1" || after.ThreadID != "thread-1" {
		t.Errorf("retry must keep same parent+thread, got %+v", after)
	}
	if after.Status != TaskDispatched {
		t.Errorf("retry status = %q, want dispatched", after.Status)
	}
	if after.SpecPath != "specs/orig.md" {
		t.Errorf("record spec = %q, want specs/orig.md", after.SpecPath)
	}
}

func TestClassifyBranchCheckResult(t *testing.T) {
	exitErr := errors.New("exit status 1")
	cases := []struct {
		name      string
		out       string
		err       error
		wantExist bool
		wantErr   bool
	}{
		{"success", "", nil, true, false},
		{"not-found-body", "gh: Not Found (HTTP 404)", exitErr, false, false},
		{"branch-not-found-body", `{"message": "Branch not found"}`, exitErr, false, false},
		{"server-error", "gh: Internal Server Error (HTTP 500)", exitErr, false, true},
		{"network-error", "gh: failed to connect: no route to host", exitErr, false, true},
		{"bare-exit-status", "", exitErr, false, true},
		{"missing-binary", "", &exec.Error{Name: "gh", Err: exec.ErrNotFound}, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exists, err := classifyBranchCheckResult([]byte(tc.out), tc.err)
			if exists != tc.wantExist {
				t.Errorf("exists = %v, want %v", exists, tc.wantExist)
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestRedispatchBranchGoneRefuses(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.tasks.UpsertWithSpec("acme/widget", "feat/x", "root-1", "thread-1", TaskClaimed, "specs/orig.md")
	srv.SetBranchChecker(func(_ context.Context, _, _ string) (BranchPRResult, error) {
		return BranchPRResult{}, nil
	})
	srv.SetBranchExistsChecker(func(_ context.Context, _, _ string) (bool, error) {
		return false, nil
	})
	rec := postDispatch(t, srv, "dispatch-token", redispatchBody())
	if rec.Code != http.StatusConflict {
		t.Fatalf("branch-gone status = %d, want 409: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(strings.ToLower(rec.Body.String()), "branch gone") {
		t.Errorf("refusal must name branch gone: %s", rec.Body.String())
	}
	if got := srv.dispatchTasks.Count(); got != 0 {
		t.Errorf("branch-gone must not create task, count=%d", got)
	}
}

func TestRedispatchPRClosedRefuses(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.tasks.UpsertWithSpec("acme/widget", "feat/x", "root-1", "thread-1", TaskClaimed, "specs/orig.md")
	srv.SetBranchChecker(func(_ context.Context, _, _ string) (BranchPRResult, error) {
		return BranchPRResult{Found: true, Number: "7", URL: "https://github.com/acme/widget/pull/7", State: "MERGED"}, nil
	})
	srv.SetBranchExistsChecker(func(_ context.Context, _, _ string) (bool, error) {
		return true, nil
	})
	rec := postDispatch(t, srv, "dispatch-token", redispatchBody())
	if rec.Code != http.StatusConflict {
		t.Fatalf("closed-PR status = %d, want 409: %s", rec.Code, rec.Body.String())
	}
}

func TestRedispatchPROpenedWithoutOpenPRRefuses(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.tasks.UpsertWithSpec("acme/widget", "feat/x", "root-1", "thread-1", TaskPROpened, "specs/orig.md")
	srv.SetBranchChecker(func(_ context.Context, _, _ string) (BranchPRResult, error) {
		return BranchPRResult{}, nil
	})
	srv.SetBranchExistsChecker(func(_ context.Context, _, _ string) (bool, error) {
		return true, nil
	})
	rec := postDispatch(t, srv, "dispatch-token", redispatchBody())
	if rec.Code != http.StatusConflict {
		t.Fatalf("pr_opened without PR status = %d, want 409: %s", rec.Code, rec.Body.String())
	}
}

func TestFirstDispatchStoresSpecForRetry(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.SetBranchExistsChecker(func(_ context.Context, _, _ string) (bool, error) {
		return true, nil
	})
	rec := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("first dispatch status = %d, want 201", rec.Code)
	}
	stored, ok := srv.tasks.Lookup("acme/widget", "feat/x")
	if !ok {
		t.Fatal("record must exist after first dispatch")
	}
	if stored.SpecPath != "specs/feat-x.md" {
		t.Errorf("first dispatch must store spec, got %q", stored.SpecPath)
	}
}
