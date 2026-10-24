package webhook

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
)

type workspaceGitCall struct {
	dir  string
	args []string
}

type workspaceGitFake struct {
	mu          sync.Mutex
	calls       []workspaceGitCall
	verifyCalls int
	verify      func(n int) (string, error)
	fetch       func(ctx context.Context, args []string) error
	remote      string
}

func (f *workspaceGitFake) Run(ctx context.Context, dir string, args ...string) (string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, workspaceGitCall{dir: dir, args: append([]string(nil), args...)})
	f.mu.Unlock()

	switch args[0] {
	case "rev-parse":
		if containsWorkspaceArg(args, "--show-toplevel") {
			return dir, nil
		}
		if containsWorkspaceArg(args, "--verify") {
			f.mu.Lock()
			f.verifyCalls++
			n := f.verifyCalls
			f.mu.Unlock()
			return f.verify(n)
		}
		return "", fmt.Errorf("unexpected rev-parse: %v", args)
	case "config":
		return f.remote, nil
	case "fetch":
		if f.fetch != nil {
			return "", f.fetch(ctx, args)
		}
		return "", nil
	case "worktree":
		if len(args) >= 4 && args[1] == "add" {
			if err := os.MkdirAll(args[3], 0755); err != nil {
				return "", err
			}
		}
		return "", nil
	default:
		return "", fmt.Errorf("unexpected git command: %v", args)
	}
}

func (f *workspaceGitFake) recorded() []workspaceGitCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]workspaceGitCall(nil), f.calls...)
}

func (f *workspaceGitFake) fetchCalls() [][]string {
	var out [][]string
	for _, call := range f.recorded() {
		if call.args[0] == "fetch" {
			out = append(out, call.args)
		}
	}
	return out
}

func (f *workspaceGitFake) worktreeAdds() int {
	count := 0
	for _, call := range f.recorded() {
		if call.args[0] == "worktree" {
			count++
		}
	}
	return count
}

func containsWorkspaceArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

const workspaceFetchHead = "85126681af2f1554b6a34161313d59f3b4afdae2"

func workspaceFetchRequest(t *testing.T) (WorkspaceRequest, string) {
	t.Helper()
	path := t.TempDir()
	root, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("eval symlinks: %v", err)
	}
	return WorkspaceRequest{
		Repository: "github.com/testowner/myrepo",
		Path:       root,
		Mode:       config.WorkspaceModeIsolated,
		Key: WebhookExecutionKey{
			Repository:   "testowner/myrepo",
			Branch:       "feat/x",
			HeadRevision: workspaceFetchHead,
		},
		DeliveryID: "delivery-fetch",
		Lane:       "review",
		PRNumber:   "42",
		TTL:        time.Hour,
	}, root
}

func newWorkspaceFetchManager(fake *workspaceGitFake) *WorkspaceManager {
	manager := NewWorkspaceManagerWithTTL(time.Hour)
	manager.runner = fake
	fake.remote = "https://github.com/testowner/myrepo.git"
	return manager
}

func TestWorkspaceFetchesMissingRevision(t *testing.T) {
	fake := &workspaceGitFake{
		verify: func(n int) (string, error) {
			if n == 1 {
				return "", errors.New("fatal: Needed a single revision")
			}
			return workspaceFetchHead, nil
		},
	}
	manager := newWorkspaceFetchManager(fake)
	req, root := workspaceFetchRequest(t)

	lease, err := manager.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("resolve workspace: %v", err)
	}
	if lease == nil {
		t.Fatal("missing revision must still produce a workspace after fetch")
	}

	fetches := fake.fetchCalls()
	if len(fetches) != 1 {
		t.Fatalf("fetch calls = %d, want exactly one: %v", len(fetches), fetches)
	}
	want := []string{"fetch", "--no-tags", "--prune", "origin", "feat/x"}
	if strings.Join(fetches[0], " ") != strings.Join(want, " ") {
		t.Fatalf("fetch args = %v, want %v", fetches[0], want)
	}
	if fetchIndex, verifyIndex := callIndex(fake.recorded(), "fetch"), secondVerifyIndex(fake.recorded()); fetchIndex > verifyIndex {
		t.Fatalf("fetch at %d must run before the second rev-parse at %d", fetchIndex, verifyIndex)
	}
	if fake.worktreeAdds() != 1 {
		t.Fatalf("worktree adds = %d, want one", fake.worktreeAdds())
	}
	if !strings.HasPrefix(lease.Path, filepath.Join(root, ".occa-workspaces")) {
		t.Fatalf("lease path %q must live under the checkout", lease.Path)
	}
}

func TestWorkspaceSkipsFetchWhenPresent(t *testing.T) {
	fake := &workspaceGitFake{
		verify: func(int) (string, error) { return workspaceFetchHead, nil },
	}
	manager := newWorkspaceFetchManager(fake)
	req, _ := workspaceFetchRequest(t)

	if _, err := manager.ResolveWorkspace(context.Background(), req); err != nil {
		t.Fatalf("resolve workspace: %v", err)
	}
	if got := len(fake.fetchCalls()); got != 0 {
		t.Fatalf("present object must not fetch, got %d fetches", got)
	}
	if got := fake.worktreeAdds(); got != 1 {
		t.Fatalf("worktree adds = %d, want one", got)
	}
}

func TestWorkspaceFetchesPullRefFallback(t *testing.T) {
	fake := &workspaceGitFake{
		verify: func(n int) (string, error) {
			if n < 3 {
				return "", errors.New("fatal: Needed a single revision")
			}
			return workspaceFetchHead, nil
		},
	}
	manager := newWorkspaceFetchManager(fake)
	req, _ := workspaceFetchRequest(t)

	if _, err := manager.ResolveWorkspace(context.Background(), req); err != nil {
		t.Fatalf("resolve workspace: %v", err)
	}
	fetches := fake.fetchCalls()
	if len(fetches) != 2 {
		t.Fatalf("fetch calls = %d, want branch then pull ref: %v", len(fetches), fetches)
	}
	fallback := "fetch --no-tags origin +refs/pull/42/head:refs/occa/pull/42"
	if strings.Join(fetches[1], " ") != fallback {
		t.Fatalf("fallback fetch = %v, want %q", fetches[1], fallback)
	}
}

func TestWorkspaceFetchErrorRemediation(t *testing.T) {
	fake := &workspaceGitFake{
		verify: func(int) (string, error) {
			return "", errors.New("fatal: Needed a single revision")
		},
	}
	manager := newWorkspaceFetchManager(fake)
	req, _ := workspaceFetchRequest(t)

	_, err := manager.ResolveWorkspace(context.Background(), req)
	if err == nil {
		t.Fatal("missing revision after fetch must fail")
	}
	if !errors.Is(err, ErrRevisionMissing) || !errors.Is(err, ErrWorkspaceUnavailable) {
		t.Fatalf("error %v must classify as missing revision", err)
	}
	for _, want := range []string{"after fetch", "origin/feat/x", "refs/pull/42/head", "action: verify the checkout remote"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q must contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "clean the worktree") {
		t.Errorf("error %q must not suggest cleaning the worktree", err)
	}
	if fake.worktreeAdds() != 0 {
		t.Fatal("a failed fetch must not create a worktree")
	}
}

func TestWorkspaceFetchHonoursContext(t *testing.T) {
	fake := &workspaceGitFake{
		verify: func(int) (string, error) {
			return "", errors.New("fatal: Needed a single revision")
		},
		fetch: func(ctx context.Context, _ []string) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	manager := newWorkspaceFetchManager(fake)
	manager.FetchTimeout = 20 * time.Millisecond
	req, _ := workspaceFetchRequest(t)

	_, err := manager.ResolveWorkspace(context.Background(), req)
	if err == nil {
		t.Fatal("a timed-out fetch must abort the workspace")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error %v must wrap the context deadline", err)
	}
	if fake.worktreeAdds() != 0 {
		t.Fatal("a timed-out fetch must not leave a worktree behind")
	}
}

func TestWorkspaceFetchLogFields(t *testing.T) {
	handler := captureServerLogs(t)
	fake := &workspaceGitFake{
		verify: func(n int) (string, error) {
			if n == 1 {
				return "", errors.New("fatal: Needed a single revision")
			}
			return workspaceFetchHead, nil
		},
	}
	manager := newWorkspaceFetchManager(fake)
	req, root := workspaceFetchRequest(t)

	if _, err := manager.ResolveWorkspace(context.Background(), req); err != nil {
		t.Fatalf("resolve workspace: %v", err)
	}
	rec := handler.find(t, "webhook: workspace fetch")
	if rec.attrs["checkout"] != root {
		t.Errorf("checkout = %v, want %q", rec.attrs["checkout"], root)
	}
	if rec.attrs["ref"] != "feat/x" {
		t.Errorf("ref = %v, want feat/x", rec.attrs["ref"])
	}
	if _, ok := rec.attrs["duration_ms"].(int64); !ok {
		t.Errorf("duration_ms = %T, want int64", rec.attrs["duration_ms"])
	}
	if rec.attrs["result"] != "ok" {
		t.Errorf("result = %v, want ok", rec.attrs["result"])
	}
}

func callIndex(calls []workspaceGitCall, command string) int {
	for i, call := range calls {
		if call.args[0] == command {
			return i
		}
	}
	return -1
}

func secondVerifyIndex(calls []workspaceGitCall) int {
	seen := 0
	for i, call := range calls {
		if call.args[0] == "rev-parse" && containsWorkspaceArg(call.args, "--verify") {
			seen++
			if seen == 2 {
				return i
			}
		}
	}
	return -1
}
