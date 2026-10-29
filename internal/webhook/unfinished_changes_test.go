package webhook

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/anggasct/occa/internal/config"
)

type recordingGitRunner struct {
	mu     sync.Mutex
	calls  []string
	status string
	failOn string
}

func (r *recordingGitRunner) Run(_ context.Context, _ string, args ...string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	call := strings.Join(args, " ")
	r.calls = append(r.calls, call)
	if r.failOn != "" && strings.HasPrefix(call, r.failOn) {
		return "", errors.New("git " + r.failOn + " failed")
	}
	switch args[0] {
	case "status":
		return r.status, nil
	case "write-tree":
		return "tree123\n", nil
	case "commit-tree":
		return "commit123\n", nil
	}
	return "", nil
}

func (r *recordingGitRunner) count(prefix string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, call := range r.calls {
		if strings.HasPrefix(call, prefix) {
			n++
		}
	}
	return n
}

func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestUnfinishedChangesSavedToRefOnRelease(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	m := NewWorkspaceManager()
	req := gitEndpointRequest(WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main"}, config.WorkspaceModeMutable)
	req.Path = root
	req.DeliveryID = "del-aborted"
	lease, err := m.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	if err := os.WriteFile(filepath.Join(lease.Path, "README.md"), []byte("# Test Repo\nrun work\n"), 0644); err != nil {
		t.Fatalf("write tracked change: %v", err)
	}
	if err := os.WriteFile(filepath.Join(lease.Path, "new.txt"), []byte("new file\n"), 0644); err != nil {
		t.Fatalf("write untracked file: %v", err)
	}

	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	if status := gitOutput(t, lease.Path, "status", "--porcelain"); status != "" {
		t.Fatalf("tree after release = %q, want clean", status)
	}
	refs := gitOutput(t, root, "for-each-ref", "--format=%(refname)", "refs/occa/unfinished/")
	if !strings.HasSuffix(refs, "-del-aborted") || strings.Contains(refs, "\n") {
		t.Fatalf("unfinished refs = %q, want one ref for del-aborted", refs)
	}
	if got := gitOutput(t, root, "show", refs+":README.md"); !strings.Contains(got, "run work") {
		t.Fatalf("saved README = %q, want the run's edit", got)
	}
	if got := gitOutput(t, root, "show", refs+":new.txt"); got != "new file" {
		t.Fatalf("saved new.txt = %q, want untracked file kept", got)
	}
	if gitOutput(t, root, "stash", "list") != "" {
		t.Fatal("shared stash must not be used")
	}

	next, err := m.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("next ResolveWorkspace after save: %v", err)
	}
	_ = next.Release(context.Background())
}

func TestUnfinishedChangesPreexistingDirtStaysProtected(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	m := NewWorkspaceManager()
	req := gitEndpointRequest(WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main"}, config.WorkspaceModeMutable)
	req.Path = root
	lease, err := m.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := os.WriteFile(filepath.Join(lease.Path, "README.md"), []byte("# Test Repo\nuser work\n"), 0644); err != nil {
		t.Fatalf("write user change: %v", err)
	}

	_, err = m.ResolveWorkspace(context.Background(), req)
	if !errors.Is(err, ErrWorkspaceDirty) || !IsWorkspaceRetryable(err) {
		t.Fatalf("dirty-before-acquire = %v, want retryable ErrWorkspaceDirty", err)
	}
	data, rErr := os.ReadFile(filepath.Join(lease.Path, "README.md"))
	if rErr != nil || !strings.Contains(string(data), "user work") {
		t.Fatalf("user content = %q, err %v, want preserved", data, rErr)
	}
	if refs := gitOutput(t, root, "for-each-ref", "refs/occa/unfinished/"); refs != "" {
		t.Fatalf("unfinished refs = %q, want none", refs)
	}
}

func TestUnfinishedChangesCleanTreeIsUntouched(t *testing.T) {
	runner := &recordingGitRunner{}
	m := NewWorkspaceManager()
	m.runner = runner
	lease := &WorkspaceLease{manager: m, Path: "/tmp/wt", Mode: config.WorkspaceModeMutable, Holder: "del-1", grantedClean: true}

	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if runner.count("add") != 0 || runner.count("reset") != 0 {
		t.Fatalf("calls = %v, want only a status check", runner.calls)
	}
}

func TestUnfinishedChangesIsolatedLeaseIsUntouched(t *testing.T) {
	runner := &recordingGitRunner{status: " M README.md\n"}
	m := NewWorkspaceManager()
	m.runner = runner
	lease := &WorkspaceLease{manager: m, Path: "/tmp/wt", Mode: config.WorkspaceModeIsolated, Holder: "del-1"}

	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("calls = %v, want none for an isolated lease", runner.calls)
	}
}

func TestUnfinishedChangesDispatchLeaseIsUntouched(t *testing.T) {
	runner := &recordingGitRunner{status: " M README.md\n"}
	m := NewWorkspaceManager()
	m.runner = runner
	lease := &WorkspaceLease{manager: m, Path: "/tmp/wt", Mode: config.WorkspaceModeMutable, Branch: "feat/x"}

	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("calls = %v, want none for a lease not granted on a clean tree", runner.calls)
	}
}

func TestUnfinishedChangesSaveFailureStillReleasesLease(t *testing.T) {
	runner := &recordingGitRunner{status: " M README.md\n", failOn: "update-ref"}
	m := NewWorkspaceManager()
	m.runner = runner
	m.leases.Store("/tmp/wt", &workspaceLeaseEntry{Holder: "del-1"})
	lease := &WorkspaceLease{manager: m, Path: "/tmp/wt", Mode: config.WorkspaceModeMutable, Holder: "del-1", grantedClean: true}

	err := lease.Release(context.Background())
	if err == nil || !strings.Contains(err.Error(), "update-ref") {
		t.Fatalf("Release error = %v, want the update-ref failure", err)
	}
	if _, held := m.leases.Load("/tmp/wt"); held {
		t.Fatal("lease still held after a failed save")
	}
	if runner.count("reset") != 0 || runner.count("clean") != 0 {
		t.Fatalf("calls = %v, want no cleanup when the ref was not written", runner.calls)
	}
}

func TestUnfinishedChangesReleaseTwiceSavesOnce(t *testing.T) {
	runner := &recordingGitRunner{status: " M README.md\n"}
	m := NewWorkspaceManager()
	m.runner = runner
	lease := &WorkspaceLease{manager: m, Path: "/tmp/wt", Mode: config.WorkspaceModeMutable, Holder: "del/1 x", grantedClean: true}

	_ = lease.Release(context.Background())
	_ = lease.Release(context.Background())

	if runner.count("update-ref") != 1 {
		t.Fatalf("calls = %v, want one save", runner.calls)
	}
	for _, call := range runner.calls {
		if strings.HasPrefix(call, "update-ref ") && !strings.Contains(call, "-del-1-x commit123") {
			t.Fatalf("ref call = %q, want a sanitised holder in the ref name", call)
		}
	}
}
