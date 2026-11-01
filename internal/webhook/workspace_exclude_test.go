package webhook

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
)

// countingRunner counts rev-parse calls on top of the real git runner.
type countingRunner struct {
	inner    gitRunner
	mu       sync.Mutex
	revParse int
}

func (c *countingRunner) Run(ctx context.Context, dir string, args ...string) (string, error) {
	if len(args) > 1 && args[0] == "rev-parse" && args[1] == "--git-common-dir" {
		c.mu.Lock()
		c.revParse++
		c.mu.Unlock()
	}
	return c.inner.Run(ctx, dir, args...)
}

func isolatedLease(t *testing.T, m *WorkspaceManager, root, rev string) *WorkspaceLease {
	t.Helper()
	req := gitEndpointRequest(WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main", HeadRevision: rev}, config.WorkspaceModeIsolated)
	req.Path = root
	lease, err := m.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	return lease
}

func gitStatus(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("git", "status", "--porcelain")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git status: %v", err)
	}
	return string(out)
}

func excludeFile(root string) string { return filepath.Join(root, ".git", "info", "exclude") }

// AC-01 + AC-03: first lease writes the line once; status stays clean; the
// common dir is looked up once per root.
func TestIsolatedLeaseExcludesWorkspaceDir(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	rev := commitFile(t, root, "NOTES.md", "v1\n")
	rev2 := commitFile(t, root, "NOTES.md", "v2\n")
	_ = os.Remove(excludeFile(root))
	m := NewWorkspaceManagerWithTTL(time.Hour)
	runner := &countingRunner{inner: m.runner}
	m.runner = runner

	isolatedLease(t, m, root, rev)
	isolatedLease(t, m, root, rev2)

	data, err := os.ReadFile(excludeFile(root))
	if err != nil {
		t.Fatalf("read exclude: %v", err)
	}
	if n := strings.Count(string(data), workspaceExcludeLine); n != 1 {
		t.Fatalf("exclude lines = %d, want 1:\n%s", n, data)
	}
	if status := gitStatus(t, root); strings.Contains(status, ".occa-workspaces") {
		t.Fatalf("git status lists the workspace dir:\n%s", status)
	}
	if runner.revParse != 1 {
		t.Fatalf("rev-parse calls = %d, want 1 (memoised)", runner.revParse)
	}
}

// AC-02: an existing equivalent line leaves the file byte-identical.
func TestIsolatedLeaseKeepsExistingExclude(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	rev := commitFile(t, root, "NOTES.md", "v1\n")
	want := "# mine\n.occa-workspaces/\n"
	if err := os.WriteFile(excludeFile(root), []byte(want), 0644); err != nil {
		t.Fatal(err)
	}
	isolatedLease(t, NewWorkspaceManagerWithTTL(time.Hour), root, rev)
	got, _ := os.ReadFile(excludeFile(root))
	if string(got) != want {
		t.Fatalf("exclude changed:\n%q", got)
	}
}

// AC-04: a linked worktree root writes to the common dir's exclude.
func TestWorkspaceExcludeUsesCommonDir(t *testing.T) {
	primary := t.TempDir()
	initTestGitRepo(t, primary)
	linked := filepath.Join(t.TempDir(), "linked")
	cmd := exec.Command("git", "worktree", "add", "--detach", linked)
	cmd.Dir = primary
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("worktree add: %v\n%s", err, out)
	}
	_ = os.Remove(excludeFile(primary))
	m := NewWorkspaceManagerWithTTL(time.Hour)
	if err := m.ensureWorkspaceExcluded(context.Background(), linked); err != nil {
		t.Fatalf("ensureWorkspaceExcluded: %v", err)
	}
	data, err := os.ReadFile(excludeFile(primary))
	if err != nil || !strings.Contains(string(data), workspaceExcludeLine) {
		t.Fatalf("common exclude = %q (%v), want the line", data, err)
	}
}

// AC-05: an unwritable info dir does not fail the lease.
func TestIsolatedLeaseSurvivesExcludeFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	logs := captureServerLogs(t)
	root := t.TempDir()
	initTestGitRepo(t, root)
	rev := commitFile(t, root, "NOTES.md", "v1\n")
	info := filepath.Join(root, ".git", "info")
	_ = os.RemoveAll(info)
	if err := os.WriteFile(info, []byte("not a dir"), 0444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(info) })
	lease := isolatedLease(t, NewWorkspaceManagerWithTTL(time.Hour), root, rev)
	if lease == nil || lease.Path == "" {
		t.Fatal("lease must still be granted")
	}
	logs.find(t, "webhook: workspace exclude failed")
}

// AC-06: a file without a trailing newline gets one before the block.
func TestWorkspaceExcludeAppendsAfterNewline(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	if err := os.WriteFile(excludeFile(root), []byte("*.log"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := NewWorkspaceManagerWithTTL(time.Hour).ensureWorkspaceExcluded(context.Background(), root); err != nil {
		t.Fatalf("ensureWorkspaceExcluded: %v", err)
	}
	got, _ := os.ReadFile(excludeFile(root))
	want := "*.log\n# occa isolated review workspaces\n/.occa-workspaces/\n"
	if string(got) != want {
		t.Fatalf("exclude = %q, want %q", got, want)
	}
}
