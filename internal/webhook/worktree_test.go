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
	"time"

	"github.com/anggasct/occa/internal/config"
)

func initTestGitRepo(t *testing.T, dir string) {
	t.Helper()
	initTestGitRepoWithRemote(t, dir, "https://github.com/testowner/myrepo.git")
}

func initTestGitRepoWithRemote(t *testing.T, dir, remote string) {
	t.Helper()
	runCmd := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
		}
	}

	runCmd("init", "-b", "main")
	runCmd("config", "user.name", "test")
	runCmd("config", "user.email", "test@example.com")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("# Test Repo\n"), 0644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runCmd("add", "README.md")
	runCmd("commit", "-m", "initial commit")
	runCmd("remote", "add", "origin", remote)
}

func commitFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	runCmd := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	runCmd("add", name)
	runCmd("commit", "-m", "add "+name)
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func headRevision(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	return strings.TrimSpace(string(out))
}

const testBinding = "github.com/testowner/myrepo"

func gitEndpointRequest(key WebhookExecutionKey, mode string) WorkspaceRequest {
	return WorkspaceRequest{
		Repository: testBinding,
		Path:       "",
		Mode:       mode,
		Key:        key,
		DeliveryID: "delivery-test",
	}
}

func TestParseWorktreePorcelain(t *testing.T) {
	sample := `worktree /srv/projects/occa
HEAD 47eef3f9704f2c07c5fed441603d472cb05b741d
branch refs/heads/main

worktree /srv/projects/occa/.worktree/feat-test
HEAD fe823634a051eba30cb01f7eddfe667278f208a5
branch refs/heads/feat/test

worktree /tmp/detached-wt
HEAD fb7f13eca0b62a7b164c3137c8457289374857e0
detached
`
	list := parseWorktreePorcelain(sample)
	if len(list) != 3 {
		t.Fatalf("expected 3 worktrees, got %d", len(list))
	}
	if list[0].Path != "/srv/projects/occa" || list[0].Branch != "refs/heads/main" {
		t.Errorf("wt[0] = %+v", list[0])
	}
	if list[1].Path != "/srv/projects/occa/.worktree/feat-test" || list[1].Branch != "refs/heads/feat/test" {
		t.Errorf("wt[1] = %+v", list[1])
	}
	if list[2].Path != "/tmp/detached-wt" || !list[2].Detached {
		t.Errorf("wt[2] = %+v", list[2])
	}
}

func TestWorkspaceRemoteMismatchFailsClosed(t *testing.T) {
	root := t.TempDir()
	initTestGitRepoWithRemote(t, root, "https://github.com/other/repo.git")
	m := NewWorkspaceManager()
	req := gitEndpointRequest(WebhookExecutionKey{}, config.WorkspaceModeIsolated)
	req.Path = root
	req.Key.HeadRevision = "0123456789abcdef0123456789abcdef01234567"

	_, err := m.ResolveWorkspace(context.Background(), req)
	if !errors.Is(err, ErrRepositoryMismatch) {
		t.Fatalf("expected ErrRepositoryMismatch, got %v", err)
	}
}

func TestWorkspacePayloadRepositoryMustMatchBinding(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	m := NewWorkspaceManager()
	req := gitEndpointRequest(WebhookExecutionKey{Repository: "someone/else", Branch: "main"}, config.WorkspaceModeMutable)
	req.Path = root

	_, err := m.ResolveWorkspace(context.Background(), req)
	if !errors.Is(err, ErrRepositoryMismatch) {
		t.Fatalf("expected ErrRepositoryMismatch for payload repo, got %v", err)
	}
}

func TestWorkspaceMissingOrInvalidPathFailsClosed(t *testing.T) {
	m := NewWorkspaceManager()
	req := gitEndpointRequest(WebhookExecutionKey{}, config.WorkspaceModeIsolated)
	req.Path = filepath.Join(t.TempDir(), "missing")
	req.Key.HeadRevision = "0123456789abcdef0123456789abcdef01234567"

	if _, err := m.ResolveWorkspace(context.Background(), req); !errors.Is(err, ErrWorkspaceUnavailable) {
		t.Fatalf("expected ErrWorkspaceUnavailable for missing path, got %v", err)
	}

	plain := t.TempDir()
	req.Path = plain
	if _, err := m.ResolveWorkspace(context.Background(), req); !errors.Is(err, ErrWorkspaceUnavailable) {
		t.Fatalf("expected ErrWorkspaceUnavailable for non-git path, got %v", err)
	}
}

func TestWorkspaceIsolatedRequiresRevision(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	m := NewWorkspaceManager()
	req := gitEndpointRequest(WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main"}, config.WorkspaceModeIsolated)
	req.Path = root

	_, err := m.ResolveWorkspace(context.Background(), req)
	if !errors.Is(err, ErrRevisionRequired) {
		t.Fatalf("expected ErrRevisionRequired, got %v", err)
	}
	if entries, _ := os.ReadDir(filepath.Join(root, ".occa-workspaces")); len(entries) != 0 {
		t.Fatalf("revision-less request must not create workspaces, got %d", len(entries))
	}
}

func TestWorkspaceIsolatedCreatesDetachedSnapshotAndCleansUp(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	rev := commitFile(t, root, "NOTES.md", "v1\n")
	m := NewWorkspaceManager()
	req := gitEndpointRequest(WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main", HeadRevision: rev}, config.WorkspaceModeIsolated)
	req.Path = root

	lease, err := m.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	if !strings.HasPrefix(lease.Path, filepath.Join(root, ".occa-workspaces")) {
		t.Fatalf("isolated workspace escaped configured root: %s", lease.Path)
	}

	head := headRevision(t, lease.Path)
	if head != rev {
		t.Fatalf("isolated worktree HEAD = %s, want exact revision %s", head, rev)
	}
	detached := exec.Command("git", "symbolic-ref", "-q", "HEAD")
	detached.Dir = lease.Path
	if err := detached.Run(); err == nil {
		t.Fatal("isolated worktree must be detached, not on a branch")
	}

	meta, err := readIsolatedMetadata(lease.Path)
	if err != nil || meta.Owner != "occa" || meta.Revision != rev || meta.DeliveryID != "delivery-test" {
		t.Fatalf("isolated ownership metadata invalid: %+v (%v)", meta, err)
	}

	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := os.Stat(lease.Path); !os.IsNotExist(err) {
		t.Fatalf("isolated worktree still exists after release: %v", err)
	}
	if _, err := os.Stat(isolatedMetadataPath(lease.Path)); !os.IsNotExist(err) {
		t.Fatal("isolated metadata sidecar still exists after release")
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("second Release must be idempotent: %v", err)
	}
}

func TestWorkspaceIsolatedUniquePerDelivery(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	rev := headRevision(t, root)
	m := NewWorkspaceManager()
	key := WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main", HeadRevision: rev}

	reqA := gitEndpointRequest(key, config.WorkspaceModeIsolated)
	reqA.Path = root
	reqA.DeliveryID = "delivery-a"
	reqB := gitEndpointRequest(key, config.WorkspaceModeIsolated)
	reqB.Path = root
	reqB.DeliveryID = "delivery-b"

	leaseA, err := m.ResolveWorkspace(context.Background(), reqA)
	if err != nil {
		t.Fatalf("ResolveWorkspace A: %v", err)
	}
	defer func() { _ = leaseA.Release(context.Background()) }()
	leaseB, err := m.ResolveWorkspace(context.Background(), reqB)
	if err != nil {
		t.Fatalf("ResolveWorkspace B: %v", err)
	}
	defer func() { _ = leaseB.Release(context.Background()) }()
	if leaseA.Path == leaseB.Path {
		t.Fatalf("two deliveries shared one isolated workspace: %s", leaseA.Path)
	}
}

func TestWorkspaceIsolatedUnknownRevisionFailsClosed(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	m := NewWorkspaceManager()
	req := gitEndpointRequest(WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main", HeadRevision: "1234567890abcdef1234567890abcdef12345678"}, config.WorkspaceModeIsolated)
	req.Path = root

	if _, err := m.ResolveWorkspace(context.Background(), req); !errors.Is(err, ErrWorkspaceUnavailable) {
		t.Fatalf("expected ErrWorkspaceUnavailable for unknown revision, got %v", err)
	}
}

func TestWorkspaceMutableReusesCleanWorktreeAndReleasesLease(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	branchCmd := exec.Command("git", "branch", "feat/cool-feature")
	branchCmd.Dir = root
	if out, err := branchCmd.CombinedOutput(); err != nil {
		t.Fatalf("git branch: %v\n%s", err, out)
	}

	m := NewWorkspaceManager()
	key := WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "feat/cool-feature"}

	lease1, err := m.ResolveWorkspace(context.Background(), func() WorkspaceRequest {
		r := gitEndpointRequest(key, config.WorkspaceModeMutable)
		r.Path = root
		return r
	}())
	if err != nil {
		t.Fatalf("ResolveWorkspace 1: %v", err)
	}
	if lease1.Path == root {
		t.Fatal("mutable resolution must never return the primary checkout")
	}
	if err := lease1.Release(context.Background()); err != nil {
		t.Fatalf("Release 1: %v", err)
	}

	lease2, err := m.ResolveWorkspace(context.Background(), func() WorkspaceRequest {
		r := gitEndpointRequest(key, config.WorkspaceModeMutable)
		r.Path = root
		return r
	}())
	if err != nil {
		t.Fatalf("ResolveWorkspace 2: %v", err)
	}
	if lease1.Path != lease2.Path {
		t.Fatalf("expected reused worktree path %q, got %q", lease1.Path, lease2.Path)
	}
	if err := lease2.Release(context.Background()); err != nil {
		t.Fatalf("Release 2: %v", err)
	}
}

func TestWorkspaceMutableLeaseExcludesConcurrentDelivery(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	m := NewWorkspaceManager()
	key := WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main"}

	reqA := gitEndpointRequest(key, config.WorkspaceModeMutable)
	reqA.Path = root
	reqA.DeliveryID = "delivery-a"
	reqB := gitEndpointRequest(key, config.WorkspaceModeMutable)
	reqB.Path = root
	reqB.DeliveryID = "delivery-b"

	leaseA, err := m.ResolveWorkspace(context.Background(), reqA)
	if err != nil {
		t.Fatalf("ResolveWorkspace A: %v", err)
	}
	defer func() { _ = leaseA.Release(context.Background()) }()

	_, err = m.ResolveWorkspace(context.Background(), reqB)
	if !errors.Is(err, ErrWorkspaceLeased) {
		t.Fatalf("expected ErrWorkspaceLeased for concurrent delivery, got %v", err)
	}
	if !IsWorkspaceRetryable(err) {
		t.Fatal("lease conflict must be retryable")
	}
}

func TestWorkspaceMutableDirtyIsRetryable(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	m := NewWorkspaceManager()
	key := WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main"}

	req := gitEndpointRequest(key, config.WorkspaceModeMutable)
	req.Path = root
	lease, err := m.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	if err := os.WriteFile(filepath.Join(lease.Path, "dirty.txt"), []byte("user work\n"), 0644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	_, err = m.ResolveWorkspace(context.Background(), req)
	if !errors.Is(err, ErrWorkspaceDirty) {
		t.Fatalf("expected ErrWorkspaceDirty, got %v", err)
	}
	if !IsWorkspaceRetryable(err) {
		t.Fatal("dirty workspace must be retryable")
	}
	if _, rErr := os.Stat(filepath.Join(lease.Path, "dirty.txt")); rErr != nil {
		t.Fatalf("dirty workspace content must be preserved: %v", rErr)
	}
}

func TestWorkspaceMutableIdentityMismatchIsTerminal(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	sharedCmd := exec.Command("git", "branch", "feat/shared")
	sharedCmd.Dir = root
	if out, err := sharedCmd.CombinedOutput(); err != nil {
		t.Fatalf("git branch: %v\n%s", err, out)
	}
	m := NewWorkspaceManager()

	reqFirst := gitEndpointRequest(WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "feat/shared"}, config.WorkspaceModeMutable)
	reqFirst.Path = root
	lease, err := m.ResolveWorkspace(context.Background(), reqFirst)
	if err != nil {
		t.Fatalf("ResolveWorkspace first: %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	reqFork := gitEndpointRequest(WebhookExecutionKey{Repository: "testowner/myrepo", HeadRepository: "testowner/fork", Branch: "feat/shared"}, config.WorkspaceModeMutable)
	reqFork.Path = root
	_, err = m.ResolveWorkspace(context.Background(), reqFork)
	if err == nil || IsWorkspaceRetryable(err) {
		t.Fatalf("expected terminal identity conflict, got %v", err)
	}
	if !errors.Is(err, ErrWorktreeConflict) {
		t.Fatalf("expected ErrWorktreeConflict, got %v", err)
	}
}

func TestWorkspaceReapExpiredIsolatedOnly(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	rev := headRevision(t, root)
	m := NewWorkspaceManager()
	m.IsolatedTTL = time.Hour

	req := gitEndpointRequest(WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main", HeadRevision: rev}, config.WorkspaceModeIsolated)
	req.Path = root
	lease, err := m.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}

	if reaped := m.ReapExpiredWorkspaces(context.Background()); reaped != 0 {
		t.Fatalf("fresh isolated workspace must not be reaped, reaped %d", reaped)
	}
	if _, err := os.Stat(lease.Path); err != nil {
		t.Fatalf("fresh workspace removed prematurely: %v", err)
	}

	// simulates a crashed process that lost its in-memory lease
	m.leases.Delete(lease.Path)

	expired := isolatedMetadata{
		Owner:      "occa",
		DeliveryID: "delivery-test",
		Revision:   rev,
		CreatedAt:  time.Now().Add(-2 * time.Hour).Unix(),
		ExpiresAt:  time.Now().Add(-time.Hour).Unix(),
	}
	if err := writeIsolatedMetadata(lease.Path, expired); err != nil {
		t.Fatalf("rewrite metadata: %v", err)
	}

	if reaped := m.ReapExpiredWorkspaces(context.Background()); reaped != 1 {
		t.Fatalf("expected 1 reaped workspace, got %d", reaped)
	}
	if _, err := os.Stat(lease.Path); !os.IsNotExist(err) {
		t.Fatal("expired workspace still exists after reap")
	}
	if reaped := m.ReapExpiredWorkspaces(context.Background()); reaped != 0 {
		t.Fatalf("reap must be idempotent, reaped %d", reaped)
	}
}

func TestWorkspaceMutableLeaseOnPreexistingWorktree(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	branchCmd := exec.Command("git", "branch", "feat/cool-feature")
	branchCmd.Dir = root
	if out, err := branchCmd.CombinedOutput(); err != nil {
		t.Fatalf("git branch: %v\n%s", err, out)
	}
	attached := filepath.Join(root, "preexisting-wt")
	wtCmd := exec.Command("git", "worktree", "add", attached, "feat/cool-feature")
	wtCmd.Dir = root
	if out, err := wtCmd.CombinedOutput(); err != nil {
		t.Fatalf("git worktree add: %v\n%s", err, out)
	}

	m := NewWorkspaceManager()
	key := WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "feat/cool-feature"}

	reqA := gitEndpointRequest(key, config.WorkspaceModeMutable)
	reqA.Path = root
	reqA.DeliveryID = "delivery-a"
	leaseA, err := m.ResolveWorkspace(context.Background(), reqA)
	if err != nil {
		t.Fatalf("ResolveWorkspace A: %v", err)
	}
	defer func() { _ = leaseA.Release(context.Background()) }()
	if filepath.Clean(leaseA.Path) != filepath.Clean(attached) {
		t.Fatalf("expected reuse of pre-existing worktree %q, got %q", attached, leaseA.Path)
	}

	reqB := gitEndpointRequest(key, config.WorkspaceModeMutable)
	reqB.Path = root
	reqB.DeliveryID = "delivery-b"
	_, err = m.ResolveWorkspace(context.Background(), reqB)
	if !errors.Is(err, ErrWorkspaceLeased) {
		t.Fatalf("expected ErrWorkspaceLeased while pre-existing worktree is in use, got %v", err)
	}
	if !IsWorkspaceRetryable(err) {
		t.Fatal("lease conflict on pre-existing worktree must be retryable")
	}

	if err := leaseA.Release(context.Background()); err != nil {
		t.Fatalf("Release A: %v", err)
	}

	reqC := gitEndpointRequest(key, config.WorkspaceModeMutable)
	reqC.Path = root
	reqC.DeliveryID = "delivery-c"
	leaseC, err := m.ResolveWorkspace(context.Background(), reqC)
	if err != nil {
		t.Fatalf("ResolveWorkspace after release: %v", err)
	}
	if filepath.Clean(leaseC.Path) != filepath.Clean(attached) {
		t.Fatalf("expected reused worktree path %q, got %q", attached, leaseC.Path)
	}
	if err := leaseC.Release(context.Background()); err != nil {
		t.Fatalf("Release C: %v", err)
	}
}

func TestReaperSkipsActiveIsolatedLease(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	rev := headRevision(t, root)
	m := NewWorkspaceManager()

	req := gitEndpointRequest(WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main", HeadRevision: rev}, config.WorkspaceModeIsolated)
	req.Path = root
	lease, err := m.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}

	expired := isolatedMetadata{
		Owner:      "occa",
		DeliveryID: "delivery-test",
		Revision:   rev,
		CreatedAt:  time.Now().Add(-2 * time.Hour).Unix(),
		ExpiresAt:  time.Now().Add(-time.Hour).Unix(),
	}
	if err := writeIsolatedMetadata(lease.Path, expired); err != nil {
		t.Fatalf("rewrite metadata: %v", err)
	}

	if reaped := m.ReapExpiredWorkspaces(context.Background()); reaped != 0 {
		t.Fatalf("expired-but-active isolated lease must not be reaped, reaped %d", reaped)
	}
	if _, err := os.Stat(lease.Path); err != nil {
		t.Fatalf("actively leased workspace was removed by reaper: %v", err)
	}

	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := os.Stat(lease.Path); !os.IsNotExist(err) {
		t.Fatal("workspace still exists after release")
	}
	if reaped := m.ReapExpiredWorkspaces(context.Background()); reaped != 0 {
		t.Fatalf("released workspace must not be reaped again, reaped %d", reaped)
	}
}

func TestWorkspaceConcurrentMutableResolutionSingleLease(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	m := NewWorkspaceManager()
	key := WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main"}

	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := gitEndpointRequest(key, config.WorkspaceModeMutable)
			req.Path = root
			req.DeliveryID = "delivery-" + string(rune('a'+i))
			lease, err := m.ResolveWorkspace(context.Background(), req)
			if err != nil {
				results <- err
				return
			}
			results <- lease.Release(context.Background())
		}(i)
	}
	wg.Wait()
	close(results)

	leased, released := 0, 0
	for err := range results {
		switch {
		case err == nil:
			released++
		case errors.Is(err, ErrWorkspaceLeased):
			leased++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if released != 1 || leased != 7 {
		t.Fatalf("expected exactly 1 lease winner and 7 leased rejections, got %d/%d", released, leased)
	}

	req := gitEndpointRequest(key, config.WorkspaceModeMutable)
	req.Path = root
	lease, err := m.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("sequential resolution after release failed: %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
}
