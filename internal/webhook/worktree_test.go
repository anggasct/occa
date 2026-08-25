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
)

func initTestGitRepo(t *testing.T, dir string) {
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
}

func TestParseWorktreePorcelain(t *testing.T) {
	sample := `worktree /home/ubuntu/projects/occa
HEAD 47eef3f9704f2c07c5fed441603d472cb05b741d
branch refs/heads/main

worktree /home/ubuntu/projects/occa/.worktree/feat-test
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
	if list[0].Path != "/home/ubuntu/projects/occa" || list[0].Branch != "refs/heads/main" {
		t.Errorf("wt[0] = %+v", list[0])
	}
	if list[1].Path != "/home/ubuntu/projects/occa/.worktree/feat-test" || list[1].Branch != "refs/heads/feat/test" {
		t.Errorf("wt[1] = %+v", list[1])
	}
	if list[2].Path != "/tmp/detached-wt" || !list[2].Detached {
		t.Errorf("wt[2] = %+v", list[2])
	}
}

func TestWorktreeResolverReusesExistingCleanWorktree(t *testing.T) {
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "myrepo")
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, repoDir)

	resolver := NewGitWorktreeResolver(tmpDir)
	key := WebhookExecutionKey{
		Repository: "myrepo",
		Branch:     "feat/cool-feature",
	}

	cmd := exec.Command("git", "branch", "feat/cool-feature")
	cmd.Dir = repoDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git branch: %v\n%s", err, out)
	}

	resolved1, err := resolver.ResolveWorktree(context.Background(), key)
	if err != nil {
		t.Fatalf("ResolveWorktree 1 failed: %v", err)
	}

	resolved2, err := resolver.ResolveWorktree(context.Background(), key)
	if err != nil {
		t.Fatalf("ResolveWorktree 2 failed: %v", err)
	}
	if resolved1 != resolved2 {
		t.Fatalf("expected reused worktree path %q, got %q", resolved1, resolved2)
	}
}

func TestWorktreeResolverDirtyConflict(t *testing.T) {
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "myrepo")
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, repoDir)

	cmd := exec.Command("git", "branch", "feat/cool-feature")
	cmd.Dir = repoDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("branch create: %v\n%s", err, out)
	}

	resolver := NewGitWorktreeResolver(tmpDir)
	key := WebhookExecutionKey{
		Repository: "myrepo",
		Branch:     "feat/cool-feature",
	}

	wtPath, err := resolver.ResolveWorktree(context.Background(), key)
	if err != nil {
		t.Fatalf("initial resolve failed: %v", err)
	}

	// Make the worktree dirty
	if err := os.WriteFile(filepath.Join(wtPath, "dirty.txt"), []byte("uncommitted"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err = resolver.ResolveWorktree(context.Background(), key)
	if err == nil {
		t.Fatal("expected ErrWorktreeConflict for dirty worktree, got nil")
	}
	if !errors.Is(err, ErrWorktreeConflict) {
		t.Fatalf("expected ErrWorktreeConflict, got %v", err)
	}
	if !strings.Contains(err.Error(), "uncommitted changes") {
		t.Fatalf("expected error message to mention uncommitted changes, got %q", err.Error())
	}
}

func TestWorktreeResolverRejectsPathTraversalAndAbsolutePaths(t *testing.T) {
	tmpDir := t.TempDir()
	resolver := NewGitWorktreeResolver(tmpDir)

	badRepos := []string{
		"/etc/passwd",
		"../../etc/shadow",
		"/tmp/repo",
		"org/repo/sub/invalid",
		"repo with space",
		"repo;evil",
		"repo$cmd",
		"..",
		".",
	}

	for _, repo := range badRepos {
		t.Run(repo, func(t *testing.T) {
			key := WebhookExecutionKey{
				Repository: repo,
				Branch:     "main",
			}
			_, err := resolver.ResolveWorktree(context.Background(), key)
			if err == nil {
				t.Fatalf("expected validation error for invalid repo %q, got nil", repo)
			}
			if !errors.Is(err, ErrInvalidRepo) && !errors.Is(err, ErrRepoNotFound) {
				t.Fatalf("expected ErrInvalidRepo or ErrRepoNotFound, got %v", err)
			}
		})
	}
}

func TestWorktreeResolverRejectsSymlinkEscapingProjectsDir(t *testing.T) {
	tmpDir := t.TempDir()
	projectsDir := filepath.Join(tmpDir, "projects")
	outsideDir := filepath.Join(tmpDir, "outside_repo")
	if err := os.MkdirAll(projectsDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outsideDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, outsideDir)

	// Create symlink projects/evil_link -> outside_repo
	symlinkPath := filepath.Join(projectsDir, "evil_link")
	if err := os.Symlink(outsideDir, symlinkPath); err != nil {
		t.Fatal(err)
	}

	resolver := NewGitWorktreeResolver(projectsDir)
	key := WebhookExecutionKey{
		Repository: "evil_link",
		Branch:     "main",
	}

	_, err := resolver.ResolveWorktree(context.Background(), key)
	if err == nil {
		t.Fatal("expected ErrRepoNotFound for symlink escaping projects root, got nil")
	}
	if !errors.Is(err, ErrRepoNotFound) {
		t.Fatalf("expected ErrRepoNotFound, got %v", err)
	}
}

func TestWorktreeResolverMissingBranchFailsClosedNoFabrication(t *testing.T) {
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "myrepo")
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, repoDir)

	resolver := NewGitWorktreeResolver(tmpDir)
	key := WebhookExecutionKey{
		Repository: "myrepo",
		Branch:     "non-existent-pr-branch",
	}

	_, err := resolver.ResolveWorktree(context.Background(), key)
	if err == nil {
		t.Fatal("expected error for missing local/remote branch, got nil")
	}
	if !strings.Contains(err.Error(), "not found in local or remote refs") {
		t.Fatalf("expected 'not found in local or remote refs' error, got %q", err.Error())
	}

	cmd := exec.Command("git", "branch", "--list", "non-existent-pr-branch")
	cmd.Dir = repoDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git branch --list: %v", err)
	}
	if strings.TrimSpace(string(out)) != "" {
		t.Fatalf("branch was erroneously fabricated from HEAD: %q", string(out))
	}
}

func TestWorktreeResolverInjectiveUniquePathAndDirectoryConflict(t *testing.T) {
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "occa")
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, repoDir)

	for _, b := range []string{
		"feat/long-branch-name-that-exceeds-thirty-characters-1",
		"feat/long-branch-name-that-exceeds-thirty-characters-2",
		"feat/same-branch",
	} {
		cmd := exec.Command("git", "branch", b)
		cmd.Dir = repoDir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("create branch %s: %v\n%s", b, err, out)
		}
	}

	resolver := NewGitWorktreeResolver(tmpDir)

	// 1. Long branches differing past 30 chars produce separate worktrees
	key1 := WebhookExecutionKey{
		Repository: "anggasct/occa",
		Branch:     "feat/long-branch-name-that-exceeds-thirty-characters-1",
	}
	key2 := WebhookExecutionKey{
		Repository: "anggasct/occa",
		Branch:     "feat/long-branch-name-that-exceeds-thirty-characters-2",
	}

	path1, err := resolver.ResolveWorktree(context.Background(), key1)
	if err != nil {
		t.Fatalf("resolve key1: %v", err)
	}
	path2, err := resolver.ResolveWorktree(context.Background(), key2)
	if err != nil {
		t.Fatalf("resolve key2: %v", err)
	}
	if path1 == path2 {
		t.Fatalf("colliding worktree paths for distinct branches: %q vs %q", path1, path2)
	}

	// 2. Different head repositories produce separate worktrees
	keyFork := WebhookExecutionKey{
		Repository:     "anggasct/occa",
		HeadRepository: "contributor/occa",
		Branch:         "feat/same-branch",
	}
	keyUpstream := WebhookExecutionKey{
		Repository:     "anggasct/occa",
		HeadRepository: "anggasct/occa",
		Branch:         "feat/same-branch",
	}
	pathFork := resolver.generateWorktreePath(repoDir, keyFork)
	pathUpstream := resolver.generateWorktreePath(repoDir, keyUpstream)
	if pathFork == pathUpstream {
		t.Fatalf("colliding worktree paths for fork vs upstream: %q vs %q", pathFork, pathUpstream)
	}

	// 3. Pre-existing unregistered directory fails closed with ErrWorktreeConflict
	unregisteredDir := resolver.generateWorktreePath(repoDir, keyUpstream)
	if err := os.MkdirAll(unregisteredDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unregisteredDir, "somefile.txt"), []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err = resolver.ResolveWorktree(context.Background(), keyUpstream)
	if err == nil {
		t.Fatal("expected ErrWorktreeConflict for unattached pre-existing directory, got nil")
	}
	if !errors.Is(err, ErrWorktreeConflict) {
		t.Fatalf("expected ErrWorktreeConflict, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(unregisteredDir, "somefile.txt")); err != nil {
		t.Fatal("pre-existing file was destructively deleted or altered")
	}
}

func TestWorktreeResolverForkVersusUpstreamSameBranch(t *testing.T) {
	tmpDir := t.TempDir()
	upstreamDir := filepath.Join(tmpDir, "occa")
	forkDir := filepath.Join(tmpDir, "contributor", "occa")
	if err := os.MkdirAll(upstreamDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(forkDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, upstreamDir)
	initTestGitRepo(t, forkDir)

	for _, d := range []string{upstreamDir, forkDir} {
		cmd := exec.Command("git", "branch", "fix/shared-name")
		cmd.Dir = d
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("create branch in %s: %v\n%s", d, err, out)
		}
	}

	resolver := NewGitWorktreeResolver(tmpDir)

	upstreamKey := WebhookExecutionKey{
		Repository:     "anggasct/occa",
		HeadRepository: "anggasct/occa",
		Branch:         "fix/shared-name",
	}
	forkKey := WebhookExecutionKey{
		Repository:     "anggasct/occa",
		HeadRepository: "contributor/occa",
		Branch:         "fix/shared-name",
	}

	upstreamPath, err := resolver.ResolveWorktree(context.Background(), upstreamKey)
	if err != nil {
		t.Fatalf("resolve upstream: %v", err)
	}
	forkPath, err := resolver.ResolveWorktree(context.Background(), forkKey)
	if err != nil {
		t.Fatalf("resolve fork: %v", err)
	}

	if upstreamPath == forkPath {
		t.Fatalf("fork and upstream shared the same worktree path: %q", upstreamPath)
	}
	if !strings.HasPrefix(upstreamPath, upstreamDir) {
		t.Fatalf("upstream path %q not in %q", upstreamPath, upstreamDir)
	}
	if !strings.HasPrefix(forkPath, forkDir) {
		t.Fatalf("fork path %q not in %q", forkPath, forkDir)
	}
}

func TestWorktreeResolverMissingForkFailsClosedNoUpstreamFallback(t *testing.T) {
	tmpDir := t.TempDir()
	upstreamDir := filepath.Join(tmpDir, "occa")
	if err := os.MkdirAll(upstreamDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, upstreamDir)

	cmd := exec.Command("git", "branch", "fix/shared-name")
	cmd.Dir = upstreamDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create branch: %v\n%s", err, out)
	}

	resolver := NewGitWorktreeResolver(tmpDir)
	forkKey := WebhookExecutionKey{
		Repository:     "anggasct/occa",
		HeadRepository: "contributor/occa",
		Branch:         "fix/shared-name",
	}

	_, err := resolver.ResolveWorktree(context.Background(), forkKey)
	if err == nil {
		t.Fatal("expected ErrRepoNotFound for missing fork repository, got nil")
	}
	if !errors.Is(err, ErrRepoNotFound) {
		t.Fatalf("expected ErrRepoNotFound, got %v", err)
	}
}

func TestWorktreeResolverReusesExistingBranchAtCustomPath(t *testing.T) {
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "myrepo")
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, repoDir)

	customWtPath := filepath.Join(repoDir, ".worktree", "my-manual-checkout")
	cmd := exec.Command("git", "worktree", "add", "-b", "feat/custom-branch", customWtPath)
	cmd.Dir = repoDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git worktree add: %v\n%s", err, out)
	}

	resolver := NewGitWorktreeResolver(tmpDir)
	key := WebhookExecutionKey{
		Repository: "myrepo",
		Branch:     "feat/custom-branch",
	}

	resolved, err := resolver.ResolveWorktree(context.Background(), key)
	if err != nil {
		t.Fatalf("ResolveWorktree failed to reuse existing attached branch: %v", err)
	}
	if resolved != customWtPath {
		t.Fatalf("expected reused custom path %q, got %q", customWtPath, resolved)
	}
}

func TestWorktreeResolverSidecarSymlinkRejected(t *testing.T) {
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "occa")
	victimFile := filepath.Join(tmpDir, "victim.txt")
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, repoDir)
	if err := os.WriteFile(victimFile, []byte("protected data\n"), 0644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("git", "branch", "feat/symlink-test")
	cmd.Dir = repoDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create branch: %v\n%s", err, out)
	}

	resolver := NewGitWorktreeResolver(tmpDir)
	key := WebhookExecutionKey{
		Repository: "anggasct/occa",
		Branch:     "feat/symlink-test",
	}

	targetPath := resolver.generateWorktreePath(repoDir, key)
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		t.Fatal(err)
	}

	// Create symlink targetPath.key -> victimFile
	sidecarPath := targetPath + ".key"
	if err := os.Symlink(victimFile, sidecarPath); err != nil {
		t.Fatal(err)
	}

	_, err := resolver.ResolveWorktree(context.Background(), key)
	if err == nil {
		t.Fatal("expected ErrWorktreeConflict on sidecar symlink, got nil")
	}
	if !errors.Is(err, ErrWorktreeConflict) {
		t.Fatalf("expected ErrWorktreeConflict, got %v", err)
	}

	// Verify victim file was NOT modified
	data, err := os.ReadFile(victimFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "protected data\n" {
		t.Fatalf("victim file was overwritten: %q", string(data))
	}
}

func TestWorktreeResolverIdentityMismatchConflict(t *testing.T) {
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "occa")
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, repoDir)

	cmd := exec.Command("git", "branch", "feat/test")
	cmd.Dir = repoDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create branch: %v\n%s", err, out)
	}

	resolver := NewGitWorktreeResolver(tmpDir)
	key := WebhookExecutionKey{
		Repository: "anggasct/occa",
		Branch:     "feat/test",
	}

	resolved, err := resolver.ResolveWorktree(context.Background(), key)
	if err != nil {
		t.Fatalf("initial resolve: %v", err)
	}

	// Tamper with key file to simulate identity mismatch
	keyFile := resolved + ".key"
	if err := os.WriteFile(keyFile, []byte("different/repo:different/repo:other-branch\n"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err = resolver.ResolveWorktree(context.Background(), key)
	if err == nil {
		t.Fatal("expected ErrWorktreeConflict on identity mismatch, got nil")
	}
	if !errors.Is(err, ErrWorktreeConflict) {
		t.Fatalf("expected ErrWorktreeConflict, got %v", err)
	}
	if !strings.Contains(err.Error(), "belongs to a different execution key") {
		t.Fatalf("expected error to mention different execution key, got %q", err.Error())
	}
}

func TestWorktreeResolverConcurrentCreationSerialized(t *testing.T) {
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "occa")
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, repoDir)

	cmd := exec.Command("git", "branch", "feat/same-branch")
	cmd.Dir = repoDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create branch: %v\n%s", err, out)
	}

	resolver := NewGitWorktreeResolver(tmpDir)
	var wg sync.WaitGroup
	errs := make(chan error, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := WebhookExecutionKey{
				Repository: "anggasct/occa",
				Branch:     "feat/same-branch",
			}
			_, err := resolver.ResolveWorktree(context.Background(), key)
			if err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent ResolveWorktree failed: %v", err)
	}
}

func TestWorktreeResolverRepoNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	resolver := NewGitWorktreeResolver(tmpDir)
	key := WebhookExecutionKey{
		Repository: "nonexistent/repo",
		Branch:     "main",
	}

	_, err := resolver.ResolveWorktree(context.Background(), key)
	if err == nil {
		t.Fatal("expected ErrRepoNotFound, got nil")
	}
	if !errors.Is(err, ErrRepoNotFound) {
		t.Fatalf("expected ErrRepoNotFound, got %v", err)
	}
}
