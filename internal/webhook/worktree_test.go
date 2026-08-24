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

	// Create local branch first so resolver can create worktree
	cmd := exec.Command("git", "branch", "feat/cool-feature")
	cmd.Dir = repoDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git branch: %v\n%s", err, out)
	}

	resolved1, err := resolver.ResolveWorktree(context.Background(), key)
	if err != nil {
		t.Fatalf("ResolveWorktree 1 failed: %v", err)
	}

	// Second resolve should reuse the exact same attached clean worktree
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

	// Verify no branch was created in the git repository
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

	// Create branches
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

	// 2. Different head repositories for same branch produce separate worktrees
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
	unregisteredDir := resolver.generateWorktreePath(repoDir, keyFork)
	if err := os.MkdirAll(unregisteredDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unregisteredDir, "somefile.txt"), []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err = resolver.ResolveWorktree(context.Background(), keyFork)
	if err == nil {
		t.Fatal("expected ErrWorktreeConflict for unattached pre-existing directory, got nil")
	}
	if !errors.Is(err, ErrWorktreeConflict) {
		t.Fatalf("expected ErrWorktreeConflict, got %v", err)
	}
	// Verify file was NOT deleted or mutated
	if _, err := os.Stat(filepath.Join(unregisteredDir, "somefile.txt")); err != nil {
		t.Fatal("pre-existing file was destructively deleted or altered")
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
