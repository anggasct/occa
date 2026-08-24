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

	// Create a branch and a worktree manually
	cmd := exec.Command("git", "worktree", "add", "-b", "feat/cool-feature", filepath.Join(repoDir, ".worktree", "cool-feature"))
	cmd.Dir = repoDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("worktree add: %v\n%s", err, out)
	}

	resolver := NewGitWorktreeResolver(tmpDir)
	key := WebhookExecutionKey{
		Repository: "myrepo",
		Branch:     "feat/cool-feature",
	}

	resolved, err := resolver.ResolveWorktree(context.Background(), key)
	if err != nil {
		t.Fatalf("ResolveWorktree failed: %v", err)
	}
	expectedPath := filepath.Join(repoDir, ".worktree", "cool-feature")
	if resolved != expectedPath {
		t.Fatalf("ResolveWorktree = %q, want %q", resolved, expectedPath)
	}
}

func TestWorktreeResolverDirtyConflict(t *testing.T) {
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "myrepo")
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, repoDir)

	wtPath := filepath.Join(repoDir, ".worktree", "cool-feature")
	cmd := exec.Command("git", "worktree", "add", "-b", "feat/cool-feature", wtPath)
	cmd.Dir = repoDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("worktree add: %v\n%s", err, out)
	}

	// Make the worktree dirty
	if err := os.WriteFile(filepath.Join(wtPath, "dirty.txt"), []byte("uncommitted"), 0644); err != nil {
		t.Fatal(err)
	}

	resolver := NewGitWorktreeResolver(tmpDir)
	key := WebhookExecutionKey{
		Repository: "myrepo",
		Branch:     "feat/cool-feature",
	}

	_, err := resolver.ResolveWorktree(context.Background(), key)
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

func TestWorktreeResolverCreatesMissingWorktree(t *testing.T) {
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "occa")
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, repoDir)

	resolver := NewGitWorktreeResolver(tmpDir)
	key := WebhookExecutionKey{
		Repository:     "anggasct/occa",
		HeadRepository: "contributor/occa",
		Branch:         "feat/new-thing",
	}

	resolved, err := resolver.ResolveWorktree(context.Background(), key)
	if err != nil {
		t.Fatalf("ResolveWorktree failed: %v", err)
	}
	if !strings.Contains(resolved, ".worktree") {
		t.Fatalf("expected resolved path inside .worktree, got %q", resolved)
	}
	if _, err := os.Stat(resolved); err != nil {
		t.Fatalf("created worktree path does not exist on disk: %v", err)
	}
}

func TestWorktreeResolverConcurrentCreationSerialized(t *testing.T) {
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "occa")
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatal(err)
	}
	initTestGitRepo(t, repoDir)

	resolver := NewGitWorktreeResolver(tmpDir)
	var wg sync.WaitGroup
	errs := make(chan error, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := WebhookExecutionKey{
				Repository: "anggasct/occa",
				Branch:     "feat/same-branch",
			}
			_, err := resolver.ResolveWorktree(context.Background(), key)
			if err != nil {
				errs <- err
			}
		}(i)
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
