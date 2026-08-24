package webhook

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var (
	ErrWorktreeConflict = errors.New("worktree conflict")
	ErrRepoNotFound     = errors.New("repository not found")
	ErrInvalidRepo      = errors.New("invalid repository identifier")
)

type WorktreeResolver interface {
	ResolveWorktree(ctx context.Context, key WebhookExecutionKey) (string, error)
}

type GitWorktreeResolver struct {
	ProjectsDir string
	runner      gitRunner
	repoMu      sync.Map
}

func NewGitWorktreeResolver(projectsDir string) *GitWorktreeResolver {
	if projectsDir == "" {
		projectsDir = "/home/ubuntu/projects"
	}
	return &GitWorktreeResolver{
		ProjectsDir: projectsDir,
		runner:      &defaultGitRunner{},
	}
}

type gitRunner interface {
	Run(ctx context.Context, dir string, args ...string) (string, error)
}

type defaultGitRunner struct{}

func (d *defaultGitRunner) Run(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		errStr := strings.TrimSpace(stderr.String())
		if errStr != "" {
			return "", fmt.Errorf("git %s: %s (%w)", strings.Join(args, " "), errStr, err)
		}
		return "", fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return stdout.String(), nil
}

func validateRepoIdentifier(repo string) error {
	if repo == "" {
		return fmt.Errorf("%w: empty repository name", ErrInvalidRepo)
	}
	if filepath.IsAbs(repo) || strings.HasPrefix(repo, "/") || strings.HasPrefix(repo, "\\") {
		return fmt.Errorf("%w: absolute repository paths are forbidden: %q", ErrInvalidRepo, repo)
	}
	cleaned := filepath.Clean(repo)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) || strings.HasPrefix(cleaned, "../") {
		return fmt.Errorf("%w: repository path traversal is forbidden: %q", ErrInvalidRepo, repo)
	}
	parts := strings.Split(cleaned, string(filepath.Separator))
	if len(parts) > 2 {
		return fmt.Errorf("%w: repository path depth exceeded: %q", ErrInvalidRepo, repo)
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("%w: invalid repository path component: %q", ErrInvalidRepo, repo)
		}
		for _, r := range part {
			if !isAllowedRepoChar(r) {
				return fmt.Errorf("%w: invalid character %q in repository identifier: %q", ErrInvalidRepo, r, repo)
			}
		}
	}
	return nil
}

func isAllowedRepoChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.'
}

func (r *GitWorktreeResolver) findRepoDir(repo string) (string, error) {
	if err := validateRepoIdentifier(repo); err != nil {
		return "", err
	}

	cleanProjectsDir := filepath.Clean(r.ProjectsDir)
	baseName := path.Base(repo)
	candidates := []string{
		filepath.Join(cleanProjectsDir, baseName),
		filepath.Join(cleanProjectsDir, repo),
	}

	for _, cand := range candidates {
		cleanCand := filepath.Clean(cand)
		rel, err := filepath.Rel(cleanProjectsDir, cleanCand)
		if err != nil || strings.HasPrefix(rel, "..") || rel == "." {
			continue
		}
		if stat, err := os.Stat(cleanCand); err == nil && stat.IsDir() {
			return cleanCand, nil
		}
	}
	return "", fmt.Errorf("%w: %q under %s", ErrRepoNotFound, repo, r.ProjectsDir)
}

type worktreeInfo struct {
	Path     string
	HEAD     string
	Branch   string
	Detached bool
}

func (r *GitWorktreeResolver) listWorktrees(ctx context.Context, repoDir string) ([]worktreeInfo, error) {
	out, err := r.runner.Run(ctx, repoDir, "worktree", "list", "--porcelain")
	if err != nil {
		return nil, err
	}
	return parseWorktreePorcelain(out), nil
}

func parseWorktreePorcelain(out string) []worktreeInfo {
	var list []worktreeInfo
	var cur worktreeInfo
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			if cur.Path != "" {
				list = append(list, cur)
				cur = worktreeInfo{}
			}
			continue
		}
		if strings.HasPrefix(line, "worktree ") {
			if cur.Path != "" {
				list = append(list, cur)
				cur = worktreeInfo{}
			}
			cur.Path = strings.TrimPrefix(line, "worktree ")
		} else if strings.HasPrefix(line, "HEAD ") {
			cur.HEAD = strings.TrimPrefix(line, "HEAD ")
		} else if strings.HasPrefix(line, "branch ") {
			cur.Branch = strings.TrimPrefix(line, "branch ")
		} else if line == "detached" {
			cur.Detached = true
		}
	}
	if cur.Path != "" {
		list = append(list, cur)
	}
	return list
}

func (r *GitWorktreeResolver) isWorktreeDirty(ctx context.Context, dir string) (bool, error) {
	out, err := r.runner.Run(ctx, dir, "status", "--porcelain")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) != "", nil
}

func (r *GitWorktreeResolver) hasLocalBranch(ctx context.Context, dir, branch string) (bool, error) {
	_, err := r.runner.Run(ctx, dir, "show-ref", "--verify", "--quiet", "refs/heads/"+branch)
	return err == nil, nil
}

func (r *GitWorktreeResolver) hasRemoteBranch(ctx context.Context, dir, branch string) (bool, error) {
	_, err := r.runner.Run(ctx, dir, "show-ref", "--verify", "--quiet", "refs/remotes/origin/"+branch)
	return err == nil, nil
}

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9]+`)

func sanitizeBranchSlug(branch string) string {
	s := nonAlphanumericRegex.ReplaceAllString(branch, "-")
	s = strings.Trim(s, "-")
	if s == "" {
		s = "branch"
	}
	return strings.ToLower(s)
}

func (r *GitWorktreeResolver) generateWorktreePath(repoDir string, key WebhookExecutionKey) string {
	headRepo := key.HeadRepository
	if headRepo == "" {
		headRepo = key.Repository
	}
	h := sha256.Sum256([]byte(key.Repository + "\x00" + headRepo + "\x00" + key.Branch))
	hashSuffix := hex.EncodeToString(h[:4])

	slug := sanitizeBranchSlug(key.Branch)
	if len(slug) > 30 {
		slug = slug[:30]
		slug = strings.Trim(slug, "-")
	}

	if headRepo != key.Repository {
		owner := sanitizeBranchSlug(strings.Split(headRepo, "/")[0])
		if len(owner) > 15 {
			owner = owner[:15]
		}
		slug = owner + "-" + slug
	}

	folderName := fmt.Sprintf("%s-%s", slug, hashSuffix)
	return filepath.Join(repoDir, ".worktree", folderName)
}

func (r *GitWorktreeResolver) ResolveWorktree(ctx context.Context, key WebhookExecutionKey) (string, error) {
	if key.Repository == "" || key.Branch == "" {
		return "", errors.New("missing repository or branch in execution key")
	}

	repoDir, err := r.findRepoDir(key.Repository)
	if err != nil {
		return "", err
	}

	mu, _ := r.repoMu.LoadOrStore(repoDir, &sync.Mutex{})
	lock := mu.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	worktrees, err := r.listWorktrees(ctx, repoDir)
	if err != nil {
		return "", fmt.Errorf("list worktrees: %w", err)
	}

	branchRef := "refs/heads/" + key.Branch
	for _, wt := range worktrees {
		if wt.Branch == branchRef || wt.Branch == key.Branch {
			dirty, dErr := r.isWorktreeDirty(ctx, wt.Path)
			if dErr != nil {
				return "", fmt.Errorf("check worktree status: %w", dErr)
			}
			if dirty {
				return "", fmt.Errorf("%w: worktree at %s has uncommitted changes", ErrWorktreeConflict, wt.Path)
			}
			return wt.Path, nil
		}
	}

	targetPath := r.generateWorktreePath(repoDir, key)

	if fi, err := os.Stat(targetPath); err == nil && fi.IsDir() {
		var isAttachedThisKey bool
		for _, wt := range worktrees {
			if filepath.Clean(wt.Path) == filepath.Clean(targetPath) && (wt.Branch == branchRef || wt.Branch == key.Branch) {
				isAttachedThisKey = true
				break
			}
		}
		if !isAttachedThisKey {
			return "", fmt.Errorf("%w: path %s already exists and is not an attached worktree for branch %s", ErrWorktreeConflict, targetPath, key.Branch)
		}
		dirty, dErr := r.isWorktreeDirty(ctx, targetPath)
		if dErr != nil {
			return "", fmt.Errorf("check worktree status: %w", dErr)
		}
		if dirty {
			return "", fmt.Errorf("%w: worktree at %s has uncommitted changes", ErrWorktreeConflict, targetPath)
		}
		return targetPath, nil
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return "", fmt.Errorf("mkdir .worktree: %w", err)
	}

	hasLocal, _ := r.hasLocalBranch(ctx, repoDir, key.Branch)
	if hasLocal {
		if _, err := r.runner.Run(ctx, repoDir, "worktree", "add", targetPath, key.Branch); err != nil {
			return "", fmt.Errorf("add worktree for branch %s: %w", key.Branch, err)
		}
	} else {
		hasRemote, _ := r.hasRemoteBranch(ctx, repoDir, key.Branch)
		if hasRemote {
			if _, err := r.runner.Run(ctx, repoDir, "worktree", "add", targetPath, "-b", key.Branch, "origin/"+key.Branch); err != nil {
				return "", fmt.Errorf("add worktree for remote branch origin/%s: %w", key.Branch, err)
			}
		} else {
			return "", fmt.Errorf("branch %q not found in local or remote refs", key.Branch)
		}
	}

	return targetPath, nil
}
