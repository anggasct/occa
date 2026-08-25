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

func (r *GitWorktreeResolver) findExactRepoDir(repo string) (string, error) {
	if err := validateRepoIdentifier(repo); err != nil {
		return "", err
	}

	cleanProjectsDir := filepath.Clean(r.ProjectsDir)
	realProjectsDir, err := filepath.EvalSymlinks(cleanProjectsDir)
	if err != nil {
		realProjectsDir = cleanProjectsDir
	}

	cand := filepath.Join(cleanProjectsDir, repo)
	cleanCand := filepath.Clean(cand)

	relLex, err := filepath.Rel(cleanProjectsDir, cleanCand)
	if err != nil || strings.HasPrefix(relLex, "..") || relLex == "." {
		return "", fmt.Errorf("%w: %q under %s", ErrRepoNotFound, repo, r.ProjectsDir)
	}

	realCand, err := filepath.EvalSymlinks(cleanCand)
	if err != nil {
		return "", fmt.Errorf("%w: %q under %s", ErrRepoNotFound, repo, r.ProjectsDir)
	}
	relReal, err := filepath.Rel(realProjectsDir, realCand)
	if err != nil || strings.HasPrefix(relReal, "..") || relReal == "." {
		return "", fmt.Errorf("%w: %q escapes root %s", ErrRepoNotFound, repo, r.ProjectsDir)
	}

	if stat, err := os.Stat(realCand); err == nil && stat.IsDir() {
		return realCand, nil
	}
	return "", fmt.Errorf("%w: %q under %s", ErrRepoNotFound, repo, r.ProjectsDir)
}

func (r *GitWorktreeResolver) findRepoDir(repo string) (string, error) {
	if err := validateRepoIdentifier(repo); err != nil {
		return "", err
	}

	cleanProjectsDir := filepath.Clean(r.ProjectsDir)
	realProjectsDir, err := filepath.EvalSymlinks(cleanProjectsDir)
	if err != nil {
		realProjectsDir = cleanProjectsDir
	}

	baseName := path.Base(repo)
	candidates := []string{
		filepath.Join(cleanProjectsDir, repo),
	}
	if strings.Contains(repo, "/") {
		candidates = append(candidates, filepath.Join(cleanProjectsDir, baseName))
	}

	for _, cand := range candidates {
		cleanCand := filepath.Clean(cand)
		relLex, err := filepath.Rel(cleanProjectsDir, cleanCand)
		if err != nil || strings.HasPrefix(relLex, "..") || relLex == "." {
			continue
		}

		realCand, err := filepath.EvalSymlinks(cleanCand)
		if err != nil {
			continue
		}
		relReal, err := filepath.Rel(realProjectsDir, realCand)
		if err != nil || strings.HasPrefix(relReal, "..") || relReal == "." {
			continue
		}

		if stat, err := os.Stat(realCand); err == nil && stat.IsDir() {
			return realCand, nil
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
	hashHex := hex.EncodeToString(h[:])

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

	folderName := fmt.Sprintf("%s-%s", slug, hashHex)
	return filepath.Join(repoDir, ".worktree", folderName)
}

func writeKeySidecar(keyPath, keyStr string) error {
	if fi, err := os.Lstat(keyPath); err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: sidecar file %s is a symlink", ErrWorktreeConflict, keyPath)
		}
		_ = os.Remove(keyPath)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("lstat sidecar: %w", err)
	}

	f, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("create sidecar file: %w", err)
	}
	defer func() { _ = f.Close() }()

	if _, err := f.WriteString(keyStr + "\n"); err != nil {
		return fmt.Errorf("write sidecar file: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync sidecar file: %w", err)
	}
	return f.Close()
}

func readKeySidecar(keyPath string) (string, error) {
	fi, err := os.Lstat(keyPath)
	if err != nil {
		return "", err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%w: sidecar file %s is a symlink", ErrWorktreeConflict, keyPath)
	}
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func (r *GitWorktreeResolver) ResolveWorktree(ctx context.Context, key WebhookExecutionKey) (string, error) {
	if key.Repository == "" || key.Branch == "" {
		return "", errors.New("missing repository or branch in execution key")
	}

	var repoDir string
	var err error

	if key.HeadRepository != "" && key.HeadRepository != key.Repository {
		repoDir, err = r.findExactRepoDir(key.HeadRepository)
		if err != nil {
			return "", fmt.Errorf("%w: fork head repository %q not found under %s", ErrRepoNotFound, key.HeadRepository, r.ProjectsDir)
		}
	} else {
		repoDir, err = r.findRepoDir(key.Repository)
		if err != nil {
			return "", err
		}
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
			sidecarPath := wt.Path + ".key"
			if storedKey, sErr := readKeySidecar(sidecarPath); sErr == nil && storedKey != "" {
				if storedKey != key.String() {
					return "", fmt.Errorf("%w: branch %s attached at %s belongs to a different execution key %q", ErrWorktreeConflict, key.Branch, wt.Path, storedKey)
				}
			} else if errors.Is(sErr, ErrWorktreeConflict) {
				return "", sErr
			}

			dirty, dErr := r.isWorktreeDirty(ctx, wt.Path)
			if dErr != nil {
				return "", fmt.Errorf("check worktree status: %w", dErr)
			}
			if dirty {
				return "", fmt.Errorf("%w: worktree at %s has uncommitted changes", ErrWorktreeConflict, wt.Path)
			}

			// Ensure sidecar metadata is written if missing
			if _, sErr := os.Lstat(sidecarPath); os.IsNotExist(sErr) {
				if err := writeKeySidecar(sidecarPath, key.String()); err != nil {
					return "", err
				}
			}
			return wt.Path, nil
		}
	}

	targetPath := r.generateWorktreePath(repoDir, key)
	if fi, err := os.Stat(targetPath); err == nil && fi.IsDir() {
		return "", fmt.Errorf("%w: path %s already exists and is not an attached worktree for %s", ErrWorktreeConflict, targetPath, key.String())
	}

	hasLocal, _ := r.hasLocalBranch(ctx, repoDir, key.Branch)
	if !hasLocal {
		hasRemote, _ := r.hasRemoteBranch(ctx, repoDir, key.Branch)
		if !hasRemote {
			return "", fmt.Errorf("branch %q not found in local or remote refs for repo %s", key.Branch, repoDir)
		}
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return "", fmt.Errorf("mkdir .worktree: %w", err)
	}

	if hasLocal {
		if _, err := r.runner.Run(ctx, repoDir, "worktree", "add", targetPath, key.Branch); err != nil {
			return "", fmt.Errorf("add worktree for branch %s: %w", key.Branch, err)
		}
	} else {
		if _, err := r.runner.Run(ctx, repoDir, "worktree", "add", targetPath, "-b", key.Branch, "origin/"+key.Branch); err != nil {
			return "", fmt.Errorf("add worktree for remote branch origin/%s: %w", key.Branch, err)
		}
	}

	sidecarPath := targetPath + ".key"
	if err := writeKeySidecar(sidecarPath, key.String()); err != nil {
		return "", err
	}

	return targetPath, nil
}
