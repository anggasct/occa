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
	"path/filepath"
	"regexp"
	"strings"
)

var (
	ErrWorktreeConflict = errors.New("worktree conflict")
	ErrInvalidRepo      = errors.New("invalid repository identifier")
)

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

type worktreeInfo struct {
	Path     string
	HEAD     string
	Branch   string
	Detached bool
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

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9]+`)

func sanitizeBranchSlug(branch string) string {
	s := nonAlphanumericRegex.ReplaceAllString(branch, "-")
	s = strings.Trim(s, "-")
	if s == "" {
		s = "branch"
	}
	return strings.ToLower(s)
}

func generateWorktreePath(repoDir string, key WebhookExecutionKey) string {
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
	if !fi.Mode().IsRegular() {
		return "", fmt.Errorf("%w: sidecar file %s is not a regular file", ErrWorktreeConflict, keyPath)
	}
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return "", fmt.Errorf("%w: read sidecar file %s: %v", ErrWorktreeConflict, keyPath, err)
	}
	stored := strings.TrimSpace(string(data))
	if stored == "" {
		return "", fmt.Errorf("%w: sidecar file %s contains empty key metadata", ErrWorktreeConflict, keyPath)
	}
	return stored, nil
}
