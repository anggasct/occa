package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/config"
)

var (
	ErrWorkspaceUnavailable = errors.New("workspace unavailable")
	ErrRepositoryMismatch   = errors.New("workspace repository mismatch")
	ErrRevisionRequired     = errors.New("workspace revision required")
	ErrWorkspaceLeased      = errors.New("workspace leased")
	ErrWorkspaceDirty       = errors.New("workspace dirty")
)

func IsWorkspaceRetryable(err error) bool {
	return errors.Is(err, ErrWorkspaceLeased) || errors.Is(err, ErrWorkspaceDirty)
}

type WorkspaceRequest struct {
	Repository string
	Path       string
	Mode       string
	Key        WebhookExecutionKey
	DeliveryID string
}

type WorkspaceResolver interface {
	ResolveWorkspace(ctx context.Context, req WorkspaceRequest) (*WorkspaceLease, error)
}

const defaultIsolatedTTL = 24 * time.Hour

type WorkspaceManager struct {
	runner      gitRunner
	leases      sync.Map
	roots       sync.Map
	repoLocks   sync.Map
	Now         func() time.Time
	IsolatedTTL time.Duration
}

func NewWorkspaceManager() *WorkspaceManager {
	return &WorkspaceManager{
		runner:      &defaultGitRunner{},
		Now:         time.Now,
		IsolatedTTL: defaultIsolatedTTL,
	}
}

type WorkspaceLease struct {
	manager  *WorkspaceManager
	Path     string
	Mode     string
	mu       sync.Mutex
	released bool
}

func (l *WorkspaceLease) Release(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	l.released = true
	if l.manager == nil {
		return nil
	}
	if l.Mode == config.WorkspaceModeIsolated {
		err := l.manager.removeIsolated(ctx, l.Path)
		l.manager.leases.Delete(l.Path)
		return err
	}
	l.manager.leases.Delete(l.Path)
	return nil
}

type workspaceLeaseEntry struct {
	Holder   string
	Acquired time.Time
}

type isolatedMetadata struct {
	Owner      string `json:"owner"`
	DeliveryID string `json:"delivery_id"`
	Revision   string `json:"revision"`
	CreatedAt  int64  `json:"created_at"`
	ExpiresAt  int64  `json:"expires_at"`
}

func (m *WorkspaceManager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *WorkspaceManager) resolveRepoRoot(ctx context.Context, path string) (string, error) {
	clean := filepath.Clean(path)
	real, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return "", fmt.Errorf("%w: workspace path %s is not reachable: %v", ErrWorkspaceUnavailable, path, err)
	}
	stat, err := os.Stat(real)
	if err != nil || !stat.IsDir() {
		return "", fmt.Errorf("%w: workspace path %s is not a directory", ErrWorkspaceUnavailable, path)
	}
	out, err := m.runner.Run(ctx, real, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", fmt.Errorf("%w: workspace path %s is not a Git repository", ErrWorkspaceUnavailable, path)
	}
	toplevel := filepath.Clean(strings.TrimSpace(out))
	if realRoot, rerr := filepath.EvalSymlinks(toplevel); rerr == nil {
		toplevel = realRoot
	}
	rel, err := filepath.Rel(toplevel, real)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("%w: workspace path %s escapes its repository root", ErrWorkspaceUnavailable, path)
	}
	return toplevel, nil
}

func (m *WorkspaceManager) validateRemoteBinding(ctx context.Context, root, binding string) (string, error) {
	out, err := m.runner.Run(ctx, root, "config", "--get", "remote.origin.url")
	if err != nil {
		return "", fmt.Errorf("%w: repository at %s has no origin remote", ErrRepositoryMismatch, root)
	}
	remoteCanonical, cerr := config.CanonicalRepository(strings.TrimSpace(out))
	if cerr != nil {
		return "", fmt.Errorf("%w: repository at %s has an unrecognizable remote: %v", ErrRepositoryMismatch, root, cerr)
	}
	if remoteCanonical == binding {
		return remoteCanonical, nil
	}
	if len(strings.Split(binding, "/")) == 2 && config.RepositoryPath(remoteCanonical) == binding {
		return remoteCanonical, nil
	}
	return "", fmt.Errorf("%w: repository at %s resolves to %q, endpoint binds %q", ErrRepositoryMismatch, root, remoteCanonical, binding)
}

func (m *WorkspaceManager) ResolveWorkspace(ctx context.Context, req WorkspaceRequest) (*WorkspaceLease, error) {
	root, err := m.resolveRepoRoot(ctx, req.Path)
	if err != nil {
		return nil, err
	}
	m.roots.Store(root, struct{}{})

	remoteCanonical, err := m.validateRemoteBinding(ctx, root, req.Repository)
	if err != nil {
		return nil, err
	}
	if req.Key.Repository != "" && config.RepositoryPath(remoteCanonical) != req.Key.Repository {
		return nil, fmt.Errorf("%w: event repository %q does not match endpoint binding %q", ErrRepositoryMismatch, req.Key.Repository, config.RepositoryPath(remoteCanonical))
	}

	switch req.Mode {
	case config.WorkspaceModeIsolated:
		return m.resolveIsolated(ctx, root, req)
	case config.WorkspaceModeMutable:
		return m.resolveMutable(ctx, root, req)
	default:
		return nil, fmt.Errorf("%w: unknown workspace mode %q", ErrWorkspaceUnavailable, req.Mode)
	}
}

func (m *WorkspaceManager) resolveIsolated(ctx context.Context, root string, req WorkspaceRequest) (*WorkspaceLease, error) {
	if req.Key.HeadRevision == "" {
		return nil, fmt.Errorf("%w: %s mode requires an exact head revision and the event did not provide one", ErrRevisionRequired, config.WorkspaceModeIsolated)
	}

	slug := sanitizeBranchSlug(req.Key.Branch)
	if len(slug) > 24 {
		slug = slug[:24]
		slug = strings.Trim(slug, "-")
	}
	revShort := req.Key.HeadRevision
	if len(revShort) > 10 {
		revShort = revShort[:10]
	}
	seed := sha256Sum(req.DeliveryID + "\x00" + req.Key.HeadRevision)
	target := filepath.Join(root, ".occa-workspaces", fmt.Sprintf("%s-%s-%s", slug, revShort, seed[:10]))

	if _, err := os.Stat(target); err == nil {
		return nil, fmt.Errorf("%w: isolated workspace target %s already exists", ErrWorkspaceUnavailable, target)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return nil, fmt.Errorf("%w: create isolated workspace parent: %v", ErrWorkspaceUnavailable, err)
	}
	if _, err := m.runner.Run(ctx, root, "worktree", "add", "--detach", target, req.Key.HeadRevision); err != nil {
		_ = os.RemoveAll(target)
		return nil, fmt.Errorf("%w: checkout revision %s into isolated workspace: %v", ErrWorkspaceUnavailable, req.Key.HeadRevision, err)
	}

	now := m.now()
	meta := isolatedMetadata{
		Owner:      "occa",
		DeliveryID: req.DeliveryID,
		Revision:   req.Key.HeadRevision,
		CreatedAt:  now.Unix(),
		ExpiresAt:  now.Add(m.isolatedTTL()).Unix(),
	}
	if err := writeIsolatedMetadata(target, meta); err != nil {
		_ = m.removeIsolated(ctx, target)
		return nil, fmt.Errorf("%w: write isolated workspace metadata: %v", ErrWorkspaceUnavailable, err)
	}

	slog.Info("webhook: isolated workspace created",
		"workspace", target,
		"revision", req.Key.HeadRevision,
		"delivery_id", req.DeliveryID,
	)
	m.leases.Store(target, &workspaceLeaseEntry{Holder: req.DeliveryID, Acquired: now})
	return &WorkspaceLease{manager: m, Path: target, Mode: config.WorkspaceModeIsolated}, nil
}

func (m *WorkspaceManager) isolatedTTL() time.Duration {
	if m.IsolatedTTL > 0 {
		return m.IsolatedTTL
	}
	return defaultIsolatedTTL
}

func (m *WorkspaceManager) resolveMutable(ctx context.Context, root string, req WorkspaceRequest) (*WorkspaceLease, error) {
	if req.Key.Repository == "" || req.Key.Branch == "" {
		return nil, fmt.Errorf("%w: mutable mode requires repository and branch identity in the event", ErrWorkspaceUnavailable)
	}

	target := generateWorktreePath(root, req.Key)
	if _, loaded := m.leases.LoadOrStore(target, &workspaceLeaseEntry{Holder: req.DeliveryID, Acquired: m.now()}); loaded {
		return nil, fmt.Errorf("%w: mutable workspace %s is held by another delivery", ErrWorkspaceLeased, target)
	}

	lease := &WorkspaceLease{manager: m, Path: "", Mode: config.WorkspaceModeMutable}
	path, err := m.attachMutable(ctx, root, req, target)
	if err != nil {
		m.leases.Delete(target)
		return nil, err
	}
	lease.Path = path
	if finalPath := filepath.Clean(path); finalPath != filepath.Clean(target) {
		if _, loaded := m.leases.LoadOrStore(finalPath, &workspaceLeaseEntry{Holder: req.DeliveryID, Acquired: m.now()}); loaded {
			m.leases.Delete(target)
			return nil, fmt.Errorf("%w: mutable workspace %s is held by another delivery", ErrWorkspaceLeased, finalPath)
		}
		m.leases.Delete(target)
	}
	return lease, nil
}

func (m *WorkspaceManager) attachMutable(ctx context.Context, root string, req WorkspaceRequest, target string) (string, error) {
	mu, _ := m.repoLocks.LoadOrStore(root, &sync.Mutex{})
	lock := mu.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	worktrees, err := m.listWorktrees(ctx, root)
	if err != nil {
		return "", fmt.Errorf("%w: list worktrees: %v", ErrWorkspaceUnavailable, err)
	}

	cleanRoot := filepath.Clean(root)
	cleanTarget := filepath.Clean(target)
	branchRef := "refs/heads/" + req.Key.Branch

	for _, wt := range worktrees {
		cleanWtPath := filepath.Clean(wt.Path)
		if cleanWtPath == cleanRoot {
			continue
		}
		isMatchingBranch := wt.Branch == branchRef || wt.Branch == req.Key.Branch
		isMatchingTarget := cleanWtPath == cleanTarget
		if !isMatchingBranch && !isMatchingTarget {
			continue
		}
		sidecarPath := wt.Path + ".key"
		var needWriteSidecar bool
		storedKey, sErr := readKeySidecar(sidecarPath)
		if sErr != nil {
			if os.IsNotExist(sErr) {
				needWriteSidecar = true
			} else {
				return "", fmt.Errorf("%w: sidecar validation failed for attached worktree %s: %v", ErrWorktreeConflict, wt.Path, sErr)
			}
		} else if storedKey != req.Key.String() {
			return "", fmt.Errorf("%w: branch %s attached at %s belongs to a different execution key %q", ErrWorktreeConflict, req.Key.Branch, wt.Path, storedKey)
		}

		dirty, dErr := m.isWorktreeDirty(ctx, wt.Path)
		if dErr != nil {
			return "", fmt.Errorf("%w: check worktree status: %v", ErrWorkspaceUnavailable, dErr)
		}
		if dirty {
			return "", fmt.Errorf("%w: worktree at %s has uncommitted changes", ErrWorkspaceDirty, wt.Path)
		}

		if needWriteSidecar {
			if err := writeKeySidecar(sidecarPath, req.Key.String()); err != nil {
				return "", fmt.Errorf("%w: write sidecar metadata for attached worktree %s: %v", ErrWorktreeConflict, wt.Path, err)
			}
		}
		return wt.Path, nil
	}

	if fi, err := os.Stat(target); err == nil && fi.IsDir() {
		return "", fmt.Errorf("%w: path %s already exists and is not an attached worktree for %s", ErrWorktreeConflict, target, req.Key.String())
	}

	hasLocal, _ := m.hasLocalBranch(ctx, root, req.Key.Branch)
	if !hasLocal {
		hasRemote, _ := m.hasRemoteBranch(ctx, root, req.Key.Branch)
		if !hasRemote {
			return "", fmt.Errorf("%w: branch %q not found in local or remote refs", ErrWorkspaceUnavailable, req.Key.Branch)
		}
	}

	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return "", fmt.Errorf("%w: mkdir .worktree: %v", ErrWorkspaceUnavailable, err)
	}

	if hasLocal {
		if _, err := m.runner.Run(ctx, root, "worktree", "add", target, req.Key.Branch); err != nil {
			if strings.Contains(err.Error(), "already used by worktree") || strings.Contains(err.Error(), "already checked out") {
				if _, err := m.runner.Run(ctx, root, "worktree", "add", "--detach", target, req.Key.Branch); err != nil {
					return "", fmt.Errorf("%w: add detached worktree for branch %s: %v", ErrWorktreeConflict, req.Key.Branch, err)
				}
			} else {
				return "", fmt.Errorf("%w: add worktree for branch %s: %v", ErrWorkspaceUnavailable, req.Key.Branch, err)
			}
		}
	} else {
		if _, err := m.runner.Run(ctx, root, "worktree", "add", target, "-b", req.Key.Branch, "origin/"+req.Key.Branch); err != nil {
			if strings.Contains(err.Error(), "already used by worktree") || strings.Contains(err.Error(), "already checked out") {
				if _, err := m.runner.Run(ctx, root, "worktree", "add", "--detach", target, "origin/"+req.Key.Branch); err != nil {
					return "", fmt.Errorf("%w: add detached worktree for remote branch origin/%s: %v", ErrWorktreeConflict, req.Key.Branch, err)
				}
			} else {
				return "", fmt.Errorf("%w: add worktree for remote branch origin/%s: %v", ErrWorkspaceUnavailable, req.Key.Branch, err)
			}
		}
	}

	if err := writeKeySidecar(target+".key", req.Key.String()); err != nil {
		return "", err
	}
	return target, nil
}

func (m *WorkspaceManager) listWorktrees(ctx context.Context, repoDir string) ([]worktreeInfo, error) {
	out, err := m.runner.Run(ctx, repoDir, "worktree", "list", "--porcelain")
	if err != nil {
		return nil, err
	}
	return parseWorktreePorcelain(out), nil
}

func (m *WorkspaceManager) isWorktreeDirty(ctx context.Context, dir string) (bool, error) {
	out, err := m.runner.Run(ctx, dir, "status", "--porcelain")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) != "", nil
}

func (m *WorkspaceManager) hasLocalBranch(ctx context.Context, dir, branch string) (bool, error) {
	_, err := m.runner.Run(ctx, dir, "show-ref", "--verify", "--quiet", "refs/heads/"+branch)
	return err == nil, nil
}

func (m *WorkspaceManager) hasRemoteBranch(ctx context.Context, dir, branch string) (bool, error) {
	_, err := m.runner.Run(ctx, dir, "show-ref", "--verify", "--quiet", "refs/remotes/origin/"+branch)
	return err == nil, nil
}

func sha256Sum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func isolatedMetadataPath(workspace string) string {
	return workspace + ".occa.json"
}

func writeIsolatedMetadata(workspace string, meta isolatedMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(isolatedMetadataPath(workspace), data, 0644)
}

func readIsolatedMetadata(workspace string) (isolatedMetadata, error) {
	data, err := os.ReadFile(isolatedMetadataPath(workspace))
	if err != nil {
		return isolatedMetadata{}, err
	}
	var meta isolatedMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return isolatedMetadata{}, err
	}
	if meta.Owner != "occa" {
		return isolatedMetadata{}, fmt.Errorf("workspace %s is not occa-owned", workspace)
	}
	return meta, nil
}

func (m *WorkspaceManager) removeIsolated(ctx context.Context, workspace string) error {
	if _, err := m.runner.Run(ctx, workspace, "worktree", "remove", "--force", "."); err != nil {
		if rErr := os.RemoveAll(workspace); rErr != nil {
			return fmt.Errorf("remove isolated workspace %s: git: %v; fs: %v", workspace, err, rErr)
		}
	}
	_ = os.Remove(isolatedMetadataPath(workspace))
	slog.Info("webhook: isolated workspace removed", "workspace", workspace)
	return nil
}

func (m *WorkspaceManager) ReapExpiredWorkspaces(ctx context.Context) int {
	reaped := 0
	m.roots.Range(func(key, _ any) bool {
		root, _ := key.(string)
		dir := filepath.Join(root, ".occa-workspaces")
		entries, err := os.ReadDir(dir)
		if err != nil {
			return true
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			workspace := filepath.Join(dir, entry.Name())
			meta, mErr := readIsolatedMetadata(workspace)
			if mErr != nil {
				continue
			}
			if m.now().Unix() < meta.ExpiresAt {
				continue
			}
			if _, leased := m.leases.Load(workspace); leased {
				continue
			}
			if rErr := m.removeIsolated(ctx, workspace); rErr != nil {
				slog.Warn("webhook: expired isolated workspace cleanup failed", "workspace", workspace, "error", rErr)
				continue
			}
			reaped++
		}
		return true
	})
	return reaped
}
