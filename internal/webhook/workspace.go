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
	ErrRevisionMissing      = errors.New("workspace revision missing")
	ErrWorkspaceLeased      = errors.New("workspace leased")
	ErrWorkspaceDirty       = errors.New("workspace dirty")
)

const defaultWorkspaceFetchTimeout = 30 * time.Second

func IsWorkspaceRetryable(err error) bool {
	return errors.Is(err, ErrWorkspaceLeased) || errors.Is(err, ErrWorkspaceDirty)
}

type WorkspaceRequest struct {
	Repository      string
	Path            string
	Mode            string
	Key             WebhookExecutionKey
	DeliveryID      string
	Lane            string
	PRNumber        string
	BindingWorktree string
	TTL             time.Duration
}

type WorkspaceResolver interface {
	ResolveWorkspace(ctx context.Context, req WorkspaceRequest) (*WorkspaceLease, error)
}

func NewWorkspaceManager() *WorkspaceManager {
	return &WorkspaceManager{
		runner: &defaultGitRunner{},
		Now:    time.Now,
	}
}

func NewWorkspaceManagerWithTTL(ttl time.Duration) *WorkspaceManager {
	return &WorkspaceManager{
		runner:      &defaultGitRunner{},
		Now:         time.Now,
		IsolatedTTL: ttl,
	}
}

type WorkspaceManager struct {
	runner       gitRunner
	leases       sync.Map
	roots        sync.Map
	repoLocks    sync.Map
	Now          func() time.Time
	IsolatedTTL  time.Duration
	FetchTimeout time.Duration
}

type WorkspaceLease struct {
	manager       *WorkspaceManager
	Path          string
	Mode          string
	Branch        string
	StartRevision string
	mu            sync.Mutex
	released      bool
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
	// Isolated checkouts live for their TTL so a re-review of the same head
	// reuses the checkout instead of paying for a fresh one.
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

	var lease *WorkspaceLease
	var resolveErr error
	switch req.Mode {
	case config.WorkspaceModeIsolated:
		lease, resolveErr = m.resolveIsolated(ctx, root, req)
	case config.WorkspaceModeMutable:
		lease, resolveErr = m.resolveMutable(ctx, root, req)
	default:
		return nil, fmt.Errorf("%w: unknown workspace mode %q", ErrWorkspaceUnavailable, req.Mode)
	}
	if resolveErr != nil {
		return nil, resolveErr
	}
	m.logBindingMismatch(req, lease)
	return lease, nil
}

func (m *WorkspaceManager) logBindingMismatch(req WorkspaceRequest, lease *WorkspaceLease) {
	binding := strings.TrimSpace(req.BindingWorktree)
	if binding == "" || lease == nil || strings.TrimSpace(lease.Path) == "" {
		return
	}
	if filepath.Clean(binding) == filepath.Clean(lease.Path) {
		return
	}
	slog.Warn("webhook: workspace binding mismatch",
		"lane", req.Lane,
		"lease", lease.Path,
		"binding", binding,
		"ignored", true,
	)
}

func (m *WorkspaceManager) canonicalRevision(ctx context.Context, root, revision, branch, prNumber string) (string, error) {
	if canonical, ok := m.verifyRevision(ctx, root, revision); ok {
		return canonical, nil
	}

	var tried []string
	if b := strings.TrimSpace(branch); b != "" {
		tried = append(tried, "origin/"+b)
		if ferr := m.fetchRevision(ctx, root, b, "fetch", "--no-tags", "--prune", "origin", b); fetchAborted(ferr) {
			return "", fmt.Errorf("%w: %w: %w", ErrWorkspaceUnavailable, ErrRevisionMissing, ferr)
		}
		if canonical, ok := m.verifyRevision(ctx, root, revision); ok {
			return canonical, nil
		}
	}

	if n := strings.TrimSpace(prNumber); n != "" {
		ref := "refs/pull/" + n + "/head"
		tried = append(tried, ref)
		if ferr := m.fetchRevision(ctx, root, ref, "fetch", "--no-tags", "origin", "+"+ref+":refs/occa/pull/"+n); fetchAborted(ferr) {
			return "", fmt.Errorf("%w: %w: %w", ErrWorkspaceUnavailable, ErrRevisionMissing, ferr)
		}
		if canonical, ok := m.verifyRevision(ctx, root, revision); ok {
			return canonical, nil
		}
	}

	if len(tried) == 0 {
		tried = append(tried, "none")
	}
	return "", fmt.Errorf("%w: %w: revision %s not in %s after fetch (refs tried: %s); action: verify the checkout remote and fetch the PR head", ErrWorkspaceUnavailable, ErrRevisionMissing, revision, root, strings.Join(tried, ", "))
}

func (m *WorkspaceManager) verifyRevision(ctx context.Context, root, revision string) (string, bool) {
	out, err := m.runner.Run(ctx, root, "rev-parse", "--verify", revision+"^{commit}")
	if err != nil {
		return "", false
	}
	canonical := strings.TrimSpace(out)
	if canonical == "" {
		return "", false
	}
	return canonical, true
}

func (m *WorkspaceManager) fetchRevision(ctx context.Context, checkout, ref string, args ...string) error {
	timeout := m.fetchTimeout()
	fetchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()
	_, err := m.runner.Run(fetchCtx, checkout, args...)
	result := "ok"
	if err != nil {
		result = "error"
	}
	slog.Info("webhook: workspace fetch",
		"checkout", checkout,
		"ref", ref,
		"duration_ms", time.Since(start).Milliseconds(),
		"result", result,
	)
	if err != nil {
		if abortErr := fetchCtx.Err(); abortErr != nil {
			return fmt.Errorf("fetch %s timed out after %s: %w", ref, timeout, abortErr)
		}
		return err
	}
	return nil
}

func (m *WorkspaceManager) fetchTimeout() time.Duration {
	if m.FetchTimeout > 0 {
		return m.FetchTimeout
	}
	return defaultWorkspaceFetchTimeout
}

func fetchAborted(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func (m *WorkspaceManager) resolveIsolated(ctx context.Context, root string, req WorkspaceRequest) (*WorkspaceLease, error) {
	ttl := req.TTL
	if ttl <= 0 {
		ttl = m.isolatedTTL()
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("%w: isolated workspace TTL is not configured", ErrWorkspaceUnavailable)
	}
	if req.Key.HeadRevision == "" {
		return nil, fmt.Errorf("%w: %s mode requires an exact head revision and the event did not provide one", ErrRevisionRequired, config.WorkspaceModeIsolated)
	}
	revision, err := m.canonicalRevision(ctx, root, req.Key.HeadRevision, req.Key.Branch, req.PRNumber)
	if err != nil {
		return nil, err
	}
	req.Key.HeadRevision = revision

	revShort := req.Key.HeadRevision
	if len(revShort) > 10 {
		revShort = revShort[:10]
	}
	// Keyed by the revision alone so two reviews of the same head share one
	// checkout; the branch may change without changing the tree under review.
	seed := sha256Sum(req.Key.HeadRevision)
	target := filepath.Join(root, ".occa-workspaces", fmt.Sprintf("review-%s-%s", revShort, seed[:10]))

	lock := m.lockFor(target)
	lock.Lock()
	defer lock.Unlock()

	lease, err := m.reuseIsolated(ctx, root, target, req, ttl)
	if err != nil {
		return nil, err
	}
	if lease != nil {
		return lease, nil
	}

	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return nil, fmt.Errorf("%w: create isolated workspace parent: %v", ErrWorkspaceUnavailable, err)
	}
	if _, err := m.runner.Run(ctx, root, "worktree", "add", "--detach", target, req.Key.HeadRevision); err != nil {
		if lease, rerr := m.reuseIsolated(ctx, root, target, req, ttl); rerr == nil && lease != nil {
			return lease, nil
		}
		_ = os.RemoveAll(target)
		return nil, fmt.Errorf("%w: checkout revision %s into isolated workspace: %v", ErrWorkspaceUnavailable, req.Key.HeadRevision, err)
	}

	now := m.now()
	meta := isolatedMetadata{
		Owner:      "occa",
		DeliveryID: req.DeliveryID,
		Revision:   req.Key.HeadRevision,
		CreatedAt:  now.Unix(),
		ExpiresAt:  now.Add(ttl).Unix(),
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
	return &WorkspaceLease{manager: m, Path: target, Mode: config.WorkspaceModeIsolated, Branch: req.Key.Branch, StartRevision: req.Key.HeadRevision}, nil
}

func (m *WorkspaceManager) reuseIsolated(ctx context.Context, root, target string, req WorkspaceRequest, ttl time.Duration) (*WorkspaceLease, error) {
	if _, err := os.Stat(target); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: inspect isolated workspace %s: %v", ErrWorkspaceUnavailable, target, err)
	}
	now := m.now()
	meta, err := readIsolatedMetadata(target)
	if err != nil {
		out, revErr := m.runner.Run(ctx, target, "rev-parse", "HEAD")
		if revErr != nil || strings.TrimSpace(out) != req.Key.HeadRevision {
			if dErr := m.discardIsolated(ctx, root, target); dErr != nil {
				return nil, dErr
			}
			return nil, nil
		}
		meta = isolatedMetadata{
			Owner:      "occa",
			DeliveryID: req.DeliveryID,
			Revision:   req.Key.HeadRevision,
			CreatedAt:  now.Unix(),
			ExpiresAt:  now.Add(ttl).Unix(),
		}
		if wErr := writeIsolatedMetadata(target, meta); wErr != nil {
			if dErr := m.discardIsolated(ctx, root, target); dErr != nil {
				return nil, dErr
			}
			return nil, fmt.Errorf("%w: restore isolated workspace metadata: %v", ErrWorkspaceUnavailable, wErr)
		}
	}
	if meta.Revision != req.Key.HeadRevision {
		if dErr := m.discardIsolated(ctx, root, target); dErr != nil {
			return nil, dErr
		}
		return nil, nil
	}

	now = m.now()
	if now.Unix() >= meta.ExpiresAt {
		if _, leased := m.leases.Load(target); !leased {
			if err := m.removeIsolated(ctx, target); err != nil {
				return nil, fmt.Errorf("%w: remove expired isolated workspace %s: %v", ErrWorkspaceUnavailable, target, err)
			}
			return nil, nil
		}
	}

	out, err := m.runner.Run(ctx, target, "rev-parse", "HEAD")
	if err != nil {
		if dErr := m.discardIsolated(ctx, root, target); dErr != nil {
			return nil, dErr
		}
		return nil, nil
	}
	if strings.TrimSpace(out) != req.Key.HeadRevision {
		if dErr := m.discardIsolated(ctx, root, target); dErr != nil {
			return nil, dErr
		}
		return nil, nil
	}
	status, err := m.runner.Run(ctx, target, "status", "--porcelain")
	if err != nil {
		return nil, fmt.Errorf("%w: inspect isolated workspace status: %v", ErrWorkspaceUnavailable, err)
	}
	if strings.TrimSpace(status) != "" {
		return nil, fmt.Errorf("%w: isolated workspace %s has uncommitted changes", ErrWorkspaceDirty, target)
	}

	meta.DeliveryID = req.DeliveryID
	meta.ExpiresAt = now.Add(ttl).Unix()
	if err := writeIsolatedMetadata(target, meta); err != nil {
		return nil, fmt.Errorf("%w: refresh isolated workspace metadata: %v", ErrWorkspaceUnavailable, err)
	}
	m.leases.Store(target, &workspaceLeaseEntry{Holder: req.DeliveryID, Acquired: now})
	slog.Info("webhook: isolated workspace reused",
		"workspace", target,
		"revision", req.Key.HeadRevision,
		"delivery_id", req.DeliveryID,
	)
	return &WorkspaceLease{manager: m, Path: target, Mode: config.WorkspaceModeIsolated, Branch: req.Key.Branch, StartRevision: req.Key.HeadRevision}, nil
}

func (m *WorkspaceManager) discardIsolated(ctx context.Context, root, target string) error {
	if _, leased := m.leases.Load(target); leased {
		return fmt.Errorf("%w: isolated workspace %s is in use by another delivery", ErrWorkspaceUnavailable, target)
	}
	if err := m.removeIsolated(ctx, target); err != nil {
		return fmt.Errorf("%w: remove unusable isolated workspace %s: %v", ErrWorkspaceUnavailable, target, err)
	}
	if _, err := m.runner.Run(ctx, root, "worktree", "prune"); err != nil {
		slog.Warn("webhook: isolated workspace prune failed", "workspace", target, "error", err)
	}
	return nil
}

func (m *WorkspaceManager) lockFor(key string) *sync.Mutex {
	mu, _ := m.repoLocks.LoadOrStore("workspace:"+key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (m *WorkspaceManager) isolatedTTL() time.Duration {
	return m.IsolatedTTL
}

func (m *WorkspaceManager) resolveMutable(ctx context.Context, root string, req WorkspaceRequest) (*WorkspaceLease, error) {
	if req.Key.Repository == "" || req.Key.Branch == "" {
		return nil, fmt.Errorf("%w: mutable mode requires repository and branch identity in the event", ErrWorkspaceUnavailable)
	}

	target := generateWorktreePath(root, req.Key)
	if _, loaded := m.leases.LoadOrStore(target, &workspaceLeaseEntry{Holder: req.DeliveryID, Acquired: m.now()}); loaded {
		return nil, fmt.Errorf("%w: mutable workspace %s is held by another delivery", ErrWorkspaceLeased, target)
	}

	lease := &WorkspaceLease{manager: m, Path: "", Mode: config.WorkspaceModeMutable, Branch: req.Key.Branch}
	path, err := m.attachMutable(ctx, root, req, target)
	if err != nil {
		m.leases.Delete(target)
		return nil, err
	}
	revision, err := m.runner.Run(ctx, path, "rev-parse", "HEAD")
	if err != nil {
		m.leases.Delete(target)
		return nil, fmt.Errorf("%w: read mutable workspace revision: %v", ErrWorkspaceUnavailable, err)
	}
	lease.Path = path
	lease.StartRevision = strings.TrimSpace(revision)
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
	tracked, untracked := classifyPorcelainStatus(out)
	if !tracked && len(untracked) > 0 {
		slog.Info("webhook: worktree has only untracked files, proceeding",
			"workspace", dir,
			"scratch_files", strings.Join(untracked, ","),
		)
		return false, nil
	}
	return tracked, nil
}

func classifyPorcelainStatus(out string) (bool, []string) {
	var untracked []string
	tracked := false
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if strings.HasPrefix(line, "??") {
			path := strings.TrimSpace(strings.TrimPrefix(line, "??"))
			if path != "" {
				untracked = append(untracked, path)
			}
			continue
		}
		tracked = true
	}
	return tracked, untracked
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
			lock := m.lockFor(workspace)
			lock.Lock()
			meta, mErr := readIsolatedMetadata(workspace)
			if mErr == nil {
				if m.now().Unix() < meta.ExpiresAt {
					lock.Unlock()
					continue
				}
			} else if !strings.HasPrefix(entry.Name(), "review-") {
				// Without a readable sidecar, only review- workspaces are
				// known to be ours; leave anything else for an operator.
				lock.Unlock()
				continue
			}
			if _, leased := m.leases.Load(workspace); leased {
				lock.Unlock()
				continue
			}
			if rErr := m.removeIsolated(ctx, workspace); rErr != nil {
				slog.Warn("webhook: expired isolated workspace cleanup failed", "workspace", workspace, "error", rErr)
				lock.Unlock()
				continue
			}
			lock.Unlock()
			reaped++
		}
		return true
	})
	return reaped
}
