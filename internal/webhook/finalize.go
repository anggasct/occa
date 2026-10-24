package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
)

var errNoWorkInLease = errors.New("no work in lease")

const harnessFinalizeTimeout = 2 * time.Minute

type FinalizeResult struct {
	Committed bool
	Pushed    bool
	Files     int
}

// Finalize closes a writable lease after a successful turn: it commits a
// dirty tree, pushes any commit the branch does not have yet, and reports a
// run that produced nothing. The agent's own commit discipline is optional.
func (l *WorkspaceLease) Finalize(ctx context.Context, lane, deliveryID string) (FinalizeResult, error) {
	var result FinalizeResult
	if l == nil || l.manager == nil {
		return result, nil
	}
	if l.Mode != config.WorkspaceModeMutable || !isFixWorkflow(lane) {
		return result, nil
	}
	m := l.manager

	status, err := m.runner.Run(ctx, l.Path, "status", "--porcelain")
	if err != nil {
		return result, fmt.Errorf("finalize: read workspace status: %w", err)
	}
	result.Files = countPorcelainEntries(status)
	if result.Files > 0 {
		if _, err := m.runner.Run(ctx, l.Path, "add", "-A"); err != nil {
			return result, fmt.Errorf("finalize: stage workspace changes: %w", err)
		}
		message := fmt.Sprintf("%s: %d file(s) (delivery %s)", strings.ToLower(strings.TrimSpace(lane)), result.Files, deliveryID)
		if _, err := m.runner.Run(ctx, l.Path, "commit", "-m", message); err != nil {
			return result, fmt.Errorf("finalize: commit workspace changes: %w", err)
		}
		result.Committed = true
		slog.Info("webhook: workspace harness commit",
			"workspace", l.Path,
			"files", result.Files,
			"delivery_id", deliveryID,
		)
	}

	head, err := m.runner.Run(ctx, l.Path, "rev-parse", "HEAD")
	if err != nil {
		return result, fmt.Errorf("finalize: read workspace head: %w", err)
	}
	head = strings.TrimSpace(head)
	if head == l.StartRevision {
		if result.Files == 0 {
			return result, fmt.Errorf("%w: %s is clean and unchanged", errNoWorkInLease, l.Path)
		}
		return result, fmt.Errorf("finalize: %s has no new revision after commit", l.Path)
	}

	pushed, err := m.pushFinalizedBranch(ctx, l.Path, l.Branch)
	if err != nil {
		return result, err
	}
	result.Pushed = pushed
	return result, nil
}

func (m *WorkspaceManager) pushFinalizedBranch(ctx context.Context, dir, branch string) (bool, error) {
	branch = strings.TrimSpace(branch)
	if branch == "" {
		return false, errors.New("finalize: lease has no branch to push")
	}
	local, err := m.runner.Run(ctx, dir, "rev-parse", "HEAD")
	if err != nil {
		return false, fmt.Errorf("finalize: read local head: %w", err)
	}
	remote, rerr := m.runner.Run(ctx, dir, "rev-parse", "--verify", "--quiet", "refs/remotes/origin/"+branch)
	if rerr == nil && strings.TrimSpace(remote) == strings.TrimSpace(local) {
		return false, nil
	}
	if _, err := m.runner.Run(ctx, dir, "push", "origin", "HEAD:refs/heads/"+branch); err != nil {
		return false, fmt.Errorf("finalize: push %s: %w", branch, err)
	}
	slog.Info("webhook: workspace branch pushed", "workspace", dir, "branch", branch)
	return true, nil
}

func countPorcelainEntries(out string) int {
	count := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}
