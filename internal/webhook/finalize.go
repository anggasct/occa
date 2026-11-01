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
	if l.Mode != config.WorkspaceModeMutable || !isWritableLane(lane) {
		return result, nil
	}
	m := l.manager

	status, err := m.runner.Run(ctx, l.Path, "status", "--porcelain")
	if err != nil {
		return result, fmt.Errorf("finalize: read workspace status: %w", err)
	}
	result.Files = countPorcelainEntries(status)
	if result.Files > 0 {
		// A merge the run left unresolved must not be committed: staging
		// would mark the conflicted files resolved, markers and all.
		if unmerged, err := m.runner.Run(ctx, l.Path, "diff", "--name-only", "--diff-filter=U"); err == nil && strings.TrimSpace(unmerged) != "" {
			return result, fmt.Errorf("finalize: unresolved merge conflicts in %s", strings.Join(strings.Fields(unmerged), ", "))
		}
		if _, err := m.runner.Run(ctx, l.Path, "add", "-A"); err != nil {
			return result, fmt.Errorf("finalize: stage workspace changes: %w", err)
		}
		if staged, err := m.runner.Run(ctx, l.Path, "diff", "--cached", "-U0"); err == nil {
			if files := conflictMarkerFiles(staged); len(files) > 0 {
				return result, fmt.Errorf("finalize: unresolved merge conflicts in %s", strings.Join(files, ", "))
			}
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

func isWritableLane(lane string) bool {
	return isFixWorkflow(lane) || strings.EqualFold(strings.TrimSpace(lane), "dispatch")
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

// conflictMarkerFiles lists the files whose added lines carry a merge
// conflict marker, from a unified diff.
func conflictMarkerFiles(diff string) []string {
	var files []string
	seen := map[string]bool{}
	current := ""
	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, "+++ b/") {
			current = strings.TrimPrefix(line, "+++ b/")
			continue
		}
		if strings.HasPrefix(line, "+<<<<<<< ") || strings.HasPrefix(line, "+>>>>>>> ") {
			if current != "" && !seen[current] {
				seen[current] = true
				files = append(files, current)
			}
		}
	}
	return files
}
