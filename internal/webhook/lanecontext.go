package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/anggasct/occa/internal/config"
)

const (
	priorReviewsMax      = 3
	priorReviewRunes     = 1500
	priorReviewsRunes    = 5000
	ciFailureLogLines    = 60
	ciFailuresRunes      = 6000
	conflictPrepReplyFmt = "merged origin/%s cleanly — no conflicts left"
)

type priorReviewsFunc func(ctx context.Context, repo, number string) (string, error)
type ciFailuresFunc func(ctx context.Context, repo, number string) (string, error)

// prepareLaneContext fills what the agent would otherwise fetch itself:
// earlier reviews for a review, failing check logs for a CI fix. A conflict
// fix gets the base merged by the harness first; skipAgent reports a merge
// that left nothing to resolve. Lookup failures leave the fields empty.
func (s *Server) prepareLaneContext(ctx context.Context, ep config.EndpointConfig, envelope WebhookEnvelope, workCtx *WebhookWorkContext, lease *WorkspaceLease) (skipAgent bool) {
	repo, number, ok := splitPRKey(workCtx.PRKey)
	if !ok {
		repo = strings.TrimSpace(stringValue(envelope["repository"]))
		number = strings.TrimSpace(stringValue(envelope["pr_number"]))
	}
	if repo == "" || number == "" {
		return false
	}
	switch {
	case isReviewWorkflow(ep.Workflow):
		fetch := s.priorReviews
		if fetch == nil {
			fetch = ghPriorReviews
		}
		text, err := fetch(ctx, repo, number)
		if err != nil {
			slog.Warn("webhook: prior reviews lookup failed", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "error", err)
		}
		envelope["prior_reviews"] = text
	case isFixWorkflow(ep.Workflow):
		kind := strings.TrimSpace(stringValue(envelope["fix_kind"]))
		if kind == fixKindCI || kind == fixKindMixed {
			fetch := s.ciFailures
			if fetch == nil {
				fetch = ghCIFailures
			}
			text, err := fetch(ctx, repo, number)
			if err != nil {
				slog.Warn("webhook: ci failure lookup failed", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "error", err)
			}
			envelope["ci_failures"] = text
		}
		if kind == fixKindConflict {
			return s.prepareConflict(ctx, ep, envelope, workCtx, lease)
		}
	}
	return false
}

// prepareConflict merges origin/<base> into the lease before the agent turn.
// A clean merge needs no agent; conflicts are listed for the agent to
// resolve; a failed fetch or merge is undone and left to the agent.
func (s *Server) prepareConflict(ctx context.Context, ep config.EndpointConfig, envelope WebhookEnvelope, workCtx *WebhookWorkContext, lease *WorkspaceLease) bool {
	base := strings.TrimSpace(stringValue(envelope["base_branch"]))
	if lease == nil || lease.manager == nil || lease.Mode != config.WorkspaceModeMutable || base == "" {
		return false
	}
	run := lease.manager.runner
	if _, err := run.Run(ctx, lease.Path, "fetch", "origin", base); err != nil {
		slog.Warn("webhook: conflict prep fetch failed", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "base", base, "error", err)
		return false
	}
	_, mergeErr := run.Run(ctx, lease.Path, "merge", "--no-edit", "origin/"+base)
	if mergeErr == nil {
		workCtx.AgentReply = fmt.Sprintf(conflictPrepReplyFmt, base)
		slog.Info("webhook: conflict prep merged cleanly", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "base", base)
		return true
	}
	out, err := run.Run(ctx, lease.Path, "diff", "--name-only", "--diff-filter=U")
	files := strings.Fields(out)
	if err != nil || len(files) == 0 {
		// Not a conflict: undo whatever the merge started and let the agent run.
		_, _ = run.Run(ctx, lease.Path, "merge", "--abort")
		slog.Warn("webhook: conflict prep merge failed", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "base", base, "error", mergeErr)
		return false
	}
	envelope["conflict_files"] = strings.Join(files, "\n")
	slog.Info("webhook: conflict prep left conflicts for the agent", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "base", base, "files", len(files))
	return false
}

func ghPriorReviews(ctx context.Context, repo, number string) (string, error) {
	out, err := runGH(ctx, "api", "repos/"+repo+"/pulls/"+number+"/reviews?per_page=100")
	if err != nil {
		return "", err
	}
	return formatPriorReviews(out)
}

// formatPriorReviews renders the last reviews that carry a body, oldest
// first, each clipped, the whole capped.
func formatPriorReviews(out []byte) (string, error) {
	var reviews []struct {
		State       string `json:"state"`
		Body        string `json:"body"`
		SubmittedAt string `json:"submitted_at"`
		CommitID    string `json:"commit_id"`
		User        struct {
			Login string `json:"login"`
		} `json:"user"`
	}
	if err := json.Unmarshal(out, &reviews); err != nil {
		return "", fmt.Errorf("reviews decode: %w", err)
	}
	type entry struct{ state, login, commit, at, body string }
	var withBody []entry
	for _, r := range reviews {
		if strings.TrimSpace(r.Body) == "" {
			continue
		}
		withBody = append(withBody, entry{r.State, r.User.Login, r.CommitID, r.SubmittedAt, r.Body})
	}
	if len(withBody) > priorReviewsMax {
		withBody = withBody[len(withBody)-priorReviewsMax:]
	}
	parts := make([]string, 0, len(withBody))
	for _, e := range withBody {
		date := e.at
		if len(date) >= 10 {
			date = date[:10]
		}
		parts = append(parts, fmt.Sprintf("— %s by %s on %s (%s)\n%s", e.state, e.login, shortRevision(e.commit), date, clipRunes(strings.TrimSpace(e.body), priorReviewRunes)))
	}
	return clipRunes(strings.Join(parts, "\n\n"), priorReviewsRunes), nil
}

var actionsRunRe = regexp.MustCompile(`/actions/runs/(\d+)`)

func ghCIFailures(ctx context.Context, repo, number string) (string, error) {
	out, err := runGH(ctx, "pr", "checks", number, "--repo", repo, "--json", "name,bucket,link")
	if err != nil && len(out) == 0 {
		// gh exits non-zero when checks fail; only an empty result is an error.
		return "", err
	}
	var checks []struct {
		Name   string `json:"name"`
		Bucket string `json:"bucket"`
		Link   string `json:"link"`
	}
	if err := json.Unmarshal(out, &checks); err != nil {
		return "", fmt.Errorf("checks decode: %w", err)
	}
	var parts []string
	seenRun := map[string]bool{}
	for _, c := range checks {
		if !strings.EqualFold(strings.TrimSpace(c.Bucket), "fail") {
			continue
		}
		part := "## " + strings.TrimSpace(c.Name)
		if m := actionsRunRe.FindStringSubmatch(c.Link); m != nil && !seenRun[m[1]] {
			seenRun[m[1]] = true
			if log, lerr := runGH(ctx, "run", "view", m[1], "--repo", repo, "--log-failed"); lerr == nil {
				part += "\n" + lastLines(string(log), ciFailureLogLines)
			}
		}
		parts = append(parts, part)
	}
	return clipRunes(strings.Join(parts, "\n\n"), ciFailuresRunes), nil
}

func lastLines(text string, n int) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
