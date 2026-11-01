package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"

	"github.com/anggasct/occa/internal/config"
)

// mergeFacts is the live PR state the code merge gate decides on.
type mergeFacts struct {
	State          string
	Draft          bool
	HeadSHA        string
	MergeState     string
	ReviewDecision string
	// HeadVerdict is the latest verdict per reviewer on the head commit,
	// trusted self-reviews included (the reconcile rule).
	HeadVerdict string
	// Checks is none, pending, red or green.
	Checks string
}

const (
	checkStateNone    = "none"
	checkStatePending = "pending"
	checkStateRed     = "red"
	checkStateGreen   = "green"
)

type mergeFactsFunc func(ctx context.Context, repo, number string) (mergeFacts, error)
type prMergeFunc func(ctx context.Context, repo, number, method, head string, deleteBranch bool) error
type mergeMethodsFunc func(ctx context.Context, repo string) ([]string, error)

// codeMergeFor reports whether occa merges this endpoint's PRs in code.
func codeMergeFor(ep config.EndpointConfig) bool {
	return ep.Merge.Code && strings.EqualFold(strings.TrimSpace(ep.Workflow), "merge")
}

// runCodeMerge is the merge lane without an agent turn. It returns a skip
// reason when a gate refuses, nil after a merge, or an error when GitHub
// could not be read or the merge call failed. A pending suite is a refusal:
// the check_suite completed event wakes the lane again, so nothing polls.
func (s *Server) runCodeMerge(ctx context.Context, ep config.EndpointConfig, workCtx *WebhookWorkContext) (string, error) {
	repo, number, ok := splitPRKey(workCtx.PRKey)
	if !ok {
		return "", fmt.Errorf("code merge: malformed pr key %q", workCtx.PRKey)
	}
	resolve := s.mergeFacts
	if resolve == nil {
		trust := s.policy.trustLogins
		resolve = func(ctx context.Context, repo, number string) (mergeFacts, error) {
			return ghMergeFacts(ctx, repo, number, trust)
		}
	}
	facts, err := resolve(ctx, repo, number)
	if err != nil {
		return "", fmt.Errorf("code merge: read PR: %w", err)
	}
	if reason := mergeGateRefusal(facts); reason != "" {
		slog.Info("webhook: code merge refused", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "reason", reason, "head", shortRevision(facts.HeadSHA))
		return reason, nil
	}
	method := ep.Merge.Method
	if method == "" {
		method, err = s.repoMergeMethod(ctx, repo)
		if err != nil {
			return "", fmt.Errorf("code merge: read repository merge methods: %w", err)
		}
	}
	merge := s.prMerger
	if merge == nil {
		merge = ghMergePR
	}
	if err := merge(ctx, repo, number, method, facts.HeadSHA, ep.Merge.DeleteBranch); err != nil {
		return "", fmt.Errorf("code merge: %w", err)
	}
	workCtx.AgentReply = fmt.Sprintf("merged by occa (%s) at %s", method, shortRevision(facts.HeadSHA))
	slog.Info("webhook: code merge done", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "method", method, "head", shortRevision(facts.HeadSHA))
	return "", nil
}

// mergeGateRefusal names the first gate a PR fails, or "" when it may merge.
func mergeGateRefusal(f mergeFacts) string {
	if state := strings.ToUpper(strings.TrimSpace(f.State)); state != "OPEN" {
		return "PR is " + strings.ToLower(state)
	}
	if f.Draft {
		return "PR is a draft"
	}
	if f.HeadVerdict != "approved" && !strings.EqualFold(strings.TrimSpace(f.ReviewDecision), "APPROVED") {
		return "no approval on the current head"
	}
	switch f.Checks {
	case checkStatePending:
		return "checks still running"
	case checkStateRed:
		return "checks failing"
	}
	switch strings.ToUpper(strings.TrimSpace(f.MergeState)) {
	case "CLEAN", "UNSTABLE", "HAS_HOOKS":
		return ""
	case "DIRTY":
		return "conflicts with the base branch"
	case "BEHIND":
		return "branch is behind the base branch"
	case "BLOCKED":
		return "blocked by branch protection"
	}
	return "mergeability not known yet"
}

// checksState folds a status check rollup: any red wins, then any pending.
func checksState(checks []ghCheckContext) string {
	if len(checks) == 0 {
		return checkStateNone
	}
	pending := false
	for _, check := range checks {
		if strings.EqualFold(strings.TrimSpace(check.TypeName), "StatusContext") {
			switch strings.ToUpper(strings.TrimSpace(check.State)) {
			case "SUCCESS":
			case "PENDING", "EXPECTED", "":
				pending = true
			default:
				return checkStateRed
			}
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(check.Status), "COMPLETED") {
			pending = true
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(check.Conclusion)) {
		case "SUCCESS", "NEUTRAL", "SKIPPED":
		default:
			return checkStateRed
		}
	}
	if pending {
		return checkStatePending
	}
	return checkStateGreen
}

// pickMergeMethod follows GitHub's own default order among allowed methods.
func pickMergeMethod(allowed map[string]bool) string {
	for _, method := range []string{"merge", "squash", "rebase"} {
		if allowed[method] {
			return method
		}
	}
	return ""
}

func (s *Server) repoMergeMethod(ctx context.Context, repo string) (string, error) {
	lookup := s.mergeMethods
	if lookup == nil {
		lookup = ghRepoMergeMethods
	}
	methods, err := lookup(ctx, repo)
	if err != nil {
		return "", err
	}
	allowed := map[string]bool{}
	for _, m := range methods {
		allowed[m] = true
	}
	method := pickMergeMethod(allowed)
	if method == "" {
		return "", fmt.Errorf("repository %s allows no merge method", repo)
	}
	return method, nil
}

func runGH(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "gh", args...)
	// Outside any checkout: gh pr merge --delete-branch would otherwise
	// switch the branch of the repository it runs in.
	cmd.Dir = os.TempDir()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		// stdout is kept: some gh commands (pr checks) exit non-zero with a
		// valid result.
		return stdout.Bytes(), fmt.Errorf("gh %s: %s", args[0], detail)
	}
	return stdout.Bytes(), nil
}

func ghMergeFacts(ctx context.Context, repo, number string, trustLogins map[string]bool) (mergeFacts, error) {
	out, err := runGH(ctx, "pr", "view", strings.TrimSpace(number), "--repo", strings.TrimSpace(repo),
		"--json", "state,isDraft,headRefOid,mergeStateStatus,reviewDecision,reviews,statusCheckRollup,author")
	if err != nil {
		return mergeFacts{}, err
	}
	var raw struct {
		State             string              `json:"state"`
		IsDraft           bool                `json:"isDraft"`
		HeadRefOid        string              `json:"headRefOid"`
		MergeStateStatus  string              `json:"mergeStateStatus"`
		ReviewDecision    string              `json:"reviewDecision"`
		Reviews           []ghReconcileReview `json:"reviews"`
		StatusCheckRollup []ghCheckContext    `json:"statusCheckRollup"`
		Author            struct {
			Login string `json:"login"`
		} `json:"author"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return mergeFacts{}, fmt.Errorf("gh pr view decode: %w", err)
	}
	return mergeFacts{
		State:          raw.State,
		Draft:          raw.IsDraft,
		HeadSHA:        normalizeRevision(raw.HeadRefOid),
		MergeState:     raw.MergeStateStatus,
		ReviewDecision: raw.ReviewDecision,
		HeadVerdict:    ghReviewVerdictForHead(raw.Reviews, raw.HeadRefOid, raw.Author.Login, trustLogins),
		Checks:         checksState(raw.StatusCheckRollup),
	}, nil
}

func ghRepoMergeMethods(ctx context.Context, repo string) ([]string, error) {
	out, err := runGH(ctx, "api", "repos/"+strings.TrimSpace(repo))
	if err != nil {
		return nil, err
	}
	var raw struct {
		Merge  bool `json:"allow_merge_commit"`
		Squash bool `json:"allow_squash_merge"`
		Rebase bool `json:"allow_rebase_merge"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, fmt.Errorf("gh api repo decode: %w", err)
	}
	var methods []string
	if raw.Merge {
		methods = append(methods, "merge")
	}
	if raw.Squash {
		methods = append(methods, "squash")
	}
	if raw.Rebase {
		methods = append(methods, "rebase")
	}
	return methods, nil
}

func ghMergePR(ctx context.Context, repo, number, method, head string, deleteBranch bool) error {
	args := []string{"pr", "merge", strings.TrimSpace(number), "--repo", strings.TrimSpace(repo), "--" + method}
	if head = strings.TrimSpace(head); head != "" {
		args = append(args, "--match-head-commit", head)
	}
	if deleteBranch {
		args = append(args, "--delete-branch")
	}
	_, err := runGH(ctx, args...)
	return err
}
