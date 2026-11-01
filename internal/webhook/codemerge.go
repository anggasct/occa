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
	// ApprovedCommit is the commit of the newest approval when every
	// reviewer's latest verdict, on any commit, is an approval; it lets an
	// approval carry over a delta of stale_approval_paths files only.
	ApprovedCommit string
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
type compareFilesFunc func(ctx context.Context, repo, base, head string) ([]string, error)

const compareFilesCap = 300

const refusalNoHeadApproval = "no approval on the current head"
const refusalConflicts = "conflicts with the base branch"

// codeMergeFor reports whether this endpoint is a merge lane; occa merges
// every PR in code.
func codeMergeFor(ep config.EndpointConfig) bool {
	return strings.EqualFold(strings.TrimSpace(ep.Workflow), "merge")
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
	reason := mergeGateRefusal(facts)
	if reason == refusalNoHeadApproval && s.approvalCarriesOver(ctx, ep, repo, facts) {
		facts.HeadVerdict = "approved"
		reason = mergeGateRefusal(facts)
	}
	if reason != "" {
		slog.Info("webhook: code merge refused", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "reason", reason, "head", shortRevision(facts.HeadSHA))
		if reason == refusalConflicts && ep.AutoFix.Conflict {
			if err := s.enqueueAutoFixCI(ctx, repo, number, facts.HeadSHA, []string{fixKindConflict}); err != nil {
				slog.Info("webhook: conflict auto-fix not enqueued", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "error", err)
			}
		}
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

// approvalCarriesOver reports whether an approval on an older commit still
// holds because every file changed since matches stale_approval_paths.
func (s *Server) approvalCarriesOver(ctx context.Context, ep config.EndpointConfig, repo string, facts mergeFacts) bool {
	patterns := ep.Merge.StaleApprovalPaths
	approved := normalizeRevision(facts.ApprovedCommit)
	if len(patterns) == 0 || approved == "" || facts.HeadSHA == "" || approved == normalizeRevision(facts.HeadSHA) {
		return false
	}
	compare := s.compareFiles
	if compare == nil {
		compare = ghCompareFiles
	}
	files, err := compare(ctx, repo, approved, facts.HeadSHA)
	if err != nil {
		slog.Warn("webhook: code merge compare failed", "endpoint", ep.Name, "repo", repo, "approved", shortRevision(approved), "head", shortRevision(facts.HeadSHA), "error", err)
		return false
	}
	if !allFilesMatch(files, patterns) {
		return false
	}
	slog.Info("webhook: code merge approval carried over", "endpoint", ep.Name, "repo", repo, "approved", shortRevision(approved), "head", shortRevision(facts.HeadSHA), "files", len(files))
	return true
}

func allFilesMatch(files, patterns []string) bool {
	if len(files) == 0 {
		return false
	}
	for _, file := range files {
		matched := false
		for _, pattern := range patterns {
			if config.MatchesApprovalPath(pattern, file) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// carriedApprovalCommit returns the commit of the newest approval when every
// reviewer's latest verdict on the PR is an approval, else "".
func carriedApprovalCommit(reviews []ghReconcileReview, prAuthor string, trustLogins map[string]bool) string {
	type observed struct {
		verdict, at, commit string
	}
	latest := make(map[string]observed)
	for index, review := range reviews {
		verdict := reviewVerdict(review, prAuthor, trustLogins)
		if verdict == "" {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(review.Author.Login))
		if key == "" {
			key = fmt.Sprintf("review:%d", index)
		}
		submitted := strings.TrimSpace(review.SubmittedAt)
		if previous, ok := latest[key]; ok && previous.at > submitted {
			continue
		}
		latest[key] = observed{verdict: verdict, at: submitted, commit: normalizeRevision(review.Commit.OID)}
	}
	newest := observed{}
	for _, item := range latest {
		if item.verdict != "approved" {
			return ""
		}
		if item.at >= newest.at {
			newest = item
		}
	}
	return newest.commit
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
		return refusalNoHeadApproval
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
		return refusalConflicts
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
		ApprovedCommit: carriedApprovalCommit(raw.Reviews, raw.Author.Login, trustLogins),
	}, nil
}

// ghCompareFiles lists the files changed between two commits.
func ghCompareFiles(ctx context.Context, repo, base, head string) ([]string, error) {
	out, err := runGH(ctx, "api", "repos/"+strings.TrimSpace(repo)+"/compare/"+strings.TrimSpace(base)+"..."+strings.TrimSpace(head))
	if err != nil {
		return nil, err
	}
	var raw struct {
		Files []struct {
			Filename         string `json:"filename"`
			PreviousFilename string `json:"previous_filename"`
		} `json:"files"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, fmt.Errorf("gh api compare decode: %w", err)
	}
	if len(raw.Files) >= compareFilesCap {
		// GitHub lists at most 300 files; the rest of the delta is unseen.
		return nil, fmt.Errorf("compare lists %d files, the delta may be truncated", len(raw.Files))
	}
	files := make([]string, 0, len(raw.Files))
	for _, f := range raw.Files {
		files = append(files, f.Filename)
		if f.PreviousFilename != "" {
			files = append(files, f.PreviousFilename)
		}
	}
	return files, nil
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
