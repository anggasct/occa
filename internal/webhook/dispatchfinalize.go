package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
)

type DispatchStatus string

const (
	DispatchCompleted      DispatchStatus = "completed"
	DispatchNoWork         DispatchStatus = "no work in lease"
	DispatchPushFailed     DispatchStatus = "push failed"
	DispatchPRCreateFailed DispatchStatus = "pr create failed"
	DispatchPermissionDeny DispatchStatus = "permission denied"
)

const (
	dispatchRunActiveTTL = 30 * time.Minute
	dispatchPRBodyBrief  = 1500
)

var errDispatchPRCreate = errors.New("dispatch: pr create failed")

type DispatchReport struct {
	Repo      string
	Branch    string
	TaskID    string
	SessionID string
	Worktree  string
	Outcome   string
	Denied    bool
}

type DispatchResult struct {
	Status    DispatchStatus
	Committed bool
	Pushed    bool
	Files     int
	PRNumber  string
	PRURL     string
	Worktree  string
	Detail    string
	Denied    bool
}

func (r DispatchResult) Retryable() bool {
	return r.Status != DispatchCompleted
}

type ghRunner interface {
	Run(ctx context.Context, dir string, args ...string) (string, error)
}

type defaultGHRunner struct{}

func (d *defaultGHRunner) Run(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "gh", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	return string(out), err
}

type dispatchRunState struct {
	TaskID    string
	SessionID string
	StartedAt time.Time
	Active    bool
	Terminal  bool
}

func (s *Server) DispatchRunStarted(repo, branch, taskID, sessionID string) {
	key := TaskThreadKey(repo, branch)
	if key == "#" {
		return
	}
	s.dispatchRunsMu.Lock()
	defer s.dispatchRunsMu.Unlock()
	if s.dispatchRuns == nil {
		s.dispatchRuns = make(map[string]dispatchRunState)
	}
	s.dispatchRuns[key] = dispatchRunState{
		TaskID:    strings.TrimSpace(taskID),
		SessionID: strings.TrimSpace(sessionID),
		StartedAt: s.dispatchNow(),
		Active:    true,
	}
}

func (s *Server) DispatchRunFinished(ctx context.Context, repo, branch, taskID, sessionID, outcome string, denied bool) {
	report := DispatchReport{
		Repo:      repo,
		Branch:    branch,
		TaskID:    taskID,
		SessionID: sessionID,
		Outcome:   outcome,
		Denied:    denied,
	}
	result, err := s.FinalizeDispatch(ctx, report)
	if err != nil {
		slog.Error("webhook: dispatch finalize failed",
			"repo", repo,
			"branch", branch,
			"task_id", taskID,
			"error", err,
		)
		s.postDispatchFinalizeError(ctx, report, err)
		return
	}
	slog.Info("webhook: dispatch run finalized",
		"repo", repo,
		"branch", branch,
		"task_id", taskID,
		"status", result.Status,
		"committed", result.Committed,
		"pushed", result.Pushed,
		"pr", result.PRNumber,
		"denied", result.Denied,
	)
}

func (s *Server) postDispatchFinalizeError(ctx context.Context, report DispatchReport, err error) {
	if s.tasks == nil {
		return
	}
	repo := normalizeRepoKey(report.Repo)
	rec, ok := s.tasks.Lookup(repo, report.Branch)
	if !ok || strings.TrimSpace(rec.ThreadID) == "" {
		return
	}
	_, ep := s.dispatchRepoEndpoint(repo)
	platform, channelID := ep.Platform, ep.ChannelID
	if platform == "" || channelID == "" {
		platform, channelID = s.dispatchNotifyTarget(repo)
	}
	if platform == "" || channelID == "" {
		return
	}
	text := "📨 Dispatch run could not be finalized for " + repo + "#" + strings.TrimSpace(report.Branch) + " — " + clipTaskField(err.Error(), 300)
	target := threadTargetFor(platform, channelID, rec.ThreadID)
	if nerr := s.notifyThread(ctx, platform, target, text); nerr != nil {
		slog.Warn("webhook: dispatch finalize error post failed", "repo", repo, "branch", report.Branch, "error", nerr)
	}
}

func (s *Server) dispatchNow() time.Time {
	if s.fallbackNow != nil {
		return s.fallbackNow()
	}
	return time.Now()
}

func (s *Server) dispatchFallbackBlocked(repo, branch, taskID string) bool {
	key := TaskThreadKey(repo, branch)
	s.dispatchRunsMu.Lock()
	state, ok := s.dispatchRuns[key]
	s.dispatchRunsMu.Unlock()
	if !ok {
		return false
	}
	if id := strings.TrimSpace(taskID); id != "" && state.TaskID != id {
		return false
	}
	if state.Active && s.dispatchNow().Sub(state.StartedAt) < dispatchRunActiveTTL {
		return true
	}
	return state.Terminal
}

func (s *Server) noteDispatchRun(repo, branch, taskID string) {
	key := TaskThreadKey(repo, branch)
	s.dispatchRunsMu.Lock()
	defer s.dispatchRunsMu.Unlock()
	if s.dispatchRuns == nil {
		s.dispatchRuns = make(map[string]dispatchRunState)
	}
	state := s.dispatchRuns[key]
	if taskID != "" {
		state.TaskID = strings.TrimSpace(taskID)
	}
	state.Active = false
	state.Terminal = true
	s.dispatchRuns[key] = state
}

// FinalizeDispatch turns a finished dispatch turn into its artifact. The turn
// outcome is not trusted: commit, push and PR presence decide the status.
func (s *Server) FinalizeDispatch(ctx context.Context, report DispatchReport) (DispatchResult, error) {
	var result DispatchResult
	report.Repo = normalizeRepoKey(report.Repo)
	report.Branch = strings.TrimSpace(report.Branch)
	if report.Repo == "" || report.Branch == "" {
		return result, errors.New("dispatch: repo and branch are required")
	}

	task, ok := s.dispatchTask(report)
	if !ok {
		return result, fmt.Errorf("dispatch: task %q not found", report.TaskID)
	}
	root, ep := s.dispatchRepoEndpoint(report.Repo)
	if root == "" {
		return result, fmt.Errorf("dispatch: no checkout configured for %s", report.Repo)
	}
	manager := s.dispatchWorkspaceManager()
	lease, err := manager.DispatchLease(ctx, root, report.Branch, report.Worktree)
	if err != nil {
		result.Status = DispatchNoWork
		if report.Denied {
			result.Status = DispatchPermissionDeny
		}
		result.Detail = err.Error()
		result.Denied = report.Denied
		s.finishDispatch(ctx, report, task, ep, result)
		return result, nil
	}
	result.Worktree = lease.Path
	start := strings.TrimSpace(task.BaseSHA)
	if start == "" {
		base := s.dispatchBaseBranch(ctx, manager, lease.Path)
		start, _ = manager.MergeBase(ctx, lease.Path, "refs/remotes/origin/"+base)
		if start == "" {
			if out, err := manager.runner.Run(ctx, lease.Path, "rev-parse", "--verify", "--quiet", "refs/remotes/origin/"+report.Branch); err == nil {
				start = strings.TrimSpace(out)
			}
		}
		if start == "" {
			start, _ = manager.DispatchHead(ctx, lease)
		}
	}
	lease.StartRevision = start

	fin, ferr := lease.Finalize(ctx, "dispatch", task.ID)
	result.Committed = fin.Committed
	result.Pushed = fin.Pushed
	result.Files = fin.Files
	result.Denied = report.Denied
	if ferr != nil {
		switch {
		case errors.Is(ferr, errNoWorkInLease):
			result.Status = DispatchNoWork
			if report.Denied {
				result.Status = DispatchPermissionDeny
			}
		default:
			result.Status = DispatchPushFailed
		}
		result.Detail = ferr.Error()
		s.finishDispatch(ctx, report, task, ep, result)
		return result, nil
	}

	head, herr := manager.DispatchHead(ctx, lease)
	if herr != nil {
		result.Status = DispatchPushFailed
		result.Detail = herr.Error()
		s.finishDispatch(ctx, report, task, ep, result)
		return result, nil
	}
	if start == "" || head == start {
		result.Status = DispatchNoWork
		if report.Denied {
			result.Status = DispatchPermissionDeny
		}
		s.finishDispatch(ctx, report, task, ep, result)
		return result, nil
	}

	base := s.dispatchBaseBranch(ctx, manager, lease.Path)
	title := dispatchPRTitle(task, report.Branch)
	body := dispatchPRBody(task)
	number, url, perr := s.openDispatchPR(ctx, lease, task.Repo, report.Branch, base, title, body)
	if perr != nil {
		result.Status = DispatchPRCreateFailed
		result.Detail = perr.Error()
		s.finishDispatch(ctx, report, task, ep, result)
		return result, nil
	}
	result.PRNumber = number
	result.PRURL = url
	result.Status = DispatchCompleted
	s.finishDispatch(ctx, report, task, ep, result)
	return result, nil
}

func (s *Server) dispatchTask(report DispatchReport) (DispatchTask, bool) {
	if s.dispatchTasks != nil && strings.TrimSpace(report.TaskID) != "" {
		if task, ok := s.dispatchTasks.Get(report.TaskID); ok {
			if task.Repo == "" {
				task.Repo = report.Repo
			}
			if task.Branch == "" {
				task.Branch = report.Branch
			}
			return task, true
		}
	}
	if s.dispatchTasks != nil {
		if task, ok := s.dispatchTasks.LatestFor(report.Repo, report.Branch); ok {
			return task, true
		}
	}
	if strings.TrimSpace(report.TaskID) == "" {
		return DispatchTask{}, false
	}
	return DispatchTask{ID: report.TaskID, Repo: report.Repo, Branch: report.Branch}, true
}

func (s *Server) dispatchRepoEndpoint(repo string) (string, config.EndpointConfig) {
	want := normalizeRepoKey(repo)
	var first config.EndpointConfig
	found := false
	for _, ep := range s.endpoints {
		if normalizeRepoKey(ep.Repository) != want || want == "" {
			continue
		}
		if !found {
			first = ep
			found = true
		}
		if path := strings.TrimSpace(ep.Workspace.Path); path != "" {
			return path, ep
		}
	}
	if found {
		return strings.TrimSpace(first.Workspace.Path), first
	}
	return "", config.EndpointConfig{}
}

func (s *Server) dispatchWorkspaceManager() *WorkspaceManager {
	if m, ok := s.workspaceResolver.(*WorkspaceManager); ok && m != nil {
		return m
	}
	return NewWorkspaceManager()
}

func (s *Server) dispatchBaseBranch(ctx context.Context, manager *WorkspaceManager, dir string) string {
	if manager != nil {
		if out, err := manager.runner.Run(ctx, dir, "symbolic-ref", "--short", "refs/remotes/origin/HEAD"); err == nil {
			ref := strings.TrimSpace(out)
			if base, ok := strings.CutPrefix(ref, "origin/"); ok && strings.TrimSpace(base) != "" {
				return strings.TrimSpace(base)
			}
		}
	}
	return "main"
}

func dispatchPRTitle(task DispatchTask, branch string) string {
	title := strings.TrimSpace(strings.SplitN(task.Brief, "\n", 2)[0])
	if title == "" {
		title = "Dispatch " + strings.TrimSpace(branch)
	}
	return clipTaskField(title, 72)
}

func dispatchPRBody(task DispatchTask) string {
	var b strings.Builder
	if spec := strings.TrimSpace(task.SpecPath); spec != "" {
		b.WriteString("Spec: ")
		b.WriteString(spec)
		b.WriteString("\n\n")
	}
	if brief := strings.TrimSpace(task.Brief); brief != "" {
		b.WriteString("Brief:\n")
		b.WriteString(clipTaskField(brief, dispatchPRBodyBrief))
		b.WriteString("\n\n")
	}
	if verify := strings.TrimSpace(task.Verify); verify != "" {
		b.WriteString("Verification:\n")
		b.WriteString(verify)
		b.WriteString("\n")
	}
	return strings.TrimSpace(b.String())
}

func (s *Server) openDispatchPR(ctx context.Context, lease *WorkspaceLease, repo, branch, base, title, body string) (string, string, error) {
	runner := s.gh
	if runner == nil {
		runner = &defaultGHRunner{}
	}
	dir := ""
	if lease != nil {
		dir = lease.Path
	}
	listOut, listErr := runner.Run(ctx, dir,
		"pr", "list",
		"--repo", repo,
		"--head", branch,
		"--state", "open",
		"--json", "number,url",
		"--limit", "1",
	)
	if listErr != nil {
		return "", "", fmt.Errorf("%w: list: %s", errDispatchPRCreate, strings.TrimSpace(listOut))
	}
	if number, url, ok := parseDispatchPRList(listOut); ok {
		return number, url, nil
	}

	args := []string{
		"pr", "create",
		"--repo", repo,
		"--base", base,
		"--head", branch,
		"--title", title,
	}
	if strings.TrimSpace(body) != "" {
		args = append(args, "--body", body)
	}
	createOut, createErr := runner.Run(ctx, dir, args...)
	if createErr != nil {
		return "", "", fmt.Errorf("%w: %s", errDispatchPRCreate, strings.TrimSpace(createOut))
	}
	url := lastNonEmptyLine(createOut)
	if url == "" {
		return "", "", fmt.Errorf("%w: no pr url in output", errDispatchPRCreate)
	}
	number := prNumberFromURL(url)
	if number == "" {
		return "", "", fmt.Errorf("%w: no pr number in %s", errDispatchPRCreate, url)
	}
	return number, url, nil
}

func parseDispatchPRList(out string) (string, string, bool) {
	var rows []struct {
		Number int    `json:"number"`
		URL    string `json:"url"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &rows); err != nil || len(rows) == 0 {
		return "", "", false
	}
	if rows[0].Number <= 0 {
		return "", "", false
	}
	return strconv.Itoa(rows[0].Number), strings.TrimSpace(rows[0].URL), true
}

func prNumberFromURL(raw string) string {
	raw = strings.TrimSpace(raw)
	idx := strings.LastIndex(raw, "/pull/")
	if idx < 0 {
		return ""
	}
	rest := raw[idx+len("/pull/"):]
	if end := strings.IndexAny(rest, "/?# "); end >= 0 {
		rest = rest[:end]
	}
	if _, err := strconv.Atoi(rest); err != nil {
		return ""
	}
	return rest
}

func lastNonEmptyLine(out string) string {
	lines := strings.Split(out, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if line := strings.TrimSpace(lines[i]); line != "" {
			return line
		}
	}
	return ""
}

func FormatDispatchFinding(repo, branch string, result DispatchResult) string {
	head := normalizeRepoKey(repo) + "#" + strings.TrimSpace(branch)
	if result.Status == DispatchCompleted {
		line := "✅ Dispatch run has PR #" + result.PRNumber + " for " + head + "."
		if result.PRURL != "" {
			line += " " + result.PRURL
		}
		if result.Denied {
			line += " A tool permission was denied during the run; the artifact was already complete."
		}
		return line
	}
	line := "📨 Dispatch run ended without a PR for " + head + " — " + string(result.Status) + "."
	if detail := strings.TrimSpace(result.Detail); detail != "" {
		line += " " + clipTaskField(detail, 300)
	}
	if result.Denied && result.Status != DispatchPermissionDeny {
		line += " A tool permission was denied during the run."
	}
	return line
}

func (s *Server) finishDispatch(ctx context.Context, report DispatchReport, task DispatchTask, ep config.EndpointConfig, result DispatchResult) {
	repo := normalizeRepoKey(report.Repo)
	if repo == "" {
		repo = task.Repo
	}
	branch := strings.TrimSpace(report.Branch)
	if branch == "" {
		branch = task.Branch
	}
	threadID, rootID := "", ""
	if s.tasks != nil {
		rec, ok := s.tasks.Lookup(repo, branch)
		if ok {
			threadID = strings.TrimSpace(rec.ThreadID)
			rootID = strings.TrimSpace(rec.RootMessageID)
		}
		if result.Status == DispatchCompleted {
			rec = s.tasks.UpsertWithSpec(repo, branch, rootID, threadID, TaskPROpened, task.SpecPath)
			if prKey := dispatchPRKey(repo, result.PRNumber); prKey != "" {
				rec = s.tasks.SetSurface(prKey, SurfaceUpdate{
					Repo:          repo,
					Branch:        branch,
					PRNumber:      result.PRNumber,
					PRURL:         result.PRURL,
					ThreadID:      threadID,
					RootMessageID: rootID,
				})
				s.tasks.LinkPRThread(prKey, rec)
			}
		} else {
			s.tasks.UpsertWithSpec(repo, branch, rootID, threadID, TaskFailed, task.SpecPath)
		}
	}
	if result.Status == DispatchCompleted {
		if prKey := dispatchPRKey(repo, result.PRNumber); prKey != "" && s.prQueues != nil {
			if _, err := s.prQueues.EnsurePR(ctx, prKey, threadID, rootID); err != nil {
				slog.Warn("webhook: dispatch pr queue ensure failed", "pr_key", prKey, "error", err)
			}
			if err := s.prQueues.UpdatePRMeta(ctx, prKey, branch, result.PRURL); err != nil {
				slog.Warn("webhook: dispatch pr queue meta failed", "pr_key", prKey, "error", err)
			}
			if threadID != "" || rootID != "" {
				if err := s.prQueues.UpdateThread(ctx, prKey, threadID, rootID); err != nil {
					slog.Warn("webhook: dispatch pr queue thread failed", "pr_key", prKey, "error", err)
				}
			}
		}
	}
	if threadID != "" {
		platform, channelID := ep.Platform, ep.ChannelID
		if platform == "" || channelID == "" {
			platform, channelID = s.dispatchNotifyTarget(repo)
		}
		if platform != "" && channelID != "" {
			target := threadTargetFor(platform, channelID, threadID)
			if err := s.notifyThread(ctx, platform, target, FormatDispatchFinding(repo, branch, result)); err != nil {
				slog.Warn("webhook: dispatch finding post failed", "repo", repo, "branch", branch, "error", err)
			}
		}
	}
	s.noteDispatchRun(repo, branch, task.ID)
}

func dispatchPRKey(repo, number string) string {
	repo = normalizeRepoKey(repo)
	number = strings.TrimSpace(number)
	if repo == "" || number == "" {
		return ""
	}
	return repo + "#" + number
}
