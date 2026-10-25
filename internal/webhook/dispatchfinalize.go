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
	DispatchBlocked        DispatchStatus = "blocked"
)

const dispatchPRBodyBrief = 1500

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
	Workdir   string
	StartedAt time.Time
	Active    bool
	Terminal  bool
	Retries   int
	Class     failureClass
}

func (s *Server) DispatchRunStarted(repo, branch, taskID, sessionID, workdir string) {
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
		Workdir:   strings.TrimSpace(workdir),
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
	finishCtx, cancel := context.WithTimeout(ctx, harnessFinalizeTimeout+harnessFinalizeTimeout)
	defer cancel()
	result, err := s.finalizeDispatch(finishCtx, report, false)
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
	s.logDispatchFinalized(report, result)
	if result.Status == DispatchCompleted {
		s.postDispatchOutcome(ctx, report, result)
		s.noteDispatchRun(report.Repo, report.Branch, report.TaskID)
		return
	}
	if s.scheduleDispatchRetry(ctx, report, result) {
		return
	}
	s.noteDispatchRun(report.Repo, report.Branch, report.TaskID)
}

func (s *Server) logDispatchFinalized(report DispatchReport, result DispatchResult) {
	slog.Info("webhook: dispatch run finalized",
		"repo", report.Repo,
		"branch", report.Branch,
		"task_id", report.TaskID,
		"status", result.Status,
		"committed", result.Committed,
		"pushed", result.Pushed,
		"pr", result.PRNumber,
		"denied", result.Denied,
	)
}

// postDispatchOutcome applies a durable artifact result to the thread and
// status surfaces without touching the repository again.
func (s *Server) postDispatchOutcome(ctx context.Context, report DispatchReport, result DispatchResult) {
	task, ok := s.dispatchTask(report)
	if !ok {
		return
	}
	_, ep := s.dispatchRepoEndpoint(report.Repo)
	s.finishDispatch(ctx, report, task, ep, result, true)
}

func dispatchFailureClass(status DispatchStatus) (failureClass, bool) {
	switch status {
	case DispatchNoWork:
		return failureNoWork, true
	case DispatchPushFailed:
		return failurePush, true
	case DispatchPRCreateFailed:
		return failurePRCreate, true
	case DispatchPermissionDeny:
		return failurePermission, true
	default:
		return failureModel, false
	}
}

// scheduleDispatchRetry spends the class retry budget on the artifact step.
// The agent turn is not replayed: commit, push and PR creation are
// idempotent, so a transient failure recovers without a second run. When the
// budget is exhausted the run is marked blocked and the operator is told.
func (s *Server) scheduleDispatchRetry(ctx context.Context, report DispatchReport, result DispatchResult) bool {
	class, ok := dispatchFailureClass(result.Status)
	if !ok {
		return false
	}
	key := TaskThreadKey(report.Repo, report.Branch)
	s.dispatchRunsMu.Lock()
	state := s.dispatchRuns[key]
	retries := state.Retries
	max, backoffs := s.retryPolicyFor(class)
	if retries >= max {
		s.dispatchRunsMu.Unlock()
		s.postDispatchBlocked(ctx, report, result, class, retries, max)
		return false
	}
	if s.dispatchRuns == nil {
		s.dispatchRuns = make(map[string]dispatchRunState)
	}
	state.Retries = retries + 1
	state.Active = true
	state.Terminal = false
	state.Class = class
	s.dispatchRuns[key] = state
	s.dispatchRunsMu.Unlock()

	wait := backoffs[0]
	if retries < len(backoffs) {
		wait = backoffs[retries]
	}
	slog.Info("webhook: dispatch finalize retry scheduled",
		"repo", report.Repo,
		"branch", report.Branch,
		"task_id", report.TaskID,
		"class", class.label(),
		"retry", fmt.Sprintf("%d/%d", retries+1, max),
	)
	go func() {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		if s.shutdownCtx != nil {
			select {
			case <-timer.C:
			case <-s.shutdownCtx.Done():
				return
			}
		} else {
			<-timer.C
		}
		retryCtx, cancel := context.WithTimeout(context.Background(), harnessFinalizeTimeout+harnessFinalizeTimeout)
		defer cancel()
		res, err := s.finalizeDispatch(retryCtx, report, false)
		if err != nil {
			slog.Error("webhook: dispatch finalize retry failed",
				"repo", report.Repo,
				"branch", report.Branch,
				"task_id", report.TaskID,
				"error", err,
			)
			s.postDispatchFinalizeError(context.Background(), report, err)
			s.noteDispatchRun(report.Repo, report.Branch, report.TaskID)
			return
		}
		s.logDispatchFinalized(report, res)
		if res.Status == DispatchCompleted {
			s.postDispatchOutcome(context.Background(), report, res)
			s.noteDispatchRun(report.Repo, report.Branch, report.TaskID)
			return
		}
		if s.scheduleDispatchRetry(context.Background(), report, res) {
			return
		}
		s.noteDispatchRun(report.Repo, report.Branch, report.TaskID)
	}()
	return true
}

func (s *Server) postDispatchBlocked(ctx context.Context, report DispatchReport, result DispatchResult, class failureClass, retries, max int) {
	detail := strings.TrimSpace(result.Detail)
	result.Status = DispatchBlocked
	result.Detail = fmt.Sprintf("retry budget exhausted after %d/%d retries (%s)", retries, max, class.label())
	if detail != "" {
		result.Detail += ": " + clipTaskField(detail, 230)
	}
	task, ok := s.dispatchTask(report)
	if !ok {
		return
	}
	_, ep := s.dispatchRepoEndpoint(report.Repo)
	s.finishDispatch(ctx, report, task, ep, result, true)
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

func (s *Server) dispatchRunState(repo, branch, taskID string) (dispatchRunState, bool) {
	key := TaskThreadKey(repo, branch)
	s.dispatchRunsMu.Lock()
	state, ok := s.dispatchRuns[key]
	s.dispatchRunsMu.Unlock()
	if !ok {
		return dispatchRunState{}, false
	}
	if id := strings.TrimSpace(taskID); id != "" && state.TaskID != id {
		return dispatchRunState{}, false
	}
	return state, true
}

func (s *Server) dispatchRunLive(state dispatchRunState) bool {
	if !state.Active || strings.TrimSpace(state.SessionID) == "" {
		return false
	}
	if s.sessionLiveness != nil {
		alive, err := s.sessionLiveness(context.Background(), state.SessionID, state.Workdir)
		if err == nil {
			return alive
		}
	}
	ttl := s.processingTimeout
	if ttl <= 0 {
		ttl = s.claimGrace
	}
	return ttl <= 0 || s.dispatchNow().Sub(state.StartedAt) < ttl
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

// finalizeDispatch turns a finished dispatch turn into its artifact. The turn
// outcome is not trusted: commit, push and PR presence decide the status.
func (s *Server) finalizeDispatch(ctx context.Context, report DispatchReport, notify bool) (DispatchResult, error) {
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
		s.finishDispatch(ctx, report, task, ep, result, notify)
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
		s.finishDispatch(ctx, report, task, ep, result, notify)
		return result, nil
	}

	head, herr := manager.DispatchHead(ctx, lease)
	if herr != nil {
		result.Status = DispatchPushFailed
		result.Detail = herr.Error()
		s.finishDispatch(ctx, report, task, ep, result, notify)
		return result, nil
	}
	if start == "" || head == start {
		result.Status = DispatchNoWork
		if report.Denied {
			result.Status = DispatchPermissionDeny
		}
		s.finishDispatch(ctx, report, task, ep, result, notify)
		return result, nil
	}

	base := s.dispatchBaseBranch(ctx, manager, lease.Path)
	title := dispatchPRTitle(task, report.Branch)
	body := dispatchPRBody(task)
	number, url, perr := s.openDispatchPR(ctx, lease, task.Repo, report.Branch, base, title, body)
	if perr != nil {
		result.Status = DispatchPRCreateFailed
		result.Detail = perr.Error()
		s.finishDispatch(ctx, report, task, ep, result, notify)
		return result, nil
	}
	result.PRNumber = number
	result.PRURL = url
	result.Status = DispatchCompleted
	s.finishDispatch(ctx, report, task, ep, result, notify)
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

func (s *Server) finishDispatch(ctx context.Context, report DispatchReport, task DispatchTask, ep config.EndpointConfig, result DispatchResult, notify bool) {
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
	if notify && threadID != "" {
		platform, channelID := ep.Platform, ep.ChannelID
		if platform == "" || channelID == "" {
			platform, channelID = s.dispatchNotifyTarget(repo)
		}
		if platform != "" && channelID != "" {
			phase := PhaseFailed
			action := "dispatch failed — " + string(result.Status)
			prKey := ""
			if result.Status == DispatchCompleted {
				phase = PhaseReview
				action = "PR #" + result.PRNumber + " open — queued for review"
				prKey = dispatchPRKey(repo, result.PRNumber)
			}
			s.ensureThreadStatus(ctx, platform, channelID, threadID, repo, branch, prKey, ThreadStatusUpdate{Phase: phase, Action: action})
			target := threadTargetFor(platform, channelID, threadID)
			if err := s.notifyThread(ctx, platform, target, FormatDispatchFinding(repo, branch, result)); err != nil {
				slog.Warn("webhook: dispatch finding post failed", "repo", repo, "branch", branch, "error", err)
			}
		}
	}
}

func dispatchPRKey(repo, number string) string {
	repo = normalizeRepoKey(repo)
	number = strings.TrimSpace(number)
	if repo == "" || number == "" {
		return ""
	}
	return repo + "#" + number
}
