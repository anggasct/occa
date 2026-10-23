package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

const mergedExtraTimeout = 5 * time.Minute

func (s *Server) checkoutForRepo(repo string) string {
	repo = normalizeRepoKey(repo)
	if repo == "" {
		return ""
	}
	for _, ep := range s.endpoints {
		if ep.Repository == "" || ep.Workspace.Type != config.WorkspaceTypeGit {
			continue
		}
		if normalizeRepoKey(ep.Repository) == repo {
			return strings.TrimSpace(ep.Workspace.Path)
		}
	}
	return ""
}

// handleMergedWorkflow admits a merged event and closes it without an agent
// turn. The merged workflow never enters the delivery path: it holds no
// execution key and creates no session or workspace.
func (s *Server) handleMergedWorkflow(item dispatchItem, receipt *store.WebhookDelivery, d *dispatcher) {
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", item.ep.CommentTrigger)
	if allowed, reason := admitDelivery(item.ep, s.policy, envelope); !allowed {
		slog.Info("webhook: delivery skipped before workspace",
			"endpoint", item.ep.Name,
			"delivery_id", item.deliveryID,
			"event_type", item.eventType,
			"reason", reason,
		)
		s.markSkipped(receipt.ID, item.ep, envelope, reason)
		return
	}
	s.handleMergedDelivery(item, receipt, envelope, d)
}

func (s *Server) markMergeConfirmed(ctx context.Context, ep config.EndpointConfig, envelope WebhookEnvelope, workCtx *WebhookWorkContext) {
	if workCtx == nil || strings.TrimSpace(workCtx.PRKey) == "" {
		return
	}
	s.syncPhaseParent(ctx, ep, workCtx.PRKey, envelope, PhaseMerged)
	if strings.TrimSpace(workCtx.ThreadID) == "" {
		return
	}
	repo, branch := TaskRepoBranch(workCtx.Key, envelope)
	s.ensureThreadStatus(ctx, ep.Platform, ep.ChannelID, workCtx.ThreadID, repo, branch, workCtx.PRKey, ThreadStatusUpdate{Phase: PhaseMerged})
}

// handleMergedDelivery closes a merged PR without an agent turn: the queue is
// drained, the task becomes terminal, a session left over from an earlier run
// is aborted, and the post-merge work runs outside the queue lane.
func (s *Server) handleMergedDelivery(item dispatchItem, receipt *store.WebhookDelivery, envelope WebhookEnvelope, d *dispatcher) {
	ctx := context.Background()
	prKey := strings.TrimSpace(item.prKey)
	repo, prNumber, _ := splitPRKey(prKey)
	if repo == "" {
		repo = strings.TrimSpace(stringValue(envelope["repository"]))
	}
	if prNumber == "" {
		prNumber = strings.TrimSpace(stringValue(envelope["pr_number"]))
	}
	if prKey != "" {
		if err := s.deliveries.SetPRKey(ctx, receipt.ID, prKey); err != nil {
			slog.Warn("webhook: pr key stamp failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", err)
		}
		if state, lookupErr := s.resolveLivePRState(ctx, prKey); lookupErr != nil || state != PRStateMerged {
			slog.Warn("webhook: merge not confirmed",
				"endpoint", item.ep.Name,
				"delivery_id", item.deliveryID,
				"pr_key", prKey,
				"state", state,
				"error", lookupErr,
			)
			ok, tErr := s.deliveries.Transition(ctx, receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted, store.WebhookStatusProcessing}, store.WebhookStatusSkipped, "merge not confirmed: PR is "+state)
			if tErr != nil {
				slog.Warn("webhook: merge skip transition failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", tErr)
			} else if !ok {
				slog.Warn("webhook: merge skip transition lost race", "endpoint", item.ep.Name, "delivery_id", item.deliveryID)
			}
			return
		}
	}
	dropped := 0
	if d != nil {
		dropped = s.drainPRQueue(d, prKey)
	}
	s.abortPRSession(ctx, prKey, repo)
	s.prMirrorClearActive(prKey)
	s.markMergedTerminal(item, envelope, prKey, repo, prNumber)
	ok, err := s.deliveries.Transition(ctx, receipt.ID, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusCompleted, "merged — queue drained")
	if err != nil {
		slog.Error("webhook: merged transition failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", err)
	} else if ok {
		slog.Info("webhook: merged delivery closed without agent turn",
			"endpoint", item.ep.Name,
			"delivery_id", item.deliveryID,
			"pr_key", prKey,
			"dropped", dropped,
		)
	}
	s.scheduleMergedDocsUpdate(item, envelope)
}

// markMergedTerminal moves the PR surface to its terminal phase: the parent
// shows merged and the thread state line agrees.
func (s *Server) markMergedTerminal(item dispatchItem, envelope WebhookEnvelope, prKey, repo, prNumber string) {
	if strings.TrimSpace(prKey) == "" {
		return
	}
	ctx := context.Background()
	threadID := ""
	rootID := ""
	branch := strings.TrimSpace(stringValue(envelope["head_branch"]))
	if detail, ok := s.tasks.SurfaceDetail(prKey); ok {
		if detail.ThreadID != "" {
			threadID = detail.ThreadID
		}
		rootID = detail.RootMessageID
		if branch == "" {
			branch = detail.Branch
		}
	}
	if rec, ok := s.tasks.ResolvePRThread(prKey); ok && threadID == "" {
		threadID = strings.TrimSpace(rec.ThreadID)
	}
	s.syncPhaseParent(ctx, item.ep, prKey, envelope, PhaseMerged)
	if threadID != "" {
		s.ensureThreadStatus(ctx, item.ep.Platform, item.ep.ChannelID, threadID, repo, branch, prKey, ThreadStatusUpdate{Phase: PhaseMerged})
	}
	s.recordTaskThread(item.ep, envelope, &WebhookWorkContext{
		PRKey:         prKey,
		ThreadID:      threadID,
		RootMessageID: rootID,
	}, TaskMerged)
}

// abortPRSession stops the agent session a previous run left behind for the
// PR, so a merged PR has no live writer.
func (s *Server) abortPRSession(ctx context.Context, prKey, repo string) (string, bool) {
	if strings.TrimSpace(prKey) == "" {
		return "", false
	}
	sessionID := ""
	workdir := ""
	if run := s.activeRunFor(prKey, "", ""); run != nil {
		sessionID, workdir = run.identity()
	}
	if sessionID == "" && s.prQueues != nil {
		if row, err := s.prQueues.Get(ctx, prKey); err == nil && row != nil {
			sessionID = strings.TrimSpace(row.ActiveSession)
		}
	}
	if sessionID == "" {
		slog.Info("webhook: merged delivery found no active session", "pr_key", prKey)
		return "", false
	}
	if workdir == "" {
		workdir = s.checkoutForRepo(repo)
	}
	if s.sessionAborter == nil || workdir == "" {
		slog.Error("webhook: merged delivery session abort skipped",
			"pr_key", prKey,
			"session_id", sessionID,
			"reason", "no abort capability",
		)
		return sessionID, false
	}
	if err := s.sessionAborter(ctx, sessionID, workdir); err != nil {
		if errors.Is(err, ErrNoLiveAgent) {
			slog.Info("webhook: merged delivery found no live agent to abort",
				"pr_key", prKey,
				"session_id", sessionID,
			)
			return sessionID, true
		}
		slog.Error("webhook: merged delivery session abort failed",
			"pr_key", prKey,
			"session_id", sessionID,
			"error", err,
		)
		return sessionID, false
	}
	slog.Info("webhook: merged delivery aborted active session",
		"pr_key", prKey,
		"session_id", sessionID,
		"session_abort_ok", true,
	)
	return sessionID, true
}

// scheduleMergedDocsUpdate runs the post-merge work outside the PR queue
// lane: it holds no PR execution key and outlives the queue item.
func (s *Server) scheduleMergedDocsUpdate(item dispatchItem, envelope WebhookEnvelope) {
	ep := item.ep
	if strings.TrimSpace(ep.Prompt) == "" && len(ep.OnMerged) == 0 {
		return
	}
	go func() {
		if !s.acquireEvent(s.shutdownCtx) {
			return
		}
		defer s.releaseEvent()
		ctx, cancel := context.WithTimeout(context.Background(), s.processingTimeout)
		defer cancel()
		s.runMergedDocsUpdate(ctx, ep, item, envelope)
	}()
}

// runMergedDocsUpdate performs the post-merge work with a context that cannot
// reach the PR parent or the thread state line.
func (s *Server) runMergedDocsUpdate(ctx context.Context, ep config.EndpointConfig, item dispatchItem, envelope WebhookEnvelope) {
	payload := map[string]any{}
	if err := json.Unmarshal(item.body, &payload); err != nil {
		payload = map[string]any{}
	}
	tmplData := map[string]any{
		"payload":       payload,
		"json":          string(item.body),
		"execution_key": "",
		"worktree":      "",
		"webhook":       envelope,
	}
	workCtx := &WebhookWorkContext{
		DeliveryID: item.deliveryID,
		Attempt:    1,
		Workflow:   ep.Workflow,
		Envelope:   mergedDocsEnvelope(envelope),
		StartTime:  time.Now(),
	}
	if prompt := strings.TrimSpace(ep.Prompt); prompt != "" {
		rendered, err := renderTemplate(prompt, tmplData)
		if err != nil {
			slog.Error("webhook: merged prompt render failed", "endpoint", ep.Name, "error", err)
		} else {
			rendered = strings.ReplaceAll(rendered, "</untrusted_payload>", "&lt;/untrusted_payload>")
			wrapped := fmt.Sprintf("<untrusted_payload>\n%s\n</untrusted_payload>", rendered)
			if err := s.executor(ctx, ep.Platform, ep.ChannelID, wrapped, workCtx); err != nil {
				slog.Warn("webhook: merged prompt turn failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", err)
			}
		}
	}
	s.runMergedExtras(ep, envelope, workCtx)
}

// mergedDocsEnvelope drops the branch so the post-merge work cannot resolve
// the PR task thread and write to its parent or state line. The key stays
// present with an empty value: a prompt that interpolates it must render,
// not fail on a missing key.
func mergedDocsEnvelope(envelope WebhookEnvelope) WebhookEnvelope {
	out := WebhookEnvelope{}
	for key, value := range envelope {
		if key == "head_branch" {
			continue
		}
		out[key] = value
	}
	out["head_branch"] = ""
	return out
}

func (s *Server) runMergedExtras(ep config.EndpointConfig, envelope WebhookEnvelope, workCtx *WebhookWorkContext) {
	if len(ep.OnMerged) == 0 {
		return
	}
	repo := strings.TrimSpace(stringValue(envelope["repository"]))
	for _, action := range ep.OnMerged {
		prompt := strings.TrimSpace(action.Prompt)
		promptFile := strings.TrimSpace(action.PromptFile)
		command := strings.TrimSpace(action.Run)
		switch {
		case prompt != "" || promptFile != "":
			content := prompt
			if promptFile != "" {
				loaded, err := config.LoadPromptFile(s.promptDir, promptFile)
				if err != nil {
					slog.Warn("webhook: merged prompt skipped", "endpoint", ep.Name, "prompt_file", promptFile, "error", err)
					_ = s.notifyThread(context.Background(), ep.Platform, threadTargetFor(ep.Platform, ep.ChannelID, workCtx.ThreadID),
						"⚠️ merged prompt skipped ("+promptFile+"): "+shortFailure(err))
					continue
				}
				content = loaded
			}
			s.runMergedPrompt(ep, envelope, workCtx, content)
		case command != "":
			s.runMergedCommand(ep, repo, command)
		}
	}
}

func shortFailure(err error) string {
	if err == nil {
		return ""
	}
	return clipRunes(strings.TrimSpace(err.Error()), 160)
}

func (s *Server) runMergedPrompt(ep config.EndpointConfig, envelope WebhookEnvelope, workCtx *WebhookWorkContext, prompt string) {
	tmplData := map[string]any{
		"payload":       map[string]any{},
		"json":          "",
		"execution_key": workCtx.Key.String(),
		"worktree":      workCtx.Worktree,
		"webhook":       envelope,
	}
	rendered, err := renderTemplate(prompt, tmplData)
	if err != nil {
		slog.Warn("webhook: merged prompt render failed", "endpoint", ep.Name, "error", err)
		return
	}
	rendered = strings.ReplaceAll(rendered, "</untrusted_payload>", "&lt;/untrusted_payload>")
	wrapped := fmt.Sprintf("<untrusted_payload>\n%s\n</untrusted_payload>", rendered)
	follow := *workCtx
	follow.StartTime = time.Now()
	follow.Attempt = workCtx.Attempt + 1
	ctx, cancel := context.WithTimeout(context.Background(), mergedExtraTimeout)
	defer cancel()
	if err := s.executor(ctx, ep.Platform, ep.ChannelID, wrapped, &follow); err != nil {
		slog.Warn("webhook: merged prompt turn failed", "endpoint", ep.Name, "delivery_id", follow.DeliveryID, "error", err)
		return
	}
}

func (s *Server) runMergedCommand(ep config.EndpointConfig, repo, command string) {
	checkout := s.checkoutForRepo(repo)
	if checkout == "" {
		slog.Warn("webhook: merged command skipped, no git checkout for repo", "endpoint", ep.Name, "repo", repo)
		return
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), mergedExtraTimeout)
	defer cancel()
	cmd := exec.CommandContext(timeoutCtx, "sh", "-c", command)
	cmd.Dir = checkout
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		slog.Warn("webhook: merged command failed", "endpoint", ep.Name, "repo", repo, "error", err, "output", clipRunes(strings.TrimSpace(out.String()), 500))
		return
	}
	slog.Info("webhook: merged command done", "endpoint", ep.Name, "repo", repo, "output", clipRunes(strings.TrimSpace(out.String()), 500))
}
