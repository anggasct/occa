package webhook

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
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
	rendered = strings.ReplaceAll(rendered, "</untrusted_payload>", "&lt;/untrusted_payload&gt;")
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
