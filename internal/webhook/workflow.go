package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func workflowAllows(workflow string, envelope WebhookEnvelope) (bool, string) {
	if workflow == "" {
		return true, ""
	}

	eventType := strings.ToLower(strings.TrimSpace(stringValue(envelope["event_type"])))
	action := strings.ToLower(stringValue(envelope["action"]))
	reviewState := strings.ToLower(stringValue(envelope["review_state"]))
	prNumber := stringValue(envelope["pr_number"])
	reviewUser := stringValue(envelope["review_user"])
	prAuthor := stringValue(envelope["pr_author"])
	reviewVerdict := strings.ToLower(stringValue(envelope["review_verdict"]))
	selfReview := reviewUser == "kumasct" && prAuthor == "kumasct" && reviewState == "commented"

	switch workflow {
	case "github_reviewer":
		if eventType == "pull_request" && containsString([]string{"opened", "synchronize", "reopened", "ready_for_review"}, action) {
			return true, ""
		}
		if eventType == "issue_comment" && action == "created" && prNumber != "" && stringValue(envelope["comment_trigger"]) == "please re-review" {
			return true, ""
		}
	case "github_fix":
		if eventType == "pull_request_review" && action == "submitted" && reviewState == "changes_requested" {
			return true, ""
		}
		if eventType == "pull_request_review" && action == "submitted" && selfReview && reviewVerdict == "request_changes" {
			return true, ""
		}
	case "github_merge":
		if eventType == "pull_request_review" && action == "submitted" && reviewState == "approved" {
			return true, ""
		}
		if eventType == "pull_request_review" && action == "submitted" && selfReview && reviewVerdict == "approved" && !boolValue(envelope["has_findings"]) {
			return true, ""
		}
	case "github_merged":
		if eventType == "pull_request" && action == "closed" && boolValue(envelope["merged"]) {
			return true, ""
		}
	}
	return false, fmt.Sprintf("workflow %s rejected %s.%s", workflow, eventType, action)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func formatAuditSummary(envelope WebhookEnvelope, workflow string, statusAndReason ...string) string {
	status, reason := "SKIP", ""
	if len(statusAndReason) == 1 {
		reason = statusAndReason[0]
	} else if len(statusAndReason) >= 2 {
		status, reason = statusAndReason[0], statusAndReason[1]
	}
	if workflow == "" {
		workflow = "webhook"
	}
	repository := auditField(stringValue(envelope["repository"]))
	if repository == "" {
		repository = "—"
	}
	pr := "PR: —"
	if number := auditField(stringValue(envelope["pr_number"])); number != "" {
		pr = "PR: #" + number
		if title := auditField(stringValue(envelope["title"])); title != "" {
			pr += " — " + clipRunes(title, 120)
		}
	}
	event := auditField(stringValue(envelope["event_type"]))
	if action := auditField(stringValue(envelope["action"])); action != "" {
		event += "." + action
	}
	if event == "" {
		event = "unknown"
	}

	lines := []string{
		"📨 GitHub Webhook · " + workflow,
		"Repo: " + repository,
		pr,
		"Event: " + event,
	}
	if head, base := auditField(stringValue(envelope["head_branch"])), auditField(stringValue(envelope["base_branch"])); head != "" || base != "" {
		lines = append(lines, "Branch: "+head+" → "+base)
	}
	if model := auditField(stringValue(envelope["model"])); model != "" {
		lines = append(lines, "Model: "+model)
		if src := auditField(stringValue(envelope["model_source"])); src != "" {
			lines = append(lines, "Model source: "+src)
		}
	}
	lines = append(lines, "Status: "+status)
	if reason != "" {
		lines = append(lines, "Reason: "+clipRunes(auditField(reason), maxErrorSummaryRunes))
	}
	delivery := auditField(stringValue(envelope["delivery_id"]))
	if delivery == "" {
		delivery = "—"
	}
	lines = append(lines, "Delivery: "+delivery)
	return strings.Join(lines, "\n")
}

func auditField(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	return strings.ReplaceAll(value, "<no value>", "")
}

func redactAuditSummary(summary, secret string) string {
	if secret == "" {
		return summary
	}
	return strings.ReplaceAll(summary, secret, "[redacted]")
}

func (s *Server) markSkipped(id int64, ep config.EndpointConfig, envelope WebhookEnvelope, reason string) {
	summary := redactAuditSummary(formatAuditSummary(envelope, ep.Workflow, "SKIP", reason), ep.Secret)
	if id != 0 {
		ok, err := s.deliveries.Transition(context.Background(), id, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusSkipped, summary)
		if err != nil {
			slog.Error("webhook: skip transition failed", "endpoint", ep.Name, "delivery_id", stringValue(envelope["delivery_id"]), "error", err)
			return
		}
		if !ok {
			slog.Warn("webhook: skip transition lost race", "endpoint", ep.Name, "delivery_id", stringValue(envelope["delivery_id"]))
			return
		}
	}
	s.emitAudit(context.Background(), ep, envelope, "SKIP", reason)
}

func (s *Server) emitAudit(ctx context.Context, ep config.EndpointConfig, envelope WebhookEnvelope, status, reason string) {
	summary := redactAuditSummary(formatAuditSummary(envelope, ep.Workflow, status, reason), ep.Secret)
	if s.notifier == nil {
		slog.Info("webhook: audit notification skipped", "endpoint", ep.Name, "status", status)
		return
	}
	if err := s.notifier(ctx, ep.Platform, ep.ChannelID, summary); err != nil {
		slog.Warn("webhook: audit notification failed", "endpoint", ep.Name, "status", status, "error", err)
	}
}
