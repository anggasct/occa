package webhook

import (
	"context"
	"strings"
)

func ThreadPhaseForWorkflow(workflow string) (string, string) {
	w := strings.ToLower(strings.TrimSpace(workflow))
	w = strings.TrimPrefix(w, "github_")
	switch w {
	case "review", "reviewer":
		return "REVIEW", "🔍"
	case "fix":
		return "FIX", "🔨"
	case "merge":
		return "MERGE", "🔀"
	case "merged":
		return "DONE", "✅"
	case "dispatch", "dispatched":
		return "REVIEW", "🔍"
	default:
		if w == "" {
			return "REVIEW", "🔍"
		}
		return "REVIEW", "🔍"
	}
}

func FormatPhaseBanner(workflow, detail string) string {
	phase, icon := ThreadPhaseForWorkflow(workflow)
	detail = strings.TrimSpace(detail)
	single := strings.Join(strings.Fields(detail), " ")
	var b strings.Builder
	b.WriteString(icon)
	b.WriteString(" ")
	b.WriteString(phase)
	switch phase {
	case "REVIEW":
		b.WriteString(" started")
	case "FIX":
		b.WriteString(" in progress")
		if single != "" {
			b.WriteString(" (")
			b.WriteString(clipTaskField(single, 120))
			b.WriteString(")")
		}
	case "MERGE":
		b.WriteString(" checking gate")
	case "DONE":
	}
	out := b.String()
	out = strings.ReplaceAll(out, "\n", " ")
	out = strings.Join(strings.Fields(out), " ")
	return out
}

func FormatGateOutcome(envelope WebhookEnvelope, workflow, deliveryStatus string) string {
	w := strings.ToLower(strings.TrimSpace(workflow))
	w = strings.TrimPrefix(w, "github_")
	status := strings.ToUpper(strings.TrimSpace(deliveryStatus))
	prState := strings.ToLower(strings.TrimSpace(stringValue(envelope["pr_state"])))
	verdict := strings.ToUpper(strings.TrimSpace(stringValue(envelope["review_verdict"])))
	conclusion := strings.ToLower(strings.TrimSpace(stringValue(envelope["conclusion"])))
	merged := false
	if v, ok := envelope["merged"].(bool); ok {
		merged = v
	}
	if prState == "merged" || merged || w == "merged" {
		return "gate checked, merged"
	}
	if status == "FAILED" {
		return "gate checked, run failed, fix running"
	}
	if conclusion == "failure" || conclusion == "failed" {
		return "gate checked, CI red, fix running"
	}
	if verdict == "REQUEST_CHANGES" || verdict == "REQUESTED_CHANGES" || verdict == "CHANGES_REQUESTED" {
		return "gate checked, review changes requested, fix running"
	}
	if prState == "closed" {
		return "gate checked, PR closed, fix running"
	}
	if w == "merge" {
		return "gate checked, CI green, merge ready"
	}
	if status == "COMPLETED" {
		return "gate checked, complete"
	}
	return "gate checked, " + strings.ToLower(status)
}

func FormatThreadSlimCard(envelope WebhookEnvelope, workflow string, status TaskStatus, icons map[string]string) string {
	repo := strings.TrimSpace(stringValue(envelope["repository"]))
	branch := strings.TrimSpace(stringValue(envelope["head_branch"]))
	prNumber := strings.TrimSpace(stringValue(envelope["pr_number"]))
	prURL := strings.TrimSpace(stringValue(envelope["pr_url"]))
	icon := TaskIcon(status, icons)
	head := strings.TrimSpace(repo)
	if branch != "" {
		if head != "" {
			head += "#"
		}
		head += strings.TrimSpace(branch)
	}
	if prNumber != "" {
		if head != "" {
			head += " · "
		}
		head += "PR #" + prNumber
	}
	if head == "" {
		head = strings.TrimSpace(workflow)
		if head == "" {
			head = "task"
		}
	}
	parts := []string{icon + " " + strings.TrimSpace(head)}
	if verdict := reviewVerdictLine(envelope); verdict != "" {
		parts = append(parts, verdict)
	}
	if link := FormatTaskLinks(prURL); link != "" {
		parts = append(parts, link)
	}
	if outcome := FormatGateOutcome(envelope, workflow, string(status)); outcome != "" && (status == TaskMerged || status == TaskFailed || status == TaskPROpened) {
		parts = append(parts, outcome)
	}
	return strings.Join(parts, " · ")
}

func FormatThreadOpener(envelope WebhookEnvelope, workflow string, status TaskStatus, icons map[string]string) string {
	return FormatThreadSlimCard(envelope, workflow, status, icons)
}

func ThreadTextHasNoLeaks(text string) bool {
	if strings.Contains(text, "Delivery:") {
		return false
	}
	if strings.Contains(text, "Model:") {
		return false
	}
	if strings.Contains(text, "Model source:") {
		return false
	}
	if strings.Contains(text, webhookMessageSeparator) {
		return false
	}
	if strings.Contains(text, "━━━") {
		return false
	}
	return true
}

func ShouldPostPhaseBanner(prevPhase, nextPhase string) bool {
	prevPhase = strings.TrimSpace(prevPhase)
	nextPhase = strings.TrimSpace(nextPhase)
	if nextPhase == "" {
		return false
	}
	return prevPhase != nextPhase
}

func (s *Server) postPhaseBannerIfChanged(ctx context.Context, platform, channelID, threadID, repo, branch, workflow, detail string) bool {
	if s == nil || s.tasks == nil {
		return false
	}
	phase, _ := ThreadPhaseForWorkflow(workflow)
	var prev string
	if rec, ok := s.tasks.Lookup(repo, branch); ok {
		prev = rec.Phase
	}
	if !ShouldPostPhaseBanner(prev, phase) {
		return false
	}
	banner := FormatPhaseBanner(workflow, detail)
	target := threadTargetFor(platform, channelID, threadID)
	if strings.TrimSpace(target) == "" || strings.TrimSpace(banner) == "" {
		return false
	}
	if err := s.notifyThread(ctx, platform, target, banner); err != nil {
		return false
	}
	s.tasks.SetPhase(repo, branch, phase)
	return true
}
