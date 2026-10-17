package webhook

import (
	"context"
	"strings"
	"unicode/utf8"
)

func nextHintForPhase(phase string) string {
	switch strings.TrimSpace(phase) {
	case "REVIEW":
		return "fix or merge gate"
	case "FIX":
		return "push + re-review"
	case "MERGE":
		return "merge or fix"
	case "DONE":
		return ""
	default:
		return ""
	}
}

func shortDeliveryID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

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

func FormatThreadStatus(phaseLabel, stepRun, lastEvent, next string) string {
	clean := func(s string) string {
		s = strings.ReplaceAll(s, "\n", " ")
		return strings.Join(strings.Fields(strings.TrimSpace(s)), " ")
	}
	phaseLabel, stepRun, lastEvent, next = clean(phaseLabel), clean(stepRun), clean(lastEvent), clean(next)
	var parts []string
	if phaseLabel != "" {
		parts = append(parts, phaseLabel)
	}
	if stepRun != "" {
		parts = append(parts, stepRun)
	}
	if lastEvent != "" {
		parts = append(parts, lastEvent)
	}
	joined := strings.Join(parts, " · ")
	if strings.TrimSpace(next) != "" {
		if joined != "" {
			joined += " · next: " + strings.TrimSpace(next)
		} else {
			joined = "next: " + strings.TrimSpace(next)
		}
	}
	if joined == "" {
		joined = "tracking"
	}
	out := "📌 STATUS: " + joined
	if utf8.RuneCountInString(out) > 500 {
		out = string([]rune(out)[:500])
	}
	return out
}

func ThreadStatusText(phase, icon, stepRun, lastEvent, next string) string {
	phase = strings.TrimSpace(phase)
	icon = strings.TrimSpace(icon)
	label := strings.TrimSpace(phase)
	if icon != "" {
		if label != "" {
			label = icon + " " + label
		} else {
			label = icon
		}
	}
	return FormatThreadStatus(label, stepRun, lastEvent, next)
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
	lines := []string{icon + " " + strings.TrimSpace(head)}
	if verdict := reviewVerdictLine(envelope); verdict != "" {
		lines = append(lines, verdict)
	}
	if link := FormatTaskLinks(prURL); link != "" {
		lines = append(lines, link)
	}
	if outcome := FormatGateOutcome(envelope, workflow, string(status)); outcome != "" && (status == TaskMerged || status == TaskFailed || status == TaskPROpened) {
		lines = append(lines, outcome)
	}
	return strings.Join(lines, "\n")
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

func (s *Server) ensureThreadStatus(ctx context.Context, platform, channelID, threadID, repo, branch, workflow, stepRun, lastEvent string) string {
	if s == nil || s.tasks == nil {
		return ""
	}
	phase, icon := ThreadPhaseForWorkflow(workflow)
	next := nextHintForPhase(phase)
	text := ThreadStatusText(phase, icon, stepRun, lastEvent, next)
	target := threadTargetFor(platform, channelID, threadID)
	if strings.TrimSpace(target) == "" {
		return ""
	}
	var existing string
	if rec, ok := s.tasks.Lookup(repo, branch); ok {
		existing = strings.TrimSpace(rec.StatusMessageID)
	}
	if existing != "" {
		if err := s.editThread(ctx, platform, target, existing, text); err == nil {
			return existing
		}
	}
	msgID, err := s.sendThread(ctx, platform, target, text)
	if err != nil || strings.TrimSpace(msgID) == "" {
		return strings.TrimSpace(msgID)
	}
	s.tasks.SetStatusMessage(repo, branch, msgID)
	return msgID
}
