package router

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/skills"
)

// DispatchTaskSource exposes a dispatched task's brief and declared scope, so
// the run a dispatch starts resolves skills against what was dispatched.
type DispatchTaskSource interface {
	DispatchTaskContext(taskID string) (brief, scope string, ok bool)
}

// SetSkills enables per-repo skill injection on the first prompt of each
// agent session. A nil injector leaves every prompt unchanged.
func (r *Router) SetSkills(in *skills.Injector) {
	r.skills = in
}

// skillInput is what a chat run declares: the operator's text, plus the
// brief and scope of a dispatched task when the message is a dispatch.
func (r *Router) skillInput(msg channel.IncomingMessage, text string) skills.Input {
	in := skills.Input{Text: text}
	if !msg.IsSelfDispatch {
		return in
	}
	_, _, taskID, ok := channel.ParseDispatchSummary(msg.Text)
	if !ok {
		return in
	}
	src, ok := r.dispatchLifecycle.(DispatchTaskSource)
	if !ok {
		return in
	}
	brief, scope, ok := src.DispatchTaskContext(taskID)
	if !ok {
		return in
	}
	in.Text = strings.TrimSpace(text + "\n" + brief)
	in.Scope = skills.SplitScope(scope)
	return in
}

// planSkills decides the skills block for one chat prompt. fresh is true when
// the session was created for this message; otherwise the agent is asked
// whether the session has had a prompt yet, and only when it matters.
func (r *Router) planSkills(ctx context.Context, msg channel.IncomingMessage, inst AgentInstance, sessionID, text string, fresh bool) skills.Plan {
	if !r.skills.Enabled() {
		return skills.Plan{}
	}
	isFirst := func() bool {
		if fresh {
			return true
		}
		msgs, err := inst.Client().ListMessages(ctx, sessionID)
		return err == nil && len(msgs) == 0
	}
	return r.skills.ForSession(ctx, sessionID, inst.Workdir(), r.skillInput(msg, text), isFirst)
}

const (
	skillsListMaxLines   = 20
	skillsListMaxSkipped = 5
)

// handleSkills lists the skills the conversation's workdir resolves to: one
// line per skill, shadowed globals marked, skipped files with their reason.
// It reads the directories only; no agent is started.
func (r *Router) handleSkills(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	if r.skills != nil && !r.skills.Enabled() {
		return "Skills are disabled (skills.enabled: false).", nil
	}
	workdir, err := r.effectiveWorkdir(ctx, msg)
	if err != nil {
		return "", fmt.Errorf("router: skills workdir: %w", err)
	}
	set := skills.Load(r.skills.Options(workdir))
	return formatSkillsList(set, workdir), nil
}

func formatSkillsList(set skills.Set, workdir string) string {
	if len(set.Skills) == 0 && len(set.Shadowed) == 0 && len(set.Errors) == 0 {
		return "no skills in this workdir"
	}
	var lines []string
	for _, sk := range set.Skills {
		lines = append(lines, fmt.Sprintf("%s · %s · %s · %d runes", sk.Name, sk.Scope, sk.Trigger, sk.Runes))
	}
	for _, sk := range set.Shadowed {
		lines = append(lines, fmt.Sprintf("%s · %s · %s · %d runes · shadowed", sk.Name, sk.Scope, sk.Trigger, sk.Runes))
	}
	if extra := len(lines) - skillsListMaxLines; extra > 0 {
		lines = append(lines[:skillsListMaxLines], fmt.Sprintf("… and %d more — occa skills list", extra))
	}
	for i, e := range set.Errors {
		if i == skillsListMaxSkipped {
			lines = append(lines, fmt.Sprintf("… and %d more skipped — occa skills lint", len(set.Errors)-i))
			break
		}
		path := e.Path
		if rel, relErr := filepath.Rel(workdir, e.Path); relErr == nil && !strings.HasPrefix(rel, "..") {
			path = rel
		}
		lines = append(lines, fmt.Sprintf("skipped %s:%d: %s", path, e.Line, e.Reason))
	}
	if set.GlobalUnreadable != nil {
		lines = append(lines, "global skills dir unreadable — occa skills list")
	}
	return strings.Join(lines, "\n")
}
