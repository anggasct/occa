package main

import (
	"context"
	"strings"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/skills"
	"github.com/anggasct/occa/internal/store"
	"github.com/anggasct/occa/internal/webhook"
)

// skillLedger stores injected skills in the session_skill table.
type skillLedger struct {
	repo store.SessionSkillRepo
}

func (l skillLedger) SessionSkills(ctx context.Context, sessionID string) ([]skills.Record, error) {
	rows, err := l.repo.List(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	out := make([]skills.Record, 0, len(rows))
	for _, r := range rows {
		out = append(out, skills.Record{Name: r.SkillName, Scope: r.Scope, Trigger: r.TriggerKind, Digest: r.SkillDigest, Runes: r.Runes})
	}
	return out, nil
}

func (l skillLedger) RecordSessionSkills(ctx context.Context, sessionID string, records []skills.Record) error {
	rows := make([]store.SessionSkill, 0, len(records))
	for _, r := range records {
		rows = append(rows, store.SessionSkill{SkillName: r.Name, Scope: r.Scope, TriggerKind: r.Trigger, SkillDigest: r.Digest, Runes: r.Runes})
	}
	return l.repo.Record(ctx, sessionID, rows)
}

func newSkillInjector(cfg config.SkillsConfig, repo store.SessionSkillRepo) *skills.Injector {
	var ledger skills.Ledger
	if repo != nil {
		ledger = skillLedger{repo: repo}
	}
	return skills.NewInjector(cfg.Enabled, cfg.Options, ledger)
}

// webhookSkillInput is what a webhook lane run declares: the dispatched
// task's brief and scope when the branch has one, the PR title, and the
// comment that triggered the run. The lane prompt template itself is not
// task text: its fixed wording would fire keyword skills on every run.
func webhookSkillInput(envelope webhook.WebhookEnvelope) skills.Input {
	var parts []string
	for _, key := range []string{"task_brief", "title", "comment_body"} {
		if v := strings.TrimSpace(webhook.EnvelopeString(envelope, key)); v != "" {
			parts = append(parts, v)
		}
	}
	return skills.Input{
		Text:  strings.Join(parts, "\n"),
		Scope: skills.SplitScope(webhook.EnvelopeString(envelope, "task_scope")),
	}
}
