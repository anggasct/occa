package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
	"github.com/anggasct/occa/internal/webhook"
)

type promptRecordingClient struct {
	mockRelayClient
	mu      sync.Mutex
	prompts []string
}

func (c *promptRecordingClient) SendMessage(ctx context.Context, sessionID, text string, model *relay.ModelRef, attachments []relay.Attachment) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prompts = append(c.prompts, text)
	return nil
}

func writeRepoSkill(t *testing.T, root, name, frontmatter, body string) {
	t.Helper()
	dir := filepath.Join(root, ".occa", "skills", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	content := "---\nname: " + name + "\ndescription: d\n" + frontmatter + "---\n" + body + "\n"
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func skillTestStore(t *testing.T) *store.SQLiteStore {
	t.Helper()
	db, err := store.OpenWithRetention(filepath.Join(t.TempDir(), "occa.db"), "", store.UsageRetention{Retention: time.Hour, MaxRows: 10}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func runSkillWebhook(t *testing.T, worktree string, envelope webhook.WebhookEnvelope, db *store.SQLiteStore) string {
	t.Helper()
	cfg := config.DefaultSkillsConfig()
	cfg.GlobalDir = ""
	client := &promptRecordingClient{mockRelayClient: mockRelayClient{events: []relay.Event{{Type: relay.EventDelta, Delta: "ok"}}}}
	exec := newWebhookExecutor([]channel.Channel{&mockDiscordChannel{name: "discord"}}, &mockAgentManager{client: client}, mockChannelStore{}, t.TempDir(),
		relay.Config{NoEventTimeout: time.Minute, WebhookAbortTimeout: time.Second, VerifyTimeout: time.Second},
		webhook.TaskOptions{Skills: newSkillInjector(cfg, db.SessionSkillRepo())})
	workCtx := &webhook.WebhookWorkContext{Workflow: "review", Envelope: envelope, DeliveryID: "del-1", Worktree: worktree}
	if err := exec(context.Background(), "discord", "chan-main", "prompt text", workCtx); err != nil {
		t.Fatalf("executor: %v", err)
	}
	if len(client.prompts) != 1 {
		t.Fatalf("prompts = %d, want 1", len(client.prompts))
	}
	return client.prompts[0]
}

// AC-12: no skill directories, the lane prompt is byte-identical.
func TestWebhookSkillsGoldenNoSkills(t *testing.T) {
	db := skillTestStore(t)
	got := runSkillWebhook(t, t.TempDir(), webhook.WebhookEnvelope{"title": "migrate store"}, db)
	if got != "prompt text" {
		t.Fatalf("prompt = %q, want byte-identical", got)
	}
	if rows, _ := db.SessionSkillRepo().List(context.Background(), "session-1"); len(rows) != 0 {
		t.Fatalf("rows = %+v", rows)
	}
}

// AC-13: a lane run resolves skills from its leased worktree, matches the
// PR title and the dispatched scope, and records what it injected.
func TestWebhookSkillsFromWorktree(t *testing.T) {
	worktree := t.TempDir()
	writeRepoSkill(t, worktree, "store-rules", "trigger: paths\npaths: [\"internal/store/**\"]\n", "Store rule.")
	writeRepoSkill(t, worktree, "migrations", "trigger: keyword\nkeywords: [migrate]\n", "Migration rule.")
	writeRepoSkill(t, worktree, "deploys", "trigger: keyword\nkeywords: [deploy]\n", "Deploy rule.")
	db := skillTestStore(t)
	got := runSkillWebhook(t, worktree, webhook.WebhookEnvelope{
		"title":      "Migrate the session table",
		"task_scope": "internal/store/sqlite.go, cmd/occa/main.go",
	}, db)
	if !strings.HasPrefix(got, `<occa-skills count="2">`) || !strings.HasSuffix(got, "</occa-skills>\n\nprompt text") {
		t.Fatalf("prompt = %q", got)
	}
	if !strings.Contains(got, "Store rule.") || !strings.Contains(got, "Migration rule.") || strings.Contains(got, "Deploy rule.") {
		t.Fatalf("wrong skills selected: %q", got)
	}
	rows, err := db.SessionSkillRepo().List(context.Background(), "session-1")
	if err != nil || len(rows) != 2 || rows[0].SkillName != "migrations" || rows[1].SkillName != "store-rules" || rows[1].TriggerKind != "paths" {
		t.Fatalf("rows = %+v, %v", rows, err)
	}
}

func TestWebhookSkillsDisabled(t *testing.T) {
	worktree := t.TempDir()
	writeRepoSkill(t, worktree, "always-rules", "trigger: always\n", "Always.")
	cfg := config.DefaultSkillsConfig()
	cfg.Enabled = false
	client := &promptRecordingClient{mockRelayClient: mockRelayClient{events: []relay.Event{{Type: relay.EventDelta, Delta: "ok"}}}}
	exec := newWebhookExecutor([]channel.Channel{&mockDiscordChannel{name: "discord"}}, &mockAgentManager{client: client}, mockChannelStore{}, t.TempDir(),
		relay.Config{NoEventTimeout: time.Minute, WebhookAbortTimeout: time.Second, VerifyTimeout: time.Second},
		webhook.TaskOptions{Skills: newSkillInjector(cfg, nil)})
	if err := exec(context.Background(), "discord", "chan-main", "prompt text", &webhook.WebhookWorkContext{Worktree: worktree, Envelope: webhook.WebhookEnvelope{}}); err != nil {
		t.Fatal(err)
	}
	if len(client.prompts) != 1 || client.prompts[0] != "prompt text" {
		t.Fatalf("prompts = %q", client.prompts)
	}
}
