package router

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/anggasct/occa/internal/channel"
)

func validRepoIdentity(repo string) bool {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(repo)), "/")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return false
		}
		for _, r := range part {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && r != '_' && r != '.' {
				return false
			}
		}
	}
	return true
}

func newWebhookSecret() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

type ConnectEndpoint struct {
	Name      string
	Path      string
	Secret    string
	Workflow  string
	Platform  string
	ChannelID string
	Repo      string
	Events    []string
}

type ConnectRepoDeps struct {
	PublicBaseURL   string
	CheckAdmin      func(ctx context.Context, repo string) (bool, error)
	CreateHook      func(ctx context.Context, repo, hookURL, secret string, events []string) (string, error)
	AppendEndpoints func(endpoints []ConnectEndpoint) error
	Restart         func(ctx context.Context) error
	PingHook        func(ctx context.Context, repo, hookID string) error
}

func (r *Router) SetConnectRepoDeps(deps *ConnectRepoDeps) {
	r.connectDeps = deps
}

func (r *Router) SetConnectRepoConfig(configPath string) {
	r.connectConfigPath = strings.TrimSpace(configPath)
}

func connectHooks() []struct {
	workflow string
	suffix   string
	events   []string
} {
	return []struct {
		workflow string
		suffix   string
		events   []string
	}{
		{"review", "review", []string{"pull_request"}},
		{"fix", "pr-fix", []string{"pull_request_review"}},
		{"merge", "merge", []string{"pull_request_review", "check_suite"}},
		{"merged", "merged", []string{"pull_request"}},
	}
}

func (r *Router) runConnectAutomation(ctx context.Context, msg channel.IncomingMessage, repo string) (string, bool, error) {
	deps := r.connectDeps
	if deps == nil || strings.TrimSpace(deps.PublicBaseURL) == "" {
		return "", false, nil
	}
	if deps.CheckAdmin == nil || deps.CreateHook == nil || deps.AppendEndpoints == nil || deps.Restart == nil || deps.PingHook == nil {
		return "", false, nil
	}
	admin, err := deps.CheckAdmin(ctx, repo)
	if err != nil || !admin {
		lines := []string{
			"⚠️ GitHub admin access required on " + repo + " to create hooks.",
			"Phase 1: use a fine-grained PAT scoped to " + repo + " with admin:repo_hook.",
			"Phase 2 (only for a shared public relay): GitHub App with short-lived per-repo tokens.",
		}
		if err != nil {
			lines = append(lines, "Check failed: "+strings.TrimSpace(err.Error()))
		}
		checklist, _ := r.connectChecklist(repo)
		return strings.Join(lines, "\n") + "\n\n" + checklist, true, nil
	}
	slug := strings.ReplaceAll(repo, "/", "-")
	base := strings.TrimSuffix(strings.TrimSpace(deps.PublicBaseURL), "/")
	hooks := connectHooks()
	secrets := make([]string, 0, len(hooks))
	for range hooks {
		secret, err := newWebhookSecret()
		if err != nil {
			return "", false, fmt.Errorf("connect-repo secrets: %w", err)
		}
		secrets = append(secrets, secret)
	}
	type hookResult struct {
		workflow string
		hookID   string
		hookURL  string
		err      error
	}
	results := make([]hookResult, 0, len(hooks))
	for i, hook := range hooks {
		hookURL := base + "/occa/webhooks/github-" + slug + "-" + hook.suffix
		hookID, err := deps.CreateHook(ctx, repo, hookURL, secrets[i], hook.events)
		results = append(results, hookResult{workflow: hook.workflow, hookID: hookID, hookURL: hookURL, err: err})
	}
	endpoints := make([]ConnectEndpoint, 0, len(hooks))
	for i, hook := range hooks {
		if results[i].err != nil {
			continue
		}
		endpoints = append(endpoints, ConnectEndpoint{
			Name:      "github-" + slug + "-" + hook.suffix,
			Path:      "/webhooks/github-" + slug + "-" + hook.suffix,
			Secret:    secrets[i],
			Workflow:  hook.workflow,
			Platform:  msg.Platform,
			ChannelID: msg.ChannelID,
			Repo:      repo,
			Events:    hook.events,
		})
	}
	var appendErr error
	appended := false
	restartNote := ""
	if len(endpoints) > 0 {
		appendErr = deps.AppendEndpoints(endpoints)
		if appendErr == nil {
			appended = true
			if err := deps.Restart(ctx); err != nil {
				restartNote = "Restart failed: " + strings.TrimSpace(err.Error()) + " — restart occa manually (config has no hot-reload)."
			} else {
				restartNote = "Restarted occa (config has no hot-reload, ~5s downtime)."
			}
		}
	}
	lines := []string{"📨 Connect " + repo + " — automation:"}
	for _, res := range results {
		if res.err != nil {
			lines = append(lines, "❌ "+res.workflow+" — create failed: "+strings.TrimSpace(res.err.Error()))
			continue
		}
		if err := deps.PingHook(ctx, repo, res.hookID); err != nil {
			lines = append(lines, "❌ "+res.workflow+" — ping failed: "+strings.TrimSpace(err.Error()))
			continue
		}
		lines = append(lines, "✅ "+res.workflow+" — "+res.hookURL)
	}
	if len(endpoints) == 0 {
		lines = append(lines, "", "⏭️ Config append skipped: no hooks were created; relay not restarted.")
	} else if appendErr != nil {
		lines = append(lines, "", "❌ Config append failed: "+strings.TrimSpace(appendErr.Error()))
	} else if appended {
		lines = append(lines, "", fmt.Sprintf("✅ Config appended (%d endpoints). %s", len(endpoints), restartNote))
	}
	lines = append(lines,
		"",
		"Phase 1 (this instance): fine-grained PAT scoped to "+repo+".",
		"Phase 2 (only for a shared public relay): GitHub App with short-lived per-repo tokens.",
		"Self-hosted relays stay on PAT — the App matters only when strangers install on their repos.",
	)
	return strings.Join(lines, "\n"), true, nil
}

func (r *Router) handleConnectRepo(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	repo := strings.ToLower(strings.TrimSpace(args))
	if repo == "" {
		return "Usage: /connect-repo owner/repo — prints the 4-hook wiring checklist for a repository.", nil
	}
	if !validRepoIdentity(repo) {
		return "⚠️ Invalid repository. Want owner/repo using letters, digits, -, _ or ..", nil
	}
	if r.connectDeps != nil && strings.TrimSpace(r.connectDeps.PublicBaseURL) != "" {
		if result, ok, err := r.runConnectAutomation(ctx, msg, repo); err != nil {
			return "", err
		} else if ok {
			return result, nil
		}
	}
	return r.connectChecklist(repo)
}

func (r *Router) connectChecklist(repo string) (string, error) {
	slug := strings.ReplaceAll(repo, "/", "-")
	secrets := make([]string, 0, 4)
	for range 4 {
		secret, err := newWebhookSecret()
		if err != nil {
			return "", fmt.Errorf("connect-repo secrets: %w", err)
		}
		secrets = append(secrets, secret)
	}
	hooks := []struct{ workflow, suffix string }{
		{"review", "review"},
		{"fix", "pr-fix"},
		{"merge", "merge"},
		{"merged", "merged"},
	}
	lines := []string{
		"📨 Connect " + repo + " — 4 webhooks, one per workflow:",
	}
	for i, hook := range hooks {
		lines = append(lines, fmt.Sprintf("• %-10s /webhooks/github-%s-%s  secret %s", hook.workflow, slug, hook.suffix, secrets[i]))
	}
	lines = append(lines,
		"",
		"Phase 1 (this instance): fine-grained PAT scoped to "+repo+".",
		"Create each hook from the endpoint template:",
		"  gh api repos/"+repo+"/hooks -f name=web -f active=true \\",
		"    -f config[url]=<relay-public-url>/occa/webhooks/github-"+slug+"-<review|pr-fix|merge|merged> \\",
		"    -f config[content_type]=json -f config[secret]=<secret> -f events[]=<event>",
		"Then append the 4 endpoints to config.yaml and restart occa (config has no hot-reload).",
		"Ping-test each hook and expect all 4 ✅ before dispatching.",
		"",
		"Phase 2 (only for a shared public relay): GitHub App with short-lived per-repo tokens.",
		"Self-hosted relays stay on PAT — the App matters only when strangers install on their repos.",
	)
	return strings.Join(lines, "\n"), nil
}
