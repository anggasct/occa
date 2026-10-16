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

func (r *Router) handleConnectRepo(_ context.Context, _ channel.IncomingMessage, args string) (string, error) {
	repo := strings.ToLower(strings.TrimSpace(args))
	if repo == "" {
		return "Usage: /connect-repo owner/repo — prints the 4-hook wiring checklist for a repository.", nil
	}
	if !validRepoIdentity(repo) {
		return "⚠️ Invalid repository. Want owner/repo using letters, digits, -, _ or ..", nil
	}
	slug := strings.ReplaceAll(repo, "/", "-")
	secrets := make([]string, 0, 4)
	for range 4 {
		secret, err := newWebhookSecret()
		if err != nil {
			return "", fmt.Errorf("connect-repo secrets: %w", err)
		}
		secrets = append(secrets, secret[:16]+"…")
	}
	workflows := []string{"review", "review-fix", "merge", "merged"}
	lines := []string{
		"📨 Connect " + repo + " — 4 webhooks, one per workflow:",
	}
	for i, workflow := range workflows {
		lines = append(lines, fmt.Sprintf("• %-10s /webhooks/github-%s-%s  secret %s", workflow, slug, workflow, secrets[i]))
	}
	lines = append(lines,
		"",
		"Phase 1 (this instance): fine-grained PAT scoped to "+repo+".",
		"Create each hook from the endpoint template:",
		"  gh api repos/"+repo+"/hooks -f name=web -f active=true \\",
		"    -f config[url]=<relay-public-url>/occa/webhooks/github-"+slug+"-<review|review-fix|merge|merged> \\",
		"    -f config[content_type]=json -f config[secret]=<secret> -f events[]=<event>",
		"Then append the 4 endpoints to config.yaml and restart occa (config has no hot-reload).",
		"Ping-test each hook and expect all 4 ✅ before dispatching.",
		"",
		"Phase 2 (only for a shared public relay): GitHub App with short-lived per-repo tokens.",
		"Self-hosted relays stay on PAT — the App matters only when strangers install on their repos.",
	)
	return strings.Join(lines, "\n"), nil
}
