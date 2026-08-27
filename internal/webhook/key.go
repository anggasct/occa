package webhook

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/anggasct/occa/internal/relay"
)

type WebhookExecutionKey struct {
	Repository     string
	HeadRepository string
	Branch         string
	HeadRevision   string
}

func (k WebhookExecutionKey) String() string {
	if k.IsZero() {
		return ""
	}
	head := k.HeadRepository
	if head == "" {
		head = k.Repository
	}
	return fmt.Sprintf("%s:%s:%s", k.Repository, head, k.Branch)
}

func (k WebhookExecutionKey) IsZero() bool {
	return k.Repository == "" && k.HeadRepository == "" && k.Branch == ""
}

type WebhookWorkContext struct {
	Key         WebhookExecutionKey
	Worktree    string
	SessionKey  string
	Model       *relay.ModelRef
	ModelSource string
}

func ExtractExecutionKey(body []byte) WebhookExecutionKey {
	if len(body) == 0 {
		return WebhookExecutionKey{}
	}

	var raw struct {
		Ref         string `json:"ref"`
		After       string `json:"after"`
		Repository  any    `json:"repository"`
		PullRequest *struct {
			Base struct {
				Repo any    `json:"repo"`
				Ref  string `json:"ref"`
			} `json:"base"`
			Head struct {
				Repo any    `json:"repo"`
				Ref  string `json:"ref"`
				SHA  string `json:"sha"`
			} `json:"head"`
		} `json:"pull_request"`
		HeadCommit *struct {
			ID string `json:"id"`
		} `json:"head_commit"`
		HeadRepository any    `json:"head_repository"`
		HeadRepo       any    `json:"head_repo"`
		HeadRevision   string `json:"head_revision"`
		Branch         string `json:"branch"`
		Repo           any    `json:"repo"`
	}

	if err := json.Unmarshal(body, &raw); err != nil {
		return WebhookExecutionKey{}
	}

	revision := ""
	if raw.PullRequest != nil {
		revision = raw.PullRequest.Head.SHA
	}
	if revision == "" && raw.HeadCommit != nil {
		revision = raw.HeadCommit.ID
	}
	if revision == "" {
		revision = raw.After
	}
	if revision == "" {
		revision = raw.HeadRevision
	}
	revision = normalizeRevision(revision)

	var baseRepo, headRepo, branch string

	if raw.PullRequest != nil {
		baseRepo = extractRepoFullName(raw.PullRequest.Base.Repo)
		headRepo = extractRepoFullName(raw.PullRequest.Head.Repo)
		branch = strings.TrimSpace(raw.PullRequest.Head.Ref)

		// For PR events, baseRepo, headRepo, and branch are all strictly required.
		// Never guess or infer headRepo from baseRepo for PR events.
		if baseRepo == "" || headRepo == "" || branch == "" {
			return WebhookExecutionKey{}
		}

		return WebhookExecutionKey{
			Repository:     baseRepo,
			HeadRepository: headRepo,
			Branch:         branch,
			HeadRevision:   revision,
		}
	}

	if baseRepo == "" {
		baseRepo = extractRepoFullName(raw.Repository)
	}
	if baseRepo == "" {
		baseRepo = extractRepoFullName(raw.Repo)
	}

	if headRepo == "" {
		headRepo = extractRepoFullName(raw.HeadRepository)
	}
	if headRepo == "" {
		headRepo = extractRepoFullName(raw.HeadRepo)
	}
	if headRepo == "" {
		headRepo = baseRepo
	}

	if branch == "" {
		branch = raw.Branch
	}
	if branch == "" && raw.Ref != "" {
		branch = strings.TrimPrefix(raw.Ref, "refs/heads/")
	}

	baseRepo = strings.TrimSpace(baseRepo)
	headRepo = strings.TrimSpace(headRepo)
	branch = strings.TrimSpace(branch)

	if baseRepo == "" || headRepo == "" || branch == "" {
		return WebhookExecutionKey{}
	}

	return WebhookExecutionKey{
		Repository:     baseRepo,
		HeadRepository: headRepo,
		Branch:         branch,
		HeadRevision:   revision,
	}
}

func normalizeRevision(rev string) string {
	rev = strings.ToLower(strings.TrimSpace(rev))
	if len(rev) < 7 || len(rev) > 40 {
		return ""
	}
	for _, r := range rev {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return ""
		}
	}
	return rev
}

func isValidRepoFullName(s string) bool {
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return false
	}
	owner, repo := parts[0], parts[1]
	if owner == "" || repo == "" || owner == "." || owner == ".." || repo == "." || repo == ".." {
		return false
	}
	for _, part := range parts {
		for _, r := range part {
			if !isAllowedRepoChar(r) {
				return false
			}
		}
	}
	return true
}

func isAllowedRepoChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.'
}

func extractRepoFullName(v any) string {
	if v == nil {
		return ""
	}
	var raw string
	switch val := v.(type) {
	case string:
		raw = strings.TrimSpace(val)
	case map[string]any:
		if fn, ok := val["full_name"].(string); ok && strings.TrimSpace(fn) != "" {
			raw = strings.TrimSpace(fn)
		}
	}
	if !isValidRepoFullName(raw) {
		return ""
	}
	return raw
}
