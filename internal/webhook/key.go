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
	Key               WebhookExecutionKey
	Repository        string
	PRNumber          string
	Worktree          string
	ProjectDocsRoot   string
	SessionKey        string
	Model             *relay.ModelRef
	ModelSource       string
	PermissionRuleset relay.PermissionRuleset
}

func ExtractExecutionKey(body []byte) WebhookExecutionKey {
	if len(body) == 0 {
		return WebhookExecutionKey{}
	}

	var raw struct {
		Ref         string `json:"ref"`
		Repository  any    `json:"repository"`
		PullRequest *struct {
			Base struct {
				Repo any    `json:"repo"`
				Ref  string `json:"ref"`
			} `json:"base"`
			Head struct {
				Repo any    `json:"repo"`
				Ref  string `json:"ref"`
			} `json:"head"`
		} `json:"pull_request"`
		HeadRepository any    `json:"head_repository"`
		HeadRepo       any    `json:"head_repo"`
		Branch         string `json:"branch"`
		Repo           any    `json:"repo"`
	}

	if err := json.Unmarshal(body, &raw); err != nil {
		return WebhookExecutionKey{}
	}

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
	}
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
