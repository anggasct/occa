package webhook

import (
	"encoding/json"
	"fmt"
	"strings"
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
	Key        WebhookExecutionKey
	Worktree   string
	SessionKey string
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
		branch = raw.PullRequest.Head.Ref
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

	if baseRepo == "" || branch == "" {
		return WebhookExecutionKey{}
	}

	return WebhookExecutionKey{
		Repository:     baseRepo,
		HeadRepository: headRepo,
		Branch:         branch,
	}
}

func extractRepoFullName(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return strings.TrimSpace(val)
	case map[string]any:
		if fn, ok := val["full_name"].(string); ok && strings.TrimSpace(fn) != "" {
			return strings.TrimSpace(fn)
		}
		if n, ok := val["name"].(string); ok && strings.TrimSpace(n) != "" {
			return strings.TrimSpace(n)
		}
	}
	return ""
}
