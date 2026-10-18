package config

import (
	"fmt"
	"strings"
	"time"
)

const (
	AutomationFull       = "full"
	AutomationReviewOnly = "review-only"
)

type RepoAutomation struct {
	Default string            `yaml:"default,omitempty"`
	Authors map[string]string `yaml:"authors,omitempty"`
}

type MergedAction struct {
	Prompt     string `yaml:"prompt,omitempty"`
	PromptFile string `yaml:"prompt_file,omitempty"`
	Run        string `yaml:"run,omitempty"`
}

type RepoConfig struct {
	Repo           string              `yaml:"repo"`
	Provider       string              `yaml:"provider"`
	Platform       string              `yaml:"platform"`
	ChannelID      string              `yaml:"channel_id"`
	Secret         string              `yaml:"secret"`
	Checkout       string              `yaml:"checkout"`
	Model          string              `yaml:"model,omitempty"`
	CommentTrigger map[string][]string `yaml:"comment_trigger,omitempty"`
	Limits         *EndpointLimits     `yaml:"limits,omitempty"`
	Automation     RepoAutomation      `yaml:"automation,omitempty"`
	OnMerged       []MergedAction      `yaml:"on_merged,omitempty"`
}

type WebhookDefaults struct {
	CommentTrigger    map[string][]string `yaml:"comment_trigger,omitempty"`
	Limits            *EndpointLimits     `yaml:"limits,omitempty"`
	Thread            *bool               `yaml:"thread,omitempty"`
	EscalationMention string              `yaml:"escalation_mention,omitempty"`
}

var defaultReviewTriggers = []string{"please re-review"}
var defaultFixTriggers = []string{"please fix review", "please fix ci"}

var defaultRepoLimits = &EndpointLimits{MaxRunsPerPR: 3, Window: 60 * time.Minute}

func lockedAdmitRules(workflow string) []AdmitRule {
	merged := true
	switch workflow {
	case "review":
		return []AdmitRule{
			{Event: "pull_request", Actions: []string{"opened", "reopened", "ready_for_review"}},
			{Event: "issue_comment", Actions: []string{"created"}, Require: []string{"comment_trigger", "pr_open"}},
		}
	case "fix":
		return []AdmitRule{
			{Event: "pull_request_review", Actions: []string{"submitted"}, ReviewState: []string{"changes_requested"}},
			{Event: "pull_request_review", Actions: []string{"submitted"}, ReviewState: []string{"commented"}, ReviewVerdict: []string{"request_changes"}},
			{Event: "issue_comment", Actions: []string{"created"}, Require: []string{"comment_trigger", "pr_open"}},
		}
	case "merge":
		return []AdmitRule{
			{Event: "pull_request_review", Actions: []string{"submitted"}, ReviewState: []string{"approved"}},
			{Event: "pull_request_review", Actions: []string{"submitted"}, ReviewState: []string{"commented"}, ReviewVerdict: []string{"approved"}, Require: []string{"approved_without_findings"}},
			{Event: "check_suite", Actions: []string{"completed"}, CheckStatus: "completed", CheckApp: "GitHub Actions", Require: []string{"pr_open", "pr_resolvable"}},
		}
	case "merged":
		return []AdmitRule{
			{Event: "pull_request", Actions: []string{"closed"}, Merged: &merged},
		}
	}
	return nil
}

func lockedPromptFile(workflow string) string {
	switch workflow {
	case "review":
		return "webhooks/github-review.md"
	case "fix":
		return "webhooks/github-review-fix.md"
	case "merge":
		return "webhooks/github-merge.md"
	case "merged":
		return "webhooks/github-merged.md"
	}
	return ""
}

func resolveTriggers(defaults WebhookDefaults, repo RepoConfig, workflow string) []string {
	if repo.CommentTrigger != nil {
		if triggers, ok := repo.CommentTrigger[workflow]; ok {
			return triggers
		}
	}
	if defaults.CommentTrigger != nil {
		if triggers, ok := defaults.CommentTrigger[workflow]; ok {
			return triggers
		}
	}
	switch workflow {
	case "review":
		return append([]string(nil), defaultReviewTriggers...)
	case "fix":
		return append([]string(nil), defaultFixTriggers...)
	}
	return nil
}

func resolveRepoLimits(defaults WebhookDefaults, repo RepoConfig) *EndpointLimits {
	if repo.Limits != nil {
		return &EndpointLimits{MaxRunsPerPR: repo.Limits.MaxRunsPerPR, Window: repo.Limits.Window}
	}
	if defaults.Limits != nil {
		return &EndpointLimits{MaxRunsPerPR: defaults.Limits.MaxRunsPerPR, Window: defaults.Limits.Window}
	}
	return &EndpointLimits{MaxRunsPerPR: defaultRepoLimits.MaxRunsPerPR, Window: defaultRepoLimits.Window}
}

func normalizeAutomation(automation RepoAutomation, context string) (RepoAutomation, error) {
	out := RepoAutomation{Authors: map[string]string{}}
	mode := strings.TrimSpace(strings.ToLower(automation.Default))
	if mode == "" {
		mode = AutomationReviewOnly
	}
	if mode != AutomationFull && mode != AutomationReviewOnly {
		return RepoAutomation{}, fmt.Errorf("config: %sautomation.default must be %q or %q, got %q", context, AutomationFull, AutomationReviewOnly, automation.Default)
	}
	out.Default = mode
	for author, authorMode := range automation.Authors {
		author = strings.TrimSpace(author)
		authorMode = strings.TrimSpace(strings.ToLower(authorMode))
		if author == "" {
			return RepoAutomation{}, fmt.Errorf("config: %sautomation.authors must not contain empty author", context)
		}
		if authorMode != AutomationFull && authorMode != AutomationReviewOnly {
			return RepoAutomation{}, fmt.Errorf("config: %sautomation.authors[%q] must be %q or %q, got %q", context, author, AutomationFull, AutomationReviewOnly, automation.Authors[author])
		}
		out.Authors[author] = authorMode
	}
	return out, nil
}

func validateMergedActions(actions []MergedAction, context string) error {
	for i, action := range actions {
		set := 0
		if strings.TrimSpace(action.Prompt) != "" {
			set++
		}
		if strings.TrimSpace(action.PromptFile) != "" {
			set++
		}
		if strings.TrimSpace(action.Run) != "" {
			set++
		}
		if set == 0 {
			return fmt.Errorf("config: %son_merged[%d] must define exactly one of prompt, prompt_file, run", context, i)
		}
		if set > 1 {
			return fmt.Errorf("config: %son_merged[%d] must define only one of prompt, prompt_file, run", context, i)
		}
	}
	return nil
}

func expandRepos(repos []RepoConfig, defaults WebhookDefaults) ([]EndpointConfig, error) {
	if defaults.Thread != nil && !*defaults.Thread {
		return nil, fmt.Errorf("config: webhooks.defaults.thread is locked true by the per-PR queue")
	}
	seen := map[string]struct{}{}
	var out []EndpointConfig
	for i := range repos {
		repo := &repos[i]
		context := fmt.Sprintf("webhooks.repos[%d] (%s): ", i, repo.Repo)
		name := strings.TrimSpace(repo.Repo)
		if name == "" || strings.ContainsAny(name, " \t\n") || !strings.Contains(name, "/") {
			return nil, fmt.Errorf("config: webhooks.repos[%d].repo must be an owner/name identity, got %q", i, repo.Repo)
		}
		short := name[strings.LastIndex(name, "/")+1:]
		if short == "" {
			return nil, fmt.Errorf("config: webhooks.repos[%d].repo must be an owner/name identity, got %q", i, repo.Repo)
		}
		if _, dup := seen[strings.ToLower(name)]; dup {
			return nil, fmt.Errorf("config: webhooks.repos[%d].repo duplicates %q", i, name)
		}
		seen[strings.ToLower(name)] = struct{}{}
		provider := strings.TrimSpace(strings.ToLower(repo.Provider))
		if provider != "github" {
			return nil, fmt.Errorf("config: %sprovider must be %q, got %q", context, "github", repo.Provider)
		}
		if strings.TrimSpace(repo.Platform) == "" {
			return nil, fmt.Errorf("config: %splatform must not be empty", context)
		}
		if strings.TrimSpace(repo.ChannelID) == "" {
			return nil, fmt.Errorf("config: %schannel_id must not be empty", context)
		}
		if strings.TrimSpace(repo.Secret) == "" {
			return nil, fmt.Errorf("config: %ssecret must not be empty", context)
		}
		if strings.TrimSpace(repo.Checkout) == "" {
			return nil, fmt.Errorf("config: %scheckout must not be empty", context)
		}
		for workflow := range repo.CommentTrigger {
			if workflow != "review" && workflow != "fix" {
				return nil, fmt.Errorf("config: %scomment_trigger supports only review and fix workflows, got %q", context, workflow)
			}
		}
		automation, err := normalizeAutomation(repo.Automation, context)
		if err != nil {
			return nil, err
		}
		if err := validateMergedActions(repo.OnMerged, context); err != nil {
			return nil, err
		}
		automationCopy := automation
		for _, workflow := range []string{"review", "fix", "merge", "merged"} {
			suffix := workflow
			if workflow == "fix" {
				suffix = "review-fix"
			}
			ep := EndpointConfig{
				Name:           "github-" + short + "-" + suffix,
				Path:           "/webhooks/github-" + short + "-" + suffix,
				Auth:           "github_hmac_sha256",
				Secret:         strings.TrimSpace(repo.Secret),
				Workflow:       workflow,
				Platform:       strings.TrimSpace(repo.Platform),
				ChannelID:      strings.TrimSpace(repo.ChannelID),
				PromptFile:     lockedPromptFile(workflow),
				CommentTrigger: resolveTriggers(defaults, *repo, workflow),
				Thread:         true,
				NotifySkip:     boolPtr(false),
				Model:          strings.TrimSpace(repo.Model),
				Repository:     "github.com/" + name,
				Admit:          lockedAdmitRules(workflow),
				Limits:         resolveRepoLimits(defaults, *repo),
				Automation:     &automationCopy,
				VerifyEndpoint: "",
			}
			if workflow == "merged" {
				ep.OnMerged = append([]MergedAction(nil), repo.OnMerged...)
			}
			if workflow == "fix" {
				ep.Workspace = EndpointWorkspace{Type: WorkspaceTypeGit, Path: strings.TrimSpace(repo.Checkout), Mode: WorkspaceModeIsolated}
			} else {
				ep.Workspace = EndpointWorkspace{Type: WorkspaceTypeNone}
			}
			out = append(out, ep)
		}
	}
	return out, nil
}

func boolPtr(v bool) *bool { return &v }
