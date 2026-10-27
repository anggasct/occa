package config

import (
	"fmt"
	"log/slog"
	"regexp"
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

type RepoPromptRef struct {
	Prompt     string `yaml:"prompt,omitempty"`
	PromptFile string `yaml:"prompt_file,omitempty"`
}

type RepoConfig struct {
	Repo            string                   `yaml:"repo"`
	Provider        string                   `yaml:"provider"`
	Platform        string                   `yaml:"platform"`
	ChannelID       string                   `yaml:"channel_id"`
	ForumChannelID  string                   `yaml:"forum_channel_id,omitempty"`
	Secret          string                   `yaml:"secret"`
	Checkout        string                   `yaml:"checkout"`
	Model           string                   `yaml:"model,omitempty"`
	CommentTrigger  map[string][]string      `yaml:"comment_trigger,omitempty"`
	CommentAuthors  []string                 `yaml:"comment_authors,omitempty"`
	IgnoreAuthors   []string                 `yaml:"ignore_authors,omitempty"`
	Limits          *EndpointLimits          `yaml:"limits,omitempty"`
	RefreshInterval time.Duration            `yaml:"refresh_interval,omitempty"`
	IdleAfter       time.Duration            `yaml:"idle_after,omitempty"`
	Automation      RepoAutomation           `yaml:"automation,omitempty"`
	OnMerged        []MergedAction           `yaml:"on_merged,omitempty"`
	Prompts         map[string]RepoPromptRef `yaml:"prompts,omitempty"`
}

type WebhookDefaults struct {
	CommentTrigger    map[string][]string `yaml:"comment_trigger,omitempty"`
	ForumChannelID    string              `yaml:"forum_channel_id,omitempty"`
	CommentAuthors    []string            `yaml:"comment_authors,omitempty"`
	IgnoreAuthors     []string            `yaml:"ignore_authors,omitempty"`
	Limits            *EndpointLimits     `yaml:"limits,omitempty"`
	Thread            *bool               `yaml:"thread,omitempty"`
	EscalationMention string              `yaml:"escalation_mention,omitempty"`
	VerboseSteps      bool                `yaml:"verbose_steps,omitempty"`
	StatusIcons       map[string]string   `yaml:"status_icons,omitempty"`
	RefreshInterval   time.Duration       `yaml:"refresh_interval,omitempty"`
	IdleAfter         time.Duration       `yaml:"idle_after,omitempty"`
}

const (
	DefaultRefreshInterval = 30 * time.Second
	DefaultIdleAfter       = 90 * time.Second
)

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

func validateRepoPrompts(prompts map[string]RepoPromptRef, context string) error {
	for workflow, ref := range prompts {
		if workflow != "review" && workflow != "fix" && workflow != "merge" && workflow != "merged" {
			return fmt.Errorf("config: %sprompts supports only review, fix, merge and merged workflows, got %q", context, workflow)
		}
		hasPrompt := strings.TrimSpace(ref.Prompt) != ""
		hasFile := strings.TrimSpace(ref.PromptFile) != ""
		if hasPrompt == hasFile {
			return fmt.Errorf("config: %sprompts[%s] must define exactly one of prompt, prompt_file", context, workflow)
		}
	}
	return nil
}

func resolveRepoPrompt(prompts map[string]RepoPromptRef, workflow string) (prompt, promptFile, source string) {
	if ref, ok := prompts[workflow]; ok {
		if strings.TrimSpace(ref.Prompt) != "" {
			return ref.Prompt, "", "inline"
		}
		return "", strings.TrimSpace(ref.PromptFile), "file"
	}
	return BuiltinPrompt(workflow), "", "builtin"
}

// resolveDeliveryChannel picks where a repository's webhook runs are posted.
// A forum destination wins over the legacy per-repo channel so one forum can
// host every project, while an unset forum keeps the pre-forum behaviour and a
// per-repo forum_channel_id isolates a project into its own forum.
func resolveDeliveryChannel(repo RepoConfig, defaults WebhookDefaults) string {
	if id := strings.TrimSpace(repo.ForumChannelID); id != "" {
		return id
	}
	if id := strings.TrimSpace(defaults.ForumChannelID); id != "" {
		return id
	}
	return strings.TrimSpace(repo.ChannelID)
}

func resolveTriggers(defaults WebhookDefaults, repo RepoConfig, workflow string) []string {
	var base []string
	if defaults.CommentTrigger != nil {
		if triggers, ok := defaults.CommentTrigger[workflow]; ok {
			base = append([]string(nil), triggers...)
		}
	}
	if base == nil {
		switch workflow {
		case "review":
			base = append([]string(nil), defaultReviewTriggers...)
		case "fix":
			base = append([]string(nil), defaultFixTriggers...)
		}
	}
	if repo.CommentTrigger != nil {
		if triggers, ok := repo.CommentTrigger[workflow]; ok {
			base = mergeTriggers(base, triggers)
		}
	}
	return base
}

func mergeTriggers(base, additions []string) []string {
	out := append([]string(nil), base...)
	seen := make(map[string]bool, len(out)+len(additions))
	for _, trigger := range out {
		seen[strings.ToLower(strings.TrimSpace(trigger))] = true
	}
	for _, trigger := range additions {
		trigger = strings.TrimSpace(trigger)
		if trigger == "" {
			continue
		}
		key := strings.ToLower(trigger)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, trigger)
	}
	return out
}

func resolveCommentAuthors(defaults WebhookDefaults, repo RepoConfig) []string {
	source := repo.CommentAuthors
	if len(source) == 0 {
		source = defaults.CommentAuthors
	}
	out := make([]string, 0, len(source))
	seen := make(map[string]bool, len(source))
	for _, login := range source {
		trimmed := strings.TrimSpace(login)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, trimmed)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

var ignoreAuthorPattern = regexp.MustCompile(`^[A-Za-z0-9._\[\]-]+$`)

func validateIgnoreAuthors(values []string, context string) error {
	for i, login := range values {
		trimmed := strings.TrimSpace(login)
		if trimmed == "" {
			return fmt.Errorf("config: %signore_authors must not contain empty entries", context)
		}
		if !ignoreAuthorPattern.MatchString(trimmed) {
			return fmt.Errorf("config: %signore_authors[%d] must match [A-Za-z0-9-_.\\[\\]]+, got %q", context, i, login)
		}
	}
	return nil
}

func resolveIgnoreAuthors(defaults WebhookDefaults, repo RepoConfig) []string {
	source := repo.IgnoreAuthors
	if len(source) == 0 {
		source = defaults.IgnoreAuthors
	}
	out := make([]string, 0, len(source))
	seen := make(map[string]bool, len(source))
	for _, login := range source {
		trimmed := strings.TrimSpace(login)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, trimmed)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func resolveRefreshInterval(defaults WebhookDefaults, repo RepoConfig) time.Duration {
	if repo.RefreshInterval > 0 {
		return repo.RefreshInterval
	}
	if defaults.RefreshInterval > 0 {
		return defaults.RefreshInterval
	}
	return DefaultRefreshInterval
}

func resolveIdleAfter(defaults WebhookDefaults, repo RepoConfig) time.Duration {
	if repo.IdleAfter > 0 {
		return repo.IdleAfter
	}
	if defaults.IdleAfter > 0 {
		return defaults.IdleAfter
	}
	return DefaultIdleAfter
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
	for _, login := range defaults.CommentAuthors {
		if strings.TrimSpace(login) == "" {
			return nil, fmt.Errorf("config: webhooks.defaults.comment_authors must not contain empty entries")
		}
	}
	if err := validateIgnoreAuthors(defaults.IgnoreAuthors, "webhooks.defaults."); err != nil {
		return nil, err
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
		if strings.TrimSpace(resolveDeliveryChannel(*repo, defaults)) == "" {
			return nil, fmt.Errorf("config: %schannel_id or forum_channel_id must not be empty", context)
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
		for _, login := range repo.CommentAuthors {
			if strings.TrimSpace(login) == "" {
				return nil, fmt.Errorf("config: %scomment_authors must not contain empty entries", context)
			}
		}
		if err := validateIgnoreAuthors(repo.IgnoreAuthors, context); err != nil {
			return nil, err
		}
		automation, err := normalizeAutomation(repo.Automation, context)
		if err != nil {
			return nil, err
		}
		if err := validateMergedActions(repo.OnMerged, context); err != nil {
			return nil, err
		}
		if err := validateRepoPrompts(repo.Prompts, context); err != nil {
			return nil, err
		}
		automationCopy := automation
		for _, workflow := range []string{"review", "fix", "merge", "merged"} {
			suffix := workflow
			if workflow == "fix" {
				suffix = "pr-fix"
			}
			prompt, promptFile, source := resolveRepoPrompt(repo.Prompts, workflow)
			ep := EndpointConfig{
				Name:            "github-" + short + "-" + suffix,
				Path:            "/webhooks/github-" + short + "-" + suffix,
				Auth:            "github_hmac_sha256",
				Secret:          strings.TrimSpace(repo.Secret),
				Workflow:        workflow,
				Platform:        strings.TrimSpace(repo.Platform),
				ChannelID:       resolveDeliveryChannel(*repo, defaults),
				Prompt:          prompt,
				PromptFile:      promptFile,
				PromptSource:    source,
				CommentTrigger:  resolveTriggers(defaults, *repo, workflow),
				CommentAuthors:  resolveCommentAuthors(defaults, *repo),
				IgnoreAuthors:   resolveIgnoreAuthors(defaults, *repo),
				Thread:          true,
				NotifySkip:      boolPtr(false),
				Model:           strings.TrimSpace(repo.Model),
				Repository:      "github.com/" + name,
				Admit:           lockedAdmitRules(workflow),
				Limits:          resolveRepoLimits(defaults, *repo),
				RefreshInterval: resolveRefreshInterval(defaults, *repo),
				IdleAfter:       resolveIdleAfter(defaults, *repo),
				Automation:      &automationCopy,
				VerifyEndpoint:  "",
			}
			if len(ep.CommentTrigger) > 0 {
				slog.Info("config: effective comment triggers", "endpoint", ep.Name, "workflow", workflow, "triggers", strings.Join(ep.CommentTrigger, ", "))
			}
			if workflow == "merged" {
				ep.OnMerged = append([]MergedAction(nil), repo.OnMerged...)
			}
			switch workflow {
			case "review":
				ep.Workspace = EndpointWorkspace{Type: WorkspaceTypeGit, Path: strings.TrimSpace(repo.Checkout), Mode: WorkspaceModeIsolated}
			case "fix":
				ep.Workspace = EndpointWorkspace{Type: WorkspaceTypeGit, Path: strings.TrimSpace(repo.Checkout), Mode: WorkspaceModeMutable}
			default:
				ep.Workspace = EndpointWorkspace{Type: WorkspaceTypeNone}
			}
			out = append(out, ep)
		}
	}
	return out, nil
}

func boolPtr(v bool) *bool { return &v }
