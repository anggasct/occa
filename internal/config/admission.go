package config

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

func mustEncodeNode(node *yaml.Node) []byte {
	var buf bytes.Buffer
	if err := yaml.NewEncoder(&buf).Encode(node); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

type AdmitRule struct {
	Event         string   `yaml:"event"`
	Actions       []string `yaml:"actions,omitempty"`
	ReviewState   []string `yaml:"review_state,omitempty"`
	ReviewVerdict []string `yaml:"review_verdict,omitempty"`
	Unless        []string `yaml:"unless,omitempty"`
	Require       []string `yaml:"require,omitempty"`
	CheckStatus   string   `yaml:"check_status,omitempty"`
	CheckApp      string   `yaml:"check_app,omitempty"`
	Merged        *bool    `yaml:"merged,omitempty"`
}

type EndpointLimits struct {
	MaxRunsPerPR int           `yaml:"max_runs_per_pr"`
	Window       time.Duration `yaml:"window"`
}

type WebhookPolicy struct {
	TrustReviewLogins []string            `yaml:"trust_review_logins"`
	Verdicts          map[string][]string `yaml:"verdicts"`
}

type WebhookRuntime struct {
	MaxBodySize            byteSize        `yaml:"max_body_size"`
	MaxConcurrentEvents    int             `yaml:"max_concurrent_events"`
	MaxQueuedPerKey        int             `yaml:"max_queued_per_key"`
	ProcessingTimeout      time.Duration   `yaml:"processing_timeout"`
	ClaimGrace             time.Duration   `yaml:"claim_grace"`
	RetryAfter             time.Duration   `yaml:"retry_after"`
	WorkspaceRetryBackoff  []time.Duration `yaml:"workspace_retry_backoff"`
	Retention              time.Duration   `yaml:"retention"`
	RetentionKeep          int             `yaml:"retention_keep"`
	PruneInterval          time.Duration   `yaml:"prune_interval"`
	DispatcherIdleTTL      time.Duration   `yaml:"dispatcher_idle_ttl"`
	HTTPReadHeaderTimeout  time.Duration   `yaml:"http_read_header_timeout"`
	HTTPReadTimeout        time.Duration   `yaml:"http_read_timeout"`
	HTTPWriteTimeout       time.Duration   `yaml:"http_write_timeout"`
	HTTPIdleTimeout        time.Duration   `yaml:"http_idle_timeout"`
	ReviewDedupeWindow     time.Duration   `yaml:"review_dedupe_window"`
	IsolatedWorkspaceTTL   time.Duration   `yaml:"isolated_workspace_ttl"`
	UsageRetention         time.Duration   `yaml:"usage_retention"`
	UsageMaxRows           int             `yaml:"usage_max_rows"`
	RecoveryEventRetention time.Duration   `yaml:"recovery_event_retention"`
}

var knownRequireFlags = map[string]bool{
	"comment_trigger":            true,
	"comment_trigger_configured": true,
	"pr_open":                    true,
	"pr_resolvable":              true,
	"approved_without_findings":  true,
}

var knownVerdictNames = map[string]bool{
	"approved":        true,
	"request_changes": true,
}

func validateAdmitRules(endpointName string, rules []AdmitRule, policy WebhookPolicy) error {
	if len(rules) == 0 {
		return fmt.Errorf("config: webhooks endpoint %q: admit is required (at least one rule)", endpointName)
	}
	for i, rule := range rules {
		prefix := fmt.Sprintf("config: webhooks endpoint %q admit[%d]", endpointName, i)
		if strings.TrimSpace(rule.Event) == "" {
			return fmt.Errorf("%s: event is required", prefix)
		}
		for _, action := range rule.Actions {
			if strings.TrimSpace(action) == "" {
				return fmt.Errorf("%s: actions must not contain empty entries", prefix)
			}
		}
		for _, state := range rule.ReviewState {
			if strings.TrimSpace(state) == "" {
				return fmt.Errorf("%s: review_state must not contain empty entries", prefix)
			}
		}
		for _, verdict := range rule.ReviewVerdict {
			name := strings.ToLower(strings.TrimSpace(verdict))
			if name == "" {
				return fmt.Errorf("%s: review_verdict must not contain empty entries", prefix)
			}
			if !knownVerdictNames[name] {
				return fmt.Errorf("%s: unknown review_verdict %q", prefix, verdict)
			}
			if _, ok := policy.Verdicts[name]; !ok {
				return fmt.Errorf("%s: review_verdict %q is not defined in webhooks.policy.verdicts", prefix, name)
			}
		}
		for _, flag := range rule.Require {
			if !knownRequireFlags[strings.TrimSpace(flag)] {
				return fmt.Errorf("%s: unknown require flag %q", prefix, flag)
			}
		}
		if rule.CheckStatus != "" && strings.ToLower(strings.TrimSpace(rule.CheckStatus)) != "completed" {
			return fmt.Errorf("%s: check_status only supports \"completed\", got %q", prefix, rule.CheckStatus)
		}
	}
	return nil
}

func validateWebhookPolicy(policy WebhookPolicy) (WebhookPolicy, error) {
	for i, login := range policy.TrustReviewLogins {
		if strings.TrimSpace(login) == "" {
			return WebhookPolicy{}, fmt.Errorf("config: webhooks.policy.trust_review_logins[%d] must not be empty", i)
		}
	}
	if len(policy.Verdicts) == 0 {
		return WebhookPolicy{}, fmt.Errorf("config: webhooks.policy.verdicts is required")
	}
	seen := make(map[string]bool, len(policy.Verdicts))
	for name, phrases := range policy.Verdicts {
		lower := strings.ToLower(strings.TrimSpace(name))
		if !knownVerdictNames[lower] {
			return WebhookPolicy{}, fmt.Errorf("config: webhooks.policy.verdicts has unknown verdict %q", name)
		}
		if seen[lower] {
			return WebhookPolicy{}, fmt.Errorf("config: webhooks.policy.verdicts duplicates verdict %q", name)
		}
		seen[lower] = true
		if len(phrases) == 0 {
			return WebhookPolicy{}, fmt.Errorf("config: webhooks.policy.verdicts[%s] must list at least one phrase", name)
		}
		for j, phrase := range phrases {
			if strings.TrimSpace(phrase) == "" {
				return WebhookPolicy{}, fmt.Errorf("config: webhooks.policy.verdicts[%s][%d] must not be empty", name, j)
			}
		}
	}
	for name := range knownVerdictNames {
		if !seen[name] {
			return WebhookPolicy{}, fmt.Errorf("config: webhooks.policy.verdicts is missing verdict %q", name)
		}
	}
	return policy, nil
}

func usesCommentTrigger(rules []AdmitRule) bool {
	for _, rule := range rules {
		for _, flag := range rule.Require {
			if strings.TrimSpace(flag) == "comment_trigger" {
				return true
			}
		}
	}
	return false
}

func validateEndpointLimits(endpointName string, limits *EndpointLimits, rules []AdmitRule) error {
	if usesCommentTrigger(rules) {
		if limits == nil {
			return fmt.Errorf("config: webhooks endpoint %q: limits is required when admit rules use comment_trigger", endpointName)
		}
		if limits.MaxRunsPerPR <= 0 {
			return fmt.Errorf("config: webhooks endpoint %q: limits.max_runs_per_pr must be > 0", endpointName)
		}
		if limits.Window <= 0 {
			return fmt.Errorf("config: webhooks endpoint %q: limits.window must be > 0", endpointName)
		}
		return nil
	}
	if limits != nil {
		if limits.MaxRunsPerPR < 0 {
			return fmt.Errorf("config: webhooks endpoint %q: limits.max_runs_per_pr must be >= 0", endpointName)
		}
		if limits.Window < 0 {
			return fmt.Errorf("config: webhooks endpoint %q: limits.window must be >= 0", endpointName)
		}
	}
	return nil
}

func validateWebhookRuntime(runtime WebhookRuntime) (WebhookRuntime, error) {
	missing := func(key string) error {
		return fmt.Errorf("config: webhooks.runtime.%s is required", key)
	}
	if runtime.MaxBodySize <= 0 {
		return WebhookRuntime{}, missing("max_body_size")
	}
	if runtime.MaxConcurrentEvents <= 0 {
		return WebhookRuntime{}, missing("max_concurrent_events")
	}
	if runtime.MaxQueuedPerKey <= 0 {
		return WebhookRuntime{}, missing("max_queued_per_key")
	}
	if runtime.ProcessingTimeout <= 0 {
		return WebhookRuntime{}, missing("processing_timeout")
	}
	if runtime.ClaimGrace <= 0 {
		return WebhookRuntime{}, missing("claim_grace")
	}
	if runtime.RetryAfter <= 0 {
		return WebhookRuntime{}, missing("retry_after")
	}
	if len(runtime.WorkspaceRetryBackoff) == 0 {
		return WebhookRuntime{}, missing("workspace_retry_backoff")
	}
	for i, backoff := range runtime.WorkspaceRetryBackoff {
		if backoff <= 0 {
			return WebhookRuntime{}, fmt.Errorf("config: webhooks.runtime.workspace_retry_backoff[%d] must be > 0", i)
		}
	}
	if runtime.Retention <= 0 {
		return WebhookRuntime{}, missing("retention")
	}
	if runtime.RetentionKeep <= 0 {
		return WebhookRuntime{}, missing("retention_keep")
	}
	if runtime.PruneInterval <= 0 {
		return WebhookRuntime{}, missing("prune_interval")
	}
	if runtime.DispatcherIdleTTL <= 0 {
		return WebhookRuntime{}, missing("dispatcher_idle_ttl")
	}
	if runtime.HTTPReadHeaderTimeout <= 0 {
		return WebhookRuntime{}, missing("http_read_header_timeout")
	}
	if runtime.HTTPReadTimeout <= 0 {
		return WebhookRuntime{}, missing("http_read_timeout")
	}
	if runtime.HTTPWriteTimeout <= 0 {
		return WebhookRuntime{}, missing("http_write_timeout")
	}
	if runtime.HTTPIdleTimeout <= 0 {
		return WebhookRuntime{}, missing("http_idle_timeout")
	}
	if runtime.ReviewDedupeWindow <= 0 {
		return WebhookRuntime{}, missing("review_dedupe_window")
	}
	if runtime.IsolatedWorkspaceTTL <= 0 {
		return WebhookRuntime{}, missing("isolated_workspace_ttl")
	}
	if runtime.UsageRetention <= 0 {
		return WebhookRuntime{}, missing("usage_retention")
	}
	if runtime.UsageMaxRows <= 0 {
		return WebhookRuntime{}, missing("usage_max_rows")
	}
	if runtime.RecoveryEventRetention <= 0 {
		return WebhookRuntime{}, missing("recovery_event_retention")
	}
	return runtime, nil
}

func ParseByteSize(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	lower := strings.ToLower(s)
	multiplier := int64(1)
	number := lower
	switch {
	case strings.HasSuffix(lower, "kb"):
		multiplier = 1024
		number = strings.TrimSpace(s[:len(s)-2])
	case strings.HasSuffix(lower, "mb"):
		multiplier = 1024 * 1024
		number = strings.TrimSpace(s[:len(s)-2])
	case strings.HasSuffix(lower, "gb"):
		multiplier = 1024 * 1024 * 1024
		number = strings.TrimSpace(s[:len(s)-2])
	case strings.HasSuffix(lower, "b"):
		number = strings.TrimSpace(s[:len(s)-1])
	default:
		if strings.ContainsAny(lower, "abcdefghijklmnopqrstuvwxyz") {
			return 0, fmt.Errorf("invalid size %q", raw)
		}
	}
	var value int64
	number = strings.TrimSpace(number)
	if number == "" {
		return 0, fmt.Errorf("invalid size %q", raw)
	}
	if strings.ContainsAny(number, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") {
		return 0, fmt.Errorf("invalid size %q", raw)
	}
	parsed, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", raw)
	}
	value = parsed
	if value <= 0 {
		return 0, fmt.Errorf("size %q must be > 0", raw)
	}
	return value * multiplier, nil
}

type byteSize int64

type ByteSize = byteSize

func (b *byteSize) UnmarshalYAML(node *yaml.Node) error {
	var raw string
	if err := node.Decode(&raw); err != nil {
		return err
	}
	parsed, err := ParseByteSize(raw)
	if err != nil {
		return err
	}
	*b = byteSize(parsed)
	return nil
}
