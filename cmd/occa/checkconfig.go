package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
)

func runWebhooksCheckConfig(args []string) int {
	fs := flag.NewFlagSet("check-config", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to config file (default ~/.occa/config.yaml)")
	shortPath := fs.String("c", "", "path to config file (shorthand)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	path := *configPath
	if path == "" {
		path = *shortPath
	}
	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "occa webhooks check-config: %v\n", err)
		return 1
	}
	rt := cfg.Webhooks.Runtime
	st := cfg.Runtime.Store
	fmt.Println("webhooks policy:")
	fmt.Printf("  trust_review_logins: [%s]\n", strings.Join(cfg.Webhooks.Policy.TrustReviewLogins, ", "))
	for verdict, phrases := range cfg.Webhooks.Policy.Verdicts {
		fmt.Printf("  verdicts.%s: [%s]\n", verdict, strings.Join(phrases, ", "))
	}
	fmt.Println("webhooks runtime:")
	fmt.Printf("  max_body_size: %d\n", int64(rt.MaxBodySize))
	fmt.Printf("  max_concurrent_events: %d\n", rt.MaxConcurrentEvents)
	fmt.Printf("  max_queued_per_key: %d\n", rt.MaxQueuedPerKey)
	fmt.Printf("  processing_timeout: %s\n", rt.ProcessingTimeout)
	fmt.Printf("  claim_grace: %s\n", rt.ClaimGrace)
	fmt.Printf("  retry_after: %s\n", rt.RetryAfter)
	fmt.Printf("  workspace_retry_backoff: %s\n", joinDurations(rt.WorkspaceRetryBackoff))
	fmt.Printf("  retention: %s keep=%d\n", rt.Retention, rt.RetentionKeep)
	fmt.Printf("  prune_interval: %s\n", rt.PruneInterval)
	fmt.Printf("  dispatcher_idle_ttl: %s\n", rt.DispatcherIdleTTL)
	fmt.Printf("  http: read_header=%s read=%s write=%s idle=%s\n", rt.HTTPReadHeaderTimeout, rt.HTTPReadTimeout, rt.HTTPWriteTimeout, rt.HTTPIdleTimeout)
	fmt.Printf("  review_dedupe_window: %s\n", rt.ReviewDedupeWindow)
	fmt.Printf("  isolated_workspace_ttl: %s\n", rt.IsolatedWorkspaceTTL)
	fmt.Printf("runtime store:\n")
	fmt.Printf("  usage_retention: %s max_rows=%d\n", st.UsageRetention, st.UsageMaxRows)
	fmt.Printf("  recovery_event_retention: %s\n", st.RecoveryEventRetention)
	fmt.Printf("runtime loop:\n")
	lp := cfg.Runtime.Loop
	fmt.Printf("  min_interval=%s max_interval=%s max_duration=%s min_duration=%s iteration_timeout=%s max_wall_age=%s min_count=%d max_count=%d max_prompt_runes=%d max_per_conversation=%d max_global=%d\n", lp.MinInterval, lp.MaxInterval, lp.MaxDuration, lp.MinDuration, lp.IterationTimeout, lp.MaxWallAge, lp.MinCount, lp.MaxCount, lp.MaxPromptRunes, lp.MaxPerConversation, lp.MaxGlobal)
	fmt.Printf("runtime relay:\n")
	rl := cfg.Runtime.Relay
	fmt.Printf("  discovery=%s client=%s max_attachment=%d max_line=%d abort=%s verify=%s stall=%s no_event=%s\n", rl.DiscoveryTimeout, rl.ClientTimeout, int64(rl.MaxAttachmentSize), rl.MaxEventLineBytes, rl.WebhookAbortTimeout, rl.VerifyTimeout, rl.StallFreshness, rl.NoEventTimeout)
	fmt.Printf("runtime router:\n")
	ro := cfg.Runtime.Router
	fmt.Printf("  stale_after=%s quiet=%s queued=%d picker=%d/%d model_ttl=%s page=%d nav=%d cap=%d agent_ttl=%s agent_cap=%d q_ttl=%s p_ttl=%s attrib=%s recovery=%s/%s/%s usage_page=%d usage_window=%s\n", ro.ContextStaleAfter, ro.ProgressQuietThreshold, ro.MaxQueuedMessages, ro.MaxPickerSessions, ro.MaxPickerPages, ro.ModelBrowserTTL, ro.ModelBrowserPage, ro.ModelBrowserNavRows, ro.ModelBrowserCap, ro.AgentBrowserTTL, ro.AgentBrowserCap, ro.QuestionTombstoneTTL, ro.PermissionTombstoneTTL, ro.AttributionTTL, ro.RecoveryBudget, ro.RecoveryBaseBackoff, ro.RecoveryMaxBackoff, ro.UsagePageSize, ro.UsageDefaultWindow)
	fmt.Printf("runtime channels:\n")
	ch := cfg.Runtime.Channels
	fmt.Printf("  discord: download=%s max=%d\n", ch.Discord.DownloadTimeout, int64(ch.Discord.MaxDownloadSize))
	fmt.Printf("  telegram: download=%s max=%d init=%s attempts=%d\n", ch.Telegram.DownloadTimeout, int64(ch.Telegram.MaxDownloadSize), ch.Telegram.InitTimeout, ch.Telegram.InitAttempts)
	fmt.Printf("runtime mcp/process/scheduler/health:\n")
	fmt.Printf("  mcp: %s %s %s %s\n", cfg.Runtime.MCP.ReadHeaderTimeout, cfg.Runtime.MCP.ReadTimeout, cfg.Runtime.MCP.WriteTimeout, cfg.Runtime.MCP.IdleTimeout)
	fmt.Printf("  process: readiness=%s grace=%s control=%s\n", cfg.Runtime.Process.ReadinessTimeout, cfg.Runtime.Process.StopGrace, cfg.Runtime.Process.ControlTimeout)
	fmt.Printf("  scheduler: grace=%s health_probe=%s\n", cfg.Runtime.Scheduler.StopGrace, cfg.Runtime.Health.ProbeTimeout)
	for _, ep := range cfg.Webhooks.Endpoints {
		fmt.Printf("endpoint %s (%s workflow=%s): %d admit rule(s)", ep.Name, ep.Path, ep.Workflow, len(ep.Admit))
		if ep.Limits != nil {
			fmt.Printf(" limits=%d/%s", ep.Limits.MaxRunsPerPR, ep.Limits.Window)
		}
		fmt.Println()
		for i, rule := range ep.Admit {
			fmt.Printf("  [%d] event=%s actions=[%s]\n", i, rule.Event, strings.Join(rule.Actions, ","))
		}
	}
	fmt.Printf("OK: %d endpoint(s)\n", len(cfg.Webhooks.Endpoints))
	return 0
}

func joinDurations(durations []time.Duration) string {
	parts := make([]string, 0, len(durations))
	for _, d := range durations {
		parts = append(parts, d.String())
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
