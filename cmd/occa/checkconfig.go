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
	fmt.Printf("  usage_retention: %s max_rows=%d\n", rt.UsageRetention, rt.UsageMaxRows)
	fmt.Printf("  recovery_event_retention: %s\n", rt.RecoveryEventRetention)
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
