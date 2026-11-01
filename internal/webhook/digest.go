package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

// digestRunWorkflows are the lanes the digest counts runs for, in order.
var digestRunWorkflows = []string{"review", "fix", "merge", "dispatch"}

// DigestNow returns the daily digest on demand (/digest).
func (s *Server) DigestNow(ctx context.Context, msg channel.IncomingMessage) (string, error) {
	if !s.operatorAllowed(msg.Platform, msg.UserID) {
		return "⛔ Not allowed.", nil
	}
	return s.buildDigest(ctx, s.digestNow())
}

func (s *Server) digestNow() time.Time {
	if s.digestClock != nil {
		return s.digestClock()
	}
	return time.Now()
}

// buildDigest summarizes the 24 hours before now: merged PRs, agent runs per
// lane, failures, and the PRs that wait on the operator. There is no cost
// line: no per-run cost is recorded, and the digest never estimates one.
func (s *Server) buildDigest(ctx context.Context, now time.Time) (string, error) {
	since := now.AddDate(0, 0, -1)
	var deliveries []store.WebhookDelivery
	if s.deliveries != nil {
		rows, err := s.deliveries.ListUpdatedSince(ctx, since.Unix())
		if err != nil {
			return "", err
		}
		deliveries = rows
	}
	merged := map[string]bool{}
	var mergedKeys []string
	runs := map[string]int{}
	total, failed := 0, 0
	for _, d := range deliveries {
		workflow := ""
		if ep, ok := s.endpointsByName[d.Endpoint]; ok {
			workflow = strings.ToLower(strings.TrimSpace(ep.Workflow))
		}
		if workflow == "merged" && d.Status == store.WebhookStatusCompleted && d.PRKey != "" && !merged[d.PRKey] {
			merged[d.PRKey] = true
			mergedKeys = append(mergedKeys, d.PRKey)
		}
		if d.StartedAt > 0 && (d.Status == store.WebhookStatusCompleted || d.Status == store.WebhookStatusFailed) && workflow != "merged" {
			runs[workflow]++
			total++
		}
		if d.Status == store.WebhookStatusFailed {
			failed++
		}
	}
	blocked, err := s.blockedRows(ctx)
	if err != nil {
		return "", err
	}
	sort.Strings(mergedKeys)

	var b strings.Builder
	b.WriteString("☀️ occa — last 24h")
	fmt.Fprintf(&b, "\n✅ merged %d", len(mergedKeys))
	if len(mergedKeys) > 0 {
		b.WriteString(": " + strings.Join(mergedKeys, ", "))
	}
	var lanes []string
	for _, wf := range digestRunWorkflows {
		if n := runs[wf]; n > 0 {
			lanes = append(lanes, fmt.Sprintf("%s %d", wf, n))
		}
	}
	fmt.Fprintf(&b, "\n🔁 runs %d", total)
	if len(lanes) > 0 {
		b.WriteString(" (" + strings.Join(lanes, " · ") + ")")
	}
	fmt.Fprintf(&b, " · failed %d", failed)
	if len(blocked) == 0 {
		b.WriteString("\n📥 nothing needs you")
	} else {
		keys := make([]string, 0, len(blocked))
		for _, row := range blocked {
			keys = append(keys, row.PRKey)
		}
		fmt.Fprintf(&b, "\n📥 needs you %d: %s — /inbox", len(blocked), strings.Join(keys, ", "))
	}
	return b.String(), nil
}

// digestLoop posts the digest once a day at the configured local time.
func (s *Server) digestLoop(cfg config.DigestConfig) {
	defer s.dispatchWG.Done()
	for {
		next := cfg.NextAt(s.digestNow())
		wait := time.Until(next)
		slog.Info("webhook: digest scheduled", "at", next.Format(time.RFC3339))
		timer := time.NewTimer(wait)
		select {
		case <-s.shutdownCtx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		s.postDigest(s.shutdownCtx, cfg)
	}
}

func (s *Server) postDigest(ctx context.Context, cfg config.DigestConfig) {
	text, err := s.buildDigest(ctx, s.digestNow())
	if err != nil {
		slog.Warn("webhook: digest build failed", "error", err)
		return
	}
	if err := s.notifyThread(ctx, cfg.Platform, cfg.ChannelID, text); err != nil {
		slog.Warn("webhook: digest post failed", "platform", cfg.Platform, "channel_id", cfg.ChannelID, "error", err)
		return
	}
	slog.Info("webhook: digest posted", "platform", cfg.Platform, "channel_id", cfg.ChannelID)
}
