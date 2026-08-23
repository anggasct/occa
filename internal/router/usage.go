package router

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

const (
	usageCallbackPrefix = "usage:"
	usagePageSize       = 5
	usageMaxPages       = 5
)

type usagePeriod string

const (
	usageToday   usagePeriod = "today"
	usageSeven   usagePeriod = "7d"
	usageSession usagePeriod = "session"
)

func (r *Router) usageRepo() store.UsageRepo {
	provider, ok := r.store.(store.UsageStore)
	if !ok {
		return nil
	}
	return provider.UsageRepo()
}

func (r *Router) handleUsage(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	period, page, ok := parseUsageArgs(args)
	if !ok {
		return "Usage: /usage [today|7d|session]", nil
	}
	text, buttons, err := r.usageView(ctx, msg, period, page)
	if err != nil {
		return "", err
	}
	if msg.IsCallback {
		if err := msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons); err != nil {
			return "", fmt.Errorf("usage callback: %w", err)
		}
		return "", errReplied
	}
	if msg.ReplyCtx == nil {
		return text, nil
	}
	if _, err := msg.ReplyCtx.SendWithButtons(text, buttons); err != nil {
		return "", fmt.Errorf("usage reply: %w", err)
	}
	return "", errReplied
}

func (r *Router) handleUsageCallback(ctx context.Context, msg channel.IncomingMessage) error {
	parts := strings.Split(strings.TrimPrefix(msg.CallbackData, usageCallbackPrefix), ":")
	if len(parts) != 2 {
		return nil
	}
	period, page, ok := parseUsageArgs(parts[0] + " " + parts[1])
	if !ok {
		return nil
	}
	text, buttons, err := r.usageView(ctx, msg, period, page)
	if err != nil {
		return err
	}
	if err := msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons); err != nil {
		return fmt.Errorf("usage callback: %w", err)
	}
	return nil
}

func parseUsageArgs(args string) (usagePeriod, int, bool) {
	parts := strings.Fields(args)
	period := usageToday
	page := 1
	if len(parts) > 0 {
		switch parts[0] {
		case string(usageToday), string(usageSeven), string(usageSession):
			period = usagePeriod(parts[0])
		default:
			return "", 0, false
		}
	}
	if len(parts) > 1 {
		var err error
		page, err = strconv.Atoi(parts[1])
		if err != nil || page < 1 {
			return "", 0, false
		}
	}
	if len(parts) > 2 {
		return "", 0, false
	}
	return period, page, true
}

func (r *Router) usageView(ctx context.Context, msg channel.IncomingMessage, period usagePeriod, page int) (string, []channel.Button, error) {
	repo := r.usageRepo()
	if repo == nil {
		return "Usage data is unavailable.", nil, nil
	}

	threadID, userID := conversationKey(msg)
	query := store.UsageQuery{
		Platform:  msg.Platform,
		ChannelID: msg.ChannelID,
		ThreadID:  threadID,
		UserID:    userID,
	}
	admin := r.isAdmin(ctx, msg)
	if admin && period != usageSession {
		query.ChannelWide = true
	}
	if period == usageSession {
		query.SessionID, _, _ = r.store.SessionRepo().Active(ctx, msg.Platform, msg.ChannelID, threadID, userID)
		if query.SessionID == "" {
			return usageHeader(period, admin) + "\nNo active session usage yet.", usageButtons(period, 1, 1), nil
		}
	}
	query.Since = usageSince(period, time.Now().UTC())
	query.Limit = usagePageSize
	query.Offset = (clampUsagePage(page) - 1) * usagePageSize

	report, err := repo.Query(ctx, query)
	if err != nil {
		return "", nil, fmt.Errorf("usage query: %w", err)
	}
	pages := usagePages(report.BreakdownTotal)
	page = clampUsagePage(page)
	if page > pages {
		page = pages
		query.Offset = (page - 1) * usagePageSize
		report, err = repo.Query(ctx, query)
		if err != nil {
			return "", nil, fmt.Errorf("usage query page: %w", err)
		}
	}

	var b strings.Builder
	b.WriteString(usageHeader(period, admin))
	b.WriteString("\nInput: ")
	b.WriteString(formatMetric(report.Totals.Input))
	b.WriteString(" cumulative\nOutput: ")
	b.WriteString(formatMetric(report.Totals.Output))
	b.WriteString("\nReasoning: ")
	b.WriteString(formatMetric(report.Totals.Reasoning))
	b.WriteString("\nCache read: ")
	b.WriteString(formatMetric(report.Totals.CacheRead))
	b.WriteString("\nCache write: ")
	b.WriteString(formatMetric(report.Totals.CacheWrite))
	b.WriteString("\nEstimated cost: ")
	if report.Totals.CostKnown {
		fmt.Fprintf(&b, "$%.2f", report.Totals.Cost)
	} else {
		b.WriteString("unknown")
	}

	if len(report.Breakdowns) > 0 {
		b.WriteString("\n\nBreakdown:")
		for _, breakdown := range report.Breakdowns {
			b.WriteString("\n• ")
			model := breakdown.Model
			if model == "" {
				model = "unknown model"
			}
			b.WriteString(truncateRunes(model, 48))
			if breakdown.Workdir != "" {
				b.WriteString(" · ")
				b.WriteString(truncateRunes(breakdown.Workdir, 60))
			}
			if admin {
				b.WriteString(" · ")
				b.WriteString(truncateRunes(usageConversationLabel(breakdown), 42))
			}
			fmt.Fprintf(&b, "\n  Input %s · Output %s · Reasoning %s", formatMetric(breakdown.Input), formatMetric(breakdown.Output), formatMetric(breakdown.Reasoning))
			fmt.Fprintf(&b, "\n  Cache %s/%s · Cost ", formatMetric(breakdown.CacheRead), formatMetric(breakdown.CacheWrite))
			if breakdown.CostKnown {
				fmt.Fprintf(&b, "$%.2f", breakdown.Cost)
			} else {
				b.WriteString("unknown")
			}
		}
	} else {
		b.WriteString("\n\nNo usage recorded in this range.")
	}
	if pages > 1 {
		fmt.Fprintf(&b, "\n\nPage %d/%d", page, pages)
	}
	return b.String(), usageButtons(period, page, pages), nil
}

func usageHeader(period usagePeriod, admin bool) string {
	scope := "current conversation"
	if admin && period != usageSession {
		scope = "channel · admin view"
	}
	return fmt.Sprintf("📊 Usage\n%s · %s", usagePeriodLabel(period), scope)
}

func usagePeriodLabel(period usagePeriod) string {
	switch period {
	case usageSeven:
		return "7 days"
	case usageSession:
		return "Session"
	default:
		return "Today"
	}
}

func usageSince(period usagePeriod, now time.Time) int64 {
	switch period {
	case usageSeven:
		return now.Add(-7 * 24 * time.Hour).Unix()
	case usageSession:
		return 0
	default:
		start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		return start.Unix()
	}
}

func usagePages(total int) int {
	if total <= 0 {
		return 1
	}
	pages := (total + usagePageSize - 1) / usagePageSize
	if pages > usageMaxPages {
		return usageMaxPages
	}
	return pages
}

func clampUsagePage(page int) int {
	if page < 1 {
		return 1
	}
	if page > usageMaxPages {
		return usageMaxPages
	}
	return page
}

func usageButtons(period usagePeriod, page, pages int) []channel.Button {
	buttons := []channel.Button{
		{Label: "Today", Value: usageCallbackPrefix + string(usageToday) + ":1", Row: 1},
		{Label: "7 days", Value: usageCallbackPrefix + string(usageSeven) + ":1", Row: 1},
		{Label: "Session", Value: usageCallbackPrefix + string(usageSession) + ":1", Row: 1},
	}
	if page > 1 {
		buttons = append(buttons, channel.Button{Label: "◀️ Prev", Value: fmt.Sprintf("%s%s:%d", usageCallbackPrefix, period, page-1), Row: 2})
	}
	if page < pages {
		buttons = append(buttons, channel.Button{Label: "Next ▶️", Value: fmt.Sprintf("%s%s:%d", usageCallbackPrefix, period, page+1), Row: 2})
	}
	return buttons
}

func usageConversationLabel(breakdown store.UsageBreakdown) string {
	if breakdown.ThreadID != "" {
		return "thread " + breakdown.ThreadID
	}
	if breakdown.UserID != "" {
		return "user " + breakdown.UserID
	}
	return "shared conversation"
}

func (r *Router) recordUsage(msg channel.IncomingMessage, inst AgentInstance, sessionID string) {
	repo := r.usageRepo()
	if repo == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := inst.Client().GetSession(ctx, sessionID)
	if err != nil || info == nil {
		return
	}
	model := ""
	if info.Model.ProviderID != "" && info.Model.ID != "" {
		model = info.Model.ProviderID + "/" + info.Model.ID
		if info.Model.Variant != "" {
			model += "@" + info.Model.Variant
		}
	}
	if model == "" {
		if effective, effectiveErr := r.effectiveModel(ctx, msg); effectiveErr == nil && effective != nil {
			model = effective.ProviderID + "/" + effective.ID
			if effective.Variant != "" {
				model += "@" + effective.Variant
			}
		}
	}
	threadID, userID := conversationKey(msg)
	if err := repo.RecordSnapshot(ctx, store.UsageSnapshot{
		Platform:   msg.Platform,
		ChannelID:  msg.ChannelID,
		ThreadID:   threadID,
		UserID:     userID,
		SessionID:  sessionID,
		Model:      model,
		Workdir:    inst.Workdir(),
		Input:      info.Tokens.Input,
		Output:     info.Tokens.Output,
		Reasoning:  info.Tokens.Reasoning,
		CacheRead:  info.Tokens.CacheRead,
		CacheWrite: info.Tokens.CacheWrite,
		Cost:       info.Cost,
		CostKnown:  info.CostKnown || info.Cost > 0,
		RecordedAt: time.Now().Unix(),
	}); err != nil {
		slog.Warn("router: usage projection failed", "platform", msg.Platform, "channel_id", msg.ChannelID, "session_id", sessionID, "error", err)
	}
}
