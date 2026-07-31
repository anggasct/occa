package router

import (
	"context"
	"fmt"
	"strings"

	"github.com/anggasct/occa/internal/channel"
)

func (r *Router) handleSchedules(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	if r.sched == nil {
		return "⚠️ Scheduler not available", nil
	}

	parts := strings.Fields(args)
	if len(parts) > 0 && parts[0] == "delete" {
		if len(parts) < 2 {
			return "Usage: /occa:schedules delete <id>", nil
		}
		var id int64
		if _, err := fmt.Sscanf(parts[1], "%d", &id); err != nil || id <= 0 {
			return "⚠️ Invalid schedule ID. Usage: /occa:schedules delete <id>", nil
		}
		if err := r.sched.RemoveSchedule(ctx, msg.Platform, msg.ChannelID, id); err != nil {
			return "", fmt.Errorf("schedules delete: %w", err)
		}
		return fmt.Sprintf("✅ Deleted schedule %d", id), nil
	}

	schedules, err := r.sched.ListSchedules(ctx, msg.Platform, msg.ChannelID)
	if err != nil {
		return "", fmt.Errorf("schedules list: %w", err)
	}
	if len(schedules) == 0 {
		return "No scheduled tasks for this channel.", nil
	}

	var sb strings.Builder
	sb.WriteString("Scheduled tasks:\n")
	for _, s := range schedules {
		human := s.HumanSchedule
		if human == "" {
			human = s.CronExpression
		}
		sb.WriteString(fmt.Sprintf("• [%d] %s — %s\n", s.ID, human, s.Prompt))
	}
	return strings.TrimRight(sb.String(), "\n"), nil
}
