package router

import (
	"context"
	"log/slog"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/health"
)

func (r *Router) SetHealthReporter(h *health.Reporter) {
	r.health = h
}

func (r *Router) recordHealthError(msg string) {
	if r.health != nil {
		r.health.RecordError(msg)
	}
}

func (r *Router) handleHealth(ctx context.Context, msg channel.IncomingMessage, _ string) (string, error) {
	if r.health == nil {
		return "⚠️ Health reporting is not configured.", nil
	}
	report := r.health.Run(ctx)
	slog.LogAttrs(ctx, slog.LevelInfo, "health", report.LogFields()...)
	return report.Render(), nil
}
