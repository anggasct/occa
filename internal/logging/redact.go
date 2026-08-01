package logging

import (
	"context"
	"log/slog"
	"sort"
	"strings"
)

const redacted = "[REDACTED]"

var sensitiveKeys = map[string]bool{
	"token": true, "secret": true, "password": true,
	"authorization": true, "api_key": true,
}

// RedactHandler wraps an slog.Handler, scrubbing registered secret values
// and denylisted field names from every record before it reaches next.
type RedactHandler struct {
	next    slog.Handler
	secrets []string // registered known secret values, longest-first (avoid partial-match ordering bugs)
}

// NewRedactHandler wraps next, registering secrets (ignoring empty values) to
// scrub from every record's message and attribute values, recursively
// through groups, in addition to the field-name denylist.
func NewRedactHandler(next slog.Handler, secrets ...string) *RedactHandler {
	registered := make([]string, 0, len(secrets))
	for _, s := range secrets {
		if s != "" {
			registered = append(registered, s)
		}
	}
	sort.Slice(registered, func(i, j int) bool { return len(registered[i]) > len(registered[j]) })
	return &RedactHandler{next: next, secrets: registered}
}

func (h *RedactHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *RedactHandler) Handle(ctx context.Context, r slog.Record) error {
	nr := slog.NewRecord(r.Time, r.Level, h.scrub(r.Message), r.PC)
	r.Attrs(func(a slog.Attr) bool {
		nr.AddAttrs(h.redactAttr(a))
		return true
	})
	return h.next.Handle(ctx, nr)
}

func (h *RedactHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		out[i] = h.redactAttr(a)
	}
	return &RedactHandler{next: h.next.WithAttrs(out), secrets: h.secrets}
}

func (h *RedactHandler) WithGroup(name string) slog.Handler {
	return &RedactHandler{next: h.next.WithGroup(name), secrets: h.secrets}
}

// redactAttr redacts a by key (denylist, regardless of value) or by scrubbing
// any registered secret substring out of its value, recursing into groups.
// An attr whose value contains no match is returned unchanged.
func (h *RedactHandler) redactAttr(a slog.Attr) slog.Attr {
	if sensitiveKeys[a.Key] {
		return slog.String(a.Key, redacted)
	}

	val := a.Value.Resolve()
	if val.Kind() == slog.KindGroup {
		group := val.Group()
		out := make([]slog.Attr, len(group))
		for i, ga := range group {
			out[i] = h.redactAttr(ga)
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(out...)}
	}

	original := val.String()
	scrubbed := h.scrub(original)
	if scrubbed == original {
		return a
	}
	return slog.String(a.Key, scrubbed)
}

func (h *RedactHandler) scrub(s string) string {
	for _, secret := range h.secrets {
		if strings.Contains(s, secret) {
			s = strings.ReplaceAll(s, secret, redacted)
		}
	}
	return s
}
