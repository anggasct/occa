package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func newHandler(t *testing.T, format string, secrets ...string) (*RedactHandler, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	var base slog.Handler
	if format == "json" {
		base = slog.NewJSONHandler(&buf, opts)
	} else {
		base = slog.NewTextHandler(&buf, opts)
	}
	return NewRedactHandler(base, secrets...), &buf
}

func TestKnownValueRedactedInMessage(t *testing.T) {
	for _, format := range []string{"text", "json"} {
		t.Run(format, func(t *testing.T) {
			h, buf := newHandler(t, format, "sekret-token-123")
			logger := slog.New(h)
			logger.Error("connect failed: token sekret-token-123 rejected")

			out := buf.String()
			if strings.Contains(out, "sekret-token-123") {
				t.Fatalf("secret leaked in message: %s", out)
			}
			if !strings.Contains(out, "[REDACTED]") {
				t.Fatalf("expected [REDACTED] marker in output: %s", out)
			}
		})
	}
}

func TestKnownValueRedactedInAttrValue(t *testing.T) {
	h, buf := newHandler(t, "json", "sekret-token-123")
	logger := slog.New(h)
	logger.Error("request failed", "url", "https://api.example.com/x?token=sekret-token-123")

	out := buf.String()
	if strings.Contains(out, "sekret-token-123") {
		t.Fatalf("secret leaked in attr value: %s", out)
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("expected [REDACTED] marker in output: %s", out)
	}
}

func TestKnownValueRedactedInWrappedError(t *testing.T) {
	h, buf := newHandler(t, "json", "sekret-token-123")
	logger := slog.New(h)
	err := fmt.Errorf("dial %s: connection refused", "https://api.example.com/x?token=sekret-token-123")
	logger.Error("failed to open store", "error", err)

	out := buf.String()
	if strings.Contains(out, "sekret-token-123") {
		t.Fatalf("secret leaked in wrapped error attr: %s", out)
	}
}

func TestKnownValueRedactedInNestedGroup(t *testing.T) {
	h, buf := newHandler(t, "json", "sekret-token-123")
	logger := slog.New(h)
	logger.Error("upstream error", slog.Group("request",
		slog.String("url", "https://api.example.com/x?token=sekret-token-123"),
		slog.Int("status", 502),
	))

	out := buf.String()
	if strings.Contains(out, "sekret-token-123") {
		t.Fatalf("secret leaked in nested group: %s", out)
	}
	var decoded map[string]any
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	req, ok := decoded["request"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested request group in output: %s", out)
	}
	if req["status"] != float64(502) {
		t.Fatalf("expected non-secret nested field preserved, got %v", req["status"])
	}
}

func TestFieldNameDenylistRedactsRegardlessOfValue(t *testing.T) {
	h, buf := newHandler(t, "json") // no registered secrets
	logger := slog.New(h)
	logger.Info("login", "token", "unregistered-but-sensitive-value")

	out := buf.String()
	if strings.Contains(out, "unregistered-but-sensitive-value") {
		t.Fatalf("denylisted key value leaked: %s", out)
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("expected [REDACTED] marker in output: %s", out)
	}
}

func TestNonSecretContentUnaffected(t *testing.T) {
	h, buf := newHandler(t, "json", "sekret-token-123")
	logger := slog.New(h)
	logger.Info("session started", "session_id", "abc-123", "channel_id", "chan-1", "count", 5)

	var decoded map[string]any
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if decoded["session_id"] != "abc-123" {
		t.Fatalf("session_id = %v, want unchanged", decoded["session_id"])
	}
	if decoded["channel_id"] != "chan-1" {
		t.Fatalf("channel_id = %v, want unchanged", decoded["channel_id"])
	}
	if decoded["count"] != float64(5) {
		t.Fatalf("count = %v, want unchanged int 5 (no type widening to string)", decoded["count"])
	}
	if strings.Contains(buf.String(), "[REDACTED]") {
		t.Fatalf("unexpected redaction of non-secret content: %s", buf.String())
	}
}

func TestEmptySecretNotRegistered(t *testing.T) {
	h, buf := newHandler(t, "json", "real-token", "")
	logger := slog.New(h)
	logger.Info("channel started", "note", "")

	out := buf.String()
	if strings.Contains(out, "[REDACTED]") {
		t.Fatalf("empty secret must not cause spurious redaction: %s", out)
	}
}

func TestWithAttrsRedacted(t *testing.T) {
	h, buf := newHandler(t, "json", "sekret-token-123")
	logger := slog.New(h).With("token", "sekret-token-123")
	logger.Info("ready")

	out := buf.String()
	if strings.Contains(out, "sekret-token-123") {
		t.Fatalf("secret leaked via With: %s", out)
	}
}

func TestWithGroupRedactsNestedAttrs(t *testing.T) {
	h, buf := newHandler(t, "json", "sekret-token-123")
	logger := slog.New(h).WithGroup("req")
	logger.Info("call", "token", "sekret-token-123")

	out := buf.String()
	if strings.Contains(out, "sekret-token-123") {
		t.Fatalf("secret leaked under WithGroup: %s", out)
	}
}

func TestEnabledDelegatesToNext(t *testing.T) {
	h, _ := newHandler(t, "json", "x")
	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Fatal("expected Debug disabled at default Info level")
	}
	if !h.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("expected Info enabled at default Info level")
	}
}

func TestLogValuerErrorIsResolvedBeforeScrub(t *testing.T) {
	h, buf := newHandler(t, "json", "sekret-token-123")
	logger := slog.New(h)
	logger.Error("op failed", "error", errors.New("token sekret-token-123 invalid"))

	out := buf.String()
	if strings.Contains(out, "sekret-token-123") {
		t.Fatalf("secret leaked in plain error value: %s", out)
	}
}
