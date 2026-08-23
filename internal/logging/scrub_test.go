package logging

import "testing"

func TestNewStringScrubberRedactsSecrets(t *testing.T) {
	scrub := NewStringScrubber("123", "token123")
	got := scrub("bearer token123 end")
	if got != "bearer [REDACTED] end" {
		t.Fatalf("scrub = %q, want %q", got, "bearer [REDACTED] end")
	}
}

func TestNewStringScrubberSkipsEmptySecrets(t *testing.T) {
	scrub := NewStringScrubber("", "abc", "")
	got := scrub("connect with abc")
	if got != "connect with [REDACTED]" {
		t.Fatalf("scrub = %q, want %q", got, "connect with [REDACTED]")
	}
}
