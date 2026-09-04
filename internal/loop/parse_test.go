package loop

import (
	"strings"
	"testing"
	"time"
)

func TestParseRequestValid(t *testing.T) {
	req, err := ParseRequest("every 2m x15 check PR status")
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if req.Interval != 2*time.Minute || req.Count != 15 || req.Prompt != "check PR status" {
		t.Errorf("parsed = %+v", req)
	}
	req, err = ParseRequest("every 30s for 1h watch deploy")
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if req.Interval != 30*time.Second || req.Length != time.Hour || req.Prompt != "watch deploy" {
		t.Errorf("parsed = %+v", req)
	}
	req, err = ParseRequest("every 2m x3 for real check")
	if err != nil {
		t.Fatalf("prompt starting with non-duration for: %v", err)
	}
	if req.Count != 3 || req.Prompt != "for real check" {
		t.Errorf("prompt with for-word kept: %+v", req)
	}
}

func TestParseRequestInvalid(t *testing.T) {
	long := strings.Repeat("a", MaxPromptRunes+1)
	cases := map[string]string{
		"":                       "empty",
		"every":                  "bare",
		"check PR status":        "no every",
		"every soon x3 hi":       "bad interval",
		"every 5s x3 hi":         "interval too short",
		"every 2h x3 hi":         "interval too long",
		"every 2m check status":  "no end",
		"every 2m x1 once":       "count too small",
		"every 2m x61 many":      "count too big",
		"every 2m xabc bad":      "count not a number",
		"every 2m for 30s short": "duration too short",
		"every 2m for 5h long":   "duration too long",
		"every 2m for soon bad":  "duration unparsable",
		"every 2m x3":            "missing prompt",
		"every 2m x3   ":         "blank prompt",
		"every 2m x3 " + long:    "prompt too long",
		"every 2m x3 for 1h hi":  "both ends (count then duration)",
		"every 2m x3 for 1h":     "both ends, no prompt",
		"every 2m for 1h x3 hi":  "both ends (duration then count)",
	}
	for args, name := range cases {
		if _, err := ParseRequest(args); err == nil {
			t.Errorf("%s (%q): expected error, got nil", name, args)
		}
	}
}

func TestParsePromptBoundary(t *testing.T) {
	exact := strings.Repeat("b", MaxPromptRunes)
	req, err := ParseRequest("every 1m x2 " + exact)
	if err != nil {
		t.Fatalf("1000-rune prompt rejected: %v", err)
	}
	if req.Prompt != exact {
		t.Error("prompt mangled at boundary")
	}
}

func TestFormatHelpers(t *testing.T) {
	if got := FormatInterval(2 * time.Minute); got != "2m" {
		t.Errorf("FormatInterval(2m) = %q", got)
	}
	if got := FormatInterval(30 * time.Second); got != "30s" {
		t.Errorf("FormatInterval(30s) = %q", got)
	}
	if got := FormatInterval(time.Hour); got != "1h" {
		t.Errorf("FormatInterval(1h) = %q", got)
	}
	if got := FormatLeft(90 * time.Minute); got != "1h30m left" {
		t.Errorf("FormatLeft(90m) = %q", got)
	}
	if got := TruncateRunes("abcdef", 10); got != "abcdef" {
		t.Errorf("TruncateRunes short = %q", got)
	}
	if got := TruncateRunes("abcdef", 3); got != "abc…" {
		t.Errorf("TruncateRunes long = %q", got)
	}
}
