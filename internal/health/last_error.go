package health

import (
	"strings"
	"sync"
	"time"
)

const maxLastErrorRunes = 200

// LastError records the most recent failure as a short, sanitized message so
// diagnostics can surface it without ever echoing raw payloads or stack traces.
type LastError struct {
	mu    sync.Mutex
	scrub func(string) string
	msg   string
	at    time.Time
}

func NewLastError(scrub func(string) string) *LastError {
	return &LastError{scrub: scrub}
}

// Set replaces the recorded error with a cleaned copy of msg.
func (l *LastError) Set(msg string) {
	cleaned := sanitize(msg)
	if l.scrub != nil {
		cleaned = l.scrub(cleaned)
	}
	cleaned = truncate(cleaned)
	l.mu.Lock()
	l.msg = cleaned
	l.at = time.Now()
	l.mu.Unlock()
}

// Get returns the current message and when it was recorded.
func (l *LastError) Get() (string, time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.msg, l.at
}

// sanitize flattens control characters and collapses whitespace.
func sanitize(msg string) string {
	msg = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		if r < 0x20 {
			return -1
		}
		return r
	}, msg)
	return strings.Join(strings.Fields(msg), " ")
}

func truncate(msg string) string {
	runes := []rune(msg)
	if len(runes) <= maxLastErrorRunes {
		return msg
	}
	return string(runes[:maxLastErrorRunes-1]) + "…"
}
