package health

import (
	"strings"
	"sync"
	"time"
	"unicode/utf8"
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

// sanitize flattens control characters, collapses whitespace, and truncates
// so a recorded error fits comfortably in a chat reply.
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
	msg = strings.Join(strings.Fields(msg), " ")
	if utf8.RuneCountInString(msg) <= maxLastErrorRunes {
		return msg
	}
	return string([]rune(msg)[:maxLastErrorRunes]) + "…"
}
