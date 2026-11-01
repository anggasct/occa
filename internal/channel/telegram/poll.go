package telegram

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const (
	pollBackoffBase    = 3 * time.Second
	pollBackoffMax     = 30 * time.Second
	pollWarnInterval   = time.Minute
	pollConflictReason = "polling conflict: another instance uses this token"
)

// isPollConflict reports whether getUpdates failed because another process
// is long-polling with the same bot token.
func isPollConflict(err error) bool {
	var apiErr *tgbotapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusConflict {
		return true
	}
	return err != nil && strings.Contains(err.Error(), "Conflict: terminated by other getUpdates request")
}

// pollHealth tracks one episode of consecutive getUpdates failures: it picks
// the backoff and keeps the log to one line per episode for a conflict and one
// line per minute for transient errors.
type pollHealth struct {
	botName string

	failures      int
	conflict      bool
	episodeStart  time.Time
	lastWarn      time.Time
	sinceLastWarn int
}

// fail records a failure at now and returns how long to wait before the next
// poll.
func (p *pollHealth) fail(err error, now time.Time) time.Duration {
	conflict := isPollConflict(err)
	if p.failures > 0 && conflict != p.conflict {
		// The failure class changed: start a fresh episode for it.
		p.recover(now)
	}
	if p.failures == 0 {
		p.episodeStart = now
		p.conflict = conflict
		p.sinceLastWarn = 0
		if conflict {
			slog.Error("telegram: another bot instance is polling with this token — updates are being split between them",
				"bot_username", p.botName)
		} else {
			slog.Warn("telegram: getUpdates failed, retrying", "error", err)
			p.lastWarn = now
		}
	} else if !conflict {
		p.sinceLastWarn++
		if now.Sub(p.lastWarn) >= pollWarnInterval {
			slog.Warn("telegram: getUpdates failed, retrying", "error", err, "failures", p.sinceLastWarn)
			p.lastWarn = now
			p.sinceLastWarn = 0
		}
	}
	p.failures++
	return pollDelay(p.failures)
}

// recover closes an open failure episode.
func (p *pollHealth) recover(now time.Time) {
	if p.failures == 0 {
		return
	}
	if p.conflict {
		slog.Info("telegram: polling conflict cleared", "conflicts", p.failures, "duration", now.Sub(p.episodeStart).Round(time.Second))
	} else {
		slog.Info("telegram: getUpdates recovered", "failures", p.failures)
	}
	p.failures = 0
	p.conflict = false
	p.sinceLastWarn = 0
}

func (p *pollHealth) inConflict() bool { return p.failures > 0 && p.conflict }

// pollDelay is 3s doubled per consecutive failure, capped at 30s.
func pollDelay(failures int) time.Duration {
	delay := pollBackoffBase
	for i := 1; i < failures && delay < pollBackoffMax; i++ {
		delay *= 2
	}
	if delay > pollBackoffMax {
		delay = pollBackoffMax
	}
	return delay
}

// waitOrDone sleeps for d unless ctx ends first; it reports whether the full
// wait elapsed.
func waitOrDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
