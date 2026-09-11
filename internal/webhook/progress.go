package webhook

import (
	"context"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/render"
)

type ProgressEditorFunc func(ctx context.Context, channelID, messageID, text string) error

type ProgressCardUpdater struct {
	mu          sync.Mutex
	channelID   string
	messageID   string
	envelope    WebhookEnvelope
	workflow    string
	threadID    string
	platform    string
	minInterval time.Duration
	editFn      ProgressEditorFunc
	now         func() time.Time

	step              int
	activeTool        string
	activeCtx         string
	toolStart         time.Time
	latestCount       int
	reasoningActive   bool
	reasoningStart    time.Time
	reasoningDuration time.Duration

	lastEditTime time.Time
	lastText     string
	pendingTimer *time.Timer
	ticker       *time.Ticker
	tickerStop   chan struct{}
	closed       bool
}

func NewProgressCardUpdater(channelID, messageID string, envelope WebhookEnvelope, workflow, threadID, platform string, editFn ProgressEditorFunc) *ProgressCardUpdater {
	u := &ProgressCardUpdater{
		channelID:   channelID,
		messageID:   messageID,
		envelope:    envelope,
		workflow:    workflow,
		threadID:    threadID,
		platform:    platform,
		minInterval: 2500 * time.Millisecond,
		editFn:      editFn,
		now:         time.Now,
		tickerStop:  make(chan struct{}),
	}
	u.startTicker(3 * time.Second)
	return u
}

func (u *ProgressCardUpdater) SetMinInterval(d time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.minInterval = d
}

// SetNowFunc overrides the clock used for elapsed-time rendering. Tests use
// it to drive deterministic Thinking… / Thought-for durations.
func (u *ProgressCardUpdater) SetNowFunc(fn func() time.Time) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if fn != nil {
		u.now = fn
	}
}

func (u *ProgressCardUpdater) nowTime() time.Time {
	if u.now != nil {
		return u.now()
	}
	return time.Now()
}

func (u *ProgressCardUpdater) platformLimit() int {
	if u.platform == "discord" {
		return render.DiscordLimit
	}
	return render.TelegramLimit
}

func (u *ProgressCardUpdater) hasContentLocked() bool {
	if u.activeTool != "" {
		return true
	}
	if u.reasoningActive || u.reasoningDuration > 0 || !u.reasoningStart.IsZero() {
		return true
	}
	return false
}

func (u *ProgressCardUpdater) finishReasoningLocked(now time.Time) {
	if !u.reasoningActive {
		return
	}
	u.reasoningActive = false
	if !u.reasoningStart.IsZero() {
		u.reasoningDuration += now.Sub(u.reasoningStart)
	}
}

// renderLocked builds the clamped edit text for the current state. It
// returns "" when there is nothing to render yet.
func (u *ProgressCardUpdater) renderLocked(now time.Time) string {
	status := relay.FormatWorkingText(relay.WorkingTextParams{
		Step:              u.step,
		Tool:              u.activeTool,
		ToolContext:       u.activeCtx,
		ToolCount:         u.latestCount,
		ToolStart:         u.toolStart,
		ReasoningActive:   u.reasoningActive,
		ReasoningStart:    u.reasoningStart,
		ReasoningDuration: u.reasoningDuration,
		Now:               now,
	})
	if status == "" {
		return ""
	}
	card := FormatRootCard(u.envelope, u.workflow, status, "", u.threadID, u.platform)
	text := FormatWebhookMessage(card)
	return render.Clamp(text, u.platformLimit())
}

func (u *ProgressCardUpdater) startTicker(interval time.Duration) {
	u.ticker = time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-u.tickerStop:
				return
			case now := <-u.ticker.C:
				u.mu.Lock()
				if u.closed || !u.hasContentLocked() {
					u.mu.Unlock()
					continue
				}
				if now.Sub(u.lastEditTime) >= u.minInterval && u.pendingTimer == nil {
					text := u.renderLocked(now)
					if text != "" && text != u.lastText {
						u.applyEditLocked(text, now)
					}
				}
				u.mu.Unlock()
			}
		}
	}()
}

// OnEvent mirrors the streamer SSE handling: reasoning deltas drive the
// Thinking… / Thought-for states, tool events update the step card.
func (u *ProgressCardUpdater) OnEvent(ev relay.Event) {
	switch ev.Type {
	case relay.EventReasoning:
		u.OnReasoning()
	case relay.EventTool:
		u.OnTool(ev.Delta, ev.ToolContext, ev.ToolSamePart)
	case relay.EventDelta:
		u.OnDelta()
	case relay.EventSegment:
		u.OnSegment()
	case relay.EventDone:
		u.OnDone()
	case relay.EventError:
		u.OnError()
	}
}

// OnReasoning marks the start of a reasoning phase, mirroring the streamer.
func (u *ProgressCardUpdater) OnReasoning() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return
	}
	now := u.nowTime()
	if !u.reasoningActive {
		u.reasoningActive = true
		u.reasoningStart = now
		u.scheduleLocked(now)
	}
}

// OnDelta ends an active reasoning phase when text starts streaming.
func (u *ProgressCardUpdater) OnDelta() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return
	}
	if !u.reasoningActive {
		return
	}
	now := u.nowTime()
	u.finishReasoningLocked(now)
	u.scheduleLocked(now)
}

// OnSegment ends an active reasoning phase at a part boundary.
func (u *ProgressCardUpdater) OnSegment() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return
	}
	if !u.reasoningActive {
		return
	}
	now := u.nowTime()
	u.finishReasoningLocked(now)
	u.scheduleLocked(now)
}

// OnDone ends an active reasoning phase at the terminal event so the final
// card keeps the Thought-for suffix.
func (u *ProgressCardUpdater) OnDone() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return
	}
	if !u.reasoningActive {
		return
	}
	now := u.nowTime()
	u.finishReasoningLocked(now)
	u.scheduleLocked(now)
}

// OnError ends an active reasoning phase on agent errors.
func (u *ProgressCardUpdater) OnError() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return
	}
	if !u.reasoningActive {
		return
	}
	now := u.nowTime()
	u.finishReasoningLocked(now)
	u.scheduleLocked(now)
}

func (u *ProgressCardUpdater) OnTool(tool, toolCtx string, samePart bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return
	}
	now := u.nowTime()

	if samePart {
		u.finishReasoningLocked(now)
		if toolCtx != "" {
			u.activeCtx = toolCtx
		}
	} else {
		u.finishReasoningLocked(now)
		if u.step > 0 && u.activeTool == tool {
			u.latestCount++
			if toolCtx != "" {
				u.activeCtx = toolCtx
			}
		} else {
			u.step++
			u.activeTool = tool
			u.activeCtx = toolCtx
			u.latestCount = 1
			u.toolStart = now
		}
	}

	u.scheduleLocked(now)
}

func (u *ProgressCardUpdater) scheduleLocked(now time.Time) {
	if u.closed || !u.hasContentLocked() {
		return
	}

	text := u.renderLocked(now)
	if text == "" {
		return
	}

	if text == u.lastText {
		return
	}

	sinceLast := now.Sub(u.lastEditTime)
	if u.lastEditTime.IsZero() || sinceLast >= u.minInterval {
		if u.pendingTimer != nil {
			u.pendingTimer.Stop()
			u.pendingTimer = nil
		}
		u.applyEditLocked(text, now)
		return
	}

	if u.pendingTimer != nil {
		return
	}

	remaining := u.minInterval - sinceLast
	u.pendingTimer = time.AfterFunc(remaining, func() {
		u.mu.Lock()
		defer u.mu.Unlock()
		if u.closed {
			return
		}
		u.pendingTimer = nil
		curNow := u.nowTime()
		curText := u.renderLocked(curNow)
		if curText != "" && curText != u.lastText {
			u.applyEditLocked(curText, curNow)
		}
	})
}

func (u *ProgressCardUpdater) applyEditLocked(text string, now time.Time) {
	if u.editFn != nil {
		_ = u.editFn(context.Background(), u.channelID, u.messageID, text)
	}
	u.lastEditTime = now
	u.lastText = text
}

func (u *ProgressCardUpdater) Stop() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return
	}
	u.closed = true
	if u.pendingTimer != nil {
		u.pendingTimer.Stop()
		u.pendingTimer = nil
	}
	if u.ticker != nil {
		u.ticker.Stop()
		close(u.tickerStop)
	}
}
