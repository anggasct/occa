package webhook

import (
	"context"
	"sync"
	"time"
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

	step       int
	activeTool string
	activeCtx  string
	toolStart  time.Time

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

func (u *ProgressCardUpdater) startTicker(interval time.Duration) {
	u.ticker = time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-u.tickerStop:
				return
			case now := <-u.ticker.C:
				u.mu.Lock()
				if u.closed || u.activeTool == "" {
					u.mu.Unlock()
					continue
				}
				if now.Sub(u.lastEditTime) >= u.minInterval && u.pendingTimer == nil {
					status := FormatProgressStatus(u.step, u.activeTool, u.activeCtx, now.Sub(u.toolStart))
					card := FormatRootCard(u.envelope, u.workflow, status, "", u.threadID, u.platform)
					text := FormatWebhookMessage(card)
					if text != u.lastText {
						u.applyEditLocked(text, now)
					}
				}
				u.mu.Unlock()
			}
		}
	}()
}

func (u *ProgressCardUpdater) OnTool(tool, toolCtx string, samePart bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return
	}

	if samePart {
		if toolCtx != "" {
			u.activeCtx = toolCtx
		}
	} else {
		u.step++
		u.activeTool = tool
		u.activeCtx = toolCtx
		u.toolStart = time.Now()
	}

	u.scheduleLocked(time.Now())
}

func (u *ProgressCardUpdater) scheduleLocked(now time.Time) {
	if u.closed || u.activeTool == "" {
		return
	}

	status := FormatProgressStatus(u.step, u.activeTool, u.activeCtx, now.Sub(u.toolStart))
	card := FormatRootCard(u.envelope, u.workflow, status, "", u.threadID, u.platform)
	text := FormatWebhookMessage(card)

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
		curNow := time.Now()
		curStatus := FormatProgressStatus(u.step, u.activeTool, u.activeCtx, curNow.Sub(u.toolStart))
		curCard := FormatRootCard(u.envelope, u.workflow, curStatus, "", u.threadID, u.platform)
		curText := FormatWebhookMessage(curCard)
		if curText != u.lastText {
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
