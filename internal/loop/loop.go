package loop

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	MinInterval    = 30 * time.Second
	MaxInterval    = time.Hour
	MinCount       = 2
	MaxCount       = 60
	MinDuration    = time.Minute
	MaxDuration    = 4 * time.Hour
	MaxPromptRunes = 1000
	MaxPerConv     = 1
	MaxGlobal      = 20

	iterationTimeout  = 10 * time.Minute
	maxWallAge        = 4 * time.Hour
	promptPrefixRunes = 40
)

type Conversation struct {
	Platform  string
	ChannelID string
	ThreadID  string
	UserID    string
}

type Executor func(ctx context.Context, conv Conversation, prompt string) (string, error)

type Notifier func(conv Conversation, text string)

type BusyFunc func(conv Conversation) bool

type Ticker interface {
	Chan() <-chan time.Time
	Stop()
}

type realTicker struct{ t *time.Ticker }

func (r realTicker) Chan() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()                  { r.t.Stop() }

type ExistsError struct{ ID int64 }

func (e *ExistsError) Error() string {
	return fmt.Sprintf("loop: conversation already has loop %d", e.ID)
}

type Request struct {
	Interval time.Duration
	Count    int
	Length   time.Duration
	Prompt   string
}

type entry struct {
	id        int64
	conv      Conversation
	prompt    string
	interval  time.Duration
	total     int
	remaining int
	executed  int
	deadline  time.Time
	ctx       context.Context
	cancel    context.CancelFunc
	ticker    Ticker
	ended     bool
}

type Info struct {
	ID       int64
	Interval time.Duration
	Prompt   string
	Total    int
	Executed int
	Deadline time.Time
}

type Looper struct {
	exec Executor
	note Notifier
	busy BusyFunc
	now  func() time.Time
	tick func(time.Duration) Ticker

	mu    sync.Mutex
	base  context.Context
	stop  context.CancelFunc
	next  int64
	loops map[int64]*entry
}

type Option func(*Looper)

func WithClock(now func() time.Time) Option {
	return func(l *Looper) { l.now = now }
}

func WithTicker(tick func(time.Duration) Ticker) Option {
	return func(l *Looper) { l.tick = tick }
}

func New(exec Executor, note Notifier, busy BusyFunc, opts ...Option) *Looper {
	if exec == nil {
		exec = func(context.Context, Conversation, string) (string, error) { return "", nil }
	}
	if note == nil {
		note = func(Conversation, string) {}
	}
	if busy == nil {
		busy = func(Conversation) bool { return false }
	}
	base, stop := context.WithCancel(context.Background())
	l := &Looper{
		exec:  exec,
		note:  note,
		busy:  busy,
		now:   time.Now,
		tick:  func(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} },
		base:  base,
		stop:  stop,
		loops: make(map[int64]*entry),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

var ErrGlobalLimit = fmt.Errorf("loop: too many active loops")

func (l *Looper) Create(conv Conversation, req Request) (Info, error) {
	if err := check(req); err != nil {
		return Info{}, err
	}
	l.mu.Lock()
	for _, e := range l.loops {
		if !e.ended && e.conv == conv {
			id := e.id
			l.mu.Unlock()
			return Info{}, &ExistsError{ID: id}
		}
	}
	live := 0
	for _, e := range l.loops {
		if !e.ended {
			live++
		}
	}
	if live >= MaxGlobal {
		l.mu.Unlock()
		return Info{}, ErrGlobalLimit
	}
	l.next++
	now := l.now()
	e := &entry{
		id:       l.next,
		conv:     conv,
		prompt:   req.Prompt,
		interval: req.Interval,
		total:    req.Count,
	}
	if req.Count > 0 {
		e.remaining = req.Count
		e.deadline = now.Add(maxWallAge)
	} else {
		e.deadline = now.Add(req.Length)
	}
	e.ctx, e.cancel = context.WithCancel(l.base)
	e.ticker = l.tick(req.Interval)
	l.loops[e.id] = e
	info := e.info()
	go l.run(e.id)
	end := fmt.Sprintf("x%d", req.Count)
	if req.Count == 0 {
		end = "for " + req.Length.String()
	}
	slog.Info("loop started", "loop_id", e.id, "interval", req.Interval, "end", end, "conversation", fingerprint(conv))
	l.mu.Unlock()
	return info, nil
}

func fingerprint(conv Conversation) string {
	return conv.Platform + ":" + conv.ChannelID + ":" + conv.ThreadID + ":" + conv.UserID
}

func check(req Request) error {
	if req.Interval < MinInterval || req.Interval > MaxInterval {
		return fmt.Errorf("loop: interval %s outside %s-%s", req.Interval, MinInterval, MaxInterval)
	}
	if req.Prompt == "" {
		return fmt.Errorf("loop: empty prompt")
	}
	if utf8.RuneCountInString(req.Prompt) > MaxPromptRunes {
		return fmt.Errorf("loop: prompt exceeds %d characters", MaxPromptRunes)
	}
	hasCount := req.Count > 0
	hasLength := req.Length > 0
	if hasCount == hasLength {
		return fmt.Errorf("loop: need exactly one of count or duration")
	}
	if hasCount && (req.Count < MinCount || req.Count > MaxCount) {
		return fmt.Errorf("loop: count %d outside %d-%d", req.Count, MinCount, MaxCount)
	}
	if hasLength && (req.Length < MinDuration || req.Length > MaxDuration) {
		return fmt.Errorf("loop: duration %s outside %s-%s", req.Length, MinDuration, MaxDuration)
	}
	return nil
}

func (e *entry) info() Info {
	return Info{
		ID:       e.id,
		Interval: e.interval,
		Prompt:   e.prompt,
		Total:    e.total,
		Executed: e.executed,
		Deadline: e.deadline,
	}
}

func (l *Looper) run(id int64) {
	l.mu.Lock()
	e, ok := l.loops[id]
	l.mu.Unlock()
	if !ok {
		return
	}
	ticker := e.ticker
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.Chan():
			l.fire(id)
		}
	}
}

func (l *Looper) fire(id int64) {
	now := l.now()
	l.mu.Lock()
	e, ok := l.loops[id]
	if !ok || e.ended {
		l.mu.Unlock()
		return
	}
	if !now.Before(e.deadline) {
		text := fmt.Sprintf("🏁 Loop %d finished: time elapsed.", e.id)
		l.terminateLocked(e, "deadline")
		l.mu.Unlock()
		l.note(e.conv, text)
		return
	}
	if l.busy(e.conv) {
		slog.Info("loop tick skipped, conversation busy", "loop_id", e.id, "iteration", e.executed+1)
		l.mu.Unlock()
		return
	}
	conv, prompt := e.conv, e.prompt
	l.mu.Unlock()

	execCtx, cancel := context.WithTimeout(e.ctx, iterationTimeout)
	out, execErr := l.exec(execCtx, conv, prompt)
	cancel()

	l.mu.Lock()
	if e.ended {
		l.mu.Unlock()
		return
	}
	var texts []string
	if execErr != nil {
		e.executed++
		if e.total > 0 {
			e.remaining--
		}
		texts = append(texts, fmt.Sprintf("⚠️ Loop %d run failed: %s", e.id, execErr.Error()))
		if e.total > 0 && e.remaining == 0 {
			texts = append(texts, fmt.Sprintf("🏁 Loop %d finished: all %d runs completed.", e.id, e.total))
			l.terminateLocked(e, "exhausted")
		}
		l.mu.Unlock()
		for _, t := range texts {
			l.note(conv, t)
		}
		return
	}
	body, done := SplitDone(out)
	if body == "" {
		body = "(no output)"
	}
	e.executed++
	if e.total > 0 {
		e.remaining--
		texts = append(texts, fmt.Sprintf("🔁 Loop %d (%d/%d):\n%s", e.id, e.executed, e.total, body))
	} else {
		texts = append(texts, fmt.Sprintf("🔁 Loop %d (#%d):\n%s", e.id, e.executed, body))
	}
	switch {
	case done:
		texts = append(texts, fmt.Sprintf("✅ Loop %d completed.", e.id))
		l.terminateLocked(e, "completed")
	case e.total > 0 && e.remaining == 0:
		texts = append(texts, fmt.Sprintf("🏁 Loop %d finished: all %d runs completed.", e.id, e.total))
		l.terminateLocked(e, "exhausted")
	}
	l.mu.Unlock()
	for _, t := range texts {
		l.note(conv, t)
	}
}

func (l *Looper) finishLocked(e *entry) {
	e.ended = true
	delete(l.loops, e.id)
	e.cancel()
}

func (l *Looper) terminateLocked(e *entry, reason string) {
	l.finishLocked(e)
	slog.Info("loop terminated", "loop_id", e.id, "reason", reason)
}

func (l *Looper) Stop(conv Conversation, id int64) bool {
	l.mu.Lock()
	e, ok := l.loops[id]
	if !ok || e.ended || e.conv != conv {
		l.mu.Unlock()
		return false
	}
	var text string
	if e.total > 0 {
		text = fmt.Sprintf("🛑 Loop %d stopped after %d of %d runs.", e.id, e.executed, e.total)
	} else {
		text = fmt.Sprintf("🛑 Loop %d stopped after %d runs.", e.id, e.executed)
	}
	l.terminateLocked(e, "stopped")
	l.mu.Unlock()
	l.note(e.conv, text)
	return true
}

func (l *Looper) List(conv Conversation) []Info {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Info
	for _, e := range l.loops {
		if !e.ended && e.conv == conv {
			out = append(out, e.info())
		}
	}
	return out
}

func (l *Looper) CancelConversation(conv Conversation) int {
	type pending struct {
		conv Conversation
		text string
	}
	l.mu.Lock()
	var stopped []pending
	for _, e := range l.loops {
		if e.ended || e.conv != conv {
			continue
		}
		var text string
		if e.total > 0 {
			text = fmt.Sprintf("🛑 Loop %d stopped after %d of %d runs.", e.id, e.executed, e.total)
		} else {
			text = fmt.Sprintf("🛑 Loop %d stopped after %d runs.", e.id, e.executed)
		}
		target := e.conv
		id := e.id
		l.finishLocked(e)
		slog.Info("loop terminated", "loop_id", id, "reason", "stopped")
		stopped = append(stopped, pending{conv: target, text: text})
	}
	l.mu.Unlock()
	for _, p := range stopped {
		l.note(p.conv, p.text)
	}
	return len(stopped)
}

func (l *Looper) StopAll() {
	l.mu.Lock()
	var cancel []context.CancelFunc
	for _, e := range l.loops {
		if !e.ended {
			e.ended = true
			cancel = append(cancel, e.cancel)
		}
	}
	l.loops = make(map[int64]*entry)
	stop := l.stop
	l.mu.Unlock()
	for _, c := range cancel {
		c()
	}
	stop()
}

func SplitDone(output string) (string, bool) {
	var kept []string
	done := false
	for _, line := range strings.Split(output, "\n") {
		if strings.EqualFold(strings.TrimSpace(line), "done") {
			done = true
			continue
		}
		kept = append(kept, line)
	}
	return strings.TrimSpace(strings.Join(kept, "\n")), done
}

func FormatInterval(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	default:
		return fmt.Sprintf("%ds", d/time.Second)
	}
}

func FormatLeft(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%dh%dm left", int(d.Hours()), int(d.Minutes())%60)
	case d >= time.Minute:
		return fmt.Sprintf("%dm left", int(d.Minutes()))
	default:
		return fmt.Sprintf("%ds left", int(d.Seconds()))
	}
}

func TruncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	var b strings.Builder
	runes := 0
	for _, r := range s {
		if runes >= n {
			break
		}
		b.WriteRune(r)
		runes++
	}
	return b.String() + "…"
}
