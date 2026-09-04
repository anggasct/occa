package loop

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type manualTicker struct {
	ch chan time.Time
}

func (m *manualTicker) Chan() <-chan time.Time { return m.ch }
func (m *manualTicker) Stop()                  {}

type harness struct {
	looper *Looper
	ticks  map[int64]*manualTicker
	now    time.Time

	mu        sync.Mutex
	execCalls int
	outputs   []string
	execErr   error
	blockExec chan struct{}
	notified  []posted
	busy      bool
	execWait  chan struct{}
}

type posted struct {
	conv Conversation
	text string
}

func newHarness() *harness {
	h := &harness{
		now:      time.Now(),
		ticks:    make(map[int64]*manualTicker),
		execWait: make(chan struct{}, 64),
	}
	h.looper = New(h.execute, h.notify, h.isBusy,
		WithClock(func() time.Time { return h.now }),
		WithTicker(func(time.Duration) Ticker {
			m := &manualTicker{ch: make(chan time.Time, 64)}
			return m
		}),
	)
	return h
}

func (h *harness) execute(_ context.Context, _ Conversation, _ string) (string, error) {
	h.mu.Lock()
	if h.blockExec != nil {
		ch := h.blockExec
		h.mu.Unlock()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			return "", errors.New("test executor timeout")
		}
		h.mu.Lock()
	}
	h.execCalls++
	out := ""
	if len(h.outputs) > 0 {
		out, h.outputs = h.outputs[0], h.outputs[1:]
	}
	err := h.execErr
	h.mu.Unlock()
	select {
	case h.execWait <- struct{}{}:
	default:
	}
	return out, err
}

func (h *harness) notify(conv Conversation, text string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.notified = append(h.notified, posted{conv: conv, text: text})
}

func (h *harness) isBusy(Conversation) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.busy
}

func (h *harness) create(t *testing.T, conv Conversation, args string) Info {
	t.Helper()
	req, err := ParseRequest(args)
	if err != nil {
		t.Fatalf("ParseRequest(%q): %v", args, err)
	}
	info, err := h.looper.Create(conv, req)
	if err != nil {
		t.Fatalf("Create(%q): %v", args, err)
	}
	return info
}

func (h *harness) tick(id int64) {
	h.looper.fire(id)
}

func (h *harness) waitExec(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.mu.Lock()
		got := h.execCalls
		h.mu.Unlock()
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited for %d exec calls, got %d", n, got)
		}
		time.Sleep(time.Millisecond)
	}
}

func (h *harness) texts() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, p := range h.notified {
		out = append(out, p.text)
	}
	return out
}

var convA = Conversation{Platform: "telegram", ChannelID: "chat1", UserID: "user1"}
var convB = Conversation{Platform: "telegram", ChannelID: "chat1", UserID: "user2"}

func TestCreateAndTickToExhaustion(t *testing.T) {
	h := newHarness()
	info := h.create(t, convA, "every 1m x3 check status")
	if info.ID == 0 {
		t.Fatal("expected nonzero loop id")
	}
	for i := 0; i < 3; i++ {
		h.tick(info.ID)
	}
	h.waitExec(t, 3)
	texts := h.texts()
	if len(texts) != 4 {
		t.Fatalf("got %d messages, want 4 (3 iterations + terminal): %q", len(texts), texts)
	}
	for i := 0; i < 3; i++ {
		want := "🔁 Loop 1 ("
		if !strings.HasPrefix(texts[i], want) {
			t.Errorf("message %d = %q, want prefix %q", i, texts[i], want)
		}
	}
	if !strings.Contains(texts[3], "finished") {
		t.Errorf("terminal = %q, want exhaustion notice", texts[3])
	}
	h.tick(info.ID)
	h.mu.Lock()
	calls := h.execCalls
	h.mu.Unlock()
	if calls != 3 {
		t.Errorf("exec calls after terminal = %d, want 3", calls)
	}
}

func TestDoneEndsEarly(t *testing.T) {
	h := newHarness()
	h.outputs = []string{"still working", "all green\nDONE"}
	info := h.create(t, convA, "every 1m x5 watch it")
	h.tick(info.ID)
	h.tick(info.ID)
	h.waitExec(t, 2)
	texts := h.texts()
	if len(texts) != 3 {
		t.Fatalf("got %d messages, want 3: %q", len(texts), texts)
	}
	if strings.Contains(texts[1], "DONE") {
		t.Errorf("DONE line leaked into relayed text: %q", texts[1])
	}
	if !strings.Contains(texts[2], "completed") {
		t.Errorf("terminal = %q, want completed notice", texts[2])
	}
	h.tick(info.ID)
	h.mu.Lock()
	calls := h.execCalls
	h.mu.Unlock()
	if calls != 2 {
		t.Errorf("exec calls after DONE = %d, want 2", calls)
	}
}

func TestDoneCaseVariants(t *testing.T) {
	for _, tc := range []struct {
		name string
		out  string
		want bool
		body string
	}{
		{"upper", "DONE", true, ""},
		{"lower", "done", true, ""},
		{"padded", "  Done  ", true, ""},
		{"middle", "line one\ndOnE\nline three", true, "line one\nline three"},
		{"substring", "all done here", false, "all done here"},
		{"prefix", "DONE with more", false, "DONE with more"},
	} {
		body, done := SplitDone(tc.out)
		if done != tc.want || body != tc.body {
			t.Errorf("%s: SplitDone(%q) = (%q, %v), want (%q, %v)", tc.name, tc.out, body, done, tc.body, tc.want)
		}
	}
}

func TestDurationDeadline(t *testing.T) {
	h := newHarness()
	info := h.create(t, convA, "every 1m for 2m watch deploy")
	h.tick(info.ID)
	h.waitExec(t, 1)
	h.now = h.now.Add(3 * time.Minute)
	h.tick(info.ID)
	h.mu.Lock()
	calls := h.execCalls
	h.mu.Unlock()
	if calls != 1 {
		t.Fatalf("exec calls = %d, want 1 (deadline, no second run)", calls)
	}
	texts := h.texts()
	last := texts[len(texts)-1]
	if !strings.Contains(last, "time elapsed") {
		t.Errorf("terminal = %q, want deadline notice", last)
	}
}

func TestBusySkipKeepsRemaining(t *testing.T) {
	h := newHarness()
	h.busy = true
	info := h.create(t, convA, "every 1m x2 poll")
	h.tick(info.ID)
	h.mu.Lock()
	calls := h.execCalls
	notified := len(h.notified)
	h.mu.Unlock()
	if calls != 0 || notified != 0 {
		t.Fatalf("busy tick executed (%d calls, %d notices), want skip", calls, notified)
	}
	h.busy = false
	h.tick(info.ID)
	h.tick(info.ID)
	h.waitExec(t, 2)
	texts := h.texts()
	if len(texts) != 3 {
		t.Fatalf("got %d messages, want 3 (busy skip consumed nothing): %q", len(texts), texts)
	}
}

func TestStopSingleTerminal(t *testing.T) {
	h := newHarness()
	info := h.create(t, convA, "every 1m x5 poll")
	h.tick(info.ID)
	h.waitExec(t, 1)
	if !h.looper.Stop(convA, info.ID) {
		t.Fatal("Stop returned false for live loop")
	}
	if h.looper.Stop(convA, info.ID) {
		t.Error("second Stop returned true, want false")
	}
	texts := h.texts()
	last := texts[len(texts)-1]
	if !strings.Contains(last, "stopped") {
		t.Errorf("terminal = %q, want stopped notice", last)
	}
	h.tick(info.ID)
	h.mu.Lock()
	calls := h.execCalls
	h.mu.Unlock()
	if calls != 1 {
		t.Errorf("exec calls after stop = %d, want 1", calls)
	}
}

func TestStopForeignConversation(t *testing.T) {
	h := newHarness()
	info := h.create(t, convA, "every 1m x3 poll")
	if h.looper.Stop(convB, info.ID) {
		t.Fatal("cross-conversation Stop returned true")
	}
	if got := len(h.looper.List(convA)); got != 1 {
		t.Errorf("loop missing after foreign stop, list = %d", got)
	}
}

func TestListIsolation(t *testing.T) {
	h := newHarness()
	h.create(t, convA, "every 1m x3 poll a")
	h.create(t, convB, "every 1m x3 poll b")
	if got := len(h.looper.List(convA)); got != 1 {
		t.Errorf("convA list = %d, want 1", got)
	}
	for _, info := range h.looper.List(convB) {
		if info.Prompt != "poll b" {
			t.Errorf("convB saw foreign prompt %q", info.Prompt)
		}
	}
}

func TestConversationCap(t *testing.T) {
	h := newHarness()
	h.create(t, convA, "every 1m x3 first")
	req, _ := ParseRequest("every 1m x3 second")
	if _, err := h.looper.Create(convA, req); err == nil {
		t.Fatal("second loop in same conversation succeeded")
	} else if _, ok := err.(*ExistsError); !ok {
		t.Fatalf("error = %T (%v), want *ExistsError", err, err)
	}
}

func TestExecErrorContinues(t *testing.T) {
	h := newHarness()
	h.execErr = errors.New("agent unreachable")
	info := h.create(t, convA, "every 1m x2 poll")
	h.tick(info.ID)
	h.tick(info.ID)
	h.waitExec(t, 2)
	texts := h.texts()
	if len(texts) != 3 {
		t.Fatalf("got %d messages, want 3 (2 failures + terminal): %q", len(texts), texts)
	}
	if !strings.Contains(texts[0], "failed") {
		t.Errorf("first notice = %q, want failure notice", texts[0])
	}
}

func TestCancelConversation(t *testing.T) {
	h := newHarness()
	h.create(t, convA, "every 1m x3 poll")
	if n := h.looper.CancelConversation(convB); n != 0 {
		t.Errorf("cancel foreign = %d, want 0", n)
	}
	if n := h.looper.CancelConversation(convA); n != 1 {
		t.Fatalf("cancel own = %d, want 1", n)
	}
	if got := len(h.looper.List(convA)); got != 0 {
		t.Errorf("list after cancel = %d, want 0", got)
	}
	texts := h.texts()
	if len(texts) != 1 || !strings.Contains(texts[0], "stopped") {
		t.Errorf("notices = %q, want single stopped terminal", texts)
	}
}

func TestStopAllCancelsInflight(t *testing.T) {
	h := newHarness()
	h.blockExec = make(chan struct{})
	info := h.create(t, convA, "every 1m x3 poll")
	done := make(chan struct{})
	go func() {
		h.tick(info.ID)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	h.looper.StopAll()
	close(h.blockExec)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("tick did not return after StopAll")
	}
	for _, text := range h.texts() {
		if strings.Contains(text, "🔁 Loop") {
			t.Errorf("in-flight result posted after StopAll: %q", text)
		}
	}
}

func TestConcurrentStopSingleTerminal(t *testing.T) {
	h := newHarness()
	info := h.create(t, convA, "every 1m x5 poll")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.tick(info.ID)
			h.looper.Stop(convA, info.ID)
		}()
	}
	wg.Wait()
	h.mu.Lock()
	terms := 0
	for _, p := range h.notified {
		if strings.Contains(p.text, "finished") || strings.Contains(p.text, "stopped") || strings.Contains(p.text, "completed") {
			terms++
		}
	}
	h.mu.Unlock()
	if terms != 1 {
		t.Errorf("terminal messages = %d, want exactly 1", terms)
	}
}

func TestGlobalLimitRejects21st(t *testing.T) {
	h := newHarness()
	for i := 0; i < MaxGlobal; i++ {
		conv := Conversation{Platform: "telegram", ChannelID: "chat1", ThreadID: "", UserID: "limituser" + string(rune('a'+i/10)) + string(rune('0'+i%10))}
		req, err := ParseRequest("every 1m x2 poll")
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		if _, err := h.looper.Create(conv, req); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}
	req, _ := ParseRequest("every 1m x2 overflow")
	if _, err := h.looper.Create(convA, req); !errors.Is(err, ErrGlobalLimit) {
		t.Fatalf("21st create err = %v, want ErrGlobalLimit", err)
	}
}

func TestRestartEmptiness(t *testing.T) {
	h := newHarness()
	h.create(t, convA, "every 1m x3 poll")
	if got := len(h.looper.List(convA)); got != 1 {
		t.Fatalf("list before restart = %d, want 1", got)
	}
	fresh := New(h.execute, h.notify, h.isBusy,
		WithClock(func() time.Time { return h.now }),
		WithTicker(func(time.Duration) Ticker {
			return &manualTicker{ch: make(chan time.Time, 64)}
		}),
	)
	if got := len(fresh.List(convA)); got != 0 {
		t.Errorf("fresh looper list = %d, want 0 (no resume after restart)", got)
	}
	h.looper.StopAll()
	if got := len(h.looper.List(convA)); got != 0 {
		t.Errorf("list after StopAll = %d, want 0", got)
	}
}

func TestCountLoopWallClockCap(t *testing.T) {
	h := newHarness()
	info := h.create(t, convA, "every 1m x60 long poll")
	h.now = h.now.Add(maxWallAge + time.Minute)
	h.tick(info.ID)
	h.mu.Lock()
	calls := h.execCalls
	h.mu.Unlock()
	if calls != 0 {
		t.Fatalf("exec calls past 4h wall cap = %d, want 0", calls)
	}
	texts := h.texts()
	if len(texts) != 1 || !strings.Contains(texts[0], "time elapsed") {
		t.Errorf("terminal = %q, want single deadline notice", texts)
	}
}
