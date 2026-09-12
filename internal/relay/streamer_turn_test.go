package relay

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

func progressCardSendCount(reply *fakeReplyContext) int {
	count := 0
	for _, m := range reply.sends {
		if strings.HasPrefix(m, "⚙️ ") {
			count++
		}
	}
	return count
}

func editsContain(edits []string, want string) bool {
	for _, e := range edits {
		if e == want {
			return true
		}
	}
	return false
}

func liveStopButtonCount(reply *fakeReplyContext) int {
	count := 0
	for id, btns := range reply.sentButtons {
		if len(btns) == 0 {
			continue
		}
		edits := reply.editButtons[id]
		if len(edits) > 0 && len(edits[len(edits)-1]) == 0 {
			continue
		}
		count++
	}
	return count
}

type flakyCardSink struct {
	reply     *fakeReplyContext
	failSends int
}

func (f *flakyCardSink) Send(ctx context.Context, text string) (EditHandle, error) {
	ref, err := f.reply.Send(text)
	if err != nil {
		return nil, err
	}
	return &channelEditHandle{reply: f.reply, ref: ref}, nil
}

func (f *flakyCardSink) SendWithButtons(ctx context.Context, text string, buttons []channel.Button) (EditHandle, error) {
	ref, err := f.reply.SendWithButtons(text, buttons)
	if err != nil {
		return nil, err
	}
	handle := &channelEditHandle{reply: f.reply, ref: ref}
	if f.failSends > 0 {
		f.failSends--
		return handle, errors.New("simulated card send failure")
	}
	return handle, nil
}

func (f *flakyCardSink) SendTyping(ctx context.Context) error {
	return f.reply.SendTyping()
}

func TestStreamerOneCardPerTurnAcrossSegments(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.workingEditInterval = -1

	events := make(chan Event, 8)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDelta, Delta: "narration between tools"}
	events <- Event{Type: EventSegment}
	events <- Event{Type: EventTool, Delta: "read"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	if got := progressCardSendCount(reply); got != 1 {
		t.Fatalf("progress card sends = %d, want 1 (sends: %v)", got, reply.sends)
	}
	if reply.sent["msg-2"] != "narration between tools" {
		t.Fatalf("narration bubble = %q, want the finalized segment text", reply.sent["msg-2"])
	}
	edits := reply.edits["msg-1"]
	if !editsContain(edits, "⚙️ [Step 2] read") {
		t.Fatalf("card never reached [Step 2] after the segment: %v", edits)
	}
	wantRollup := "✅ 2 tool calls\n\n<blockquote expandable>\n• bash ×1\n• read ×1\n</blockquote>"
	if len(edits) == 0 || edits[len(edits)-1] != wantRollup {
		t.Fatalf("rollup = %v, want last %q", edits, wantRollup)
	}
}

func TestStreamerStepMonotonicAcrossSegments(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.workingEditInterval = -1

	events := make(chan Event, 12)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDelta, Delta: "one"}
	events <- Event{Type: EventSegment}
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDelta, Delta: "two"}
	events <- Event{Type: EventSegment}
	events <- Event{Type: EventTool, Delta: "read"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	if got := progressCardSendCount(reply); got != 1 {
		t.Fatalf("progress card sends = %d, want 1 (sends: %v)", got, reply.sends)
	}
	edits := reply.edits["msg-1"]
	if !editsContain(edits, "⚙️ [Step 1] bash ×2") {
		t.Fatalf("recurring tool did not consolidate across the segment: %v", edits)
	}
	if !editsContain(edits, "⚙️ [Step 2] read") {
		t.Fatalf("card never reached [Step 2]: %v", edits)
	}
	for _, e := range edits {
		if strings.HasPrefix(e, "⚙️ [Step 1]") && e != "⚙️ [Step 1] bash" && e != "⚙️ [Step 1] bash ×2" {
			t.Fatalf("unexpected [Step 1] recurrence: %v", edits)
		}
	}
	wantRollup := "✅ 3 tool calls\n\n<blockquote expandable>\n• bash ×2\n• read ×1\n</blockquote>"
	if len(edits) == 0 || edits[len(edits)-1] != wantRollup {
		t.Fatalf("rollup = %v, want last %q", edits, wantRollup)
	}
}

func TestStreamerSupersededCardStripsStopButton(t *testing.T) {
	reply := newFakeReplyContext()
	sink := &flakyCardSink{reply: reply, failSends: 1}
	s := NewStreamerWithSink(sink, render.New(), render.Telegram)
	s.SetStopCallbackData("stop:turn-1")
	s.workingEditInterval = -1

	events := make(chan Event, 8)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDelta, Delta: "narration"}
	events <- Event{Type: EventSegment}
	events <- Event{Type: EventTool, Delta: "read"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	ghostBtns := reply.sentButtons["msg-1"]
	if len(ghostBtns) != 1 || ghostBtns[0].Value != "stop:turn-1" {
		t.Fatalf("ghost card buttons = %+v, want the stop button at send time", ghostBtns)
	}
	ghostEdits := reply.editButtons["msg-1"]
	if len(ghostEdits) == 0 || len(ghostEdits[len(ghostEdits)-1]) != 0 {
		t.Fatalf("ghost card buttons not stripped: %v", ghostEdits)
	}
	replacementBtns := reply.sentButtons["msg-3"]
	if len(replacementBtns) != 1 || replacementBtns[0].Value != "stop:turn-1" {
		t.Fatalf("replacement card buttons = %+v, want the stop button", replacementBtns)
	}
	replacementEdits := reply.editButtons["msg-3"]
	if len(replacementEdits) == 0 || len(replacementEdits[len(replacementEdits)-1]) != 0 {
		t.Fatalf("replacement card buttons not cleared at terminal: %v", replacementEdits)
	}
	if got := liveStopButtonCount(reply); got != 0 {
		t.Fatalf("live stop buttons at turn end = %d, want 0", got)
	}
}

func TestStreamerStreamEndStripsAllTurnCards(t *testing.T) {
	reply := newFakeReplyContext()
	sink := &flakyCardSink{reply: reply, failSends: 1}
	s := NewStreamerWithSink(sink, render.New(), render.Telegram)
	s.SetStopCallbackData("stop:turn-2")
	s.workingEditInterval = -1

	events := make(chan Event, 4)
	events <- Event{Type: EventTool, Delta: "bash"}
	close(events)

	err := s.Run(context.Background(), events)
	if !errors.Is(err, ErrIncompleteStream) {
		t.Fatalf("Run error = %v, want ErrIncompleteStream", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	if len(reply.sentButtons["msg-1"]) != 1 {
		t.Fatalf("ghost card was sent without buttons: %+v", reply.sentButtons["msg-1"])
	}
	ghostEdits := reply.editButtons["msg-1"]
	if len(ghostEdits) == 0 || len(ghostEdits[len(ghostEdits)-1]) != 0 {
		t.Fatalf("stream end did not strip ghost card buttons: %v", ghostEdits)
	}
	if got := liveStopButtonCount(reply); got != 0 {
		t.Fatalf("live stop buttons after stream end = %d, want 0", got)
	}
}

func TestStreamerReasoningContinuityAcrossSegments(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.workingEditInterval = -1

	var now atomic.Int64
	now.Store(100)
	s.now = func() time.Time { return time.Unix(now.Load(), 0) }

	events := make(chan Event)
	go func() {
		events <- Event{Type: EventTool, Delta: "bash"}
		for {
			reply.mu.Lock()
			n := len(reply.sends)
			reply.mu.Unlock()
			if n > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		events <- Event{Type: EventDelta, Delta: "narration"}
		events <- Event{Type: EventSegment}
		for {
			reply.mu.Lock()
			n := len(reply.sends)
			reply.mu.Unlock()
			if n > 1 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		now.Store(102)
		events <- Event{Type: EventReasoning}
		for {
			reply.mu.Lock()
			n := len(reply.edits["msg-1"])
			reply.mu.Unlock()
			if n > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		now.Store(105)
		events <- Event{Type: EventTool, Delta: "read"}
		for {
			reply.mu.Lock()
			n := len(reply.edits["msg-1"])
			reply.mu.Unlock()
			if n > 1 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		events <- Event{Type: EventDone}
		close(events)
	}()

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	if got := progressCardSendCount(reply); got != 1 {
		t.Fatalf("progress card sends = %d, want 1 (sends: %v)", got, reply.sends)
	}
	edits := reply.edits["msg-1"]
	if !editsContain(edits, "🧠 Thinking… · [Step 1] bash") {
		t.Fatalf("reasoning did not attach to the existing card: %v", edits)
	}
	if !editsContain(edits, "⚙️ [Step 2] read · 🧠 Thought for 3s") {
		t.Fatalf("reasoning duration did not carry across the segment: %v", edits)
	}
	wantRollup := "✅ 2 tool calls · 🧠 Thought for 3s\n\n<blockquote expandable>\n• bash ×1\n• read ×1\n</blockquote>"
	if len(edits) == 0 || edits[len(edits)-1] != wantRollup {
		t.Fatalf("rollup = %v, want last %q", edits, wantRollup)
	}
}

type countingPermissionHandler struct {
	calls   int
	gotID   string
	gotTool string
}

func (h *countingPermissionHandler) Prompt(_ context.Context, req PermissionRequest) error {
	h.calls++
	h.gotID = req.ID
	h.gotTool = req.Tool
	return nil
}

type countingQuestionHandler struct {
	calls int
	gotID string
}

func (h *countingQuestionHandler) Prompt(_ context.Context, req QuestionRequest) error {
	h.calls++
	h.gotID = req.ID
	return nil
}

func TestStreamerPromptsDuringInterleavedTurn(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.SetStopCallbackData("stop:turn-3")
	s.workingEditInterval = -1

	perm := &countingPermissionHandler{}
	quest := &countingQuestionHandler{}
	s.SetPermissionPromptHandler(perm)
	s.SetQuestionPromptHandler(quest)

	events := make(chan Event, 10)
	events <- Event{Type: EventTool, Delta: "bash"}
	events <- Event{Type: EventDelta, Delta: "narration"}
	events <- Event{Type: EventSegment}
	events <- Event{Type: "permission_asked", Permission: &PermissionRequest{ID: "perm-1", SessionID: "sess", Permission: "allow", Tool: "bash"}}
	events <- Event{Type: "question_asked", Question: &QuestionRequest{ID: "quest-1", SessionID: "sess"}}
	events <- Event{Type: EventTool, Delta: "read"}
	events <- Event{Type: EventDone}
	close(events)

	if err := s.Run(context.Background(), events); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reply.mu.Lock()
	defer reply.mu.Unlock()

	if perm.calls != 1 || quest.calls != 1 {
		t.Fatalf("prompt handler calls = perm %d, quest %d; want 1 each", perm.calls, quest.calls)
	}
	if perm.gotID != "perm-1" || perm.gotTool != "bash" {
		t.Fatalf("permission request = id %q tool %q", perm.gotID, perm.gotTool)
	}
	if quest.gotID != "quest-1" {
		t.Fatalf("question request = %q", quest.gotID)
	}
	if got := progressCardSendCount(reply); got != 1 {
		t.Fatalf("progress card sends = %d, want 1 (sends: %v)", got, reply.sends)
	}
	if len(reply.sentButtons["msg-1"]) != 1 {
		t.Fatalf("live card lost its stop button: %+v", reply.sentButtons["msg-1"])
	}
	cardEdits := reply.edits["msg-1"]
	btnEdits := reply.editButtons["msg-1"]
	if len(btnEdits) == 0 || len(btnEdits[len(btnEdits)-1]) != 0 {
		t.Fatalf("terminal card buttons not cleared: %v", btnEdits)
	}
	if !editsContain(cardEdits, "⚙️ [Step 2] read") {
		t.Fatalf("card never reached [Step 2] after prompts: %v", cardEdits)
	}
}
