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

type auditingReply struct {
	*fakeReplyContext
	maxLive int
}

func (a *auditingReply) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	ref, err := a.fakeReplyContext.SendWithButtons(text, buttons)
	a.audit()
	return ref, err
}

func (a *auditingReply) EditWithButtons(ref channel.MessageRef, text string, buttons []channel.Button) error {
	err := a.fakeReplyContext.EditWithButtons(ref, text, buttons)
	a.audit()
	return err
}

func (a *auditingReply) audit() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n := liveStopButtonCount(a.fakeReplyContext); n > a.maxLive {
		a.maxLive = n
	}
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

func TestStreamerPerPhaseCardsStepCarry(t *testing.T) {
	reply := &auditingReply{fakeReplyContext: newFakeReplyContext()}
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.SetStopCallbackData("stop:turn-1")
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

	wantSends := []string{"⚙️ [Step 1] bash", "narration between tools", "⚙️ [Step 2] read"}
	if len(reply.sends) != len(wantSends) {
		t.Fatalf("sends = %v, want chronological %v", reply.sends, wantSends)
	}
	for i, want := range wantSends {
		if reply.sends[i] != want {
			t.Fatalf("send %d = %q, want %q (all sends: %v)", i, reply.sends[i], want, reply.sends)
		}
	}

	frozenEdits := reply.editButtons["msg-1"]
	if len(frozenEdits) == 0 || frozenEdits[len(frozenEdits)-1] != nil {
		t.Fatalf("previous card not stripped of buttons when the new card was sent: %v", frozenEdits)
	}
	if reply.edits["msg-1"] != nil && !editsContain(reply.edits["msg-1"], "⚙️ [Step 1] bash") {
		t.Fatalf("frozen card text drift: %v", reply.edits["msg-1"])
	}

	terminalEdits := reply.editButtons["msg-3"]
	if len(terminalEdits) == 0 || len(terminalEdits[len(terminalEdits)-1]) != 0 {
		t.Fatalf("terminal card buttons not cleared: %v", terminalEdits)
	}
	wantRollup := "✅ 2 tool calls\n\n<blockquote expandable>\n• bash ×1\n• read ×1\n</blockquote>"
	cardEdits := reply.edits["msg-3"]
	if len(cardEdits) == 0 || cardEdits[len(cardEdits)-1] != wantRollup {
		t.Fatalf("rollup = %v, want last %q", cardEdits, wantRollup)
	}

	if got := liveStopButtonCount(reply.fakeReplyContext); got != 0 {
		t.Fatalf("live stop buttons at turn end = %d, want 0", got)
	}
	if reply.maxLive > 1 {
		t.Fatalf("max live stop buttons during turn = %d, want at most 1", reply.maxLive)
	}
}

func TestStreamerStepMonotonicAcrossPhases(t *testing.T) {
	reply := newFakeReplyContext()
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.workingEditInterval = -1

	events := make(chan Event, 12)
	events <- Event{Type: EventTool, Delta: "bash"}
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

	wantSends := []string{
		"⚙️ [Step 1] bash",
		"one",
		"⚙️ [Step 2] bash",
		"two",
		"⚙️ [Step 3] read",
	}
	if len(reply.sends) != len(wantSends) {
		t.Fatalf("sends = %v, want chronological %v", reply.sends, wantSends)
	}
	for i, want := range wantSends {
		if reply.sends[i] != want {
			t.Fatalf("send %d = %q, want %q (all sends: %v)", i, reply.sends[i], want, reply.sends)
		}
	}

	for msgID, want := range map[string]string{
		"msg-1": "⚙️ [Step 1] bash ×2",
		"msg-3": "⚙️ [Step 2] bash",
		"msg-5": "⚙️ [Step 3] read",
	} {
		if !editsContain(reply.edits[msgID], want) && reply.sent[msgID] != want {
			t.Fatalf("card %s never showed %q: sent %q edits %v", msgID, want, reply.sent[msgID], reply.edits[msgID])
		}
	}

	wantRollup := "✅ 4 tool calls\n\n<blockquote expandable>\n• bash ×3\n• read ×1\n</blockquote>"
	edits := reply.edits["msg-5"]
	if len(edits) == 0 || edits[len(edits)-1] != wantRollup {
		t.Fatalf("rollup = %v, want last %q", edits, wantRollup)
	}
}

func TestStreamerSupersededCardStripsStopButton(t *testing.T) {
	reply := newFakeReplyContext()
	sink := &flakyCardSink{reply: reply, failSends: 1}
	s := NewStreamerWithSink(sink, render.New(), render.Telegram)
	s.SetStopCallbackData("stop:turn-2")
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
	if len(ghostBtns) != 1 || ghostBtns[0].Value != "stop:turn-2" {
		t.Fatalf("ghost card buttons = %+v, want the stop button at send time", ghostBtns)
	}
	ghostEdits := reply.editButtons["msg-1"]
	if len(ghostEdits) == 0 || len(ghostEdits[len(ghostEdits)-1]) != 0 {
		t.Fatalf("ghost card buttons not stripped: %v", ghostEdits)
	}
	replacementBtns := reply.sentButtons["msg-3"]
	if len(replacementBtns) != 1 || replacementBtns[0].Value != "stop:turn-2" {
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
	s.SetStopCallbackData("stop:turn-3")
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

func TestStreamerReasoningFreezesOnPhaseCard(t *testing.T) {
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
		now.Store(104)
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
		now.Store(106)
		events <- Event{Type: EventTool, Delta: "read"}
		for {
			reply.mu.Lock()
			n := len(reply.sends)
			reply.mu.Unlock()
			if n > 2 {
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

	if got := progressCardSendCount(reply); got != 2 {
		t.Fatalf("progress card sends = %d, want 2 (sends: %v)", got, reply.sends)
	}
	frozen := reply.edits["msg-1"]
	if len(frozen) == 0 || frozen[len(frozen)-1] != "⚙️ [Step 1] bash (4s) · 🧠 Thought for 2s" {
		t.Fatalf("phase 1 card did not freeze with its reasoning suffix: %v", frozen)
	}
	if reply.sent["msg-3"] != "⚙️ [Step 2] read" {
		t.Fatalf("phase 2 card = %q, want carried [Step 2]", reply.sent["msg-3"])
	}
	wantRollup := "✅ 2 tool calls · 🧠 Thought for 2s\n\n<blockquote expandable>\n• bash ×1\n• read ×1\n</blockquote>"
	card2 := reply.edits["msg-3"]
	if len(card2) == 0 || card2[len(card2)-1] != wantRollup {
		t.Fatalf("rollup = %v, want last %q", card2, wantRollup)
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
	reply := &auditingReply{fakeReplyContext: newFakeReplyContext()}
	s := NewStreamer(reply, render.New(), render.Telegram)
	s.SetStopCallbackData("stop:turn-4")
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
	if got := progressCardSendCount(reply.fakeReplyContext); got != 2 {
		t.Fatalf("progress card sends = %d, want 2 (sends: %v)", got, reply.sends)
	}
	if len(reply.sentButtons["msg-3"]) != 1 {
		t.Fatalf("live card did not carry the stop button: %+v", reply.sentButtons["msg-3"])
	}
	if len(reply.editButtons["msg-1"]) == 0 || reply.editButtons["msg-1"][len(reply.editButtons["msg-1"])-1] != nil {
		t.Fatalf("previous card buttons not migrated away: %v", reply.editButtons["msg-1"])
	}
	terminalEdits := reply.editButtons["msg-3"]
	if len(terminalEdits) == 0 || len(terminalEdits[len(terminalEdits)-1]) != 0 {
		t.Fatalf("terminal card buttons not cleared: %v", terminalEdits)
	}
	if !editsContain(reply.edits["msg-3"], "⚙️ [Step 2] read") && reply.sent["msg-3"] != "⚙️ [Step 2] read" {
		t.Fatalf("card never reached [Step 2] after prompts: %v", reply.edits["msg-3"])
	}
	if reply.maxLive > 1 {
		t.Fatalf("max live stop buttons during turn = %d, want at most 1", reply.maxLive)
	}
}
