package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

const (
	noEventTimeout = 15 * time.Minute
	typingInterval = 4 * time.Second
)

const (
	taskTimeoutMessage           = "⚠️ Task timed out (no response for 15 minutes). Send a message to resume or check /status."
	taskTimeoutPermissionMessage = "⚠️ Task timed out waiting for your permission (no response for 15 minutes). Send a message to resume or check /status."
)

var (
	ErrIncompleteStream = errors.New("response stream ended before completion")
	ErrStreamFailed     = errors.New("response stream failed")
	ErrStreamRead       = errors.New("response stream read failed")
)

const incompleteStreamMessage = "⚠️ Response stream ended before completion. The task may still be running; check /status."

const (
	continuationReserve = 64
	continuationMarker  = "↪️ *(continued)*\n\n"
)

type Streamer struct {
	reply                      channel.ReplyContext
	renderer                   render.Renderer
	platform                   render.Platform
	permissionHandler          PermissionPromptHandler
	questionHandler            QuestionPromptHandler
	scheduleAttributionHandler func(input map[string]any) error
	reactionSetter             channel.ReactionSetter
	reactionTarget             channel.MessageRef
	firstRef                   channel.MessageRef
	noEventTimeout             time.Duration
	typingInterval             time.Duration
	permissionPendingFunc      func() bool
	now                        func() time.Time
	workingEditInterval        time.Duration
}

type toolBubble struct {
	ref     channel.MessageRef
	count   int
	context string
}

type workingState struct {
	ref           channel.MessageRef
	total         int
	latestName    string
	latestContext string
	rendered      string
	pending       string
	lastEditAt    time.Time
	hasLastEditAt bool
}

type PermissionPromptHandler interface {
	Prompt(ctx context.Context, request PermissionRequest) error
}

type QuestionPromptHandler interface {
	Prompt(ctx context.Context, request QuestionRequest) error
}

func NewStreamer(reply channel.ReplyContext, renderer render.Renderer, platform render.Platform) *Streamer {
	return &Streamer{
		reply:               reply,
		renderer:            renderer,
		platform:            platform,
		noEventTimeout:      noEventTimeout,
		typingInterval:      typingInterval,
		now:                 time.Now,
		workingEditInterval: 2 * time.Second,
	}
}

func (s *Streamer) SetPermissionPromptHandler(handler PermissionPromptHandler) {
	s.permissionHandler = handler
}

func (s *Streamer) SetQuestionPromptHandler(handler QuestionPromptHandler) {
	s.questionHandler = handler
}

func (s *Streamer) SetScheduleAttributionHandler(handler func(input map[string]any) error) {
	s.scheduleAttributionHandler = handler
}

func (s *Streamer) SetReactionSetter(setter channel.ReactionSetter) {
	s.reactionSetter = setter
}

// SetReactionTarget redirects the 👀/✅/❌ lifecycle reactions onto the
// triggering source message (a genuine read-receipt) instead of occa's own
// first reply. When set, setReaction targets it; otherwise it falls back to
// the first reply ref.
func (s *Streamer) SetReactionTarget(ref channel.MessageRef) {
	s.reactionTarget = ref
}

func (s *Streamer) SetNoEventTimeout(d time.Duration) {
	if d > 0 {
		s.noEventTimeout = d
	}
}

// SetPermissionPendingFunc wires a live check consulted when the no-event
// timeout fires, so the notice can say the stall was caused by a still-pending
// permission approval instead of the generic copy.
func (s *Streamer) SetPermissionPendingFunc(fn func() bool) {
	s.permissionPendingFunc = fn
}

// setReaction drives the status reaction, targeting the source message
// (read-receipt) when a target is set, else the first reply. Failures are
// logged and never fail the stream; a missing setter is a silent no-op.
func (s *Streamer) setReaction(state channel.ReactionState) {
	if s.reactionSetter == nil {
		return
	}
	target := s.reactionTarget
	if target == nil {
		target = s.firstRef
	}
	if target == nil {
		return
	}
	if err := s.reactionSetter.SetReaction(target, state); err != nil {
		slog.Warn("streaming: reaction failed", "state", state, "error", err)
	}
}

// trackFirstRef records the first reply message once it exists so status
// reactions can attach to it when no source target is set.
func (s *Streamer) trackFirstRef(ref channel.MessageRef) {
	if s.firstRef == nil && ref != nil {
		s.firstRef = ref
		if s.reactionTarget == nil {
			s.setReaction(channel.ReactionProcessing)
		}
	}
}

func (s *Streamer) Run(ctx context.Context, events <-chan Event) error {
	var buf strings.Builder
	var refs []channel.MessageRef
	var lastChunks []string
	toolBubbles := make(map[string]*toolBubble)
	cappedTools := make(map[string]bool)
	var working workingState
	totalToolCalls := 0

	typingTicker := time.NewTicker(s.typingInterval)
	defer typingTicker.Stop()
	if err := s.reply.SendTyping(); err != nil {
		slog.Debug("streaming: initial typing indicator failed", "error", err)
	}

	intervals := []time.Duration{500 * time.Millisecond, 1 * time.Second, 2 * time.Second, 3 * time.Second}
	intervalIdx := 0

	timer := time.NewTimer(intervals[0])
	defer timer.Stop()

	timeoutTimer := time.NewTimer(s.noEventTimeout)
	defer timeoutTimer.Stop()

	dirty := false

	// A read-receipt: when a source message target is set, signal "received,
	// processing" (👀) on it before the first reply is emitted.
	if s.reactionTarget != nil {
		s.setReaction(channel.ReactionProcessing)
	}

	for {
		select {
		case <-ctx.Done():
			s.flushWorking(&working)
			return ctx.Err()

		case <-typingTicker.C:
			if err := s.reply.SendTyping(); err != nil {
				slog.Debug("streaming: typing indicator failed", "error", err)
			}

		case <-timeoutTimer.C:
			s.flushWorking(&working)
			msg := taskTimeoutMessage
			if s.permissionPendingFunc != nil && s.permissionPendingFunc() {
				msg = taskTimeoutPermissionMessage
			}
			s.notice(msg)
			s.setReaction(channel.ReactionError)
			return ErrTimeout

		case ev, ok := <-events:
			if !ok {
				s.flushWorking(&working)
				syncErr := s.finalSync(&refs, &lastChunks, buf.String())
				s.notice(incompleteStreamMessage)
				s.setReaction(channel.ReactionError)
				if syncErr != nil {
					return fmt.Errorf("%w: final sync: %v", ErrIncompleteStream, syncErr)
				}
				return ErrIncompleteStream
			}

			timeoutTimer.Reset(s.noEventTimeout)

			switch ev.Type {
			case EventDelta:
				buf.WriteString(ev.Delta)
				dirty = true
			case EventDone:
				s.flushWorking(&working)
				if buf.Len() == 0 {
					s.completedNotice(&refs)
					s.setReaction(channel.ReactionSuccess)
					return nil
				}
				if err := s.finalSync(&refs, &lastChunks, buf.String()); err != nil {
					s.setReaction(channel.ReactionError)
					return err
				}
				s.completedNotice(&refs)
				s.setReaction(channel.ReactionSuccess)
				return nil
			case EventError:
				s.flushWorking(&working)
				s.notice("⚠️ Agent error: " + ev.Delta)
				s.setReaction(channel.ReactionError)
				return fmt.Errorf("%w: %s", ErrStreamFailed, ev.Delta)
			case EventSegment:
				if utf8.RuneCountInString(buf.String()) >= maxToolBubbleResetRunes {
					s.resetToolPhase(&toolBubbles, &cappedTools, &totalToolCalls, &working)
				}
				if buf.Len() > 0 {
					slog.Debug("streaming: segment break", "finalized_len", buf.Len())
					s.finalizeSegment(&refs, &lastChunks, buf.String())
					buf.Reset()
				}
			case EventTool:
				if buf.Len() > 0 {
					s.finalizeSegment(&refs, &lastChunks, buf.String())
					buf.Reset()
				}
				name := ev.Delta
				if name == "" {
					name = "Tool call"
				}
				if s.scheduleAttributionHandler != nil && (name == "schedule_task" || strings.HasSuffix(name, "schedule_task")) && len(ev.ToolInput) > 0 {
					var input map[string]any
					if err := json.Unmarshal(ev.ToolInput, &input); err == nil && len(input) > 0 {
						if err := s.scheduleAttributionHandler(input); err != nil {
							slog.Warn("streaming: schedule attribution handler failed", "error", err)
						}
					}
				}
				ctxStr := normalizeToolContext(ev.ToolContext)
				if ev.ToolSamePart {
					if bubble, ok := toolBubbles[name]; ok {
						if ctxStr != "" && ctxStr != bubble.context {
							bubble.context = ctxStr
							if err := s.reply.Edit(bubble.ref, formatToolLabel(name, bubble.context, bubble.count)); err != nil {
								slog.Warn("streaming: tool notice context edit failed", "tool", name, "error", err)
							}
						}
					} else if cappedTools[name] && ctxStr != "" {
						working.latestName = name
						working.latestContext = ctxStr
						s.queueWorking(&working)
					}
					break
				}
				totalToolCalls++
				if bubble, ok := toolBubbles[name]; ok {
					bubble.count++
					if ctxStr != "" {
						bubble.context = ctxStr
					}
					if err := s.reply.Edit(bubble.ref, formatToolLabel(name, bubble.context, bubble.count)); err != nil {
						slog.Warn("streaming: tool notice edit failed", "tool", name, "error", err)
					}
					break
				}
				if len(toolBubbles) < maxToolBubbles {
					ref, err := s.reply.Send(formatToolLabel(name, ctxStr, 1))
					if err != nil {
						slog.Warn("streaming: tool notice send failed", "tool", name, "error", err)
						break
					}
					bubble := &toolBubble{ref: ref, count: 1, context: ctxStr}
					toolBubbles[name] = bubble
					s.trackFirstRef(ref)
					break
				}
				cappedTools[name] = true
				working.total = totalToolCalls
				working.latestName = name
				working.latestContext = ctxStr
				s.queueWorking(&working)
			case "permission_asked":
				if ev.Permission != nil {
					if s.permissionHandler != nil {
						if err := s.permissionHandler.Prompt(ctx, *ev.Permission); err != nil {
							slog.Warn("streaming: permission prompt failed", "error", err)
						}
					} else {
						slog.Warn("streaming: permission prompt handler unavailable")
					}
				}
			case "question_asked":
				if ev.Question != nil {
					if s.questionHandler != nil {
						if err := s.questionHandler.Prompt(ctx, *ev.Question); err != nil {
							slog.Warn("streaming: question prompt failed", "error", err)
						}
					} else {
						slog.Warn("streaming: question prompt handler unavailable")
					}
				}
			}

		case <-timer.C:
			if dirty {
				dirty = false
				s.syncMessages(&refs, &lastChunks, buf.String())
			}

			if intervalIdx < len(intervals)-1 {
				intervalIdx++
			}
			timer.Reset(intervals[intervalIdx])
		}
	}
}

// maxToolBubbles bounds the distinct tool-type bubbles in one phase before
// the Working indicator takes over.
const maxToolBubbles = 5

// maxToolBubbleResetRunes is the minimum text (runes) accumulated since the
// last phase break before an EventSegment resets the tool-bubble budget.
// Reasoning models interleave empty/short text parts between tool calls;
// resetting on those would give every tool a fresh phase and defeat the cap.
const maxToolBubbleResetRunes = 60

const workingIndicator = "🔄 Working…"

const maxToolContextRunes = 40

func (s *Streamer) currentTime() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *Streamer) workingText(working *workingState) string {
	latest := working.latestName
	if working.latestContext != "" {
		latest += ": " + working.latestContext
	}
	return fmt.Sprintf("%s · %d tool calls · latest: %s", workingIndicator, working.total, latest)
}

func (s *Streamer) updateWorking(working *workingState) {
	if working.ref == nil {
		ref, rendered, err := s.sendSingle(working.pending)
		if err != nil {
			slog.Warn("streaming: working notice send failed", "error", err)
			return
		}
		working.ref = ref
		working.rendered = rendered
		working.pending = ""
		working.lastEditAt = s.currentTime()
		working.hasLastEditAt = true
		s.trackFirstRef(ref)
		return
	}
	s.maybeEditWorking(working)
}

func (s *Streamer) queueWorking(working *workingState) {
	text := s.workingText(working)
	if working.ref != nil {
		working.pending = s.renderedSingle(text)
	} else {
		working.pending = text
	}
	s.updateWorking(working)
}

func (s *Streamer) maybeEditWorking(working *workingState) {
	if working.ref == nil || working.pending == "" || working.pending == working.rendered {
		return
	}
	interval := s.workingEditInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	now := s.currentTime()
	if working.hasLastEditAt && now.Sub(working.lastEditAt) < interval {
		return
	}
	if err := s.reply.Edit(working.ref, working.pending); err != nil {
		slog.Warn("streaming: working notice edit failed", "error", err)
		return
	}
	working.rendered = working.pending
	working.pending = ""
	working.lastEditAt = now
	working.hasLastEditAt = true
}

func (s *Streamer) flushWorking(working *workingState) {
	if working.ref == nil || working.pending == "" || working.pending == working.rendered {
		return
	}
	if err := s.reply.Edit(working.ref, working.pending); err != nil {
		slog.Warn("streaming: working notice flush failed", "error", err)
		return
	}
	working.rendered = working.pending
	working.pending = ""
	working.lastEditAt = s.currentTime()
	working.hasLastEditAt = true
}

func (s *Streamer) resetToolPhase(bubbles *map[string]*toolBubble, capped *map[string]bool, total *int, working *workingState) {
	s.flushWorking(working)
	if working.ref != nil {
		if remover, ok := s.reply.(channel.MessageRemover); ok {
			if err := remover.Delete(working.ref); err != nil {
				slog.Warn("streaming: working notice removal failed", "error", err)
			}
		}
	}
	*bubbles = make(map[string]*toolBubble)
	*capped = make(map[string]bool)
	*total = 0
	*working = workingState{}
}

func (s *Streamer) sendSingle(raw string) (channel.MessageRef, string, error) {
	rendered := s.renderedSingle(raw)
	if rendered == "" {
		return nil, "", errors.New("rendered status message is empty")
	}
	ref, err := s.reply.Send(rendered)
	if err != nil {
		return nil, "", err
	}
	return ref, rendered, nil
}

func (s *Streamer) renderedSingle(raw string) string {
	chunks := nonEmptyChunks(s.renderChunks(raw))
	if len(chunks) == 0 {
		return ""
	}
	if len(chunks) > 1 {
		slog.Warn("streaming: status message exceeded one chunk", "chunks", len(chunks))
	}
	return chunks[0]
}

func normalizeToolContext(raw string) string {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return ""
	}
	s := strings.Join(fields, " ")
	runes := []rune(s)
	if len(runes) > maxToolContextRunes {
		return string(runes[:maxToolContextRunes-1]) + "…"
	}
	return s
}

// formatToolLabel renders one tool bubble, with its context and repeat count when
// it ran more than once in the current phase.
func formatToolLabel(name, context string, count int) string {
	ctx := normalizeToolContext(context)
	if ctx != "" {
		if count > 1 {
			return fmt.Sprintf("⚙️ %s ×%d: %s", name, count, ctx)
		}
		return fmt.Sprintf("⚙️ %s: %s", name, ctx)
	}
	if count > 1 {
		return fmt.Sprintf("⚙️ %s ×%d", name, count)
	}
	return "⚙️ " + name
}

// notice sends a status line that did not come from the response buffer. It
// goes through the renderer like everything else so agent-supplied error text
// cannot break the platform's parser.
func (s *Streamer) notice(text string) {
	for _, chunk := range s.renderChunks(text) {
		if _, err := s.reply.Send(chunk); err != nil {
			slog.Warn("streaming: notice failed", "error", err)
			return
		}
	}
}

func (s *Streamer) renderChunks(raw string) []string {
	limit := render.TelegramLimit
	if s.platform == render.Discord {
		limit = render.DiscordLimit
	}

	chunks, err := s.renderer.Render(raw, s.platform)
	if err != nil || len(chunks) == 0 {
		return []string{raw}
	}

	if len(chunks) == 1 {
		return chunks
	}

	chunks, err = s.renderer.RenderWithLimit(raw, s.platform, limit-continuationReserve)
	if err != nil || len(chunks) == 0 {
		return []string{raw}
	}

	for i := 1; i < len(chunks); i++ {
		chunks[i] = continuationMarker + chunks[i]
	}
	return chunks
}

func (s *Streamer) syncMessages(refs *[]channel.MessageRef, lastChunks *[]string, raw string) {
	chunks := s.renderChunks(raw)
	chunks = nonEmptyChunks(chunks)

	if len(chunks) > 1 {
		slog.Debug("streaming: multi-message response", "chunks", len(chunks))
	}

	for i, chunk := range chunks {
		if i < len(*refs) {
			if i == len(chunks)-1 {
				if i >= len(*lastChunks) || (*lastChunks)[i] != chunk {
					if err := s.reply.Edit((*refs)[i], chunk); err != nil {
						slog.Warn("streaming: edit failed", "error", err, "chunk", i)
					}
				}
			}
		} else {
			ref, err := s.reply.Send(chunk)
			if err != nil {
				slog.Warn("streaming: send failed", "error", err, "chunk", i)
				break
			}
			*refs = append(*refs, ref)
			s.trackFirstRef(ref)
		}
	}

	*lastChunks = chunks
}

// completedNotice sends a fallback confirmation when the agent finished
// without delivering any text content.
func (s *Streamer) completedNotice(refs *[]channel.MessageRef) {
	if len(*refs) == 0 {
		s.notice("✅ Task completed")
	}
}

// nonEmptyChunks drops chunks that render to nothing (whitespace-only or
// markup that produces no text) so platforms never reject an empty message.
func nonEmptyChunks(chunks []string) []string {
	out := make([]string, 0, len(chunks))
	for _, c := range chunks {
		if strings.TrimSpace(c) != "" {
			out = append(out, c)
		}
	}
	return out
}

// finalizeSegment seals the current message as a permanent reply and resets
// the streamer's bookkeeping so the next delta starts a fresh message.
func (s *Streamer) finalizeSegment(refs *[]channel.MessageRef, lastChunks *[]string, raw string) {
	if err := s.finalSync(refs, lastChunks, raw); err != nil {
		slog.Warn("streaming: segment finalize failed", "error", err)
	}
	*refs = nil
	*lastChunks = nil
}

func (s *Streamer) finalSync(refs *[]channel.MessageRef, lastChunks *[]string, raw string) error {
	chunks := nonEmptyChunks(s.renderChunks(raw))

	for i, chunk := range chunks {
		if i < len(*refs) {
			if i >= len(*lastChunks) || (*lastChunks)[i] != chunk {
				if err := s.reply.Edit((*refs)[i], chunk); err != nil {
					return err
				}
			}
		} else {
			ref, err := s.reply.Send(chunk)
			if err != nil {
				return err
			}
			*refs = append(*refs, ref)
			s.trackFirstRef(ref)
		}
	}

	*lastChunks = chunks
	return nil
}
