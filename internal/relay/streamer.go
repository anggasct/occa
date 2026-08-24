package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

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
	name    string
	count   int
	context string
}

// toolPhaseState tracks one contiguous phase of tool activity: the run still
// accepting same-type continuations, how many bubbles it has spent, and its
// live Working notice. Every agent text segment starts a fresh phase.
type toolPhaseState struct {
	run     *toolBubble
	bubbles int
	calls   int
	working workingState
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
	var phase toolPhaseState
	respTotal := 0
	respTypes := make(map[string]int)

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
			s.flushWorking(&phase.working)
			return ctx.Err()

		case <-typingTicker.C:
			if err := s.reply.SendTyping(); err != nil {
				slog.Debug("streaming: typing indicator failed", "error", err)
			}

		case <-timeoutTimer.C:
			s.flushWorking(&phase.working)
			msg := taskTimeoutMessage
			if s.permissionPendingFunc != nil && s.permissionPendingFunc() {
				msg = taskTimeoutPermissionMessage
			}
			s.notice(msg)
			s.setReaction(channel.ReactionError)
			return ErrTimeout

		case ev, ok := <-events:
			if !ok {
				s.flushWorking(&phase.working)
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
				s.resolveWorking(&phase.working, true, respTotal, respTypes)
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
				s.resolveWorking(&phase.working, false, respTotal, respTypes)
				s.notice("⚠️ Agent error: " + ev.Delta)
				s.setReaction(channel.ReactionError)
				return fmt.Errorf("%w: %s", ErrStreamFailed, ev.Delta)
			case EventSegment:
				s.resetToolPhase(&phase)
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
					switch {
					case phase.run != nil && phase.run.name == name && ctxStr != "" && ctxStr != phase.run.context:
						phase.run.context = ctxStr
						if err := s.reply.Edit(phase.run.ref, formatToolLabel(name, phase.run.context, phase.run.count)); err != nil {
							slog.Warn("streaming: tool notice context edit failed", "tool", name, "error", err)
						}
					case phase.working.ref != nil && phase.working.latestName == name && ctxStr != "" && ctxStr != phase.working.latestContext:
						phase.working.latestContext = ctxStr
						s.queueWorking(&phase.working)
					}
					break
				}
				respTotal++
				respTypes[name]++
				phase.calls++
				if phase.run != nil && phase.run.name == name {
					phase.run.count++
					if ctxStr != "" {
						phase.run.context = ctxStr
					}
					if err := s.reply.Edit(phase.run.ref, formatToolLabel(name, phase.run.context, phase.run.count)); err != nil {
						slog.Warn("streaming: tool notice edit failed", "tool", name, "error", err)
					}
					break
				}
				if phase.bubbles < maxToolBubbles {
					ref, err := s.reply.Send(formatToolLabel(name, ctxStr, 1))
					if err != nil {
						slog.Warn("streaming: tool notice send failed", "tool", name, "error", err)
						break
					}
					phase.run = &toolBubble{ref: ref, name: name, count: 1, context: ctxStr}
					phase.bubbles++
					s.trackFirstRef(ref)
					break
				}
				phase.working.total = phase.calls
				phase.working.latestName = name
				phase.working.latestContext = ctxStr
				s.queueWorking(&phase.working)
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

// maxToolBubbles bounds the tool-run bubbles in one phase before the Working
// indicator takes over.
const maxToolBubbles = 5

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

func (s *Streamer) resetToolPhase(phase *toolPhaseState) {
	s.flushWorking(&phase.working)
	if phase.working.ref != nil {
		if remover, ok := s.reply.(channel.MessageRemover); ok {
			if err := remover.Delete(phase.working.ref); err != nil {
				slog.Warn("streaming: working notice removal failed", "error", err)
			}
		}
	}
	*phase = toolPhaseState{}
}

// resolveWorking replaces a visible Working notice with a final response-wide
// rollup on done/error. With no Working bubble on screen it does nothing: all
// tool activity is already visible as persisted bubbles. The edit bypasses
// the live throttle because it is the bubble's terminal state.
func (s *Streamer) resolveWorking(working *workingState, success bool, total int, types map[string]int) {
	if working.ref == nil {
		return
	}
	icon := "⚠️"
	if success {
		icon = "✅"
	}
	text := s.renderedSingle(rollupText(icon, total, types))
	if text == "" || text == working.rendered {
		return
	}
	if err := s.reply.Edit(working.ref, text); err != nil {
		slog.Warn("streaming: working rollup failed", "error", err)
		return
	}
	working.rendered = text
	working.pending = ""
}

// maxRollupTypes bounds how many per-type entries the final rollup lists;
// the remainder collapses into a "+N more" suffix.
const maxRollupTypes = 8

func rollupText(icon string, total int, types map[string]int) string {
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		if types[names[i]] != types[names[j]] {
			return types[names[i]] > types[names[j]]
		}
		return names[i] < names[j]
	})
	var b strings.Builder
	fmt.Fprintf(&b, "%s %d tool calls", icon, total)
	listed := len(names)
	if listed > maxRollupTypes {
		listed = maxRollupTypes
	}
	for _, name := range names[:listed] {
		fmt.Fprintf(&b, " · %s ×%d", name, types[name])
	}
	if rest := len(names) - listed; rest > 0 {
		fmt.Fprintf(&b, " · +%d more", rest)
	}
	return b.String()
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

// formatToolLabel renders one tool bubble, with its context and repeat count
// when the contiguous run made more than one call.
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
