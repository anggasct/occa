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
	lastWorkingRef             channel.MessageRef
	noEventTimeout             time.Duration
	typingInterval             time.Duration
	permissionPendingFunc      func() bool
	now                        func() time.Time
	workingEditInterval        time.Duration
	stopCallbackData           string
}

type toolPhaseState struct {
	calls   int
	working workingState
}

type workingState struct {
	ref               channel.MessageRef
	total             int
	step              int
	latestName        string
	latestContext     string
	latestCount       int
	toolStart         time.Time
	reasoningActive   bool
	reasoningStart    time.Time
	reasoningDuration time.Duration
	rendered          string
	pending           string
	lastEditAt        time.Time
	hasLastEditAt     bool
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

func (s *Streamer) SetStopCallbackData(data string) {
	s.stopCallbackData = data
}

func (s *Streamer) activeButtons() []channel.Button {
	if s.stopCallbackData == "" {
		return nil
	}
	return []channel.Button{{Label: "🛑 Stop", Value: s.stopCallbackData}}
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
	var respReasoning time.Duration

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
			if s.stopCallbackData != "" && phase.working.ref != nil {
				_ = s.reply.EditWithButtons(phase.working.ref, phase.working.rendered, nil)
			}
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
			case EventReasoning:
				if buf.Len() > 0 {
					s.finalizeSegment(&refs, &lastChunks, buf.String())
					buf.Reset()
				}
				if !phase.working.reasoningActive {
					phase.working.reasoningActive = true
					phase.working.reasoningStart = s.currentTime()
					s.queueWorking(&phase.working)
				}
			case EventDelta:
				if phase.working.reasoningActive {
					phase.working.reasoningActive = false
					if !phase.working.reasoningStart.IsZero() {
						elapsed := s.currentTime().Sub(phase.working.reasoningStart)
						phase.working.reasoningDuration += elapsed
						respReasoning += elapsed
					}
					if phase.working.step == 0 && phase.working.ref != nil {
						s.resolveWorking(&phase.working, true, respTotal, respTypes, respReasoning)
					}
				}
				buf.WriteString(ev.Delta)
				dirty = true
			case EventDone:
				if phase.working.reasoningActive {
					phase.working.reasoningActive = false
					if !phase.working.reasoningStart.IsZero() {
						elapsed := s.currentTime().Sub(phase.working.reasoningStart)
						phase.working.reasoningDuration += elapsed
						respReasoning += elapsed
					}
				}
				s.resolveWorking(&phase.working, true, respTotal, respTypes, respReasoning)
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
				if phase.working.reasoningActive {
					phase.working.reasoningActive = false
					if !phase.working.reasoningStart.IsZero() {
						elapsed := s.currentTime().Sub(phase.working.reasoningStart)
						phase.working.reasoningDuration += elapsed
						respReasoning += elapsed
					}
				}
				s.resolveWorking(&phase.working, false, respTotal, respTypes, respReasoning)
				s.notice("⚠️ Agent error: " + ev.Delta)
				s.setReaction(channel.ReactionError)
				return fmt.Errorf("%w: %s", ErrStreamFailed, ev.Delta)
			case EventSegment:
				if phase.working.reasoningActive {
					phase.working.reasoningActive = false
					if !phase.working.reasoningStart.IsZero() {
						elapsed := s.currentTime().Sub(phase.working.reasoningStart)
						phase.working.reasoningDuration += elapsed
						respReasoning += elapsed
					}
				}
				s.resetToolPhase(&phase)
				if buf.Len() > 0 {
					slog.Debug("streaming: segment break", "finalized_len", buf.Len())
					s.finalizeSegment(&refs, &lastChunks, buf.String())
					buf.Reset()
				}
			case EventTool:
				if phase.working.reasoningActive {
					phase.working.reasoningActive = false
					if !phase.working.reasoningStart.IsZero() {
						elapsed := s.currentTime().Sub(phase.working.reasoningStart)
						phase.working.reasoningDuration += elapsed
						respReasoning += elapsed
					}
				}
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
					if phase.working.ref != nil && phase.working.latestName == name && ctxStr != "" && ctxStr != phase.working.latestContext {
						phase.working.latestContext = ctxStr
						s.queueWorking(&phase.working)
					}
					break
				}
				respTotal++
				respTypes[name]++
				phase.calls++
				if phase.working.step > 0 && phase.working.latestName == name {
					phase.working.latestCount++
					phase.working.total = respTotal
					if ctxStr != "" {
						phase.working.latestContext = ctxStr
					}
					s.queueWorking(&phase.working)
				} else {
					phase.working.step++
					phase.working.latestName = name
					phase.working.latestContext = ctxStr
					phase.working.latestCount = 1
					phase.working.total = respTotal
					phase.working.toolStart = s.currentTime()
					s.queueWorking(&phase.working)
				}
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
			if phase.working.ref != nil {
				s.queueWorking(&phase.working)
			}

			if intervalIdx < len(intervals)-1 {
				intervalIdx++
			}
			timer.Reset(intervals[intervalIdx])
		}
	}
}

const maxToolContextRunes = 40

func (s *Streamer) currentTime() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *Streamer) workingText(working *workingState) string {
	if working.step == 0 {
		if working.reasoningActive {
			var elapsed time.Duration
			if !working.reasoningStart.IsZero() {
				elapsed = s.currentTime().Sub(working.reasoningStart)
			}
			dur := formatDuration(elapsed)
			if dur != "" {
				return fmt.Sprintf("🧠 Thinking… (%s)", dur)
			}
			return "🧠 Thinking…"
		}
		if td := formatDuration(working.reasoningDuration); td != "" {
			return "🧠 Thought for " + td
		}
		if working.reasoningDuration > 0 || !working.reasoningStart.IsZero() {
			return "🧠 Thought"
		}
		return ""
	}

	if working.reasoningActive {
		var elapsed time.Duration
		if !working.reasoningStart.IsZero() {
			elapsed = s.currentTime().Sub(working.reasoningStart)
		}
		dur := formatDuration(elapsed)
		toolPart := formatToolLabel(working.latestName, working.latestContext, working.latestCount)
		cleanTool := strings.TrimPrefix(toolPart, "⚙️ ")
		if dur != "" {
			return fmt.Sprintf("🧠 Thinking… (%s) · [Step %d] %s", dur, working.step, cleanTool)
		}
		return fmt.Sprintf("🧠 Thinking… · [Step %d] %s", working.step, cleanTool)
	}

	toolPart := formatToolLabel(working.latestName, working.latestContext, working.latestCount)
	cleanTool := strings.TrimPrefix(toolPart, "⚙️ ")
	step := working.step
	if step <= 0 {
		step = 1
	}
	var elapsed time.Duration
	if !working.toolStart.IsZero() {
		elapsed = s.currentTime().Sub(working.toolStart)
	}
	dur := formatDuration(elapsed)

	thoughtSuffix := ""
	if td := formatDuration(working.reasoningDuration); td != "" {
		thoughtSuffix = " · 🧠 Thought for " + td
	}

	if dur != "" {
		return fmt.Sprintf("⚙️ [Step %d] %s (%s)%s", step, cleanTool, dur, thoughtSuffix)
	}
	return fmt.Sprintf("⚙️ [Step %d] %s%s", step, cleanTool, thoughtSuffix)
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Second {
		return ""
	}
	m := int(d / time.Minute)
	s := int((d % time.Minute) / time.Second)
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func (s *Streamer) updateWorking(working *workingState) {
	if working.ref == nil {
		ref, rendered, err := s.sendWorking(working.pending)
		if err != nil {
			slog.Warn("streaming: working notice send failed", "error", err)
			return
		}
		working.ref = ref
		working.rendered = rendered
		working.pending = ""
		working.lastEditAt = s.currentTime()
		working.hasLastEditAt = true
		s.lastWorkingRef = ref
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
	if interval == 0 {
		interval = 2 * time.Second
	}
	now := s.currentTime()
	if interval > 0 && working.hasLastEditAt && now.Sub(working.lastEditAt) < interval {
		return
	}
	var err error
	buttons := s.activeButtons()
	if len(buttons) > 0 {
		err = s.reply.EditWithButtons(working.ref, working.pending, buttons)
	} else {
		err = s.reply.Edit(working.ref, working.pending)
	}
	if err != nil {
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
	var err error
	buttons := s.activeButtons()
	if len(buttons) > 0 {
		err = s.reply.EditWithButtons(working.ref, working.pending, buttons)
	} else {
		err = s.reply.Edit(working.ref, working.pending)
	}
	if err != nil {
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
	*phase = toolPhaseState{}
}

func (s *Streamer) resolveWorking(working *workingState, success bool, total int, types map[string]int, reasoning time.Duration) {
	ref := working.ref
	if ref == nil {
		ref = s.lastWorkingRef
	}
	if ref == nil {
		return
	}
	icon := "⚠️"
	if success {
		icon = "✅"
	}
	text := s.renderedSingle(s.rollupText(icon, total, types, reasoning))
	if text == "" {
		return
	}
	var err error
	if s.stopCallbackData != "" {
		err = s.reply.EditWithButtons(ref, text, nil)
	} else {
		err = s.reply.Edit(ref, text)
	}
	if err != nil {
		slog.Warn("streaming: working rollup failed", "error", err)
		return
	}
	working.rendered = text
	working.pending = ""
}

const maxRollupTypes = 8

func (s *Streamer) rollupText(icon string, total int, types map[string]int, reasoning time.Duration) string {
	thoughtSuffix := ""
	if dur := formatDuration(reasoning); dur != "" {
		thoughtSuffix = " · 🧠 Thought for " + dur
	} else if reasoning > 0 {
		thoughtSuffix = " · 🧠 Thought"
	}

	if total == 0 {
		if thoughtSuffix != "" {
			return strings.TrimPrefix(thoughtSuffix, " · ")
		}
		return ""
	}

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
	if total == 1 {
		fmt.Fprintf(&b, "%s 1 tool call", icon)
	} else {
		fmt.Fprintf(&b, "%s %d tool calls", icon, total)
	}

	listed := len(names)
	if listed > maxRollupTypes {
		listed = maxRollupTypes
	}

	if s.platform == render.Telegram && listed > 1 {
		b.WriteString(thoughtSuffix)
		b.WriteString("\n<blockquote expandable>\n")
		for _, name := range names[:listed] {
			fmt.Fprintf(&b, "• %s ×%d\n", name, types[name])
		}
		if rest := len(names) - listed; rest > 0 {
			fmt.Fprintf(&b, "• +%d more\n", rest)
		}
		b.WriteString("</blockquote>")
	} else {
		for _, name := range names[:listed] {
			fmt.Fprintf(&b, " · %s ×%d", name, types[name])
		}
		if rest := len(names) - listed; rest > 0 {
			fmt.Fprintf(&b, " · +%d more", rest)
		}
		b.WriteString(thoughtSuffix)
	}
	return b.String()
}

func (s *Streamer) sendWorking(raw string) (channel.MessageRef, string, error) {
	rendered := s.renderedSingle(raw)
	if rendered == "" {
		return nil, "", errors.New("rendered status message is empty")
	}
	var ref channel.MessageRef
	var err error
	buttons := s.activeButtons()
	if len(buttons) > 0 {
		ref, err = s.reply.SendWithButtons(rendered, buttons)
	} else {
		ref, err = s.reply.Send(rendered)
	}
	if err != nil {
		return nil, "", err
	}
	return ref, rendered, nil
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

func FormatToolLabel(name, context string, count int) string {
	return formatToolLabel(name, context, count)
}

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
// without delivering any text content and without any tool progress card.
func (s *Streamer) completedNotice(refs *[]channel.MessageRef) {
	if len(*refs) == 0 && s.lastWorkingRef == nil {
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
