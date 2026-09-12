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
	sink                       Sink
	renderer                   render.Renderer
	platform                   render.Platform
	permissionHandler          PermissionPromptHandler
	questionHandler            QuestionPromptHandler
	scheduleAttributionHandler func(input map[string]any) error
	reactionSetter             channel.ReactionSetter
	reactionTarget             channel.MessageRef
	firstRef                   channel.MessageRef
	lastWorkingRef             channel.MessageRef
	lastWorkingHandle          EditHandle
	lastWorkingRendered        string
	cards                      []sentCard
	turnStep                   int
	noEventTimeout             time.Duration
	typingInterval             time.Duration
	permissionPendingFunc      func() bool
	now                        func() time.Time
	workingEditInterval        time.Duration
	stopCallbackData           string
}

type toolPhaseState struct {
	working workingState
}

type sentCard struct {
	handle   EditHandle
	rendered string
	stripped bool
}

type workingState struct {
	ref               channel.MessageRef
	handle            EditHandle
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
	return NewStreamerWithSink(NewChannelSink(reply), renderer, platform)
}

func NewStreamerWithSink(sink Sink, renderer render.Renderer, platform render.Platform) *Streamer {
	return &Streamer{
		sink:                sink,
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

func (s *Streamer) SetReactionTarget(ref channel.MessageRef) {
	s.reactionTarget = ref
}

func (s *Streamer) SetNoEventTimeout(d time.Duration) {
	if d > 0 {
		s.noEventTimeout = d
	}
}

func (s *Streamer) SetPermissionPendingFunc(fn func() bool) {
	s.permissionPendingFunc = fn
}

func (s *Streamer) SetStopCallbackData(data string) {
	s.stopCallbackData = data
}

func (s *Streamer) SetNowFunc(fn func() time.Time) {
	s.now = fn
}

func (s *Streamer) SetWorkingEditInterval(d time.Duration) {
	s.workingEditInterval = d
}

func (s *Streamer) activeButtons() []channel.Button {
	if s.stopCallbackData == "" {
		return nil
	}
	return []channel.Button{{Label: "🛑 Stop", Value: s.stopCallbackData}}
}

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

func (s *Streamer) trackFirstRef(ref channel.MessageRef) {
	if s.firstRef == nil && ref != nil {
		s.firstRef = ref
		if s.reactionTarget == nil {
			s.setReaction(channel.ReactionProcessing)
		}
	}
}

func (s *Streamer) workingHandle(working *workingState) EditHandle {
	if working == nil {
		return nil
	}
	if working.handle != nil {
		return working.handle
	}
	if working.ref != nil {
		if p, ok := s.sink.(RefHandleProvider); ok {
			working.handle = p.HandleFromRef(working.ref)
			return working.handle
		}
	}
	return nil
}

func (s *Streamer) Run(ctx context.Context, events <-chan Event) error {
	var buf strings.Builder
	var handles []EditHandle
	var lastChunks []string
	var phase toolPhaseState
	respTotal := 0
	respTypes := make(map[string]int)
	var respReasoning time.Duration

	s.cards = nil
	s.turnStep = 0
	defer func() { s.cards = nil }()

	typingTicker := time.NewTicker(s.typingInterval)
	defer typingTicker.Stop()
	if s.sink != nil {
		if err := s.sink.SendTyping(ctx); err != nil {
			slog.Debug("streaming: initial typing indicator failed", "error", err)
		}
	}

	intervals := []time.Duration{500 * time.Millisecond, 1 * time.Second, 2 * time.Second, 3 * time.Second}
	intervalIdx := 0

	timer := time.NewTimer(intervals[0])
	defer timer.Stop()

	timeoutTimer := time.NewTimer(s.noEventTimeout)
	defer timeoutTimer.Stop()

	dirty := false

	if s.reactionTarget != nil {
		s.setReaction(channel.ReactionProcessing)
	}

	for {
		select {
		case <-ctx.Done():
			s.flushWorking(&phase.working)
			s.clearWorkingStopButton(&phase.working)
			return ctx.Err()

		case <-typingTicker.C:
			if s.sink != nil {
				if err := s.sink.SendTyping(ctx); err != nil {
					slog.Debug("streaming: typing indicator failed", "error", err)
				}
			}

		case <-timeoutTimer.C:
			s.flushWorking(&phase.working)
			s.clearWorkingStopButton(&phase.working)
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
				s.clearWorkingStopButton(&phase.working)
				syncErr := s.finalSync(&handles, &lastChunks, buf.String())
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
					s.finalizeSegment(&handles, &lastChunks, buf.String())
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
					if phase.working.step == 0 && (phase.working.handle != nil || phase.working.ref != nil) {
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
					s.completedNotice(&handles)
					s.setReaction(channel.ReactionSuccess)
					return nil
				}
				if err := s.finalSync(&handles, &lastChunks, buf.String()); err != nil {
					s.setReaction(channel.ReactionError)
					return err
				}
				s.completedNotice(&handles)
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
					s.finalizeSegment(&handles, &lastChunks, buf.String())
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
					s.finalizeSegment(&handles, &lastChunks, buf.String())
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
					if (phase.working.handle != nil || phase.working.ref != nil) && phase.working.latestName == name && ctxStr != "" && ctxStr != phase.working.latestContext {
						phase.working.latestContext = ctxStr
						s.queueWorking(&phase.working)
					}
					break
				}
				respTotal++
				respTypes[name]++
				if phase.working.step > 0 && phase.working.latestName == name {
					phase.working.latestCount++
					phase.working.total = respTotal
					if ctxStr != "" {
						phase.working.latestContext = ctxStr
					}
					s.queueWorking(&phase.working)
				} else {
					phase.working.step++
					s.turnStep++
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
				s.syncMessages(&handles, &lastChunks, buf.String())
			}
			if phase.working.handle != nil || phase.working.ref != nil {
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
	step := 0
	if working.step > 0 {
		step = s.turnStep
	}
	return FormatWorkingText(WorkingTextParams{
		Step:              step,
		Tool:              working.latestName,
		ToolContext:       working.latestContext,
		ToolCount:         working.latestCount,
		ToolStart:         working.toolStart,
		ReasoningActive:   working.reasoningActive,
		ReasoningStart:    working.reasoningStart,
		ReasoningDuration: working.reasoningDuration,
		Now:               s.currentTime(),
	})
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
	if working.handle == nil && working.ref == nil {
		s.stripStaleCards(nil)
		handle, rendered, err := s.sendWorking(working.pending)
		if handle != nil {
			s.cards = append(s.cards, sentCard{handle: handle, rendered: rendered})
		}
		if err != nil {
			slog.Warn("streaming: working notice send failed", "error", err)
			return
		}
		working.handle = handle
		working.ref = handle.Ref()
		working.rendered = rendered
		working.pending = ""
		working.lastEditAt = s.currentTime()
		working.hasLastEditAt = true
		s.lastWorkingHandle = handle
		s.lastWorkingRef = handle.Ref()
		s.lastWorkingRendered = rendered
		s.trackFirstRef(handle.Ref())
		return
	}
	s.maybeEditWorking(working)
}

func (s *Streamer) queueWorking(working *workingState) {
	text := s.workingText(working)
	if working.handle != nil || working.ref != nil {
		working.pending = s.renderedSingle(text)
	} else {
		working.pending = text
	}
	s.updateWorking(working)
}

func (s *Streamer) maybeEditWorking(working *workingState) {
	handle := s.workingHandle(working)
	if handle == nil || working.pending == "" || working.pending == working.rendered {
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
		err = handle.EditWithButtons(context.Background(), working.pending, buttons)
	} else {
		err = handle.Edit(context.Background(), working.pending)
	}
	if err != nil {
		slog.Warn("streaming: working notice edit failed", "error", err)
		return
	}
	working.rendered = working.pending
	working.pending = ""
	working.lastEditAt = now
	working.hasLastEditAt = true
	s.lastWorkingRendered = working.rendered
}

func (s *Streamer) flushWorking(working *workingState) {
	handle := s.workingHandle(working)
	if handle == nil || working.pending == "" || working.pending == working.rendered {
		return
	}
	var err error
	buttons := s.activeButtons()
	if len(buttons) > 0 {
		err = handle.EditWithButtons(context.Background(), working.pending, buttons)
	} else {
		err = handle.Edit(context.Background(), working.pending)
	}
	if err != nil {
		slog.Warn("streaming: working notice flush failed", "error", err)
		return
	}
	working.rendered = working.pending
	working.pending = ""
	working.lastEditAt = s.currentTime()
	working.hasLastEditAt = true
	s.lastWorkingRendered = working.rendered
}

func (s *Streamer) resetToolPhase(phase *toolPhaseState) {
	if phase.working.handle != nil || phase.working.ref != nil {
		s.queueWorking(&phase.working)
	}
	s.flushWorking(&phase.working)
	s.syncCardRendered(s.workingHandle(&phase.working), phase.working.rendered)
	*phase = toolPhaseState{}
}

func (s *Streamer) syncCardRendered(handle EditHandle, rendered string) {
	if handle == nil || rendered == "" {
		return
	}
	ref := handle.Ref()
	if ref == nil {
		return
	}
	id := ref.ID()
	for i := range s.cards {
		card := &s.cards[i]
		if card.handle == nil || card.handle.Ref() == nil || card.handle.Ref().ID() != id {
			continue
		}
		card.rendered = rendered
		return
	}
}

func (s *Streamer) stripStaleCards(current EditHandle) {
	if s.stopCallbackData == "" {
		return
	}
	var currentID string
	if current != nil {
		if ref := current.Ref(); ref != nil {
			currentID = ref.ID()
		}
	}
	for i := range s.cards {
		card := &s.cards[i]
		if card.stripped || card.handle == nil {
			continue
		}
		if currentID != "" {
			if ref := card.handle.Ref(); ref != nil && ref.ID() == currentID {
				continue
			}
		}
		if card.rendered != "" {
			if err := card.handle.EditWithButtons(context.Background(), card.rendered, nil); err != nil {
				slog.Warn("streaming: stale card button strip failed", "error", err)
			}
		}
		card.stripped = true
	}
}

func (s *Streamer) resolveWorking(working *workingState, success bool, total int, types map[string]int, reasoning time.Duration) {
	handle := s.workingHandle(working)
	if handle == nil {
		handle = s.lastWorkingHandle
	}
	s.stripStaleCards(handle)
	if handle == nil {
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
		err = handle.EditWithButtons(context.Background(), text, nil)
	} else {
		err = handle.Edit(context.Background(), text)
	}
	if err != nil {
		slog.Warn("streaming: working rollup failed", "error", err)
		return
	}
	working.rendered = text
	working.pending = ""
	s.lastWorkingRendered = text
}

func (s *Streamer) clearWorkingStopButton(working *workingState) {
	if s.stopCallbackData == "" {
		return
	}
	handle := s.workingHandle(working)
	text := working.rendered
	if handle == nil {
		handle = s.lastWorkingHandle
		text = s.lastWorkingRendered
	}
	if handle != nil && text != "" {
		_ = handle.EditWithButtons(context.Background(), text, nil)
	}
	s.stripStaleCards(handle)
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

func (s *Streamer) sendWorking(raw string) (EditHandle, string, error) {
	if s.sink == nil {
		return nil, "", errors.New("streaming: nil sink")
	}
	rendered := s.renderedSingle(raw)
	if rendered == "" {
		return nil, "", errors.New("rendered status message is empty")
	}
	var handle EditHandle
	var err error
	buttons := s.activeButtons()
	if len(buttons) > 0 {
		handle, err = s.sink.SendWithButtons(context.Background(), rendered, buttons)
	} else {
		handle, err = s.sink.Send(context.Background(), rendered)
	}
	if err != nil {
		return handle, rendered, err
	}
	return handle, rendered, nil
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

func (s *Streamer) notice(text string) {
	if s.sink == nil {
		return
	}
	for _, chunk := range s.renderChunks(text) {
		if _, err := s.sink.Send(context.Background(), chunk); err != nil {
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

func (s *Streamer) syncMessages(handles *[]EditHandle, lastChunks *[]string, raw string) {
	if s.sink == nil {
		return
	}
	chunks := s.renderChunks(raw)
	chunks = nonEmptyChunks(chunks)

	if len(chunks) > 1 {
		slog.Debug("streaming: multi-message response", "chunks", len(chunks))
	}

	for i, chunk := range chunks {
		if i < len(*handles) {
			if i == len(chunks)-1 {
				if i >= len(*lastChunks) || (*lastChunks)[i] != chunk {
					if err := (*handles)[i].Edit(context.Background(), chunk); err != nil {
						slog.Warn("streaming: edit failed", "error", err, "chunk", i)
					}
				}
			}
		} else {
			handle, err := s.sink.Send(context.Background(), chunk)
			if err != nil {
				slog.Warn("streaming: send failed", "error", err, "chunk", i)
				break
			}
			*handles = append(*handles, handle)
			s.trackFirstRef(handle.Ref())
		}
	}

	*lastChunks = chunks
}

func (s *Streamer) completedNotice(handles *[]EditHandle) {
	if len(*handles) == 0 && s.lastWorkingHandle == nil {
		s.notice("✅ Task completed")
	}
}

func nonEmptyChunks(chunks []string) []string {
	out := make([]string, 0, len(chunks))
	for _, c := range chunks {
		if strings.TrimSpace(c) != "" {
			out = append(out, c)
		}
	}
	return out
}

func (s *Streamer) finalizeSegment(handles *[]EditHandle, lastChunks *[]string, raw string) {
	if err := s.finalSync(handles, lastChunks, raw); err != nil {
		slog.Warn("streaming: segment finalize failed", "error", err)
	}
	*handles = nil
	*lastChunks = nil
}

func (s *Streamer) finalSync(handles *[]EditHandle, lastChunks *[]string, raw string) error {
	if s.sink == nil {
		return errors.New("streaming: nil sink")
	}
	chunks := nonEmptyChunks(s.renderChunks(raw))

	for i, chunk := range chunks {
		if i < len(*handles) {
			if i >= len(*lastChunks) || (*lastChunks)[i] != chunk {
				if err := (*handles)[i].Edit(context.Background(), chunk); err != nil {
					return err
				}
			}
		} else {
			handle, err := s.sink.Send(context.Background(), chunk)
			if err != nil {
				return err
			}
			*handles = append(*handles, handle)
			s.trackFirstRef(handle.Ref())
		}
	}

	*lastChunks = chunks
	return nil
}
