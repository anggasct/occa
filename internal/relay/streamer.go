package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

const (
	noEventTimeout = 15 * time.Minute
	typingInterval = 4 * time.Second
)

var (
	ErrIncompleteStream = errors.New("response stream ended before completion")
	ErrStreamFailed     = errors.New("response stream failed")
	ErrStreamRead       = errors.New("response stream read failed")
)

const incompleteStreamMessage = "⚠️ Response stream ended before completion. The task may still be running; check /occa:status."

const (
	continuationReserve = 64
	continuationMarker  = "↪️ *(continued)*\n\n"
)

type Streamer struct {
	reply             channel.ReplyContext
	renderer          render.Renderer
	platform          render.Platform
	permissionHandler PermissionPromptHandler
	reactionSetter    channel.ReactionSetter
	firstRef          channel.MessageRef
	noEventTimeout    time.Duration
	typingInterval    time.Duration
}

type PermissionPromptHandler interface {
	Prompt(ctx context.Context, request PermissionRequest) error
}

func NewStreamer(reply channel.ReplyContext, renderer render.Renderer, platform render.Platform) *Streamer {
	return &Streamer{
		reply:          reply,
		renderer:       renderer,
		platform:       platform,
		noEventTimeout: noEventTimeout,
		typingInterval: typingInterval,
	}
}

func (s *Streamer) SetPermissionPromptHandler(handler PermissionPromptHandler) {
	s.permissionHandler = handler
}

func (s *Streamer) SetReactionSetter(setter channel.ReactionSetter) {
	s.reactionSetter = setter
}

// setReaction drives the reply's status reaction. Failures are logged and
// never fail the stream; a missing setter is a silent no-op.
func (s *Streamer) setReaction(state channel.ReactionState) {
	if s.reactionSetter == nil || s.firstRef == nil {
		return
	}
	if err := s.reactionSetter.SetReaction(s.firstRef, state); err != nil {
		slog.Warn("streaming: reaction failed", "state", state, "error", err)
	}
}

// trackFirstRef records the first reply message once it exists so status
// reactions can attach to it.
func (s *Streamer) trackFirstRef(ref channel.MessageRef) {
	if s.firstRef == nil && ref != nil {
		s.firstRef = ref
		s.setReaction(channel.ReactionProcessing)
	}
}

func (s *Streamer) Run(ctx context.Context, events <-chan Event) error {
	var buf strings.Builder
	var refs []channel.MessageRef
	var lastChunks []string
	// A tool phase covers consecutive runs of the same tool: repeats edit
	// the current bubble in place. Any other tool or a text block starts a
	// new phase. bubbleCount/workingShown persist for the whole response.
	var (
		curTool      string
		curRef       channel.MessageRef
		curCount     int
		bubbleCount  int
		workingShown bool
	)

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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-typingTicker.C:
			if err := s.reply.SendTyping(); err != nil {
				slog.Debug("streaming: typing indicator failed", "error", err)
			}

		case <-timeoutTimer.C:
			s.notice("⚠️ Task timed out (no events for 15 minutes). It may still be running, check /occa:status")
			s.setReaction(channel.ReactionError)
			return ErrTimeout

		case ev, ok := <-events:
			if !ok {
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
				curTool = ""
				buf.WriteString(ev.Delta)
				dirty = true
			case EventDone:
				if buf.Len() == 0 {
					s.setReaction(channel.ReactionSuccess)
					return nil
				}
				if err := s.finalSync(&refs, &lastChunks, buf.String()); err != nil {
					s.setReaction(channel.ReactionError)
					return err
				}
				s.setReaction(channel.ReactionSuccess)
				return nil
			case EventError:
				s.notice("⚠️ Agent error: " + ev.Delta)
				s.setReaction(channel.ReactionError)
				return fmt.Errorf("%w: %s", ErrStreamFailed, ev.Delta)
			case EventSegment:
				curTool = ""
				if buf.Len() > 0 {
					slog.Debug("streaming: segment break", "finalized_len", buf.Len())
					s.finalizeSegment(&refs, &lastChunks, buf.String())
					buf.Reset()
				}
			case EventTool:
				// Live tool bubbles: the first tool of a phase finalizes the
				// current preview, then consecutive repeats of the same tool
				// edit that bubble in place with a count. A different tool
				// starts a new bubble. After 5 bubbles the cap kicks in and a
				// single working indicator keeps the chat visibly active.
				if curTool == "" && buf.Len() > 0 {
					s.finalizeSegment(&refs, &lastChunks, buf.String())
					buf.Reset()
				}
				name := ev.Delta
				if name == "" {
					name = "Tool call"
				}
				if name == curTool {
					curCount++
					if err := s.reply.Edit(curRef, formatToolLabel(name, curCount)); err != nil {
						slog.Warn("streaming: tool notice edit failed", "tool", name, "error", err)
					}
					slog.Debug("streaming: tool repeat", "tool", name, "count", curCount)
					break
				}
				if bubbleCount >= maxToolBubbles {
					if !workingShown {
						s.notice(workingIndicator)
						workingShown = true
					}
					slog.Debug("streaming: tool bubble cap reached", "tool", name)
					break
				}
				ref, err := s.reply.Send(formatToolLabel(name, 1))
				if err != nil {
					slog.Warn("streaming: tool notice send failed", "tool", name, "error", err)
					break
				}
				curTool, curRef, curCount = name, ref, 1
				bubbleCount++
				slog.Debug("streaming: tool bubble", "tool", name)
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

// maxToolBubbles bounds the live tool bubbles per response before the
// working indicator takes over.
const maxToolBubbles = 5

const workingIndicator = "🔄 Working…"

// formatToolLabel renders one tool bubble, with its repeat count when it ran
// more than once in the current phase.
func formatToolLabel(name string, count int) string {
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
	chunks := s.renderChunks(raw)

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
