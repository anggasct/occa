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

const noEventTimeout = 10 * time.Minute

var (
	ErrIncompleteStream = errors.New("response stream ended before completion")
	ErrStreamFailed     = errors.New("response stream failed")
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
}

type PermissionPromptHandler interface {
	Prompt(ctx context.Context, request PermissionRequest) error
}

func NewStreamer(reply channel.ReplyContext, renderer render.Renderer, platform render.Platform) *Streamer {
	return &Streamer{
		reply:    reply,
		renderer: renderer,
		platform: platform,
	}
}

func (s *Streamer) SetPermissionPromptHandler(handler PermissionPromptHandler) {
	s.permissionHandler = handler
}

func (s *Streamer) Run(ctx context.Context, events <-chan Event) error {
	var buf strings.Builder
	var refs []channel.MessageRef
	var lastChunks []string

	intervals := []time.Duration{500 * time.Millisecond, 1 * time.Second, 2 * time.Second, 3 * time.Second}
	intervalIdx := 0

	timer := time.NewTimer(intervals[0])
	defer timer.Stop()

	timeoutTimer := time.NewTimer(noEventTimeout)
	defer timeoutTimer.Stop()

	dirty := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-timeoutTimer.C:
			s.reply.Send("⚠️ Task timed out (no events for 10 minutes). It may still be running, check /occa:status")
			return ErrTimeout

		case ev, ok := <-events:
			if !ok {
				syncErr := s.finalSync(&refs, &lastChunks, buf.String())
				if _, err := s.reply.Send(incompleteStreamMessage); err != nil {
					slog.Warn("streaming: incomplete response notice failed", "error", err)
				}
				if syncErr != nil {
					return fmt.Errorf("%w: final sync: %v", ErrIncompleteStream, syncErr)
				}
				return ErrIncompleteStream
			}

			timeoutTimer.Reset(noEventTimeout)

			switch ev.Type {
			case "delta":
				buf.WriteString(ev.Delta)
				dirty = true
			case "done":
				return s.finalSync(&refs, &lastChunks, buf.String())
			case "error":
				s.reply.Send("⚠️ Agent error: " + ev.Delta)
				return fmt.Errorf("%w: %s", ErrStreamFailed, ev.Delta)
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
		}
	}

	*lastChunks = chunks
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
		}
	}

	*lastChunks = chunks
	return nil
}
