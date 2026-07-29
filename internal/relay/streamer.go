package relay

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/render"
)

const noEventTimeout = 10 * time.Minute

type Streamer struct {
	reply    channel.ReplyContext
	renderer render.Renderer
	platform render.Platform
}

func NewStreamer(reply channel.ReplyContext, renderer render.Renderer, platform render.Platform) *Streamer {
	return &Streamer{
		reply:    reply,
		renderer: renderer,
		platform: platform,
	}
}

func (s *Streamer) Run(ctx context.Context, events <-chan Event) error {
	var buf strings.Builder
	var lastRendered string
	var ref channel.MessageRef

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
				return s.finalEdit(ref, buf.String(), lastRendered)
			}

			timeoutTimer.Reset(noEventTimeout)

			switch ev.Type {
			case "delta":
				buf.WriteString(ev.Delta)
				dirty = true
			case "done":
				return s.finalEdit(ref, buf.String(), lastRendered)
			case "error":
				s.reply.Send("⚠️ OpenCode error: " + ev.Delta)
				return nil
			}

		case <-timer.C:
			if dirty {
				dirty = false
				rendered := s.renderContent(buf.String())
				if rendered != lastRendered {
					var err error
					if ref == nil {
						ref, err = s.reply.Send(rendered)
					} else {
						err = s.reply.Edit(ref, rendered)
					}
					if err != nil {
						slog.Warn("streaming: edit failed", "error", err)
					} else {
						lastRendered = rendered
					}
				}
			}

			if intervalIdx < len(intervals)-1 {
				intervalIdx++
			}
			timer.Reset(intervals[intervalIdx])
		}
	}
}

func (s *Streamer) finalEdit(ref channel.MessageRef, raw, lastRendered string) error {
	rendered := s.renderContent(raw)
	if rendered == lastRendered {
		return nil
	}
	if ref == nil {
		_, err := s.reply.Send(rendered)
		return err
	}
	return s.reply.Edit(ref, rendered)
}

func (s *Streamer) renderContent(raw string) string {
	chunks, err := s.renderer.Render(raw, s.platform)
	if err != nil || len(chunks) == 0 {
		return raw
	}
	return chunks[0]
}
