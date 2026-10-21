package webhook

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// lockedBuffer collects log output from the writers under test.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// dispatchStatusWriter wires the dispatch writer (executor side) to the
// recorder of the PR-queue writer, so both write into one thread.
func dispatchStatusWriter(srv *Server, rec *surfaceRecorder, pinned *[]string) *ThreadStatusWriter {
	return &ThreadStatusWriter{
		Registry: srv.tasks,
		Phase:    PhaseDispatch,
		Send: func(channelID, text string) (string, error) {
			rec.add(&rec.threadSent, surfaceMessage{channelID: channelID, messageID: "status-1", text: text})
			return "status-1", nil
		},
		Edit: func(channelID, messageID, text string) error {
			rec.add(&rec.edited, surfaceMessage{channelID: channelID, messageID: messageID, text: text})
			return nil
		},
		Pin: func(_, messageID string) {
			*pinned = append(*pinned, messageID)
		},
	}
}

// TestThreadStateLineIsOnePerThreadAcrossBothWriters asserts the thread
// contract: the dispatch writer and the PR-queue writer write one state line
// per thread, edit it in place and pin it once — in either order.
func TestThreadStateLineIsOnePerThreadAcrossBothWriters(t *testing.T) {
	for _, order := range []string{"dispatch-writer-first", "pr-queue-writer-first"} {
		t.Run(order, func(t *testing.T) {
			srv, _, rec := newSurfaceServer(t)
			var pinned []string
			srv.SetPinner(func(_ context.Context, _, _, messageID string) error {
				pinned = append(pinned, messageID)
				return nil
			})
			dispatch := dispatchStatusWriter(srv, rec, &pinned)
			ctx := context.Background()
			threadID := "thread-9"
			prKey := "acme/widget#181"
			dispatchText := FormatThreadStatusLine(PhaseDispatch, "", "", NextStepForPhase(PhaseDispatch), "", nil)

			if order == "dispatch-writer-first" {
				dispatch.Write(threadID, threadID, "acme/widget", "feat/x", prKey, dispatchText)
				srv.ensureThreadStatus(ctx, "discord", "chan-1", threadID, "acme/widget", "feat/x", prKey, ThreadStatusUpdate{
					Phase: PhaseReview,
					Next:  NextStepForPhase(PhaseReview),
				})
			} else {
				srv.ensureThreadStatus(ctx, "discord", "chan-1", threadID, "acme/widget", "feat/x", prKey, ThreadStatusUpdate{
					Phase: PhaseReview,
					Next:  NextStepForPhase(PhaseReview),
				})
				dispatch.Write(threadID, threadID, "acme/widget", "feat/x", prKey, dispatchText)
			}

			if len(rec.threadSent) != 1 {
				t.Fatalf("state line sends = %d, want exactly one: %+v", len(rec.threadSent), rec.threadSent)
			}
			if len(rec.edited) != 1 {
				t.Fatalf("state line edits = %d, want the second writer to edit in place: %+v", len(rec.edited), rec.edited)
			}
			if rec.edited[0].messageID != "status-1" {
				t.Fatalf("edited message = %q, want the state line both writers share", rec.edited[0].messageID)
			}
			if len(pinned) != 1 {
				t.Fatalf("pins = %d, want exactly one: %+v", len(pinned), pinned)
			}
			if id := srv.tasks.StatusMessageIDForThread(threadID); id != "status-1" {
				t.Fatalf("state line id for the thread = %q, want status-1", id)
			}
		})
	}
}

// TestThreadStateLineWriteRefusedOnTerminalPR asserts a terminal PR keeps its
// state line: no writer rewrites it and the refusal is logged.
func TestThreadStateLineWriteRefusedOnTerminalPR(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	var pinned []string
	srv.SetPinner(func(_ context.Context, _, _, messageID string) error {
		pinned = append(pinned, messageID)
		return nil
	})
	prKey := "acme/widget#181"
	threadID := "thread-9"
	srv.tasks.SetSurface(prKey, SurfaceUpdate{
		ThreadID: threadID,
		Phase:    string(PhaseMerged),
	})

	logs := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	dispatch := dispatchStatusWriter(srv, rec, &pinned)
	dispatchText := FormatThreadStatusLine(PhaseDispatch, "", "", NextStepForPhase(PhaseDispatch), "", nil)
	if msgID := dispatch.Write(threadID, threadID, "acme/widget", "feat/x", prKey, dispatchText); msgID != "" {
		t.Fatalf("state line write = %q, want the write refused on a terminal PR", msgID)
	}
	if msgID := srv.ensureThreadStatus(context.Background(), "discord", "chan-1", threadID, "acme/widget", "feat/x", prKey, ThreadStatusUpdate{
		Phase: PhaseReview,
		Next:  NextStepForPhase(PhaseReview),
	}); msgID != "" {
		t.Fatalf("PR-queue state line write = %q, want the write refused on a terminal PR", msgID)
	}

	if len(rec.threadSent) != 0 {
		t.Fatalf("a terminal PR got %d state line writes: %+v", len(rec.threadSent), rec.threadSent)
	}
	if len(rec.edited) != 0 {
		t.Fatalf("a terminal PR got %d state line edits: %+v", len(rec.edited), rec.edited)
	}
	if len(pinned) != 0 {
		t.Fatalf("a terminal PR got %d pins: %+v", len(pinned), pinned)
	}
	if !strings.Contains(logs.String(), "write refused, PR terminal") {
		t.Fatalf("the refused write was not logged: %s", logs.String())
	}
}
