package webhook

import (
	"errors"
	"log/slog"
	"strings"
)

var errNoThreadStatusEditor = errors.New("webhook: thread status writer cannot edit")

// ThreadStatusWriter writes the state line of a thread. The dispatch writer
// (executor side) and the PR-queue writer (audit side) share it: one writer
// means one state line per thread, one message id and one pin.
type ThreadStatusWriter struct {
	// Registry holds the message id of the state line per thread.
	Registry *TaskThreadRegistry
	// Phase is the phase the state line renders. A write that asks for
	// another phase on a terminal PR is refused.
	Phase Phase
	// Send posts a new state line into the thread and returns its id.
	Send func(target, text string) (string, error)
	// Edit rewrites the state line the thread already has.
	Edit func(target, messageID, text string) error
	// Pin pins the state line in its thread. Pinning is best-effort: a
	// platform without pin support must not fail the write.
	Pin func(target, messageID string)
}

// Write edits the state line the thread already has, or sends it once. The id
// of the line is recorded for the thread, so the next writer of the same
// thread edits the same message instead of posting a second one.
func (w *ThreadStatusWriter) Write(target, threadID, repo, branch, prKey, text string) string {
	if w == nil || w.Registry == nil || strings.TrimSpace(threadID) == "" {
		return ""
	}
	if strings.TrimSpace(prKey) != "" && SurfaceFrozen(w.Registry, prKey, w.Phase) {
		slog.Info("webhook: write refused, PR terminal",
			"pr_key", prKey,
			"surface", "thread status",
			"run", string(w.Phase),
		)
		return ""
	}
	if existing := w.Registry.StatusMessageIDForThread(threadID); existing != "" {
		if err := w.edit(target, existing, text); err == nil {
			w.Registry.RecordStatusMessage(threadID, repo, branch, prKey, existing)
			return existing
		}
	}
	msgID, err := w.send(target, text)
	if err != nil || strings.TrimSpace(msgID) == "" {
		return strings.TrimSpace(msgID)
	}
	w.Registry.RecordStatusMessage(threadID, repo, branch, prKey, msgID)
	w.pin(target, msgID)
	return msgID
}

func (w *ThreadStatusWriter) send(target, text string) (string, error) {
	if w.Send == nil {
		return "", nil
	}
	return w.Send(target, text)
}

func (w *ThreadStatusWriter) edit(target, messageID, text string) error {
	if w.Edit == nil {
		return errNoThreadStatusEditor
	}
	return w.Edit(target, messageID, text)
}

func (w *ThreadStatusWriter) pin(target, messageID string) {
	if w.Pin == nil {
		return
	}
	if strings.TrimSpace(target) == "" || strings.TrimSpace(messageID) == "" {
		return
	}
	w.Pin(target, messageID)
}
