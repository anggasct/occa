package webhook

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
)

const surfaceHealWindow = 10 * time.Minute

type parentLoss struct {
	reanchoredAt time.Time
	suppressed   bool
}

func (s *Server) parentChannel(ep config.EndpointConfig, prKey string) string {
	s.surfaceHealMu.Lock()
	defer s.surfaceHealMu.Unlock()
	if channelID, ok := s.parentChannels[prKey]; ok {
		return channelID
	}
	return ep.ChannelID
}

func (s *Server) rememberParentChannel(prKey, channelID string) {
	s.surfaceHealMu.Lock()
	defer s.surfaceHealMu.Unlock()
	if s.parentChannels == nil {
		s.parentChannels = make(map[string]string)
	}
	s.parentChannels[prKey] = channelID
}

func (s *Server) parentSuppressed(prKey string) bool {
	s.surfaceHealMu.Lock()
	defer s.surfaceHealMu.Unlock()
	loss, ok := s.parentLosses[prKey]
	return ok && loss.suppressed && time.Since(loss.reanchoredAt) < surfaceHealWindow
}

func (s *Server) surfaceNoticeDue(key string) bool {
	s.surfaceHealMu.Lock()
	defer s.surfaceHealMu.Unlock()
	if last, ok := s.surfaceNotices[key]; ok && time.Since(last) < surfaceHealWindow {
		return false
	}
	if s.surfaceNotices == nil {
		s.surfaceNotices = make(map[string]time.Time)
	}
	s.surfaceNotices[key] = time.Now()
	return true
}

// editPRParent edits a PR's parent card where it actually lives. A card posted
// before the endpoint moved to a forum still sits in the old text channel; an
// edit addressed at the forum reports it missing, so the old channel is tried
// with the same message id before the card is declared lost and re-posted.
// It returns the root id that now carries the card.
func (s *Server) editPRParent(ctx context.Context, ep config.EndpointConfig, prKey, rootID, text string, buttons []channel.Button, age bool) (string, error) {
	edit := s.editPhaseParent
	if age {
		if s.parentSuppressed(prKey) {
			return rootID, nil
		}
		edit = s.editAgePhaseParent
	}
	target := s.parentChannel(ep, prKey)
	err := edit(ctx, ep.Platform, target, rootID, text, buttons)
	if !errors.Is(err, channel.ErrMessageNotFound) {
		return rootID, err
	}
	if legacy := strings.TrimSpace(ep.LegacyChannelID); legacy != "" && legacy != target {
		legacyErr := edit(ctx, ep.Platform, legacy, rootID, text, buttons)
		if legacyErr == nil {
			s.rememberParentChannel(prKey, legacy)
			slog.Info("webhook: parent surface found in legacy channel", "pr_key", prKey, "channel_id", legacy)
			return rootID, nil
		}
		if !errors.Is(legacyErr, channel.ErrMessageNotFound) {
			return rootID, legacyErr
		}
	}
	return s.reanchorParent(ctx, ep, prKey, rootID, text, buttons, err)
}

func (s *Server) reanchorParent(ctx context.Context, ep config.EndpointConfig, prKey, oldRoot, text string, buttons []channel.Button, cause error) (string, error) {
	s.surfaceHealMu.Lock()
	loss := s.parentLosses[prKey]
	if !loss.reanchoredAt.IsZero() && time.Since(loss.reanchoredAt) < surfaceHealWindow {
		first := !loss.suppressed
		loss.suppressed = true
		s.parentLosses[prKey] = loss
		s.surfaceHealMu.Unlock()
		if first {
			slog.Debug("webhook: parent surface lost again, re-anchor cooling down", "pr_key", prKey, "root_id", oldRoot)
		}
		return oldRoot, cause
	}
	s.surfaceHealMu.Unlock()

	newRoot, err := s.sendPhaseParent(ctx, ep.Platform, ep.ChannelID, text, buttons)
	if err != nil {
		return oldRoot, err
	}
	newRoot = strings.TrimSpace(newRoot)
	s.surfaceHealMu.Lock()
	if s.parentLosses == nil {
		s.parentLosses = make(map[string]parentLoss)
	}
	s.parentLosses[prKey] = parentLoss{reanchoredAt: time.Now()}
	delete(s.parentChannels, prKey)
	s.surfaceHealMu.Unlock()

	newThread := ""
	if deliversToForum(ep) {
		newThread = newRoot
	}
	s.tasks.SetSurface(prKey, SurfaceUpdate{RootMessageID: newRoot, ThreadID: newThread})
	if s.prQueues != nil {
		if row, getErr := s.prQueues.Get(ctx, prKey); getErr == nil && row != nil {
			threadID := strings.TrimSpace(row.ThreadID)
			if newThread != "" {
				threadID = newThread
			}
			if updErr := s.prQueues.UpdateThread(ctx, prKey, threadID, newRoot); updErr != nil {
				slog.Warn("webhook: re-anchored parent persist failed", "pr_key", prKey, "error", updErr)
			}
		}
	}
	slog.Info("webhook: parent surface re-anchored", "pr_key", prKey, "old", oldRoot, "new", newRoot)
	return newRoot, nil
}

// mergedRepairAllowed decides from GitHub's live state, not from the event,
// whether a merged surface may be reopened. Event payloads are snapshots: one
// generated before the merge but processed after it still says "open".
func (s *Server) mergedRepairAllowed(ctx context.Context, prKey, envelopeEvidence string) (string, bool) {
	state, err := s.resolveLivePRState(ctx, prKey)
	if err != nil {
		slog.Warn("webhook: merged repair deferred", "pr_key", prKey, "error", err)
		return "", false
	}
	if state == PRStateMerged {
		if s.surfaceNoticeDue("merged-repair-refused|" + prKey) {
			slog.Info("webhook: merged repair refused", "pr_key", prKey, "evidence", "live_state", "envelope", envelopeEvidence)
		} else {
			slog.Debug("webhook: merged repair refused", "pr_key", prKey, "evidence", "live_state", "envelope", envelopeEvidence)
		}
		return "", false
	}
	return "live pr_state=" + state, true
}
