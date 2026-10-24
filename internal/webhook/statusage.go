package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

type runAgeCardState struct {
	nextAttempt time.Time
	parentBase  string
	parentText  string
	parentGen   uint64
	parentPhase Phase
	threadBase  string
	threadText  string
	threadGen   uint64
	threadPhase Phase
}

type runAgeState struct {
	mu    sync.Mutex
	cards map[string]*runAgeCardState
	nowFn func() time.Time
}

func (s *Server) ageNow() time.Time {
	if s.ageState != nil && s.ageState.nowFn != nil {
		return s.ageState.nowFn()
	}
	return time.Now()
}

func (s *Server) runAgeCard(prKey string) *runAgeCardState {
	if s.ageState.cards == nil {
		s.ageState.cards = make(map[string]*runAgeCardState)
	}
	state, ok := s.ageState.cards[prKey]
	if !ok {
		state = &runAgeCardState{}
		s.ageState.cards[prKey] = state
	}
	return state
}

func (s *Server) rememberParentAge(prKey, base, text string, phase Phase) {
	if s == nil || s.ageState == nil || strings.TrimSpace(prKey) == "" {
		return
	}
	s.ageState.mu.Lock()
	defer s.ageState.mu.Unlock()
	state := s.runAgeCard(strings.TrimSpace(prKey))
	state.parentBase = base
	state.parentText = text
	state.parentPhase = phase
	state.parentGen++
}

// rememberParentBase records a phase whose platform write has not landed. The
// cached text stays stale on purpose: the next tick renders the new phase
// because its text differs, so a throttled write repairs itself.
func (s *Server) rememberParentBase(prKey, base string, phase Phase) {
	if s == nil || s.ageState == nil || strings.TrimSpace(prKey) == "" {
		return
	}
	s.ageState.mu.Lock()
	defer s.ageState.mu.Unlock()
	state := s.runAgeCard(strings.TrimSpace(prKey))
	state.parentBase = base
	state.parentPhase = phase
	state.parentGen++
}

func (s *Server) rememberThreadAge(prKey, base, text string, phase Phase) {
	if s == nil || s.ageState == nil || strings.TrimSpace(prKey) == "" {
		return
	}
	s.ageState.mu.Lock()
	defer s.ageState.mu.Unlock()
	state := s.runAgeCard(strings.TrimSpace(prKey))
	state.threadBase = base
	state.threadText = text
	state.threadPhase = phase
	state.threadGen++
}

func (s *Server) rememberThreadBase(prKey, base string, phase Phase) {
	if s == nil || s.ageState == nil || strings.TrimSpace(prKey) == "" {
		return
	}
	s.ageState.mu.Lock()
	defer s.ageState.mu.Unlock()
	state := s.runAgeCard(strings.TrimSpace(prKey))
	state.threadBase = base
	state.threadPhase = phase
	state.threadGen++
}

func FormatRunAgeSegment(now, startedAt, lastActivity time.Time, phase Phase, idleAfter time.Duration) string {
	if startedAt.IsZero() {
		return ""
	}
	age := now.Sub(startedAt)
	if age < 0 {
		age = 0
	}
	if IsTerminalPhase(phase) {
		return "· " + formatRunAge(age) + " total"
	}
	segment := "· ⏱️ " + formatRunAge(age)
	if idleAfter > 0 && !lastActivity.IsZero() {
		idle := now.Sub(lastActivity)
		if idle >= idleAfter {
			segment += " · idle " + formatRunAge(idle)
		}
	}
	return segment
}

func formatRunAge(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	return fmt.Sprintf("%dm", int(d/time.Minute))
}

func deliveryStartedAt(d *store.WebhookDelivery) time.Time {
	if d == nil {
		return time.Time{}
	}
	if d.StartedAt > 0 {
		return time.Unix(d.StartedAt, 0)
	}
	if d.CreatedAt > 0 {
		return time.Unix(d.CreatedAt, 0)
	}
	return time.Time{}
}

func deliveryInFlight(d *store.WebhookDelivery) bool {
	if d == nil {
		return false
	}
	switch d.Status {
	case store.WebhookStatusReceived, store.WebhookStatusAccepted, store.WebhookStatusProcessing:
		return true
	}
	return false
}

func (s *Server) runAgeReceipt(ctx context.Context, prKey string) *store.WebhookDelivery {
	if s == nil || s.deliveries == nil {
		return nil
	}
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(ctx, prKey); err == nil && row != nil && row.ActiveDelivery != 0 {
			if receipt, err := s.deliveries.GetByID(ctx, row.ActiveDelivery); err == nil && receipt != nil {
				return receipt
			}
		}
	}
	receipt, err := s.deliveries.LatestForPR(ctx, prKey)
	if err != nil {
		return nil
	}
	return receipt
}

func (s *Server) runAgeSegment(ctx context.Context, prKey string, phase Phase) string {
	if s == nil || strings.TrimSpace(prKey) == "" {
		return ""
	}
	receipt := s.runAgeReceipt(ctx, prKey)
	if receipt == nil {
		return ""
	}
	if !deliveryInFlight(receipt) && !IsTerminalPhase(phase) {
		return ""
	}
	idleAfter := config.DefaultIdleAfter
	if ep, ok := s.endpointForPR(prKey, phase); ok {
		idleAfter = ep.EffectiveIdleAfter()
	}
	return FormatRunAgeSegment(s.ageNow(), deliveryStartedAt(receipt), s.runAgeActivity(prKey), phase, idleAfter)
}

func (s *Server) endpointForPR(prKey string, phase Phase) (config.EndpointConfig, bool) {
	repo, _, ok := splitPRKey(prKey)
	if !ok || strings.TrimSpace(repo) == "" {
		return config.EndpointConfig{}, false
	}
	if ep, ok := s.endpointForWorkflow(repo, string(phase)); ok {
		return ep, true
	}
	want := normalizeRepoKey(repo)
	for _, ep := range s.endpoints {
		if normalizeRepoKey(ep.Repository) == want {
			return ep, true
		}
	}
	return config.EndpointConfig{}, false
}

func (s *Server) activeRunForPR(prKey string) *activeRun {
	if s == nil || strings.TrimSpace(prKey) == "" {
		return nil
	}
	s.activeRunsMu.Lock()
	defer s.activeRunsMu.Unlock()
	return s.activeRuns["pr:"+strings.TrimSpace(prKey)]
}

func (s *Server) runAgeActivity(prKey string) time.Time {
	var last time.Time
	if run := s.activeRunForPR(prKey); run != nil {
		last = run.progressAt()
	}
	if detail, ok := s.tasks.SurfaceDetail(prKey); ok && detail.LastWriteAt.After(last) {
		last = detail.LastWriteAt
	}
	// The delivery row's updated_at only moves on claim/retry, not on agent
	// progress. Treating it as the last write would report a healthy run as
	// idle after a restart, so an unknown activity time stays unknown.
	return last
}

func (s *Server) runAgePhase(row store.PRRun) Phase {
	if _, phase, _, ok := s.tasks.SurfaceState(row.PRKey); ok && phase != "" {
		return phase
	}
	return PhaseForWorkflow(row.ActiveKind)
}

func (s *Server) runAgeLoop() {
	defer s.dispatchWG.Done()
	ticker := time.NewTicker(s.runAgeLoopInterval())
	defer ticker.Stop()
	for {
		select {
		case <-s.shutdownCtx.Done():
			return
		case now := <-ticker.C:
			s.refreshRunAges(s.shutdownCtx, now)
		}
	}
}

func (s *Server) runAgeLoopInterval() time.Duration {
	interval := config.DefaultRefreshInterval
	for _, ep := range s.endpoints {
		if value := ep.EffectiveRefreshInterval(); value < interval {
			interval = value
		}
	}
	return interval
}

func (s *Server) refreshRunAges(ctx context.Context, now time.Time) int {
	if s == nil || s.ageState == nil || s.prQueues == nil || s.deliveries == nil {
		return 0
	}
	rows, err := s.prQueues.ListUnresolved(ctx)
	if err != nil {
		slog.Warn("webhook: run age list failed", "error", err)
		return 0
	}
	edits := 0
	for _, row := range rows {
		if ctx.Err() != nil {
			return edits
		}
		if strings.TrimSpace(row.PRKey) == "" || row.ActiveDelivery == 0 {
			continue
		}
		receipt, err := s.deliveries.GetByID(ctx, row.ActiveDelivery)
		if err != nil || !deliveryInFlight(receipt) {
			continue
		}
		phase := s.runAgePhase(row)
		if phase == "" || IsTerminalPhase(phase) {
			continue
		}
		ep, ok := s.endpointForPR(row.PRKey, phase)
		if !ok {
			continue
		}
		interval := ep.EffectiveRefreshInterval()
		s.ageState.mu.Lock()
		state := s.runAgeCard(row.PRKey)
		if !state.nextAttempt.IsZero() && now.Before(state.nextAttempt) {
			s.ageState.mu.Unlock()
			continue
		}
		state.nextAttempt = now.Add(interval)
		parentBase := state.parentBase
		parentText := state.parentText
		parentGen := state.parentGen
		threadBase := state.threadBase
		threadText := state.threadText
		threadGen := state.threadGen
		threadPhase := state.threadPhase
		s.ageState.mu.Unlock()

		repo, prNumber, _ := splitPRKey(row.PRKey)
		rootID, threadID, prURL, branch := s.runAgeSurface(row)
		if parentBase == "" {
			parentBase = FormatPhaseParent(repo, prNumber, phase, s.statusIcons)
		}
		if threadBase == "" {
			threadBase = strings.TrimSpace(row.ThreadStatusBase)
		}
		startedAt := deliveryStartedAt(receipt)
		lastActivity := s.runAgeActivity(row.PRKey)
		idleAfter := ep.EffectiveIdleAfter()
		segmentFor := func(p Phase) string {
			return FormatRunAgeSegment(now, startedAt, lastActivity, p, idleAfter)
		}
		parentSegment := segmentFor(phase)
		threadSegment := parentSegment
		if threadPhase != "" {
			threadSegment = segmentFor(threadPhase)
		}
		if rootID != "" && branch != "" {
			text := appendStatusSegment(parentBase, parentSegment)
			if text != parentText {
				if err := s.editParentAge(ctx, ep, row.PRKey, repo, rootID, branch, prURL, threadID, parentBase, segmentFor, phase, parentGen, state); err != nil {
					slog.Warn("webhook: run age edit failed", "pr_key", row.PRKey, "surface", "parent", "error", err)
					if errors.Is(err, channel.ErrRateLimited) {
						s.ageState.mu.Lock()
						state.nextAttempt = now.Add(2 * interval)
						s.ageState.mu.Unlock()
					}
				} else {
					edits++
				}
			}
		}
		if threadID != "" && threadBase != "" {
			msgID := s.tasks.StatusMessageIDForThread(threadID)
			if msgID == "" {
				if detail, ok := s.tasks.SurfaceDetail(row.PRKey); ok {
					msgID = strings.TrimSpace(detail.StatusMessageID)
				}
			}
			if msgID == "" {
				msgID = strings.TrimSpace(row.ThreadStatusMsgID)
			}
			if msgID != "" {
				threadPhaseValue := threadPhase
				if threadPhaseValue == "" {
					threadPhaseValue = phase
				}
				text := appendStatusSegment(threadBase, threadSegment)
				if text != threadText {
					target := threadTargetFor(ep.Platform, ep.ChannelID, threadID)
					if err := s.editThreadAge(ctx, ep.Platform, target, msgID, threadBase, segmentFor, threadPhaseValue, threadGen, state); err != nil {
						slog.Warn("webhook: run age edit failed", "pr_key", row.PRKey, "surface", "thread", "error", err)
						if errors.Is(err, channel.ErrRateLimited) {
							s.ageState.mu.Lock()
							state.nextAttempt = now.Add(2 * interval)
							s.ageState.mu.Unlock()
						}
					} else {
						edits++
					}
				}
			}
		}
	}
	return edits
}

// editParentAge writes an age-only update and repairs it when a phase
// transition lands while the update is in flight. The generation check makes
// the newer phase render win instead of letting the stale age snapshot
// overwrite it.
func (s *Server) editParentAge(ctx context.Context, ep config.EndpointConfig, prKey, repo, rootID, branch, prURL, threadID, base string, segmentFor func(Phase) string, phase Phase, gen uint64, state *runAgeCardState) error {
	for attempt := 0; ; attempt++ {
		text := appendStatusSegment(base, segmentFor(phase))
		buttons := s.phaseParentButtons(ep, prKey, repo, branch, prURL, threadID)
		if err := s.editAgePhaseParent(ctx, ep.Platform, ep.ChannelID, rootID, text, buttons); err != nil {
			return err
		}
		s.ageState.mu.Lock()
		currentGen := state.parentGen
		currentBase := state.parentBase
		currentPhase := state.parentPhase
		if currentGen == gen {
			state.parentText = text
			s.ageState.mu.Unlock()
			return nil
		}
		s.ageState.mu.Unlock()
		if attempt >= 2 || currentBase == "" || currentBase == base {
			return nil
		}
		gen = currentGen
		base = currentBase
		if currentPhase != "" {
			phase = currentPhase
		}
	}
}

// editThreadAge mirrors editParentAge for the pinned thread line.
func (s *Server) editThreadAge(ctx context.Context, platform, target, messageID, base string, segmentFor func(Phase) string, phase Phase, gen uint64, state *runAgeCardState) error {
	for attempt := 0; ; attempt++ {
		text := appendStatusSegment(base, segmentFor(phase))
		if err := s.editAgeThread(ctx, platform, target, messageID, text); err != nil {
			return err
		}
		s.ageState.mu.Lock()
		currentGen := state.threadGen
		currentBase := state.threadBase
		currentPhase := state.threadPhase
		if currentGen == gen {
			state.threadText = text
			if state.threadBase == "" {
				state.threadBase = base
				state.threadPhase = phase
			}
			s.ageState.mu.Unlock()
			return nil
		}
		s.ageState.mu.Unlock()
		if attempt >= 2 || currentBase == "" || currentBase == base {
			return nil
		}
		gen = currentGen
		base = currentBase
		if currentPhase != "" {
			phase = currentPhase
		}
	}
}

func (s *Server) runAgeSurface(row store.PRRun) (string, string, string, string) {
	rootID, threadID, prURL, branch := "", strings.TrimSpace(row.ThreadID), strings.TrimSpace(row.PRURL), strings.TrimSpace(row.Branch)
	if detail, ok := s.tasks.SurfaceDetail(row.PRKey); ok {
		rootID = strings.TrimSpace(detail.RootMessageID)
		if value := strings.TrimSpace(detail.ThreadID); value != "" {
			threadID = value
		}
		if value := strings.TrimSpace(detail.PRURL); value != "" {
			prURL = value
		}
		if value := strings.TrimSpace(detail.Branch); value != "" {
			branch = value
		}
	}
	if rootID == "" {
		rootID = strings.TrimSpace(row.RootMsgID)
	}
	return rootID, threadID, prURL, branch
}
