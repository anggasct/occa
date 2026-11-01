package webhook

import (
	"context"
	"log/slog"
	"strings"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

// queuedMark records which delivery moved a PR card to queued and what the
// card showed before ("" when the surface was unknown, e.g. after a restart).
type queuedMark struct {
	endpoint   string
	deliveryID string
	previous   Phase
}

func (s *Server) rememberQueuedBy(prKey, endpoint, deliveryID string, previous Phase) {
	s.queuedMarksMu.Lock()
	defer s.queuedMarksMu.Unlock()
	if s.queuedMarks == nil {
		s.queuedMarks = make(map[string]queuedMark)
	}
	s.queuedMarks[prKey] = queuedMark{endpoint: endpoint, deliveryID: deliveryID, previous: previous}
}

func (s *Server) takeQueuedMark(prKey, endpoint, deliveryID string) (queuedMark, bool) {
	s.queuedMarksMu.Lock()
	defer s.queuedMarksMu.Unlock()
	mark, ok := s.queuedMarks[prKey]
	if !ok || mark.endpoint != endpoint || mark.deliveryID != deliveryID {
		return queuedMark{}, false
	}
	delete(s.queuedMarks, prKey)
	return mark, true
}

// skipBeforeRun marks a delivery skipped by a gate inside the run path
// (admission, review duplicate, trigger rate cap) and puts the PR card back
// when this delivery is what moved it to queued.
func (s *Server) skipBeforeRun(id int64, ep config.EndpointConfig, envelope WebhookEnvelope, prKey, deliveryID, reason string) {
	s.markSkipped(id, ep, envelope, reason)
	s.settleQueuedAfterSkip(ep, prKey, deliveryID, envelope)
}

// settleQueuedAfterSkip returns a card that a skipped delivery moved to
// queued to what it showed before. With nothing known from before, it shows
// the phase of the PR's latest delivery that ran. Other queued work keeps the
// card queued.
func (s *Server) settleQueuedAfterSkip(ep config.EndpointConfig, prKey, deliveryID string, envelope WebhookEnvelope) {
	prKey = strings.TrimSpace(prKey)
	if s == nil || s.tasks == nil || prKey == "" {
		return
	}
	mark, ok := s.takeQueuedMark(prKey, ep.Name, deliveryID)
	if !ok {
		return
	}
	if _, phase, _, known := s.tasks.SurfaceState(prKey); !known || phase != PhaseQueued {
		return
	}
	if busy := s.prDispatchState(prKey); busy.queued > 0 {
		return
	}
	target := mark.previous
	if target == "" {
		target = s.lastRunPhase(prKey)
	}
	if target == "" || target == PhaseQueued {
		return
	}
	slog.Info("webhook: queued card settled after skip", "pr_key", prKey, "delivery_id", deliveryID, "phase", string(target))
	s.syncPhaseParent(context.Background(), ep, prKey, envelope, target)
}

// lastRunPhase is the phase of the PR's latest completed or failed delivery.
func (s *Server) lastRunPhase(prKey string) Phase {
	if s.deliveries == nil {
		return ""
	}
	last, err := s.deliveries.LatestSettledForPR(context.Background(), prKey)
	if err != nil || last == nil {
		return ""
	}
	if last.Status == store.WebhookStatusFailed {
		return PhaseFailed
	}
	if ep, ok := s.endpointsByName[last.Endpoint]; ok {
		return PhaseForWorkflow(ep.Workflow)
	}
	return ""
}

// reopenFailedSurfaceForRun unfreezes a card left on failed when a new run of
// the PR starts: a failed run is over, and the card must show the run that
// replaced it. A blocked PR stays frozen until the operator acts.
func (s *Server) reopenFailedSurfaceForRun(prKey string) {
	if s.currentSurfacePhase(prKey) != PhaseFailed {
		return
	}
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(context.Background(), prKey); err == nil && row != nil && row.Status == store.PRStatusBlocked {
			return
		}
	}
	s.reopenPRSurface(prKey)
}
