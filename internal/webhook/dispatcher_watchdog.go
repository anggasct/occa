package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/store"
)

func (s *Server) SetDispatcherStuckAfter(d time.Duration) {
	if d > 0 {
		s.dispatcherStuckAfter = d
	}
}

func (s *Server) SetDispatcherWatchdogInterval(d time.Duration) {
	if d > 0 {
		s.watchdogInterval = d
	}
}

func (s *Server) stuckAfter() time.Duration {
	if s.dispatcherStuckAfter > 0 {
		return s.dispatcherStuckAfter
	}
	return 5 * time.Minute
}

func (s *Server) watchdogTick() time.Duration {
	if s.watchdogInterval > 0 {
		return s.watchdogInterval
	}
	return time.Minute
}

func (s *Server) ensureEnqueueReceipt(item dispatchItem) {
	ctx := context.Background()
	created, err := s.deliveries.Create(ctx, store.WebhookDelivery{
		Endpoint:    item.ep.Name,
		DeliveryID:  item.deliveryID,
		EventType:   item.eventType,
		PayloadHash: item.payloadHash,
		Attempt:     1,
	})
	if err != nil {
		slog.Error("webhook: enqueue receipt failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", err)
		return
	}
	if created {
		slog.Info("webhook: receipt recorded at enqueue", "endpoint", item.ep.Name, "delivery_id", item.deliveryID)
	}
}

func (s *Server) watchdogLoop() {
	ticker := time.NewTicker(s.watchdogTick())
	defer ticker.Stop()
	for {
		select {
		case <-s.shutdownCtx.Done():
			return
		case now := <-ticker.C:
			s.sweepStuckDispatchers(now)
		}
	}
}

func (s *Server) sweepStuckDispatchers(now time.Time) int {
	after := s.stuckAfter()
	type candidate struct {
		d    *dispatcher
		snap dispatcherSnapshot
	}
	var stuck []candidate
	s.dispatchMu.Lock()
	for _, d := range s.dispatchers {
		if d.exiting {
			continue
		}
		snap := d.snapshot()
		if snap.stuckMarked {
			continue
		}
		if snap.hasActive {
			if snap.startedAt.IsZero() {
				continue
			}
			if now.Sub(snap.startedAt) < after {
				continue
			}
			stuck = append(stuck, candidate{d, snap})
			continue
		}
		if snap.queueLen > 0 && !snap.lastActive.IsZero() && now.Sub(snap.lastActive) >= after {
			stuck = append(stuck, candidate{d, snap})
		}
	}
	s.dispatchMu.Unlock()
	for _, c := range stuck {
		c.d.markStuckReported()
		s.alarmStuckDispatcher(c.d, c.snap, now, after)
		s.releaseStuckDispatcher(c.d)
	}
	return len(stuck)
}

func stuckRepoBranch(s *Server, item dispatchItem) (string, string) {
	key := ExtractExecutionKey(item.body)
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", item.ep.CommentTrigger)
	repo, branch := TaskRepoBranch(key, envelope)
	if repo != "" && branch != "" {
		return repo, branch
	}
	return normalizeRepoKey(key.Repository), normalizeBranchKey(key.Branch)
}

func (s *Server) alarmStuckDispatcher(d *dispatcher, snap dispatcherSnapshot, now time.Time, after time.Duration) {
	item := snap.active
	if !snap.hasActive && snap.queueLen > 0 {
		select {
		case peek := <-d.ch:
			item = peek
			select {
			case d.ch <- peek:
			default:
				s.failDelivery(peek.ep, 0, peek.deliveryID, peek.eventType, normalizeWebhook(peek.body, peek.eventType, peek.deliveryID, s.verdicts, false, "", peek.ep.CommentTrigger), "watchdog queue overflow while peeking", &WebhookWorkContext{DeliveryID: peek.deliveryID})
			}
		default:
		}
	}
	repo, branch := stuckRepoBranch(s, item)
	if repo == "" || branch == "" {
		if key := strings.TrimSpace(d.key); key != "" {
			repo, branch = key, "-"
		}
	}
	stuckSince := snap.startedAt
	if stuckSince.IsZero() {
		stuckSince = snap.lastActive
	}
	elapsed := now.Sub(stuckSince).Round(time.Second)
	card := fmt.Sprintf("🚨 STUCK dispatcher %s#%s · delivery %s · stuck since %s (%s, threshold %s) · queue %d/%d — watchdog released the key so the next item proceeds. Check delivery log and sessions.",
		repo, branch, strings.TrimSpace(item.deliveryID), stuckSince.UTC().Format(time.RFC3339), elapsed, after, snap.queueLen, snap.queueCap)
	slog.Error("webhook: dispatcher stuck, key released",
		"lock", d.key,
		"repo", repo,
		"branch", branch,
		"delivery_id", strings.TrimSpace(item.deliveryID),
		"stuck_since", stuckSince.UTC().Format(time.RFC3339),
		"elapsed", elapsed.String(),
		"queue", fmt.Sprintf("%d/%d", snap.queueLen, snap.queueCap),
	)
	if repo == "" || branch == "" || branch == "-" {
		return
	}
	threadID := ""
	if rec, ok := s.tasks.Lookup(repo, branch); ok {
		threadID = strings.TrimSpace(rec.ThreadID)
	}
	if threadID == "" || s.notifier == nil {
		return
	}
	target := threadID
	if strings.EqualFold(strings.TrimSpace(item.ep.Platform), "telegram") && !strings.Contains(strings.TrimSpace(item.ep.ChannelID), ":") && strings.TrimSpace(item.ep.ChannelID) != "" {
		target = strings.TrimSpace(item.ep.ChannelID) + ":" + threadID
	} else if !strings.EqualFold(strings.TrimSpace(item.ep.Platform), "discord") && strings.TrimSpace(item.ep.ChannelID) != "" {
		target = auditTargetChannel(item.ep.Platform, item.ep.ChannelID, threadID)
		if strings.TrimSpace(target) == "" {
			target = threadID
		}
	}
	if err := s.notifier(context.Background(), item.ep.Platform, target, card); err != nil {
		slog.Warn("webhook: stuck alarm card failed", "repo", repo, "branch", branch, "delivery_id", strings.TrimSpace(item.deliveryID), "error", err)
	}
}

func (s *Server) releaseStuckDispatcher(old *dispatcher) {
	s.dispatchMu.Lock()
	current, ok := s.dispatchers[old.key]
	if !ok || current != old {
		s.dispatchMu.Unlock()
		return
	}
	old.exiting = true
	delete(s.dispatchers, old.key)
	fresh := &dispatcher{server: s, key: old.key, ch: make(chan dispatchItem, s.maxQueuedPerKey), lastActivity: time.Now()}
	s.dispatchers[old.key] = fresh
	s.dispatchWG.Add(1)
	go fresh.run(s.shutdownCtx)
	var queued []dispatchItem
	for {
		select {
		case item := <-old.ch:
			queued = append(queued, item)
		default:
			goto drained
		}
	}
drained:
	s.dispatchMu.Unlock()
	moved := 0
	for _, item := range queued {
		select {
		case fresh.ch <- item:
			moved++
		default:
			s.abandon(item, "watchdog queue overflow after key release")
		}
	}
	slog.Error("webhook: dispatcher key released after stuck", "lock", old.key, "moved", moved, "queued", len(queued))
}

func (s *Server) handleDispatcherPanic(d *dispatcher, item dispatchItem, r any) {
	stack := strings.TrimSpace(string(debug.Stack()))
	summary := redactSummary(fmt.Sprintf("panic: %v", r), maxErrorSummaryRunes)
	slog.Error("webhook: dispatcher panic, rebuilding",
		"lock", d.key,
		"endpoint", item.ep.Name,
		"delivery_id", item.deliveryID,
		"panic", fmt.Sprint(r),
		"stack", clipRunes(stack, 2000),
	)
	if receipt, err := s.deliveries.Get(context.Background(), item.ep.Name, item.deliveryID); err == nil && receipt != nil {
		_, _ = s.deliveries.Transition(context.Background(), receipt.ID,
			[]store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted, store.WebhookStatusProcessing},
			store.WebhookStatusFailed, summary)
	} else if err != nil {
		slog.Error("webhook: panic receipt lookup failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", err)
	}
	s.releaseStuckDispatcher(d)
}

func (s *Server) rebuildAfterPanic(d *dispatcher, item dispatchItem, r any) {
	stack := strings.TrimSpace(string(debug.Stack()))
	slog.Error("webhook: dispatcher goroutine panic, rebuilding",
		"lock", d.key,
		"panic", fmt.Sprint(r),
		"stack", clipRunes(stack, 2000),
	)
	if strings.TrimSpace(item.deliveryID) != "" {
		if receipt, err := s.deliveries.Get(context.Background(), item.ep.Name, item.deliveryID); err == nil && receipt != nil {
			_, _ = s.deliveries.Transition(context.Background(), receipt.ID,
				[]store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted, store.WebhookStatusProcessing},
				store.WebhookStatusFailed, redactSummary(fmt.Sprintf("panic: %v", r), maxErrorSummaryRunes))
		}
	}
	s.dispatchMu.Lock()
	if current, ok := s.dispatchers[d.key]; ok && current == d {
		d.exiting = true
		delete(s.dispatchers, d.key)
	}
	var queued []dispatchItem
	select {
	case q := <-d.ch:
		queued = append(queued, q)
		for {
			select {
			case q := <-d.ch:
				queued = append(queued, q)
			default:
				goto drained
			}
		}
	default:
	}
drained:
	freshNeeded := len(queued) > 0 || strings.TrimSpace(item.deliveryID) != ""
	var fresh *dispatcher
	if freshNeeded {
		fresh = &dispatcher{server: s, key: d.key, ch: make(chan dispatchItem, s.maxQueuedPerKey), lastActivity: time.Now()}
		s.dispatchers[d.key] = fresh
		s.dispatchWG.Add(1)
		go fresh.run(s.shutdownCtx)
	}
	s.dispatchMu.Unlock()
	if fresh == nil {
		return
	}
	if strings.TrimSpace(item.deliveryID) != "" {
		select {
		case fresh.ch <- item:
		default:
			s.abandon(item, "panic rebuild queue overflow")
		}
	}
	for _, q := range queued {
		select {
		case fresh.ch <- q:
		default:
			s.abandon(q, "panic rebuild queue overflow")
		}
	}
}
