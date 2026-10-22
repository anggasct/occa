package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

type replayDecision int

const (
	replayGo replayDecision = iota
	replayUndecidable
	replaySuperseded
)

type ReplayChecker func(ctx context.Context, replay store.WebhookReplay) (replayDecision, error)

// recordRejection persists a rejected delivery before the 429 is returned. A
// storage failure must never fail open: the request is still rejected, and the
// lost body is reported at ERROR.
func (s *Server) recordRejection(ep config.EndpointConfig, item dispatchItem, reason string) {
	ctx := context.Background()
	if s.deliveries != nil {
		if _, err := s.deliveries.Create(ctx, store.WebhookDelivery{
			Endpoint:     ep.Name,
			DeliveryID:   item.deliveryID,
			EventType:    item.eventType,
			PayloadHash:  item.payloadHash,
			PRKey:        item.prKey,
			Status:       store.WebhookStatusRejected,
			ErrorSummary: reason,
		}); err != nil {
			slog.Error("webhook: reject record failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", err)
		}
	}
	if s.replays == nil {
		s.notifyRejection(ep, item, reason)
		return
	}
	now := time.Now().Unix()
	backoff := s.replayBackoffFor(0)
	_, created, err := s.replays.Create(ctx, store.WebhookReplay{
		Endpoint:      ep.Name,
		DeliveryID:    item.deliveryID,
		EventType:     item.eventType,
		PRKey:         item.prKey,
		Body:          item.body,
		Reason:        reason,
		NextAttemptAt: now + int64(backoff/time.Second),
		ExpiresAt:     now + int64(s.replayTTL/time.Second),
		Status:        store.WebhookReplayPending,
		CreatedAt:     now,
		UpdatedAt:     now,
	})
	if err != nil {
		slog.Error("webhook: reject record failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", err)
		return
	}
	if !created {
		return
	}
	slog.Info("webhook: replay scheduled", "endpoint", ep.Name, "delivery_id", item.deliveryID, "event_type", item.eventType, "next_attempt_at", now+int64(backoff/time.Second))
	s.notifyRejection(ep, item, reason)
}

func (s *Server) recordSuperseded(item dispatchItem, reason string) {
	if s.replays == nil {
		return
	}
	now := time.Now().Unix()
	_, _, err := s.replays.Create(context.Background(), store.WebhookReplay{
		Endpoint:   item.ep.Name,
		DeliveryID: item.deliveryID,
		EventType:  item.eventType,
		PRKey:      item.prKey,
		Body:       item.body,
		Reason:     reason,
		Status:     store.WebhookReplaySuperseded,
		CreatedAt:  now,
		UpdatedAt:  now,
	})
	if err != nil {
		slog.Warn("webhook: superseded record failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", err)
	}
}

func (s *Server) notifyRejection(ep config.EndpointConfig, item dispatchItem, reason string) {
	if s.notifier == nil {
		return
	}
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", ep.CommentTrigger)
	target := strings.TrimSpace(stringValue(envelope["repository"]))
	if number := strings.TrimSpace(stringValue(envelope["pr_number"])); number != "" {
		if target != "" {
			target += "#"
		}
		target += number
	}
	if target == "" {
		target = item.eventType
	}
	backoff := s.replayBackoffFor(0)
	text := fmt.Sprintf("⚠️ WEBHOOK REJECTED — %s · %s · replay in ~%s", target, reason, formatReplayDelay(backoff))
	if err := s.notifier(context.Background(), ep.Platform, ep.ChannelID, text); err != nil {
		slog.Warn("webhook: rejection notice failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", err)
	}
}

func formatReplayDelay(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d/time.Second))
	}
	return fmt.Sprintf("%dm", int(d/time.Minute))
}

func (s *Server) replayBackoffFor(attempt int) time.Duration {
	if len(s.replayBackoff) == 0 {
		return 30 * time.Second
	}
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= len(s.replayBackoff) {
		attempt = len(s.replayBackoff) - 1
	}
	return s.replayBackoff[attempt]
}

func (s *Server) replayLoop(ctx context.Context) {
	defer s.dispatchWG.Done()
	if s.replays == nil || s.replayInterval <= 0 {
		return
	}
	ticker := time.NewTicker(s.replayInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.replayPending(ctx)
		}
	}
}

func (s *Server) replayPending(ctx context.Context) {
	if s.replays == nil {
		return
	}
	rows, err := s.replays.ListDue(ctx, time.Now().Unix(), 50)
	if err != nil {
		slog.Warn("webhook: replay list failed", "error", err)
		return
	}
	for i := range rows {
		if ctx.Err() != nil {
			return
		}
		s.replayOne(ctx, rows[i])
	}
	if _, err := s.replays.Prune(ctx, s.replayStoreMax, s.replayStoreMaxBytes); err != nil {
		slog.Warn("webhook: replay prune failed", "error", err)
	}
}

func (s *Server) replayOne(ctx context.Context, row store.WebhookReplay) {
	now := time.Now().Unix()
	if row.ExpiresAt > 0 && now >= row.ExpiresAt {
		s.expireReplay(ctx, row, "replay TTL reached")
		return
	}
	if row.Attempts >= s.replayMaxAttempts {
		s.expireReplay(ctx, row, "replay attempts exhausted")
		return
	}
	ep, ok := s.endpointsByName[row.Endpoint]
	if !ok {
		s.expireReplay(ctx, row, "endpoint no longer configured")
		return
	}
	checker := s.replayChecker
	if checker == nil {
		checker = defaultReplayChecker
	}
	decision, err := checker(ctx, row)
	if err != nil {
		slog.Info("webhook: replay result", "endpoint", row.Endpoint, "delivery_id", row.DeliveryID, "result", "undecidable", "error", err)
		s.rescheduleReplay(ctx, row)
		return
	}
	switch decision {
	case replayUndecidable:
		slog.Info("webhook: replay result", "endpoint", row.Endpoint, "delivery_id", row.DeliveryID, "result", "undecidable")
		s.rescheduleReplay(ctx, row)
		return
	case replaySuperseded:
		if err := s.replays.Mark(ctx, row.ID, store.WebhookReplaySuperseded, 0); err != nil {
			slog.Warn("webhook: replay mark failed", "delivery_id", row.DeliveryID, "error", err)
			return
		}
		slog.Info("webhook: replay result", "endpoint", row.Endpoint, "delivery_id", row.DeliveryID, "result", "superseded")
		return
	}
	item := dispatchItem{
		ep:          ep,
		body:        row.Body,
		deliveryID:  row.DeliveryID,
		eventType:   row.EventType,
		payloadHash: sha256Hex(row.Body),
		prKey:       row.PRKey,
	}
	if _, admitted := s.enqueue(ep, ExtractExecutionKey(row.Body), item); !admitted {
		slog.Info("webhook: replay result", "endpoint", row.Endpoint, "delivery_id", row.DeliveryID, "result", "deferred")
		s.rescheduleReplay(ctx, row)
		return
	}
	if err := s.replays.Mark(ctx, row.ID, store.WebhookReplayReplayed, 0); err != nil {
		slog.Warn("webhook: replay mark failed", "delivery_id", row.DeliveryID, "error", err)
	}
	slog.Info("webhook: replay result", "endpoint", row.Endpoint, "delivery_id", row.DeliveryID, "result", "replayed")
}

func (s *Server) rescheduleReplay(ctx context.Context, row store.WebhookReplay) {
	if row.Attempts+1 >= s.replayMaxAttempts {
		s.expireReplay(ctx, row, "replay attempts exhausted")
		return
	}
	next := time.Now().Add(s.replayBackoffFor(row.Attempts)).Unix()
	if row.ExpiresAt > 0 && next >= row.ExpiresAt {
		s.expireReplay(ctx, row, "replay TTL reached")
		return
	}
	if err := s.replays.Mark(ctx, row.ID, store.WebhookReplayPending, next); err != nil {
		slog.Warn("webhook: replay reschedule failed", "delivery_id", row.DeliveryID, "error", err)
		return
	}
	slog.Info("webhook: replay scheduled", "endpoint", row.Endpoint, "delivery_id", row.DeliveryID, "next_attempt_at", next)
}

func (s *Server) expireReplay(ctx context.Context, row store.WebhookReplay, reason string) {
	if err := s.replays.Mark(ctx, row.ID, store.WebhookReplayExpired, 0); err != nil {
		slog.Warn("webhook: replay expire failed", "delivery_id", row.DeliveryID, "error", err)
		return
	}
	slog.Info("webhook: replay result", "endpoint", row.Endpoint, "delivery_id", row.DeliveryID, "result", "expired", "reason", reason)
	if s.notifier == nil {
		return
	}
	ep, ok := s.endpointsByName[row.Endpoint]
	if !ok {
		return
	}
	envelope := normalizeWebhook(row.Body, row.EventType, row.DeliveryID, s.verdicts, false, "", ep.CommentTrigger)
	target := strings.TrimSpace(stringValue(envelope["repository"]))
	if number := strings.TrimSpace(stringValue(envelope["pr_number"])); number != "" {
		if target != "" {
			target += "#"
		}
		target += number
	}
	if target == "" {
		target = row.EventType
	}
	text := fmt.Sprintf("❌ WEBHOOK LOST — %s · replay exhausted · manual redelivery required", target)
	if err := s.notifier(context.Background(), ep.Platform, ep.ChannelID, text); err != nil {
		slog.Warn("webhook: lost notice failed", "endpoint", row.Endpoint, "delivery_id", row.DeliveryID, "error", err)
	}
}

func defaultReplayChecker(ctx context.Context, row store.WebhookReplay) (replayDecision, error) {
	repo, number, ok := splitPRKey(row.PRKey)
	if !ok {
		return replayUndecidable, nil
	}
	prNumber, err := strconv.Atoi(strings.TrimSpace(number))
	if err != nil || prNumber <= 0 {
		return replayUndecidable, nil
	}
	timeout := 20 * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	switch strings.ToLower(strings.TrimSpace(row.EventType)) {
	case "pull_request", "check_suite":
		state, err := ghPRState(ctx, repo, prNumber)
		if err != nil {
			return replayUndecidable, err
		}
		if !strings.EqualFold(state, "OPEN") {
			return replaySuperseded, nil
		}
		return replayGo, nil
	case "pull_request_review":
		var payload struct {
			Review struct {
				ID    int64  `json:"id"`
				State string `json:"state"`
			} `json:"review"`
		}
		if err := json.Unmarshal(row.Body, &payload); err != nil || payload.Review.ID == 0 {
			return replayUndecidable, nil
		}
		latest, err := ghLatestReviewID(ctx, repo, prNumber)
		if err != nil {
			return replayUndecidable, err
		}
		if latest != payload.Review.ID {
			return replaySuperseded, nil
		}
		return replayGo, nil
	case "issue_comment":
		state, err := ghPRState(ctx, repo, prNumber)
		if err != nil {
			return replayUndecidable, err
		}
		if !strings.EqualFold(state, "OPEN") {
			return replaySuperseded, nil
		}
		return replayGo, nil
	}
	return replayUndecidable, nil
}

func ghPRState(ctx context.Context, repo string, number int) (string, error) {
	cmd := exec.CommandContext(ctx, "gh", "pr", "view", strconv.Itoa(number), "--repo", repo, "--json", "state", "-q", ".state")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("gh pr view: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

func ghLatestReviewID(ctx context.Context, repo string, number int) (int64, error) {
	cmd := exec.CommandContext(ctx, "gh", "pr", "view", strconv.Itoa(number), "--repo", repo, "--json", "reviews", "-q", ".reviews[-1].id")
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("gh pr view reviews: %w", err)
	}
	id, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("gh pr view reviews: %w", err)
	}
	return id, nil
}
