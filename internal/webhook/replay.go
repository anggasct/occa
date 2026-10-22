package webhook

import (
	"bytes"
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
	ctx, cancel := s.notificationContext()
	defer cancel()
	if err := s.notifier(ctx, ep.Platform, ep.ChannelID, text); err != nil {
		slog.Warn("webhook: rejection notice failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", err)
	}
}

func (s *Server) notificationContext() (context.Context, context.CancelFunc) {
	if s.processingTimeout > 0 {
		return context.WithTimeout(context.Background(), s.processingTimeout)
	}
	return context.WithCancel(context.Background())
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
		replayID:    row.ID,
	}
	if _, admitted, _ := s.enqueue(ep, ExtractExecutionKey(row.Body), item); !admitted {
		slog.Info("webhook: replay result", "endpoint", row.Endpoint, "delivery_id", row.DeliveryID, "result", "deferred")
		s.rescheduleReplay(ctx, row)
		return
	}
	slog.Info("webhook: replay result", "endpoint", row.Endpoint, "delivery_id", row.DeliveryID, "result", "queued")
}

func (s *Server) rescheduleReplay(ctx context.Context, row store.WebhookReplay) {
	nextAttempt := row.Attempts + 1
	if nextAttempt >= s.replayMaxAttempts {
		s.expireReplay(ctx, row, "replay attempts exhausted")
		return
	}
	next := time.Now().Add(s.replayBackoffFor(nextAttempt)).Unix()
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
	noticeCtx, cancel := s.notificationContext()
	defer cancel()
	if err := s.notifier(noticeCtx, ep.Platform, ep.ChannelID, text); err != nil {
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
	eventType := strings.ToLower(strings.TrimSpace(row.EventType))
	switch eventType {
	case "pull_request", "check_suite":
		facts, err := ghPRFacts(ctx, repo, prNumber)
		if err != nil {
			return replayUndecidable, err
		}
		if !strings.EqualFold(facts.State, "OPEN") {
			return replaySuperseded, nil
		}
		storedHead := replayEventHead(eventType, row.Body)
		liveHead := normalizeRevision(facts.HeadRefOid)
		if storedHead == "" || liveHead == "" {
			return replayUndecidable, nil
		}
		if storedHead == liveHead {
			return replayGo, nil
		}
		relation, err := ghHeadRelation(ctx, repo, storedHead, liveHead)
		if err != nil {
			return replayUndecidable, err
		}
		switch relation {
		case "ahead", "identical":
			return replayGo, nil
		case "behind", "diverged":
			return replaySuperseded, nil
		}
		return replayUndecidable, nil
	case "pull_request_review":
		var payload struct {
			Review struct {
				ID int64 `json:"id"`
			} `json:"review"`
		}
		if err := json.Unmarshal(row.Body, &payload); err != nil || payload.Review.ID == 0 {
			return replayUndecidable, nil
		}
		state, found, err := ghReviewState(ctx, repo, prNumber, payload.Review.ID)
		if err != nil {
			return replayUndecidable, err
		}
		if !found {
			return replaySuperseded, nil
		}
		switch state {
		case "APPROVED", "CHANGES_REQUESTED":
			return replayGo, nil
		}
		return replaySuperseded, nil
	case "issue_comment":
		var payload struct {
			Comment struct {
				ID int64 `json:"id"`
			} `json:"comment"`
		}
		if err := json.Unmarshal(row.Body, &payload); err != nil || payload.Comment.ID == 0 {
			return replayUndecidable, nil
		}
		facts, err := ghPRFacts(ctx, repo, prNumber)
		if err != nil {
			return replayUndecidable, err
		}
		if !strings.EqualFold(facts.State, "OPEN") {
			return replaySuperseded, nil
		}
		exists, err := ghCommentExists(ctx, repo, payload.Comment.ID)
		if err != nil {
			return replayUndecidable, err
		}
		if !exists {
			return replaySuperseded, nil
		}
		return replayGo, nil
	}
	return replayUndecidable, nil
}

func replayEventHead(eventType string, body []byte) string {
	var payload struct {
		PullRequest struct {
			Head struct {
				SHA string `json:"sha"`
			} `json:"head"`
		} `json:"pull_request"`
		CheckSuite struct {
			HeadSHA string `json:"head_sha"`
		} `json:"check_suite"`
		HeadSHA string `json:"head_sha"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	switch eventType {
	case "pull_request":
		return normalizeRevision(payload.PullRequest.Head.SHA)
	case "check_suite":
		if head := normalizeRevision(payload.CheckSuite.HeadSHA); head != "" {
			return head
		}
		return normalizeRevision(payload.HeadSHA)
	}
	return ""
}

type ghPRSnapshot struct {
	State      string `json:"state"`
	HeadRefOid string `json:"headRefOid"`
}

func ghPRFacts(ctx context.Context, repo string, number int) (ghPRSnapshot, error) {
	out, err := replayGHRun(ctx, "pr", "view", strconv.Itoa(number), "--repo", repo, "--json", "state,headRefOid")
	if err != nil {
		return ghPRSnapshot{}, err
	}
	var facts ghPRSnapshot
	if err := json.Unmarshal(out, &facts); err != nil {
		return ghPRSnapshot{}, fmt.Errorf("gh pr view: %w", err)
	}
	if strings.TrimSpace(facts.State) == "" {
		return ghPRSnapshot{}, fmt.Errorf("gh pr view: empty state")
	}
	return facts, nil
}

func ghReviewState(ctx context.Context, repo string, number int, reviewID int64) (string, bool, error) {
	reviews, err := ghReviews(ctx, repo, number)
	if err != nil {
		return "", false, err
	}
	replayState := ""
	for _, review := range reviews {
		if review.ID == reviewID {
			replayState = strings.ToUpper(strings.TrimSpace(review.State))
			break
		}
	}
	if replayState == "" {
		return "", false, nil
	}
	switch replayState {
	case "APPROVED", "CHANGES_REQUESTED":
		decision := ghReviewDecision(reviews)
		if !strings.EqualFold(decision, replayState) {
			return decision, false, nil
		}
		return replayState, true, nil
	}
	return replayState, true, nil
}

type ghReview struct {
	ID    int64  `json:"id"`
	State string `json:"state"`
	User  struct {
		Login string `json:"login"`
	} `json:"user"`
}

// ghReviewDecision derives the live PR review decision the way GitHub does:
// each reviewer's latest verdict replaces their earlier one, a dismissal clears
// it, and any outstanding change request blocks even when another reviewer
// approved later.
func ghReviewDecision(reviews []ghReview) string {
	latest := make(map[string]ghReview)
	for _, review := range reviews {
		state := strings.ToUpper(strings.TrimSpace(review.State))
		switch state {
		case "APPROVED", "CHANGES_REQUESTED", "DISMISSED":
		default:
			continue
		}
		key := strings.ToLower(strings.TrimSpace(review.User.Login))
		if key == "" {
			key = fmt.Sprintf("review:%d", review.ID)
		}
		if prev, ok := latest[key]; ok && prev.ID > review.ID {
			continue
		}
		latest[key] = ghReview{ID: review.ID, State: state}
	}
	decision := ""
	for _, review := range latest {
		switch review.State {
		case "CHANGES_REQUESTED":
			return "CHANGES_REQUESTED"
		case "APPROVED":
			decision = "APPROVED"
		}
	}
	return decision
}

// ghReviews returns every review of a PR in submission order.
func ghReviews(ctx context.Context, repo string, number int) ([]ghReview, error) {
	const perPage = 100
	var all []ghReview
	for page := 1; page <= 50; page++ {
		out, err := replayGHRun(ctx, "api", fmt.Sprintf("repos/%s/pulls/%d/reviews?per_page=%d&page=%d", repo, number, perPage, page))
		if err != nil {
			return nil, err
		}
		var reviews []ghReview
		if err := json.Unmarshal(out, &reviews); err != nil {
			return nil, fmt.Errorf("gh review list: %w", err)
		}
		all = append(all, reviews...)
		if len(reviews) < perPage {
			break
		}
	}
	return all, nil
}

// ghHeadRelation reports how the live head relates to the stored head, using
// the compare status of stored...live: "ahead" means the stored head is an
// ancestor of the live head.
func ghHeadRelation(ctx context.Context, repo, storedHead, liveHead string) (string, error) {
	out, err := replayGHRun(ctx, "api", fmt.Sprintf("repos/%s/compare/%s...%s", repo, storedHead, liveHead))
	if err != nil {
		return "", err
	}
	var relation struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(out, &relation); err != nil {
		return "", fmt.Errorf("gh compare: %w", err)
	}
	return strings.ToLower(strings.TrimSpace(relation.Status)), nil
}

func ghCommentExists(ctx context.Context, repo string, commentID int64) (bool, error) {
	if _, err := replayGHRun(ctx, "api", fmt.Sprintf("repos/%s/issues/comments/%d", repo, commentID)); err != nil {
		if isGHNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func isGHNotFound(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "404") || strings.Contains(text, "not found")
}

var replayGHRun = func(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "gh", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return nil, fmt.Errorf("gh %s: %s", strings.Join(args, " "), detail)
	}
	return stdout.Bytes(), nil
}
