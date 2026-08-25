package webhook

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"
	"unicode/utf8"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

const (
	maxWebhookBodySize         int64 = 10 * 1024 * 1024
	maxConcurrentWebhookEvents       = 16

	maxDeliveryIDRunes   = 128
	maxEventTypeRunes    = 64
	maxErrorSummaryRunes = 240

	retentionKeep     = 500
	retentionAge      = 30 * 24 * time.Hour
	pruneInterval     = 10 * time.Minute
	processingTimeout = 30 * time.Minute
	claimGrace        = processingTimeout + 2*time.Minute

	readHeaderTimeout = 10 * time.Second
	readTimeout       = 30 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 2 * time.Minute
)

// DeliveryStore is the durable receipt backend the server drives. The full
// repo lives in internal/store; the server only needs the transitions below.
type DeliveryStore interface {
	Create(ctx context.Context, d store.WebhookDelivery) (bool, error)
	Get(ctx context.Context, endpoint, deliveryID string) (*store.WebhookDelivery, error)
	Transition(ctx context.Context, id int64, from []store.WebhookStatus, to store.WebhookStatus, summary string) (bool, error)
	ClaimStale(ctx context.Context, id, cutoff int64) (bool, error)
	Prune(ctx context.Context, cutoff int64, keep int) (int, error)
	FailStale(ctx context.Context, cutoff int64, summary string) (int, error)
}

type ChannelStore interface {
	Get(ctx context.Context, platform, channelID string) (*store.Channel, error)
}

type Executor func(ctx context.Context, platform, channelID, prompt string, workCtx WebhookWorkContext) error

type Notifier func(ctx context.Context, platform, channelID, text string) error

type Server struct {
	bind                string
	bindAddr            string
	endpoints           map[string]config.EndpointConfig
	executor            Executor
	notifier            Notifier
	deliveries          DeliveryStore
	channels            ChannelStore
	worktreeResolver    WorktreeResolver
	pullRequestResolver PullRequestResolver
	httpSrv             *http.Server
	listener            net.Listener
	eventSlots          chan struct{}
	sessionMu           sync.Map
	processingTimeout   time.Duration
	pruneMu             sync.Mutex
	lastPrune           time.Time
	pruneInterval       time.Duration
	readHeaderTimeout   time.Duration
	readTimeout         time.Duration
	writeTimeout        time.Duration
	idleTimeout         time.Duration
	listening           atomic.Bool
}

func New(cfg config.WebhookConfig, executor Executor, deliveries DeliveryStore) *Server {
	endpoints := make(map[string]config.EndpointConfig)
	for _, ep := range cfg.Endpoints {
		ep.Auth = strings.TrimSpace(strings.ToLower(ep.Auth))
		ep.Workflow = strings.TrimSpace(strings.ToLower(ep.Workflow))
		endpoints[ep.Path] = ep
	}
	return &Server{
		bind:              cfg.Bind,
		endpoints:         endpoints,
		executor:          executor,
		deliveries:        deliveries,
		eventSlots:        make(chan struct{}, maxConcurrentWebhookEvents),
		processingTimeout: processingTimeout,
		pruneInterval:     pruneInterval,
		readHeaderTimeout: readHeaderTimeout,
		readTimeout:       readTimeout,
		writeTimeout:      writeTimeout,
		idleTimeout:       idleTimeout,
	}
}

func (s *Server) SetWorktreeResolver(r WorktreeResolver) {
	s.worktreeResolver = r
}

func (s *Server) SetPullRequestResolver(r PullRequestResolver) {
	s.pullRequestResolver = r
}

func (s *Server) SetNotifier(n Notifier) {
	s.notifier = n
}

func (s *Server) SetChannelStore(c ChannelStore) {
	s.channels = c
}

func (s *Server) tryAcquireEvent() bool {
	select {
	case s.eventSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) releaseEvent() {
	<-s.eventSlots
}

// recoverStale marks deliveries a previous process left in flight as failed so
// diagnostics never report a phantom running delivery after a crash. Pruning
// bounds table growth at startup.
func (s *Server) recoverStale(ctx context.Context) {
	now := time.Now()
	if failed, err := s.deliveries.FailStale(ctx, now.Add(-claimGrace).Unix(), "interrupted by restart"); err != nil {
		slog.Error("webhook: restart recovery failed", "error", err)
	} else if failed > 0 {
		slog.Info("webhook: restart recovery", "failed_deliveries", failed)
	}
	if pruned, err := s.deliveries.Prune(ctx, now.Add(-retentionAge).Unix(), retentionKeep); err != nil {
		slog.Error("webhook: prune failed", "error", err)
	} else if pruned > 0 {
		slog.Info("webhook: pruned old deliveries", "pruned", pruned)
	}
	s.pruneMu.Lock()
	s.lastPrune = now
	s.pruneMu.Unlock()
}

func (s *Server) pruneIfDue() {
	s.pruneMu.Lock()
	if time.Since(s.lastPrune) < s.pruneInterval {
		s.pruneMu.Unlock()
		return
	}
	s.lastPrune = time.Now()
	s.pruneMu.Unlock()

	pruned, err := s.deliveries.Prune(context.Background(), time.Now().Add(-retentionAge).Unix(), retentionKeep)
	if err != nil {
		if errors.Is(err, sql.ErrConnDone) || strings.Contains(err.Error(), "database is closed") {
			return
		}
		slog.Error("webhook: prune failed", "error", err)
		return
	}
	if pruned > 0 {
		slog.Info("webhook: pruned old deliveries", "pruned", pruned)
	}
}

func (s *Server) Start(ctx context.Context) error {
	s.recoverStale(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRequest)

	ln, err := net.Listen("tcp", s.bind)
	if err != nil {
		return fmt.Errorf("webhook: listen %s: %w", s.bind, err)
	}
	s.bindAddr = ln.Addr().String()
	s.listener = ln

	s.httpSrv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: s.readHeaderTimeout,
		ReadTimeout:       s.readTimeout,
		WriteTimeout:      s.writeTimeout,
		IdleTimeout:       s.idleTimeout,
	}
	s.listening.Store(true)

	go func() {
		<-ctx.Done()
		s.listening.Store(false)
		_ = s.httpSrv.Close()
	}()

	go func() {
		defer s.listening.Store(false)
		if err := s.httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("webhook: serve error", "error", err)
		}
	}()

	slog.Info("webhook server started", "bind", s.bind, "endpoints", len(s.endpoints))
	return nil
}

func (s *Server) Addr() string { return s.bindAddr }

// Healthy reports whether the listener is up.
func (s *Server) Healthy() bool { return s.listening.Load() }

func (s *Server) Stop(ctx context.Context) error {
	if s.httpSrv == nil {
		return nil
	}
	s.listening.Store(false)
	return s.httpSrv.Shutdown(ctx)
}

func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	ep, ok := s.endpoints[r.URL.Path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if strings.TrimSpace(ep.Secret) == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.ContentLength > maxWebhookBodySize {
		http.Error(w, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBodySize))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "Bad Request", http.StatusBadRequest)
		}
		return
	}

	authMode := strings.TrimSpace(strings.ToLower(ep.Auth))
	if authMode == "github_hmac_sha256" {
		if !VerifyGitHubSignature(body, r.Header.Get("X-Hub-Signature-256"), ep.Secret) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	} else {
		secret := r.Header.Get("X-Webhook-Secret")
		if secret == "" {
			secret = r.URL.Query().Get("secret")
		}
		if subtle.ConstantTimeCompare([]byte(secret), []byte(ep.Secret)) != 1 {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	if !s.tryAcquireEvent() {
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
		return
	}

	payloadHash := sha256Hex(body)
	eventType := providerEventType(r, body)
	deliveryID := providerDeliveryID(r, payloadHash)

	receipt, shouldProcess, alreadyClaimed, err := s.claimReceipt(r.Context(), ep, deliveryID, eventType, payloadHash)
	if err != nil {
		s.releaseEvent()
		slog.Error("webhook: receipt claim failed", "endpoint", ep.Name, "delivery_id", deliveryID, "error", err)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	if !shouldProcess {
		slog.Info("webhook: duplicate delivery", "endpoint", ep.Name, "delivery_id", deliveryID, "event_type", eventType, "observed_status", receipt.Status)
		s.releaseEvent()
		w.WriteHeader(http.StatusOK)
		return
	}

	if s.shouldSkip(ep, eventType) {
		reason := "configured event skip"
		envelope := normalizeWebhook(body, eventType, deliveryID, true, reason)
		summary := redactAuditSummary(formatAuditSummary(envelope, ep.Workflow, "SKIP", reason), ep.Secret)
		if ok, tErr := s.deliveries.Transition(r.Context(), receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted, store.WebhookStatusProcessing}, store.WebhookStatusSkipped, summary); tErr != nil {
			slog.Error("webhook: skip transition failed", "endpoint", ep.Name, "delivery_id", deliveryID, "error", tErr)
			s.releaseEvent()
			http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
			return
		} else if !ok {
			slog.Warn("webhook: skip transition lost race", "endpoint", ep.Name, "delivery_id", deliveryID, "observed_status", receipt.Status)
		} else {
			slog.Info("webhook: delivery skipped", "endpoint", ep.Name, "delivery_id", deliveryID, "event_type", eventType)
			s.emitAudit(r.Context(), ep, envelope, "SKIP", reason)
		}
		s.releaseEvent()
		w.WriteHeader(http.StatusOK)
		return
	}

	if !alreadyClaimed {
		claimed, claimErr := s.claimProcessing(r.Context(), receipt, deliveryID)
		if claimErr != nil {
			s.releaseEvent()
			http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
			return
		}
		if !claimed {
			s.releaseEvent()
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	slog.Info("webhook: delivery accepted", "endpoint", ep.Name, "delivery_id", deliveryID, "event_type", eventType, "status", store.WebhookStatusProcessing)
	w.WriteHeader(http.StatusOK)

	go func() {
		defer s.releaseEvent()
		s.processAsync(ep, body, receipt.ID, deliveryID, eventType)
		s.pruneIfDue()
	}()
}

// claimReceipt inserts a durable receipt for a validated delivery. A brand-new
// delivery reports shouldProcess=true. A duplicate of a terminal delivery, or
// one still being processed within the claim grace window, observes the
// existing status and does not start a second session. A receipt still in
// received has not completed its first state transition, so it remains
// retryable; the next handler step performs the transition with CAS. A
// duplicate whose in-flight attempt went stale (the earlier process died
// mid-request) is re-claimed atomically via ClaimStale and reports
// alreadyClaimed so the handler skips the redundant CAS.
func (s *Server) claimReceipt(ctx context.Context, ep config.EndpointConfig, deliveryID, eventType, payloadHash string) (*store.WebhookDelivery, bool, bool, error) {
	created, err := s.deliveries.Create(ctx, store.WebhookDelivery{
		Endpoint:    ep.Name,
		DeliveryID:  deliveryID,
		EventType:   eventType,
		PayloadHash: payloadHash,
		Attempt:     1,
	})
	if err != nil {
		return nil, false, false, err
	}

	receipt, err := s.deliveries.Get(ctx, ep.Name, deliveryID)
	if err != nil {
		return nil, false, false, err
	}
	if receipt == nil {
		return nil, false, false, errors.New("receipt missing after claim")
	}
	if created {
		return receipt, true, false, nil
	}

	if isTerminal(receipt.Status) {
		return receipt, false, false, nil
	}
	if receipt.Status == store.WebhookStatusReceived {
		return receipt, true, false, nil
	}
	if time.Now().Unix()-receipt.UpdatedAt < int64(claimGrace.Seconds()) {
		return receipt, false, false, nil
	}

	ok, tErr := s.deliveries.ClaimStale(ctx, receipt.ID, time.Now().Add(-claimGrace).Unix())
	if tErr != nil {
		return nil, false, false, tErr
	}
	if !ok {
		return receipt, false, false, nil
	}
	receipt.Status = store.WebhookStatusProcessing
	return receipt, true, true, nil
}

// claimProcessing moves a fresh receipt to processing under compare-and-swap
// so a lost race leaves the delivery retryable instead of double-starting a
// session. Receipts already claimed through ClaimStale skip this call.
func (s *Server) claimProcessing(ctx context.Context, receipt *store.WebhookDelivery, deliveryID string) (bool, error) {
	ok, err := s.deliveries.Transition(ctx, receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusProcessing, "")
	if err != nil {
		slog.Error("webhook: processing claim failed", "endpoint", receipt.Endpoint, "delivery_id", deliveryID, "error", err)
		return false, err
	}
	return ok, nil
}

func (s *Server) sessionLock(ep config.EndpointConfig, key WebhookExecutionKey) *sync.Mutex {
	var lockKey string
	if key.IsZero() {
		lockKey = "endpoint:" + ep.Platform + "|" + ep.ChannelID
	} else {
		lockKey = "key:" + key.String()
	}
	mu, _ := s.sessionMu.LoadOrStore(lockKey, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (s *Server) processAsync(ep config.EndpointConfig, body []byte, id int64, deliveryID, eventType string) {
	key := ExtractExecutionKey(body)
	var workCtx WebhookWorkContext
	workCtx.Key = key
	if !key.IsZero() {
		workCtx.SessionKey = key.String()
	}
	envelope := normalizeWebhook(body, eventType, deliveryID, false, "")

	defer func() {
		if r := recover(); r != nil {
			slog.Error("webhook: panic recovered in delivery processing",
				"endpoint", ep.Name,
				"delivery_id", deliveryID,
				"event_type", eventType,
				"execution_key", workCtx.Key.String(),
				"worktree", workCtx.Worktree,
				"panic", fmt.Sprint(r),
			)
			s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary(fmt.Sprintf("panic: %v", r), maxErrorSummaryRunes, ep.Secret), workCtx)
		}
	}()

	if allowed, reason := workflowAllows(ep.Workflow, envelope); !allowed {
		s.markSkipped(id, ep, envelope, reason)
		return
	}

	if key.IsZero() && eventType == "issue_comment" && isWebhookWorkflow(ep.Workflow) {
		ref, err := extractIssueCommentPullRequestRef(body)
		if err != nil {
			s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary("pull request reference invalid: "+err.Error(), maxErrorSummaryRunes, ep.Secret), workCtx)
			return
		}
		if s.pullRequestResolver == nil {
			s.failDelivery(ep, id, deliveryID, eventType, envelope, "pull request resolver required for issue comments", workCtx)
			return
		}
		key, err = s.pullRequestResolver.ResolvePullRequest(context.Background(), ref)
		if err != nil {
			s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary("pull request inspection failed: "+err.Error(), maxErrorSummaryRunes, ep.Secret), workCtx)
			return
		}
		if !strings.EqualFold(key.Repository, ref.Repository) || !isValidRepoFullName(key.HeadRepository) || !isSafeBranch(key.Branch) {
			s.failDelivery(ep, id, deliveryID, eventType, envelope, "pull request inspection returned an invalid execution key", workCtx)
			return
		}
		workCtx.Key = key
		workCtx.SessionKey = key.String()
	}

	mu := s.sessionLock(ep, key)
	mu.Lock()
	defer mu.Unlock()

	if !key.IsZero() {
		if s.worktreeResolver == nil {
			slog.Warn("webhook: worktree resolver missing for project key",
				"endpoint", ep.Name,
				"delivery_id", deliveryID,
				"execution_key", key.String(),
			)
			s.failDelivery(ep, id, deliveryID, eventType, envelope, "worktree resolver required for project execution key", workCtx)
			return
		}

		worktree, err := s.worktreeResolver.ResolveWorktree(context.Background(), key)
		if err != nil {
			slog.Warn("webhook: worktree resolution failed", "endpoint", ep.Name, "delivery_id", deliveryID, "execution_key", key.String(), "error", err)
			if errors.Is(err, ErrWorktreeConflict) {
				s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary("worktree conflict: "+err.Error(), maxErrorSummaryRunes, ep.Secret), workCtx)
			} else {
				s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary("worktree resolution failed: "+err.Error(), maxErrorSummaryRunes, ep.Secret), workCtx)
			}
			return
		}
		workCtx.Worktree = worktree
	}

	if isWebhookWorkflow(ep.Workflow) {
		if key.IsZero() || workCtx.Worktree == "" {
			s.failDelivery(ep, id, deliveryID, eventType, envelope, "webhook permission policy requires an exact PR worktree", workCtx)
			return
		}
		workCtx.Repository = key.Repository
		workCtx.PRNumber = stringValue(envelope["pr_number"])
		workCtx.ProjectDocsRoot = projectDocsRoot(workCtx.Worktree)
		ruleset, policyErr := BuildPermissionPolicy(PermissionPolicyInput{
			Workflow:        ep.Workflow,
			Repository:      workCtx.Repository,
			PRNumber:        workCtx.PRNumber,
			Worktree:        workCtx.Worktree,
			ProjectDocsRoot: workCtx.ProjectDocsRoot,
			Branch:          key.Branch,
		})
		if policyErr != nil {
			s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary("webhook permission policy failed: "+policyErr.Error(), maxErrorSummaryRunes, ep.Secret), workCtx)
			return
		}
		workCtx.PermissionRuleset = ruleset
	}

	if s.channels != nil {
		ch, err := s.channels.Get(context.Background(), ep.Platform, ep.ChannelID)
		if err != nil {
			slog.Error("webhook: channel repo get failed", "endpoint", ep.Name, "platform", ep.Platform, "channel_id", ep.ChannelID, "error", err)
			s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary("channel configuration error: "+err.Error(), maxErrorSummaryRunes, ep.Secret), workCtx)
			return
		}
		if ch != nil && strings.TrimSpace(ch.Model) != "" {
			ref, err := relay.ParseModelRef(strings.TrimSpace(ch.Model))
			if err != nil {
				slog.Error("webhook: malformed channel model", "endpoint", ep.Name, "platform", ep.Platform, "channel_id", ep.ChannelID)
				s.failDelivery(ep, id, deliveryID, eventType, envelope, "invalid channel model", workCtx)
				return
			}
			workCtx.Model = &ref
			workCtx.ModelSource = "channel"
			envelope["model"] = relay.FormatModelRef(ref)
			envelope["model_source"] = "channel"
		} else {
			workCtx.Model = nil
			workCtx.ModelSource = "fallback"
			envelope["model"] = "agent/session default"
			envelope["model_source"] = "fallback"
		}
	} else {
		workCtx.Model = nil
		workCtx.ModelSource = "fallback"
		envelope["model"] = "agent/session default"
		envelope["model_source"] = "fallback"
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		payload = map[string]any{}
	}

	tmplData := map[string]any{
		"payload":       payload,
		"json":          string(body),
		"execution_key": key.String(),
		"worktree":      workCtx.Worktree,
		"webhook":       envelope,
	}

	rendered, err := renderTemplate(ep.Prompt, tmplData)
	if err != nil {
		slog.Error("webhook: template render failed", "endpoint", ep.Name, "delivery_id", deliveryID, "payload_bytes", len(body), "error", err)
		s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary("template render failed: "+err.Error(), maxErrorSummaryRunes, ep.Secret), workCtx)
		return
	}

	rendered = strings.ReplaceAll(rendered, "</untrusted_payload>", "&lt;/untrusted_payload&gt;")
	wrapped := fmt.Sprintf("<untrusted_payload>\n%s\n</untrusted_payload>", rendered)

	ctx, cancel := context.WithTimeout(context.Background(), s.processingTimeout)
	defer cancel()
	err = s.executor(ctx, ep.Platform, ep.ChannelID, wrapped, workCtx)

	switch {
	case err == nil:
		ok, tErr := s.deliveries.Transition(context.Background(), id, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusCompleted, "")
		if tErr != nil {
			slog.Error("webhook: completed transition failed", "endpoint", ep.Name, "delivery_id", deliveryID, "error", tErr)
		} else if ok {
			s.emitAudit(context.Background(), ep, envelope, "COMPLETED", "")
		}
		slog.Info("webhook: delivery completed",
			"endpoint", ep.Name,
			"delivery_id", deliveryID,
			"event_type", eventType,
			"execution_key", workCtx.Key.String(),
			"worktree", workCtx.Worktree,
		)
	case errors.Is(err, context.DeadlineExceeded), ctx.Err() == context.DeadlineExceeded:
		s.failDelivery(ep, id, deliveryID, eventType, envelope, "timed out after "+s.processingTimeout.String(), workCtx)
	default:
		s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary(err.Error(), maxErrorSummaryRunes, ep.Secret), workCtx)
	}
}

func (s *Server) failDelivery(ep config.EndpointConfig, id int64, deliveryID, eventType string, envelope WebhookEnvelope, summary string, workCtx WebhookWorkContext) {
	ok, err := s.deliveries.Transition(context.Background(), id, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusFailed, summary)
	if err != nil {
		slog.Error("webhook: failed transition", "endpoint", ep.Name, "delivery_id", deliveryID, "error", err)
	} else if ok {
		s.emitAudit(context.Background(), ep, envelope, "FAILED", summary)
	}
	slog.Warn("webhook: delivery failed",
		"endpoint", ep.Name,
		"delivery_id", deliveryID,
		"event_type", eventType,
		"execution_key", workCtx.Key.String(),
		"worktree", workCtx.Worktree,
		"error_summary", summary,
	)
}

func (s *Server) shouldSkip(ep config.EndpointConfig, eventType string) bool {
	if eventType == "" || len(ep.SkipEvents) == 0 {
		return false
	}
	for _, e := range ep.SkipEvents {
		if e == eventType {
			return true
		}
	}
	return false
}

func isTerminal(status store.WebhookStatus) bool {
	switch status {
	case store.WebhookStatusCompleted, store.WebhookStatusSkipped, store.WebhookStatusFailed:
		return true
	}
	return false
}

func providerDeliveryID(r *http.Request, payloadHash string) string {
	for _, header := range []string{"X-GitHub-Delivery", "X-Webhook-Delivery"} {
		if id := r.Header.Get(header); id != "" {
			return clipRunes(id, maxDeliveryIDRunes)
		}
	}
	return fmt.Sprintf("gen-%d-%s", time.Now().UnixNano(), payloadHash[:12])
}

func providerEventType(r *http.Request, body []byte) string {
	if typ := r.Header.Get("X-GitHub-Event"); typ != "" {
		return clipRunes(typ, maxEventTypeRunes)
	}
	var payload struct {
		EventType string `json:"event_type"`
		Type      string `json:"type"`
		Action    string `json:"action"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return ""
	}
	for _, v := range []string{payload.EventType, payload.Type, payload.Action} {
		if v != "" {
			return clipRunes(v, maxEventTypeRunes)
		}
	}
	return ""
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func clipRunes(s string, max int) string {
	if max <= 0 || utf8.RuneCountInString(s) <= max {
		return s
	}
	runes := []rune(s)
	return string(runes[:max])
}

func redactSummary(s string, max int, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, "[redacted]")
		}
	}
	s = strings.Join(strings.Fields(s), " ")
	if max <= 0 {
		max = maxErrorSummaryRunes
	}
	return clipRunes(s, max)
}

func renderTemplate(prompt string, data any) (string, error) {
	var buf bytes.Buffer
	if strings.Contains(prompt, "{{") {
		tmpl, err := template.New("webhook").Option("missingkey=error").Parse(prompt)
		if err != nil {
			return "", fmt.Errorf("webhook: parse template: %w", err)
		}
		if err := tmpl.Execute(&buf, data); err != nil {
			return "", fmt.Errorf("webhook: execute template: %w", err)
		}
	} else {
		buf.WriteString(prompt)
	}
	if strings.Contains(buf.String(), "<no value>") {
		return "", errors.New("webhook: template rendered an unresolved value")
	}
	return buf.String(), nil
}
