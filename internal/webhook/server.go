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
	"strconv"
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
	maxQueuedPerKey                  = 8

	maxDeliveryIDRunes   = 128
	maxEventTypeRunes    = 64
	maxErrorSummaryRunes = 240

	retentionKeep     = 500
	retentionAge      = 30 * 24 * time.Hour
	pruneInterval     = 10 * time.Minute
	processingTimeout = 30 * time.Minute
	claimGrace        = processingTimeout + 2*time.Minute
	dispatcherIdleTTL = time.Hour
	retryAfterSeconds = 30

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
	bind                  string
	bindAddr              string
	endpoints             map[string]config.EndpointConfig
	executor              Executor
	notifier              Notifier
	deliveries            DeliveryStore
	channels              ChannelStore
	workspaceResolver     WorkspaceResolver
	httpSrv               *http.Server
	listener              net.Listener
	eventSlots            chan struct{}
	dispatchMu            sync.Mutex
	dispatchers           map[string]*dispatcher
	dispatchWG            sync.WaitGroup
	shutdownCtx           context.Context
	shutdownCancel        context.CancelFunc
	processingTimeout     time.Duration
	dispatcherIdleTTL     time.Duration
	workspaceRetryBackoff []time.Duration
	workspaceRetrySleep   func(ctx context.Context, d time.Duration) bool
	pruneMu               sync.Mutex
	lastPrune             time.Time
	pruneInterval         time.Duration
	readHeaderTimeout     time.Duration
	readTimeout           time.Duration
	writeTimeout          time.Duration
	idleTimeout           time.Duration
	listening             atomic.Bool
}

func New(cfg config.WebhookConfig, executor Executor, deliveries DeliveryStore) *Server {
	endpoints := make(map[string]config.EndpointConfig)
	for _, ep := range cfg.Endpoints {
		ep.Auth = strings.TrimSpace(strings.ToLower(ep.Auth))
		ep.Workflow = strings.TrimSpace(strings.ToLower(ep.Workflow))
		endpoints[ep.Path] = ep
	}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	return &Server{
		bind:              cfg.Bind,
		endpoints:         endpoints,
		executor:          executor,
		deliveries:        deliveries,
		eventSlots:        make(chan struct{}, maxConcurrentWebhookEvents),
		dispatchers:       make(map[string]*dispatcher),
		shutdownCtx:       shutdownCtx,
		shutdownCancel:    shutdownCancel,
		processingTimeout: processingTimeout,
		dispatcherIdleTTL: dispatcherIdleTTL,
		workspaceRetryBackoff: []time.Duration{
			30 * time.Second,
			60 * time.Second,
			120 * time.Second,
		},
		workspaceRetrySleep: sleepWithContext,
		pruneInterval:       pruneInterval,
		readHeaderTimeout:   readHeaderTimeout,
		readTimeout:         readTimeout,
		writeTimeout:        writeTimeout,
		idleTimeout:         idleTimeout,
	}
}

func (s *Server) SetWorkspaceResolver(r WorkspaceResolver) {
	s.workspaceResolver = r
}

func (s *Server) SetNotifier(n Notifier) {
	if n == nil {
		s.notifier = nil
		return
	}
	s.notifier = func(ctx context.Context, platform, channelID, text string) error {
		return n(ctx, platform, channelID, FormatWebhookMessage(text))
	}
}

func (s *Server) SetChannelStore(c ChannelStore) {
	s.channels = c
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
	if reaper, ok := s.workspaceResolver.(interface {
		ReapExpiredWorkspaces(ctx context.Context) int
	}); ok {
		if reaped := reaper.ReapExpiredWorkspaces(context.Background()); reaped > 0 {
			slog.Info("webhook: reaped expired isolated workspaces", "reaped", reaped)
		}
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
		s.shutdownCancel()
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
	s.listening.Store(false)
	var err error
	if s.httpSrv != nil {
		err = s.httpSrv.Shutdown(ctx)
	}
	s.shutdownCancel()
	done := make(chan struct{})
	go func() {
		s.dispatchWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		slog.Warn("webhook: shutdown drain timed out", "error", ctx.Err())
	}
	return err
}

func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	requestPath := r.URL.Path
	if strings.HasPrefix(requestPath, "/occa/") {
		requestPath = strings.TrimPrefix(requestPath, "/occa")
	}
	ep, ok := s.endpoints[requestPath]
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

	payloadHash := sha256Hex(body)
	eventType := providerEventType(r, body)
	deliveryID := providerDeliveryID(r, payloadHash)

	item := dispatchItem{
		ep:          ep,
		body:        body,
		deliveryID:  deliveryID,
		eventType:   eventType,
		payloadHash: payloadHash,
	}
	d, ok := s.enqueue(ep, ExtractExecutionKey(body), item)
	if !ok {
		slog.Warn("webhook: queue full",
			"endpoint", ep.Name,
			"delivery_id", deliveryID,
			"event_type", eventType,
		)
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
		return
	}

	slog.Info("webhook: delivery enqueued", "endpoint", ep.Name, "delivery_id", deliveryID, "event_type", eventType, "lock", d.key)
	w.WriteHeader(http.StatusOK)
}

// enqueue atomically resolves the live dispatcher for a lock identity and
// appends the item to its queue. Lookup, retirement checks, and the channel
// send all happen under dispatchMu — retirement also flips exiting under that
// lock before any drain — so an accepted send is always ordered before
// retirement completes and can never land in a channel nobody will read. A
// rejected send (queue full) leaves the dispatcher untouched.
func (s *Server) enqueue(ep config.EndpointConfig, key WebhookExecutionKey, item dispatchItem) (*dispatcher, bool) {
	var lockKey string
	if key.IsZero() {
		lockKey = "endpoint:" + ep.Platform + "|" + ep.ChannelID
	} else {
		lockKey = "key:" + key.String()
	}
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	d, ok := s.dispatchers[lockKey]
	if !ok || d.exiting {
		d = &dispatcher{server: s, key: lockKey, ch: make(chan dispatchItem, maxQueuedPerKey)}
		s.dispatchers[lockKey] = d
		s.dispatchWG.Add(1)
		go d.run(s.shutdownCtx)
	}
	select {
	case d.ch <- item:
		return d, true
	default:
		return nil, false
	}
}

func (s *Server) dispatcherCount() int {
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	return len(s.dispatchers)
}

func (s *Server) tryAcquireEvent() bool {
	select {
	case s.eventSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) acquireEvent(ctx context.Context) bool {
	select {
	case s.eventSlots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Server) releaseEvent() {
	<-s.eventSlots
}

type dispatchItem struct {
	ep          config.EndpointConfig
	body        []byte
	deliveryID  string
	eventType   string
	payloadHash string
}

type dispatcher struct {
	server  *Server
	key     string
	ch      chan dispatchItem
	exiting bool
}

// run serially executes queued deliveries until the queue stays idle for the
// eviction TTL or the shutdown context is cancelled. Idle retirement only
// succeeds while the queue is provably empty under dispatchMu, so no accepted
// send is ever stranded; on shutdown every leftover is recorded as failed.
func (d *dispatcher) run(ctx context.Context) {
	defer d.server.dispatchWG.Done()
	timer := time.NewTimer(d.server.dispatcherIdleTTL)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			d.retire("shutting down")
			return
		case item := <-d.ch:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d.server.dispatcherIdleTTL)
			if ctx.Err() != nil {
				d.server.abandon(item, "shutting down")
				continue
			}
			d.handle(item)
		case <-timer.C:
			if !d.tryRetireIdle() {
				timer.Reset(d.server.dispatcherIdleTTL)
				continue
			}
			return
		}
	}
}

// tryRetireIdle retires the dispatcher only when its queue is empty under the
// registry lock. Because enqueues take the same lock, a successful retirement
// freezes an empty channel that no future send can reach; a failed attempt
// means activity raced the timer and the dispatcher stays live.
func (d *dispatcher) tryRetireIdle() bool {
	s := d.server
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	if len(d.ch) > 0 {
		return false
	}
	d.exiting = true
	if current, ok := s.dispatchers[d.key]; ok && current == d {
		delete(s.dispatchers, d.key)
	}
	return true
}

// retire marks the dispatcher exiting under the registry lock — after this,
// enqueues build a fresh dispatcher — and records every queued item as failed.
// Leftovers are never executed here so a retiring dispatcher cannot overlap a
// replacement on the same identity.
func (d *dispatcher) retire(reason string) {
	s := d.server
	s.dispatchMu.Lock()
	d.exiting = true
	if current, ok := s.dispatchers[d.key]; ok && current == d {
		delete(s.dispatchers, d.key)
	}
	s.dispatchMu.Unlock()
	for {
		select {
		case item := <-d.ch:
			s.abandon(item, reason)
		default:
			return
		}
	}
}

func (d *dispatcher) handle(item dispatchItem) {
	s := d.server
	receipt := s.beginExecution(item)
	if receipt == nil {
		return
	}
	for attempt := 0; ; attempt++ {
		if !s.acquireEvent(s.shutdownCtx) {
			s.failAbandonedReceipt(receipt, "shutting down")
			return
		}
		lease, werr := s.resolveWorkspace(item)
		if werr == nil {
			s.executeDelivery(item.ep, item.body, receipt.ID, item.deliveryID, item.eventType, receipt.Attempt, lease)
			s.releaseEvent()
			s.pruneIfDue()
			return
		}
		s.releaseEvent()
		if IsWorkspaceRetryable(werr) && attempt < len(s.workspaceRetryBackoff) {
			slog.Warn("webhook: workspace busy, retrying",
				"endpoint", item.ep.Name,
				"delivery_id", item.deliveryID,
				"attempt", attempt+1,
				"error", werr,
			)
			if !s.workspaceRetrySleep(s.shutdownCtx, s.workspaceRetryBackoff[attempt]) {
				s.failAbandonedReceipt(receipt, "shutting down")
				return
			}
			continue
		}
		s.failWorkspace(item, receipt, werr)
		return
	}
}

func (s *Server) resolveWorkspace(item dispatchItem) (*WorkspaceLease, error) {
	ep := item.ep
	if ep.Workspace.Type != config.WorkspaceTypeGit {
		return nil, nil
	}
	if s.workspaceResolver == nil {
		return nil, fmt.Errorf("%w: workspace resolver unavailable", ErrWorkspaceUnavailable)
	}
	req := WorkspaceRequest{
		Repository: ep.Repository,
		Path:       ep.Workspace.Path,
		Mode:       ep.Workspace.Mode,
		Key:        ExtractExecutionKey(item.body),
		DeliveryID: item.deliveryID,
	}
	lease, err := s.workspaceResolver.ResolveWorkspace(context.Background(), req)
	if err != nil {
		slog.Warn("webhook: workspace resolution failed",
			"endpoint", ep.Name,
			"delivery_id", item.deliveryID,
			"mode", ep.Workspace.Mode,
			"error", err,
		)
		return nil, err
	}
	return lease, nil
}

func (s *Server) failWorkspace(item dispatchItem, receipt *store.WebhookDelivery, err error) {
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, false, "")
	workCtx := WebhookWorkContext{Key: ExtractExecutionKey(item.body)}
	summary := redactSummary(err.Error(), maxErrorSummaryRunes, item.ep.Secret)
	s.failDelivery(item.ep, receipt.ID, item.deliveryID, item.eventType, envelope, summary, workCtx)
}

// beginExecution claims a delivery at the moment it reaches the head of its
// queue: the receipt row is created here and moved to processing with a fresh
// updated_at, so the stale-claim grace window always measures active execution
// time rather than queue wait. Duplicate deliveries resolve against the stored
// receipt exactly as before; only the winner proceeds.
func (s *Server) beginExecution(item dispatchItem) *store.WebhookDelivery {
	ctx := context.Background()
	ep := item.ep

	created, err := s.deliveries.Create(ctx, store.WebhookDelivery{
		Endpoint:    ep.Name,
		DeliveryID:  item.deliveryID,
		EventType:   item.eventType,
		PayloadHash: item.payloadHash,
		Attempt:     1,
	})
	if err != nil {
		slog.Error("webhook: receipt claim failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", err)
		return nil
	}

	receipt, err := s.deliveries.Get(ctx, ep.Name, item.deliveryID)
	if err != nil {
		slog.Error("webhook: receipt load failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", err)
		return nil
	}
	if receipt == nil {
		slog.Error("webhook: receipt missing after claim", "endpoint", ep.Name, "delivery_id", item.deliveryID)
		return nil
	}

	staleClaimed := false
	if !created {
		switch {
		case isTerminal(receipt.Status):
			s.logDuplicate(item, receipt.Status)
			return nil
		case receipt.Status == store.WebhookStatusReceived:
		case time.Now().Unix()-receipt.UpdatedAt < int64(claimGrace.Seconds()):
			s.logDuplicate(item, receipt.Status)
			return nil
		default:
			ok, tErr := s.deliveries.ClaimStale(ctx, receipt.ID, time.Now().Add(-claimGrace).Unix())
			if tErr != nil {
				slog.Error("webhook: stale claim failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", tErr)
				return nil
			}
			if !ok {
				s.logDuplicate(item, receipt.Status)
				return nil
			}
			staleClaimed = true
		}
	}

	if s.shouldSkip(ep, item.eventType) {
		reason := "configured event skip"
		envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, true, reason)
		summary := redactAuditSummary(formatAuditSummary(envelope, ep.Workflow, "SKIP", reason), ep.Secret)
		ok, tErr := s.deliveries.Transition(ctx, receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted, store.WebhookStatusProcessing}, store.WebhookStatusSkipped, summary)
		if tErr != nil {
			slog.Error("webhook: skip transition failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", tErr)
			return nil
		}
		if !ok {
			slog.Warn("webhook: skip transition lost race", "endpoint", ep.Name, "delivery_id", item.deliveryID, "observed_status", receipt.Status)
			return nil
		}
		slog.Info("webhook: delivery skipped", "endpoint", ep.Name, "delivery_id", item.deliveryID, "event_type", item.eventType)
		s.emitAudit(ctx, ep, envelope, "SKIP", reason)
		return nil
	}

	if !staleClaimed {
		ok, tErr := s.deliveries.Transition(ctx, receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusProcessing, "")
		if tErr != nil {
			slog.Error("webhook: processing claim failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", tErr)
			return nil
		}
		if !ok {
			slog.Warn("webhook: processing claim lost race", "endpoint", ep.Name, "delivery_id", item.deliveryID, "observed_status", receipt.Status)
			return nil
		}
	}

	slog.Info("webhook: delivery accepted", "endpoint", ep.Name, "delivery_id", item.deliveryID, "event_type", item.eventType, "status", store.WebhookStatusProcessing)
	return receipt
}

func (s *Server) logDuplicate(item dispatchItem, observed store.WebhookStatus) {
	slog.Info("webhook: duplicate delivery",
		"endpoint", item.ep.Name,
		"delivery_id", item.deliveryID,
		"event_type", item.eventType,
		"observed_status", observed,
	)
}

// abandon records a delivery that was accepted but will never execute — the
// process is shutting down with it still queued. The receipt is created only
// now so overload rejections and queue waits leave no rows behind.
func (s *Server) abandon(item dispatchItem, reason string) {
	ctx := context.Background()
	created, err := s.deliveries.Create(ctx, store.WebhookDelivery{
		Endpoint:    item.ep.Name,
		DeliveryID:  item.deliveryID,
		EventType:   item.eventType,
		PayloadHash: item.payloadHash,
		Attempt:     1,
	})
	if err != nil {
		slog.Error("webhook: abandoned delivery receipt failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", err)
		return
	}
	if !created {
		return
	}
	receipt, err := s.deliveries.Get(ctx, item.ep.Name, item.deliveryID)
	if err != nil || receipt == nil {
		slog.Error("webhook: abandoned delivery receipt missing", "endpoint", item.ep.Name, "delivery_id", item.deliveryID)
		return
	}
	s.failAbandonedReceipt(receipt, reason)
}

func (s *Server) failAbandonedReceipt(receipt *store.WebhookDelivery, reason string) {
	ok, err := s.deliveries.Transition(context.Background(), receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived, store.WebhookStatusAccepted, store.WebhookStatusProcessing}, store.WebhookStatusFailed, reason)
	if err != nil {
		slog.Error("webhook: abandoned delivery transition failed", "endpoint", receipt.Endpoint, "delivery_id", receipt.DeliveryID, "error", err)
		return
	}
	if ok {
		slog.Warn("webhook: delivery abandoned", "endpoint", receipt.Endpoint, "delivery_id", receipt.DeliveryID, "reason", reason)
	}
}

// executeDelivery runs the claimed delivery. It must be called with the event
// slot held and only after beginExecution succeeded for this receipt. The
// lease, when present, is released after the terminal transition so workspace
// cleanup failure cannot change the delivery outcome.
func (s *Server) executeDelivery(ep config.EndpointConfig, body []byte, id int64, deliveryID, eventType string, attempt int, lease *WorkspaceLease) {
	key := ExtractExecutionKey(body)

	var workCtx WebhookWorkContext
	workCtx.Key = key
	workCtx.DeliveryID = deliveryID
	workCtx.Attempt = attempt
	if lease != nil {
		workCtx.Worktree = lease.Path
		defer func() {
			if rErr := lease.Release(context.Background()); rErr != nil {
				slog.Warn("webhook: workspace cleanup failed", "endpoint", ep.Name, "delivery_id", deliveryID, "workspace", lease.Path, "error", rErr)
			}
		}()
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
			"attempt", attempt,
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

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
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
