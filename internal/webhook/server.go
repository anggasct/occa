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

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

const (
	maxDeliveryIDRunes   = 128
	maxEventTypeRunes    = 64
	maxErrorSummaryRunes = 240
)

// DeliveryStore is the durable receipt backend the server drives. The full
// repo lives in internal/store; the server only needs the transitions below.
type DeliveryStore interface {
	Create(ctx context.Context, d store.WebhookDelivery) (bool, error)
	Ensure(ctx context.Context, d store.WebhookDelivery) (bool, error)
	Get(ctx context.Context, endpoint, deliveryID string) (*store.WebhookDelivery, error)
	GetByID(ctx context.Context, id int64) (*store.WebhookDelivery, error)
	Transition(ctx context.Context, id int64, from []store.WebhookStatus, to store.WebhookStatus, summary string) (bool, error)
	BumpAttempt(ctx context.Context, id int64) error
	ClaimStale(ctx context.Context, id, cutoff int64) (bool, error)
	Prune(ctx context.Context, cutoff int64, keep int) (int, error)
	FailStale(ctx context.Context, cutoff int64, summary string) (int, error)
	SetReviewKey(ctx context.Context, id int64, reviewKey string) error
	SetPRKey(ctx context.Context, id int64, prKey string) error
	Reopen(ctx context.Context, id int64, from []store.WebhookStatus) (bool, error)
	LatestForPR(ctx context.Context, prKey string) (*store.WebhookDelivery, error)
	FindReviewDuplicate(ctx context.Context, endpoint, reviewKey string, excludeID, cutoff int64) (*store.WebhookDelivery, error)
	CountReviewDeliveries(ctx context.Context, endpoint, reviewKeyPrefix string, cutoff int64) (int, error)
}

type ChannelStore interface {
	Get(ctx context.Context, platform, channelID string) (*store.Channel, error)
}

type TakeoverStore interface {
	SetActive(ctx context.Context, platform, channelID, threadID, userID, sessionID string, agentPID int) error
	Deactivate(ctx context.Context, platform, channelID, threadID, userID string) error
	MarkTakeoverEligible(ctx context.Context, platform, channelID, threadID, sessionID string, agentPID int, seed string) error
	ClearTakeoverEligible(ctx context.Context, platform, channelID, threadID string) error
	LinkThreadRoot(ctx context.Context, platform, channelID, threadID, messageID, channel, card string) error
}

type Executor func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error

type Notifier func(ctx context.Context, platform, channelID, text string) error

type Editor func(ctx context.Context, platform, channelID, messageID, text string) error

type Sender func(ctx context.Context, platform, channelID, text string) (string, error)

type ThreadStarter func(ctx context.Context, platform, channelID, messageID, name string) (string, error)

type SenderWithButtons func(ctx context.Context, platform, channelID, text string, buttons []channel.Button) (string, error)

type EditorWithButtons func(ctx context.Context, platform, channelID, messageID, text string, buttons []channel.Button) error

type GuildResolver func(ctx context.Context, platform, channelID string) string

// MentionResolver maps a configured escalation name to a platform user id.
type MentionResolver func(ctx context.Context, platform, name string) (string, bool)

// MentionNotifier posts a channel message that may ping a single user.
type MentionNotifier func(ctx context.Context, platform, channelID, text, mentionUserID string) error

// MergeGateChecker reports whether the merge gate would pass for a PR right
// now. A nil checker falls back to the queue-state gate.
type MergeGateChecker func(ctx context.Context, prKey string) bool

// Pinner pins a message in its channel. A nil pinner leaves the message
// unpinned.
type Pinner func(ctx context.Context, platform, channelID, messageID string) error

type Server struct {
	bind                  string
	bindAddr              string
	endpoints             map[string]config.EndpointConfig
	policy                admissionPolicy
	verdicts              map[string][]string
	executor              Executor
	notifier              Notifier
	editor                Editor
	sender                Sender
	senderButtons         SenderWithButtons
	editorButtons         EditorWithButtons
	rawSenderButtons      SenderWithButtons
	rawEditorButtons      EditorWithButtons
	rawNotifier           Notifier
	rawSender             Sender
	rawEditor             Editor
	threadStarter         ThreadStarter
	guildResolver         GuildResolver
	pinner                Pinner
	deliveries            DeliveryStore
	prQueues              store.PRQueueRepo
	escalationMention     string
	sessionAborter        func(ctx context.Context, sessionID, workdir string) error
	sessionLiveness       SessionLiveness
	prStateResolver       PRStateResolver
	activeRunsMu          sync.Mutex
	activeRuns            map[string]*activeRun
	operatorAuthorizer    func(platform, userID string) bool
	promptDir             string
	retryBackoffs         map[failureClass][]time.Duration
	retryBodyBuilder      func(ctx context.Context, repo, number, kind string) (body []byte, eventType string, err error)
	prAuthorResolver      func(ctx context.Context, repo, number string) (string, error)
	channels              ChannelStore
	sessions              TakeoverStore
	workspaceResolver     WorkspaceResolver
	httpSrv               *http.Server
	listener              net.Listener
	eventSlots            chan struct{}
	dispatchMu            sync.Mutex
	dispatchers           map[string]*dispatcher
	dispatchWG            sync.WaitGroup
	dispatchCreateMu      sync.Mutex
	shutdownCtx           context.Context
	shutdownCancel        context.CancelFunc
	maxBodySize           int64
	maxQueuedPerKey       int
	stateShare            float64
	overflowSlots         int
	replays               store.WebhookReplayRepo
	replayInterval        time.Duration
	replayBackoff         []time.Duration
	replayMaxAttempts     int
	replayTTL             time.Duration
	replayStoreMax        int
	replayStoreMaxBytes   int64
	replayChecker         ReplayChecker
	ackBudget             time.Duration
	spoolDir              string
	reconcileInterval     time.Duration
	reconcileFloor        time.Duration
	reconcileMaxPRs       int
	reconcileFacts        ReconcileFactsResolver
	reconcileMu           sync.Mutex
	reconcileLast         map[string]time.Time
	reconcileRunning      atomic.Bool
	retryAfter            time.Duration
	retentionAge          time.Duration
	retentionKeep         int
	claimGrace            time.Duration
	claimGraceSeconds     int64
	processingTimeout     time.Duration
	stallFreshness        time.Duration
	dispatcherIdleTTL     time.Duration
	dispatcherStuckAfter  time.Duration
	watchdogInterval      time.Duration
	workspaceRetryBackoff []time.Duration
	workspaceRetrySleep   func(ctx context.Context, d time.Duration) bool
	isolatedWorkspaceTTL  time.Duration
	pruneMu               sync.Mutex
	lastPrune             time.Time
	pruneInterval         time.Duration
	readHeaderTimeout     time.Duration
	readTimeout           time.Duration
	writeTimeout          time.Duration
	idleTimeout           time.Duration
	listening             atomic.Bool
	reviewDedupeWindow    time.Duration
	reviewDedupeNow       func() time.Time
	triggerLimits         map[string]config.EndpointLimits
	prHeadResolver        PRHeadResolver
	tasks                 *TaskThreadRegistry
	dispatchEnabled       bool
	dispatchPath          string
	dispatchToken         string
	dispatchTasks         *DispatchStore
	fallbackAfter         time.Duration
	branchChecker         BranchPRChecker
	branchExists          BranchExistsChecker
	statusIcons           map[string]string
	statusColors          map[string]int
	endpointsByName       map[string]config.EndpointConfig
	mentionResolver       MentionResolver
	mentionNotifier       MentionNotifier
	mergeGate             MergeGateChecker
	escalationMu          sync.Mutex
	escalated             map[string]bool
}

func New(cfg config.WebhookConfig, executor Executor, deliveries DeliveryStore) *Server {
	endpoints := make(map[string]config.EndpointConfig, len(cfg.Endpoints))
	endpointsByName := make(map[string]config.EndpointConfig, len(cfg.Endpoints))
	triggerLimits := make(map[string]config.EndpointLimits, len(cfg.Endpoints))
	for _, ep := range cfg.Endpoints {
		ep.Auth = strings.TrimSpace(strings.ToLower(ep.Auth))
		ep.Workflow = strings.TrimSpace(strings.ToLower(ep.Workflow))
		endpoints[ep.Path] = ep
		if name := strings.TrimSpace(ep.Name); name != "" {
			endpointsByName[name] = ep
		}
		if ep.Limits != nil {
			triggerLimits[ep.Name] = *ep.Limits
		}
	}
	rt := cfg.Runtime
	policy := policyFromConfig(cfg.Policy)
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	srv := &Server{
		bind:                  cfg.Bind,
		endpoints:             endpoints,
		endpointsByName:       endpointsByName,
		policy:                policy,
		verdicts:              policy.verdicts,
		executor:              executor,
		deliveries:            deliveries,
		dispatchers:           make(map[string]*dispatcher),
		activeRuns:            make(map[string]*activeRun),
		shutdownCtx:           shutdownCtx,
		shutdownCancel:        shutdownCancel,
		maxBodySize:           int64(rt.MaxBodySize),
		maxQueuedPerKey:       rt.MaxQueuedPerKey,
		stateShare:            rt.EffectiveStateLaneShare(),
		overflowSlots:         rt.EffectivePriorityOverflow(),
		replayInterval:        rt.EffectiveReplayInterval(),
		replayBackoff:         rt.EffectiveReplayBackoff(),
		replayMaxAttempts:     rt.EffectiveReplayMaxAttempts(),
		replayTTL:             rt.EffectiveReplayTTL(),
		replayStoreMax:        rt.EffectiveReplayStoreMax(),
		replayStoreMaxBytes:   int64(rt.EffectiveReplayStoreMaxBytes()),
		ackBudget:             rt.EffectiveAckBudget(),
		spoolDir:              strings.TrimSpace(rt.SpoolDir),
		reconcileInterval:     rt.EffectiveReconcileInterval(),
		reconcileFloor:        rt.EffectiveReconcileFloor(),
		reconcileMaxPRs:       rt.EffectiveReconcileMaxPRs(),
		reconcileLast:         make(map[string]time.Time),
		retryAfter:            rt.RetryAfter,
		retentionAge:          rt.Retention,
		retentionKeep:         rt.RetentionKeep,
		claimGrace:            rt.ClaimGrace,
		claimGraceSeconds:     int64(rt.ClaimGrace / time.Second),
		processingTimeout:     rt.ProcessingTimeout,
		dispatcherIdleTTL:     rt.DispatcherIdleTTL,
		dispatcherStuckAfter:  rt.DispatcherStuckAfter,
		watchdogInterval:      rt.WatchdogInterval,
		workspaceRetryBackoff: rt.WorkspaceRetryBackoff,
		workspaceRetrySleep:   sleepWithContext,
		isolatedWorkspaceTTL:  rt.IsolatedWorkspaceTTL,
		pruneInterval:         rt.PruneInterval,
		readHeaderTimeout:     rt.HTTPReadHeaderTimeout,
		readTimeout:           rt.HTTPReadTimeout,
		writeTimeout:          rt.HTTPWriteTimeout,
		idleTimeout:           rt.HTTPIdleTimeout,
		reviewDedupeWindow:    rt.ReviewDedupeWindow,
		triggerLimits:         triggerLimits,
		prHeadResolver:        &ghPRHeadResolver{timeout: rt.ProcessingTimeout},
		escalationMention:     strings.TrimSpace(cfg.Defaults.EscalationMention),
	}
	srv.eventSlots = make(chan struct{}, rt.MaxConcurrentEvents)
	srv.tasks = NewTaskThreadRegistry()
	srv.tasks.SetStatusConfig(cfg.StatusIcons, cfg.StatusColors)
	srv.statusIcons = cfg.StatusIcons
	srv.statusColors = cfg.StatusColors
	srv.dispatchEnabled = cfg.Dispatch.Enabled
	srv.dispatchPath = strings.TrimSpace(cfg.Dispatch.Path)
	if srv.dispatchPath == "" {
		srv.dispatchPath = defaultDispatchPath
	}
	srv.dispatchToken = strings.TrimSpace(cfg.Dispatch.Token)
	srv.dispatchTasks = NewDispatchStore()
	srv.escalated = make(map[string]bool)
	srv.fallbackAfter = cfg.FallbackPollAfter
	srv.branchChecker = ghBranchPRChecker
	srv.branchExists = ghBranchExistsChecker
	return srv
}

func (s *Server) reviewDedupeCutoff() int64 {
	now := time.Now()
	if s.reviewDedupeNow != nil {
		now = s.reviewDedupeNow()
	}
	return now.Add(-s.reviewDedupeWindow).Unix()
}

func (s *Server) maxConcurrentEvents() int { return cap(s.eventSlots) }

func (s *Server) queueCap() int {
	return s.maxQueuedPerKey
}

func (s *Server) stateLaneShare() int {
	share := int(float64(s.maxQueuedPerKey) * s.stateShare)
	if share < 0 {
		return 0
	}
	if share > s.maxQueuedPerKey {
		return s.maxQueuedPerKey
	}
	return share
}

func (s *Server) priorityOverflow() int {
	if s.overflowSlots < 0 {
		return 0
	}
	return s.overflowSlots
}

func (s *Server) SetWorkspaceResolver(r WorkspaceResolver) {
	s.workspaceResolver = r
}

func (s *Server) SetPRHeadResolver(r PRHeadResolver) {
	s.prHeadResolver = r
}

func (s *Server) SetSessionStore(st TakeoverStore) {
	s.sessions = st
}

func (s *Server) SetNotifier(n Notifier) {
	if n == nil {
		s.notifier = nil
		s.rawNotifier = nil
		return
	}
	s.rawNotifier = n
	s.notifier = func(ctx context.Context, platform, channelID, text string) error {
		return n(ctx, platform, channelID, FormatWebhookMessage(text))
	}
}

func (s *Server) SetEditor(e Editor) {
	if e == nil {
		s.editor = nil
		s.rawEditor = nil
		return
	}
	s.rawEditor = e
	s.editor = func(ctx context.Context, platform, channelID, messageID, text string) error {
		return e(ctx, platform, channelID, messageID, FormatWebhookMessage(text))
	}
}

func (s *Server) SetSender(n Sender) {
	if n == nil {
		s.sender = nil
		s.rawSender = nil
		return
	}
	s.rawSender = n
	s.sender = func(ctx context.Context, platform, channelID, text string) (string, error) {
		return n(ctx, platform, channelID, FormatWebhookMessage(text))
	}
}

func (s *Server) SetSenderWithButtons(n SenderWithButtons) {
	if n == nil {
		s.senderButtons = nil
		s.rawSenderButtons = nil
		return
	}
	s.rawSenderButtons = n
	s.senderButtons = func(ctx context.Context, platform, channelID, text string, buttons []channel.Button) (string, error) {
		return n(ctx, platform, channelID, FormatWebhookMessage(text), buttons)
	}
}

func (s *Server) SetEditorWithButtons(n EditorWithButtons) {
	if n == nil {
		s.editorButtons = nil
		s.rawEditorButtons = nil
		return
	}
	s.rawEditorButtons = n
	s.editorButtons = func(ctx context.Context, platform, channelID, messageID, text string, buttons []channel.Button) error {
		return n(ctx, platform, channelID, messageID, FormatWebhookMessage(text), buttons)
	}
}

func (s *Server) SetThreadStarter(n ThreadStarter) {
	if n == nil {
		s.threadStarter = nil
		return
	}
	s.threadStarter = n
}

func (s *Server) SetChannelStore(c ChannelStore) {
	s.channels = c
}

func (s *Server) SetPRQueueRepo(r store.PRQueueRepo) {
	s.prQueues = r
}

func (s *Server) SetWebhookReplayRepo(r store.WebhookReplayRepo) {
	s.replays = r
}

func (s *Server) SetSpoolDir(dir string) {
	s.spoolDir = strings.TrimSpace(dir)
}

func (s *Server) SetReconcileFactsResolver(fn ReconcileFactsResolver) {
	s.reconcileFacts = fn
}

func (s *Server) SetReplayChecker(c ReplayChecker) {
	s.replayChecker = c
}

func (s *Server) SetSessionAborter(fn func(ctx context.Context, sessionID, workdir string) error) {
	s.sessionAborter = fn
}

func (s *Server) SetOperatorAuthorizer(fn func(platform, userID string) bool) {
	s.operatorAuthorizer = fn
}

func (s *Server) SetPromptDir(dir string) {
	s.promptDir = dir
}

func (s *Server) SetRetryBodyBuilder(fn func(ctx context.Context, repo, number, kind string) (body []byte, eventType string, err error)) {
	s.retryBodyBuilder = fn
}

func (s *Server) SetMentionResolver(fn MentionResolver) {
	s.mentionResolver = fn
}

func (s *Server) SetMentionNotifier(fn MentionNotifier) {
	s.mentionNotifier = fn
}

func (s *Server) SetMergeGate(fn MergeGateChecker) {
	s.mergeGate = fn
}

func (s *Server) SetPinner(fn Pinner) {
	s.pinner = fn
}

func (s *Server) SetGuildResolver(fn GuildResolver) {
	s.guildResolver = fn
}

func (s *Server) guildIDFor(platform, channelID, threadID string) string {
	if s == nil || s.guildResolver == nil {
		return ""
	}
	if !strings.EqualFold(strings.TrimSpace(platform), "discord") {
		return ""
	}
	ctx := context.Background()
	if s.shutdownCtx != nil {
		ctx = s.shutdownCtx
	}
	for _, id := range []string{threadID, channelID} {
		id = strings.TrimSpace(id)
		if id == "" || strings.Contains(id, "://") || strings.Contains(id, ":") {
			continue
		}
		if guild := strings.TrimSpace(s.guildResolver(ctx, platform, id)); guild != "" {
			return guild
		}
	}
	return ""
}

func (s *Server) sendParent(ctx context.Context, platform, channelID, text string, buttons []channel.Button) (string, error) {
	if len(buttons) > 0 && s.senderButtons != nil {
		return s.senderButtons(ctx, platform, channelID, text, buttons)
	}
	if s.sender != nil {
		return s.sender(ctx, platform, channelID, text)
	}
	if s.notifier == nil {
		return "", nil
	}
	if err := s.notifier(ctx, platform, channelID, text); err != nil {
		return "", err
	}
	return "", nil
}

func (s *Server) editParent(ctx context.Context, platform, channelID, messageID, text string, buttons []channel.Button) error {
	if len(buttons) > 0 && s.editorButtons != nil {
		return s.editorButtons(ctx, platform, channelID, messageID, text, buttons)
	}
	if s.editor == nil {
		return nil
	}
	return s.editor(ctx, platform, channelID, messageID, text)
}

func (s *Server) notifyThread(ctx context.Context, platform, channelID, text string) error {
	if s.rawNotifier != nil {
		return s.rawNotifier(ctx, platform, channelID, text)
	}
	if s.notifier == nil {
		return nil
	}
	return s.notifier(ctx, platform, channelID, strings.ReplaceAll(text, webhookMessageSeparator, ""))
}

func (s *Server) sendThread(ctx context.Context, platform, channelID, text string) (string, error) {
	if s.rawSender != nil {
		return s.rawSender(ctx, platform, channelID, text)
	}
	if s.rawNotifier != nil {
		if err := s.rawNotifier(ctx, platform, channelID, text); err != nil {
			return "", err
		}
		return "", nil
	}
	if s.notifier == nil {
		return "", nil
	}
	return "", s.notifier(ctx, platform, channelID, text)
}

func (s *Server) editThread(ctx context.Context, platform, channelID, messageID, text string) error {
	if s.rawEditor != nil {
		return s.rawEditor(ctx, platform, channelID, messageID, text)
	}
	if s.editor == nil {
		return nil
	}
	return s.editor(ctx, platform, channelID, messageID, text)
}

func threadTargetFor(platform, channelID, threadID string) string {
	if threadID == "" {
		return channelID
	}
	if platform == "telegram" && !strings.Contains(channelID, ":") {
		return channelID + ":" + threadID
	}
	if platform == "discord" {
		return threadID
	}
	return channelID
}

func (s *Server) SetStallFreshness(d time.Duration) {
	s.stallFreshness = d
}

func (s *Server) SetTaskRegistry(t *TaskThreadRegistry) {
	if t == nil {
		t = NewTaskThreadRegistry()
	}
	s.tasks = t
}

func (s *Server) TaskRegistry() *TaskThreadRegistry {
	return s.tasks
}

func (s *Server) SetBranchChecker(c BranchPRChecker) {
	if c == nil {
		c = ghBranchPRChecker
	}
	s.branchChecker = c
}

func (s *Server) SetBranchExistsChecker(c BranchExistsChecker) {
	if c == nil {
		c = ghBranchExistsChecker
	}
	s.branchExists = c
}

func (s *Server) SetFallbackPollAfter(d time.Duration) {
	if d > 0 {
		s.fallbackAfter = d
	}
}

// recoverStale marks deliveries a previous process left in flight as failed so
// diagnostics never report a phantom running delivery after a crash. Pruning
// bounds table growth at startup.
func (s *Server) recoverStale(ctx context.Context) {
	now := time.Now()
	if failed, err := s.deliveries.FailStale(ctx, now.Add(-s.claimGrace).Unix(), "interrupted by restart"); err != nil {
		slog.Error("webhook: restart recovery failed", "error", err)
	} else if failed > 0 {
		slog.Info("webhook: restart recovery", "failed_deliveries", failed)
	}
	if pruned, err := s.deliveries.Prune(ctx, now.Add(-s.retentionAge).Unix(), s.retentionKeep); err != nil {
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

	pruned, err := s.deliveries.Prune(context.Background(), time.Now().Add(-s.retentionAge).Unix(), s.retentionKeep)
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

// repairPRQueueInvariants repairs phantom active rows left behind by a
// delivery exit. A non-empty prKey keeps the sweep scoped to that PR; an empty
// prKey is the startup sweep. Exit-path calls never pass an empty key.
func (s *Server) repairPRQueueInvariants(ctx context.Context, prKey string) {
	if s.prQueues == nil {
		return
	}
	repaired, err := s.prQueues.RepairInvariants(ctx, prKey)
	if err != nil {
		slog.Warn("webhook: pr queue invariant check failed", "error", err)
		return
	}
	for _, key := range repaired {
		slog.Warn("webhook: pr queue invariant repaired", "pr_key", key, "status", "drained")
	}
}

func (s *Server) Start(ctx context.Context) error {
	s.recoverStale(ctx)
	slog.Info("webhook: " + s.RecoverPRQueue())
	s.repairPRQueueInvariants(ctx, "")

	mux := http.NewServeMux()
	if s.dispatchEnabled {
		mux.HandleFunc(s.dispatchPath, s.handleDispatch)
		if prefixed := "/occa" + s.dispatchPath; prefixed != s.dispatchPath {
			mux.HandleFunc(prefixed, s.handleDispatch)
		}
	}
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

	go s.watchdogLoop()
	s.dispatchWG.Add(1)
	go s.replayLoop(s.shutdownCtx)
	s.dispatchWG.Add(1)
	go s.reconcileLoop(s.shutdownCtx)

	slog.Info("webhook server started", "bind", s.bind, "endpoints", len(s.endpoints))
	for _, ep := range s.endpoints {
		slog.Info("webhook: endpoint prompt", "endpoint", ep.Name, "prompt_source", ep.PromptSource)
	}
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

	if r.ContentLength > s.maxBodySize {
		http.Error(w, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBodySize))
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
	spooled, persisted := s.recordDelivery(item)
	if !persisted {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	d, ok, reason := s.enqueue(ep, ExtractExecutionKey(body), item)
	if !ok {
		if reason == "queue ceiling reached" {
			slog.Warn("webhook: queue full",
				"endpoint", ep.Name,
				"delivery_id", deliveryID,
				"event_type", eventType,
			)
		} else {
			slog.Warn("webhook: delivery rejected",
				"endpoint", ep.Name,
				"delivery_id", deliveryID,
				"event_type", eventType,
				"reason", reason,
			)
		}
		w.Header().Set("Retry-After", strconv.Itoa(int(s.retryAfter/time.Second)))
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
		return
	}

	slog.Info("webhook: delivery enqueued", "endpoint", ep.Name, "delivery_id", deliveryID, "event_type", eventType, "lock", d.key)
	if spooled {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// recordDelivery writes the receipt before the event enters the in-memory
// queue, so a delivery that never executes still leaves a row behind. When the
// write cannot finish inside the ACK budget the raw body goes to the spool and
// the caller acknowledges with 202; the spool drain inserts the row later.
func (s *Server) recordDelivery(item dispatchItem) (spooled bool, persisted bool) {
	if s.deliveries == nil {
		return false, true
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.ackBudget)
	defer cancel()
	_, err := s.deliveries.Create(ctx, store.WebhookDelivery{
		Endpoint:    item.ep.Name,
		DeliveryID:  item.deliveryID,
		EventType:   item.eventType,
		PayloadHash: item.payloadHash,
		Status:      store.WebhookStatusReceived,
	})
	if err == nil {
		return false, true
	}
	path, spoolErr := s.writeSpool(item)
	if spoolErr != nil {
		slog.Error("webhook: delivery persistence failed and spool unavailable",
			"endpoint", item.ep.Name,
			"delivery_id", item.deliveryID,
			"error", err,
			"spool_error", spoolErr,
		)
		return false, false
	}
	slog.Warn("webhook: delivery spooled",
		"endpoint", item.ep.Name,
		"delivery_id", item.deliveryID,
		"event_type", item.eventType,
		"path", path,
		"error", err,
	)
	return true, true
}

// enqueue atomically resolves the live dispatcher for a lock identity and
// appends the item to its priority lane. Lookup, retirement checks, and the
// lane send all happen under dispatchMu — retirement also flips exiting under
// that lock before any drain — so an accepted send is always ordered before
// retirement completes and can never land in a channel nobody will read. A
// rejected send (queue full) leaves the dispatcher untouched.
func (s *Server) enqueue(ep config.EndpointConfig, key WebhookExecutionKey, item dispatchItem) (*dispatcher, bool, string) {
	lockKey := dispatchLockKey(ep, key, item)
	lane := laneOfWorkflow(ep.Workflow)
	if prKey, ok := s.prKeyForItem(ep, key, item); ok {
		lockKey = "pr:" + prKey
		item.prKey = prKey
	}
	item.lane = lane
	item.actionClass = stateActionClass(item.eventType, webhookAction(item.body))
	s.dispatchMu.Lock()
	d, ok := s.dispatchers[lockKey]
	if !ok || d.exiting {
		d = newDispatcher(s, lockKey)
		s.dispatchers[lockKey] = d
		s.dispatchWG.Add(1)
		go d.run(s.shutdownCtx)
	}
	decision := d.admit(item, s.maxQueuedPerKey, s.stateLaneShare(), s.priorityOverflow())
	if !decision.admitted {
		reason := "queue ceiling reached"
		if item.lane == numPRLanes-1 {
			if d.laneLen(item.lane) >= s.stateLaneShare() {
				reason = "state lane at capacity"
			} else {
				reason = "queue budget exhausted"
			}
		}
		queueLen := d.queueLen()
		s.dispatchMu.Unlock()
		slog.Warn("webhook: admission rejected",
			"endpoint", ep.Name,
			"delivery_id", item.deliveryID,
			"event_type", item.eventType,
			"lane", item.lane,
			"queue_len", queueLen,
			"reason", reason,
		)
		s.recordRejection(ep, item, reason)
		return nil, false, reason
	}
	if decision.replaced != nil {
		slog.Info("webhook: evicted", "endpoint", ep.Name, "delivery_id", item.deliveryID, "replaced_delivery_id", decision.replaced.deliveryID, "pr_key", item.prKey, "lane", item.lane)
		s.mirrorDelete(decision.replaced.queueID)
		if decision.replaced.deliveryID != item.deliveryID {
			s.recordSuperseded(*decision.replaced, "coalesced by a newer state event")
		}
	}
	if decision.evicted != nil {
		slog.Info("webhook: evicted", "endpoint", ep.Name, "delivery_id", item.deliveryID, "evicted_delivery_id", decision.evicted.deliveryID, "pr_key", item.prKey, "lane", item.lane)
		s.mirrorDelete(decision.evicted.queueID)
		s.recordSuperseded(*decision.evicted, "evicted for a higher-priority delivery")
	}
	if item.prKey != "" {
		item.queueID = s.mirrorEnqueue(item.prKey, ep.Name, item.eventType, 0, lanePriority(item.lane))
	}
	d.push(item)
	s.dispatchMu.Unlock()
	return d, true, ""
}

// stateActionClass names the coalescing shape of a state event. Only state
// events coalesce: a review or a comment is a distinct user action, never a
// newer version of an earlier one.
func stateActionClass(eventType, action string) string {
	switch strings.ToLower(strings.TrimSpace(eventType)) {
	case "pull_request":
		switch strings.ToLower(strings.TrimSpace(action)) {
		case "opened", "edited", "synchronize":
			return "state"
		default:
			return strings.ToLower(strings.TrimSpace(action))
		}
	case "check_suite":
		return "checks"
	case "pull_request_review", "issue_comment":
		return ""
	}
	return ""
}

func webhookAction(body []byte) string {
	var payload struct {
		Action string `json:"action"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return ""
	}
	return strings.TrimSpace(payload.Action)
}

func lanePriority(lane int) int {
	switch lane {
	case 0:
		return 1
	case 1:
		return 2
	case 2:
		return 3
	default:
		return 4
	}
}

func (s *Server) prKeyForItem(ep config.EndpointConfig, key WebhookExecutionKey, item dispatchItem) (string, bool) {
	if ep.Automation == nil {
		return "", false
	}
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", ep.CommentTrigger)
	return prKeyForEndpoint(ep, key, envelope)
}

func dispatchLockKey(ep config.EndpointConfig, key WebhookExecutionKey, item dispatchItem) string {
	if !key.IsZero() {
		return "key:" + key.String()
	}
	base := "endpoint:" + ep.Platform + "|" + ep.ChannelID
	if item.eventType != "issue_comment" {
		return base
	}
	if identity := issueCommentPRIdentity(item.body); identity != "" {
		return base + "#issue_comment:" + identity
	}
	return base
}

func issueCommentPRIdentity(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var payload struct {
		Repository any `json:"repository"`
		Issue      *struct {
			Number any `json:"number"`
		} `json:"issue"`
	}
	if json.Unmarshal(body, &payload) != nil || payload.Issue == nil {
		return ""
	}
	number := formatIssueNumber(payload.Issue.Number)
	if number == "" {
		return ""
	}
	if repo := extractRepoFullName(payload.Repository); repo != "" {
		return repo + "#" + number
	}
	return number
}

func formatIssueNumber(v any) string {
	switch n := v.(type) {
	case float64:
		if n <= 0 || n != float64(int(n)) {
			return ""
		}
		return strconv.Itoa(int(n))
	case float32:
		if n <= 0 || n != float32(int(n)) {
			return ""
		}
		return strconv.Itoa(int(n))
	case int:
		if n <= 0 {
			return ""
		}
		return strconv.Itoa(n)
	case int64:
		if n <= 0 {
			return ""
		}
		return strconv.FormatInt(n, 10)
	case json.Number:
		i, err := n.Int64()
		if err != nil || i <= 0 {
			return ""
		}
		return strconv.FormatInt(i, 10)
	case string:
		trimmed := strings.TrimSpace(n)
		if trimmed == "" {
			return ""
		}
		i, err := strconv.Atoi(trimmed)
		if err != nil || i <= 0 {
			return ""
		}
		return strconv.Itoa(i)
	default:
		return ""
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
	prKey       string
	actionClass string
	lane        int
	queueID     int64
	replayID    int64
	requeues    int
}

type dispatcher struct {
	server          *Server
	key             string
	lanes           [numPRLanes]*prLane
	notify          chan struct{}
	exiting         bool
	mu              sync.Mutex
	active          dispatchItem
	activeStartedAt time.Time
	hasActive       bool
	lastActivity    time.Time
	stuckReported   bool
	watchdogAborted bool
	backingOff      bool
	cmdMu           sync.Mutex
	skipRequested   bool
	cancelRequested bool
	retryRequested  bool
}

func (d *dispatcher) touch() {
	d.mu.Lock()
	d.lastActivity = time.Now()
	d.mu.Unlock()
}

func (d *dispatcher) setActive(item dispatchItem) {
	d.mu.Lock()
	d.active = item
	d.activeStartedAt = time.Now()
	d.hasActive = true
	d.lastActivity = d.activeStartedAt
	d.stuckReported = false
	d.mu.Unlock()
}

func (d *dispatcher) clearActive() {
	d.mu.Lock()
	d.hasActive = false
	d.lastActivity = time.Now()
	d.mu.Unlock()
}

func (d *dispatcher) isExiting() bool {
	d.server.dispatchMu.Lock()
	defer d.server.dispatchMu.Unlock()
	return d.exiting
}

type dispatcherSnapshot struct {
	hasActive   bool
	active      dispatchItem
	startedAt   time.Time
	lastActive  time.Time
	queueLen    int
	queueCap    int
	stuckMarked bool
}

func (d *dispatcher) snapshot() dispatcherSnapshot {
	d.mu.Lock()
	snap := dispatcherSnapshot{
		hasActive:   d.hasActive,
		active:      d.active,
		startedAt:   d.activeStartedAt,
		lastActive:  d.lastActivity,
		stuckMarked: d.stuckReported,
	}
	d.mu.Unlock()
	snap.queueLen = d.queueLen()
	snap.queueCap = d.queueCap()
	if snap.lastActive.IsZero() {
		snap.lastActive = snap.startedAt
	}
	return snap
}

func (d *dispatcher) markStuckReported() {
	d.mu.Lock()
	d.stuckReported = true
	d.mu.Unlock()
}

func (d *dispatcher) markWatchdogTookOver() {
	d.mu.Lock()
	d.watchdogAborted = true
	d.mu.Unlock()
}

func (d *dispatcher) watchdogTookOver() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.watchdogAborted
}

// run serially executes queued deliveries until the queue stays idle for the
// eviction TTL or the shutdown context is cancelled. Idle retirement only
// succeeds while the queue is provably empty under dispatchMu, so no accepted
// send is ever stranded; on shutdown every leftover is recorded as failed.
func (d *dispatcher) run(ctx context.Context) {
	defer d.server.dispatchWG.Done()
	defer func() {
		if r := recover(); r != nil {
			d.mu.Lock()
			active := d.active
			hasActive := d.hasActive
			d.mu.Unlock()
			var failed dispatchItem
			if hasActive {
				failed = active
			}
			d.server.rebuildAfterPanic(d, failed, r)
		}
	}()
	timer := time.NewTimer(d.server.dispatcherIdleTTL)
	defer timer.Stop()
	for {
		if op := d.takeIntent(); op == wakeCancel {
			prKey := strings.TrimSpace(strings.TrimPrefix(d.key, "pr:"))
			for _, it := range d.drain() {
				d.server.mirrorDelete(it.queueID)
			}
			if prKey != "" && strings.HasPrefix(d.key, "pr:") && d.server.prQueues != nil {
				_ = d.server.prQueues.ClearActive(context.Background(), prKey)
			}
			slog.Info("webhook: queue cancelled by operator", "lock", d.key)
			continue
		}
		if item, ok := d.pop(); ok {
			d.server.mirrorDelete(item.queueID)
			d.execute(item, ctx, timer)
			if d.isExiting() {
				return
			}
			continue
		}
		select {
		case <-ctx.Done():
			d.retire("shutting down")
			return
		case <-d.notify:
		case <-timer.C:
			if !d.tryRetireIdle() {
				timer.Reset(d.server.dispatcherIdleTTL)
				continue
			}
			return
		}
	}
}

func (d *dispatcher) execute(item dispatchItem, ctx context.Context, timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d.server.dispatcherIdleTTL)
	if ctx.Err() != nil {
		d.server.abandon(item, "shutting down")
		return
	}
	d.setActive(item)
	func() {
		defer func() {
			if r := recover(); r != nil {
				d.server.handleDispatcherPanic(d, item, r)
			}
		}()
		d.handle(item)
	}()
	d.clearActive()
}

// tryRetireIdle retires the dispatcher only when its queue is empty under the
// registry lock. Because enqueues take the same lock, a successful retirement
// freezes an empty channel that no future send can reach; a failed attempt
// means activity raced the timer and the dispatcher stays live.
func (d *dispatcher) tryRetireIdle() bool {
	s := d.server
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	if d.queueLen() > 0 {
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
	for _, item := range d.drain() {
		s.abandon(item, reason)
	}
}

func (d *dispatcher) handle(item dispatchItem) {
	s := d.server
	if item.prKey != "" {
		defer s.repairPRQueueInvariants(context.Background(), item.prKey)
	}
	receipt := s.beginExecution(item)
	if receipt == nil {
		return
	}
	if item.ep.Workflow == "merged" {
		s.handleMergedWorkflow(item, receipt, d)
		return
	}
	if item.prKey != "" && strings.EqualFold(strings.TrimSpace(item.ep.Workflow), "merge") {
		envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", item.ep.CommentTrigger)
		s.recordMergeGate(item.prKey, envelope)
		if verdict, blocked := s.mergeGateBlocked(item.prKey); blocked {
			commit := s.mergeGateCommit(item.prKey)
			slog.Info("webhook: merge refused, review verdict blocks the gate",
				"endpoint", item.ep.Name,
				"delivery_id", item.deliveryID,
				"pr_key", item.prKey,
				"review_verdict", verdict,
				"review_commit", commit,
			)
			reason := "merge blocked: " + verdict
			if commit != "" {
				reason += " recorded for " + commit
			}
			s.markSkipped(receipt.ID, item.ep, envelope, reason)
			return
		}
	}
	if item.prKey != "" {
		if row, _ := s.blockedQueueRow(context.Background(), item.prKey); row != nil {
			s.markSkipped(receipt.ID, item.ep, normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", item.ep.CommentTrigger), blockedQueueSkipReason(item.prKey, row.BlockedReason, row.ActiveKind))
			return
		}
		if err := s.deliveries.SetPRKey(context.Background(), receipt.ID, item.prKey); err != nil {
			slog.Warn("webhook: pr key stamp failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", err)
		}
		s.prMirrorSetActive(item, receipt.ID)
		defer s.prMirrorClearActive(item.prKey)
	}
	held := false
	defer func() {
		if held {
			s.releaseEvent()
		}
	}()
	incompleteRetried := false
	enriched := false
	for attempt := 0; ; attempt++ {
		if !s.acquireEvent(s.shutdownCtx) {
			s.failAbandonedReceipt(receipt, "shutting down")
			return
		}
		held = true
		if !enriched {
			newBody, err := s.enrichCommentBody(item)
			if err != nil {
				held = false
				s.releaseEvent()
				s.failEnrichment(item, receipt, err)
				return
			}
			item.body = newBody
			enriched = true
		}
		earlyEnvelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", item.ep.CommentTrigger)
		if !s.automationAllows(item.ep, ExtractExecutionKey(item.body), earlyEnvelope) {
			held = false
			s.releaseEvent()
			slog.Info("webhook: delivery skipped by author policy",
				"endpoint", item.ep.Name,
				"delivery_id", item.deliveryID,
				"event_type", item.eventType,
			)
			s.markSkipped(receipt.ID, item.ep, earlyEnvelope, "review-only author")
			return
		}
		if allowed, reason := admitDelivery(item.ep, s.policy, earlyEnvelope); !allowed {
			held = false
			s.releaseEvent()
			slog.Info("webhook: delivery skipped before workspace",
				"endpoint", item.ep.Name,
				"delivery_id", item.deliveryID,
				"event_type", item.eventType,
				"reason", reason,
			)
			s.markSkipped(receipt.ID, item.ep, earlyEnvelope, reason)
			return
		}
		if item.prKey != "" {
			s.markPRQueued(item.prKey, item.ep, earlyEnvelope)
			if item.ep.Workflow != "merged" {
				if state := strings.TrimSpace(stringValue(earlyEnvelope["pr_state"])); state == "merged" || state == "closed" {
					held = false
					s.releaseEvent()
					dropped := s.drainPRQueue(d, item.prKey)
					slog.Info("webhook: delivery skipped, PR already terminal",
						"endpoint", item.ep.Name,
						"delivery_id", item.deliveryID,
						"pr_key", item.prKey,
						"pr_state", state,
						"dropped", dropped,
					)
					s.markSkipped(receipt.ID, item.ep, earlyEnvelope, "PR already "+state)
					return
				}
			}
			if item.ep.Workflow == "merge" && d.hasQueuedHigher() {
				if item.requeues >= 3 {
					held = false
					s.releaseEvent()
					s.prMirrorBlocked(item.prKey, "merge starved — manual action")
					s.markSkipped(receipt.ID, item.ep, earlyEnvelope, "merge starved — manual action")
					return
				}
				item.requeues++
				item.queueID = s.mirrorEnqueue(item.prKey, item.ep.Name, item.eventType, 0, lanePriority(item.lane))
				held = false
				s.releaseEvent()
				d.push(item)
				return
			}
		}
		lease, werr := s.resolveWorkspace(item)
		if werr == nil {
			allowRetry := func() bool { return !incompleteRetried }
			execErr := s.executeDelivery(item.ep, item.body, receipt.ID, item.deliveryID, item.eventType, receipt.Attempt, lease, allowRetry, d.touch)
			held = false
			s.releaseEvent()
			s.pruneIfDue()
			if errors.Is(execErr, errRetryScheduled) && item.prKey != "" {
				switch d.sleepForRetry(item.prKey, s.shutdownCtx) {
				case wakeFire:
					if d.watchdogTookOver() {
						slog.Error("webhook: retry dropped, run taken over by watchdog",
							"endpoint", item.ep.Name,
							"delivery_id", item.deliveryID,
							"pr_key", item.prKey,
						)
						return
					}
					if !s.retryStillAllowed(item.ep, item.prKey, receipt.ID) {
						return
					}
					receipt.Attempt++
					continue
				case wakeSkip:
					s.markSkipped(receipt.ID, item.ep, earlyEnvelope, "skipped by operator")
					return
				case wakeCancel:
					s.drainPRQueue(d, item.prKey)
					s.markSkipped(receipt.ID, item.ep, earlyEnvelope, "cancelled by operator")
					return
				default:
					s.failAbandonedReceipt(receipt, "shutting down")
					return
				}
			}
			if errors.Is(execErr, errOpSkipped) {
				s.markSkipped(receipt.ID, item.ep, earlyEnvelope, "skipped by operator")
				return
			}
			if errors.Is(execErr, errOpCancelled) {
				s.drainPRQueue(d, item.prKey)
				s.markSkipped(receipt.ID, item.ep, earlyEnvelope, "cancelled by operator")
				return
			}
			if execErr != nil && errors.Is(execErr, relay.ErrWebhookResponseIncomplete) && !incompleteRetried {
				incompleteRetried = true
				if tErr := s.deliveries.BumpAttempt(context.Background(), receipt.ID); tErr != nil {
					slog.Error("webhook: self-heal retry bump failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", tErr)
				}
				receipt.Attempt++
				continue
			}
			return
		}
		held = false
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
		if errors.Is(werr, ErrWorkspaceLeased) {
			s.standDownBusy(item, receipt, werr)
			return
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

func (s *Server) standDownBusy(item dispatchItem, receipt *store.WebhookDelivery, err error) {
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", item.ep.CommentTrigger)
	key := ExtractExecutionKey(item.body)
	repo, branch := TaskRepoBranch(key, envelope)
	slog.Info("webhook: workspace busy, standing down",
		"endpoint", item.ep.Name,
		"delivery_id", item.deliveryID,
		"repo", repo,
		"branch", branch,
		"error", err,
	)
	summary := redactSummary(err.Error(), maxErrorSummaryRunes, item.ep.Secret)
	ok, tErr := s.deliveries.Transition(context.Background(), receipt.ID, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusSkipped, summary)
	if tErr != nil {
		slog.Error("webhook: stand-down transition failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", tErr)
		return
	}
	if !ok {
		slog.Warn("webhook: stand-down transition lost race", "endpoint", item.ep.Name, "delivery_id", item.deliveryID)
	}
}

func (s *Server) failWorkspace(item dispatchItem, receipt *store.WebhookDelivery, err error) {
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", item.ep.CommentTrigger)
	key := ExtractExecutionKey(item.body)
	workCtx := &WebhookWorkContext{
		Key:          key,
		DeliveryID:   item.deliveryID,
		Attempt:      receipt.Attempt,
		Thread:       item.ep.Thread,
		ProgressCard: item.ep.ProgressCard,
		Workflow:     item.ep.Workflow,
		Envelope:     envelope,
	}
	if repo, branch := TaskRepoBranch(key, envelope); repo != "" && branch != "" {
		if rec, ok := s.tasks.ResolveTaskThread(repo, branch, "workspace-fail"); ok {
			workCtx.ThreadID = rec.ThreadID
			workCtx.RootMessageID = rec.RootMessageID
		}
	}
	base := redactSummary(err.Error(), maxErrorSummaryRunes, item.ep.Secret)
	summary := base
	if !strings.Contains(strings.ToLower(summary), "action:") {
		combined := base + " — Action: clean the worktree for repo+branch or dispatch manually."
		summary = clipRunes(combined, maxErrorSummaryRunes)
	}
	s.failDelivery(item.ep, receipt.ID, item.deliveryID, item.eventType, envelope, summary, workCtx)
	threadID := workCtx.ThreadID
	if threadID == "" {
		if _, _, rec, ok := s.tasks.ResolveForDelivery(WebhookExecutionKey{}, envelope, "workspace-fail-envelope"); ok {
			threadID = rec.ThreadID
		}
	}
	if threadID == "" {
		return
	}
	slim := redactAuditSummary(FormatThreadSlimCard(envelope, item.ep.Workflow, TaskFailed, s.statusIcons), item.ep.Secret)
	target := threadTargetFor(item.ep.Platform, item.ep.ChannelID, threadID)
	if repo, branch := TaskRepoBranch(WebhookExecutionKey{}, envelope); repo != "" && branch != "" {
		s.postPhaseBannerIfChanged(context.Background(), item.ep.Platform, item.ep.ChannelID, threadID, repo, branch, item.ep.Workflow, summary)
		s.ensureThreadStatus(context.Background(), item.ep.Platform, item.ep.ChannelID, threadID, repo, branch, "", ThreadStatusUpdate{
			Phase:   PhaseFailed,
			Waiting: waitingReasonForGate(envelope),
		})
	}
	if strings.TrimSpace(summary) != "" {
		slim = strings.TrimSpace(slim) + "\n" + strings.TrimSpace(summary)
	}
	if strings.TrimSpace(slim) == "" {
		return
	}
	if nErr := s.notifyThread(context.Background(), item.ep.Platform, target, slim); nErr != nil {
		slog.Warn("webhook: workspace failure thread card failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "thread_id", threadID, "error", nErr)
	}
}

func (s *Server) enrichCommentBody(item dispatchItem) ([]byte, error) {
	ep := item.ep
	if item.eventType != "issue_comment" {
		slog.Info("webhook: comment enrichment skipped",
			"endpoint", ep.Name,
			"delivery_id", item.deliveryID,
			"gate", "non-issue-comment",
			"event_type", item.eventType,
		)
		return item.body, nil
	}
	if ep.Workspace.Type != config.WorkspaceTypeGit {
		slog.Info("webhook: comment enrichment skipped",
			"endpoint", ep.Name,
			"delivery_id", item.deliveryID,
			"gate", "non-git-workspace",
			"workspace_type", ep.Workspace.Type,
		)
		return item.body, nil
	}
	if ExtractExecutionKey(item.body).Branch != "" {
		slog.Info("webhook: comment enrichment skipped",
			"endpoint", ep.Name,
			"delivery_id", item.deliveryID,
			"gate", "branch-present",
		)
		return item.body, nil
	}
	repo, number, ok := parseIssueCommentPR(item.body)
	if !ok {
		slog.Info("webhook: comment enrichment skipped",
			"endpoint", ep.Name,
			"delivery_id", item.deliveryID,
			"gate", "parse-failed",
		)
		return item.body, nil
	}
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", ep.CommentTrigger)
	if stringValue(envelope["comment_trigger"]) == "" {
		slog.Info("webhook: comment enrichment skipped",
			"endpoint", ep.Name,
			"delivery_id", item.deliveryID,
			"gate", "no-comment-trigger",
			"repo", repo,
			"pr_number", number,
		)
		return item.body, nil
	}
	if allowed, reason := admitDelivery(ep, s.policy, envelope); !allowed {
		slog.Info("webhook: comment enrichment skipped",
			"endpoint", ep.Name,
			"delivery_id", item.deliveryID,
			"gate", "admission-denied",
			"repo", repo,
			"pr_number", number,
			"reason", reason,
		)
		return item.body, nil
	}
	resolver := s.prHeadResolver
	if resolver == nil {
		resolver = &ghPRHeadResolver{timeout: s.processingTimeout}
	}
	slog.Info("webhook: resolving PR head for comment",
		"endpoint", ep.Name,
		"delivery_id", item.deliveryID,
		"repo", repo,
		"pr_number", number,
	)
	head, err := resolver.ResolvePRHead(context.Background(), repo, number)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(head.Branch) == "" || normalizeRevision(head.SHA) == "" || !isValidRepoFullName(strings.TrimSpace(head.HeadRepo)) {
		return nil, fmt.Errorf("incomplete head identity for %s#%d", repo, number)
	}
	if !strings.EqualFold(strings.TrimSpace(head.HeadRepo), repo) {
		return nil, fmt.Errorf("fork PR %s#%d (head %s) is not supported", repo, number, strings.TrimSpace(head.HeadRepo))
	}
	enriched, err := seedEnrichedBody(item.body, head, repo)
	if err != nil {
		return nil, err
	}
	slog.Info("webhook: comment enriched with PR head",
		"endpoint", ep.Name,
		"delivery_id", item.deliveryID,
		"repo", repo,
		"pr_number", number,
		"branch", strings.TrimSpace(head.Branch),
	)
	return enriched, nil
}

func (s *Server) commentRoutingIdentity(body []byte, eventType string, ep config.EndpointConfig, envelope WebhookEnvelope) (string, string, bool) {
	if eventType != "issue_comment" {
		return "", "", false
	}
	if ExtractExecutionKey(body).Branch != "" {
		return "", "", false
	}
	repo, number, ok := parseIssueCommentPR(body)
	if !ok {
		return "", "", false
	}
	if strings.TrimSpace(stringValue(envelope["comment_trigger"])) == "" {
		return "", "", false
	}
	if allowed, _ := admitDelivery(ep, s.policy, envelope); !allowed {
		return "", "", false
	}
	resolver := s.prHeadResolver
	if resolver == nil {
		resolver = &ghPRHeadResolver{timeout: s.processingTimeout}
	}
	head, err := resolver.ResolvePRHead(context.Background(), repo, number)
	if err != nil {
		return "", "", false
	}
	if strings.TrimSpace(head.Branch) == "" || normalizeRevision(head.SHA) == "" || !isValidRepoFullName(strings.TrimSpace(head.HeadRepo)) {
		return "", "", false
	}
	if !strings.EqualFold(strings.TrimSpace(head.HeadRepo), repo) {
		return "", "", false
	}
	return repo, strings.TrimSpace(head.Branch), true
}

func (s *Server) failEnrichment(item dispatchItem, receipt *store.WebhookDelivery, err error) {
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, false, "", item.ep.CommentTrigger)
	workCtx := &WebhookWorkContext{
		Key:        ExtractExecutionKey(item.body),
		DeliveryID: item.deliveryID,
		Attempt:    receipt.Attempt,
	}
	summary := redactSummary("pr head enrichment failed: "+err.Error(), maxErrorSummaryRunes, item.ep.Secret)
	s.failDelivery(item.ep, receipt.ID, item.deliveryID, item.eventType, envelope, summary, workCtx)
}

func sessionLogAttrs(workCtx *WebhookWorkContext) []any {
	if workCtx == nil || workCtx.SessionID == "" {
		return nil
	}
	return []any{
		"session_id", workCtx.SessionID,
		"session_aborted", workCtx.SessionAborted,
		"session_abort_ok", workCtx.SessionAbortOK,
	}
}

// beginExecution claims a delivery at the moment it reaches the head of its
// queue: the receipt row is created here and moved to processing with a fresh
// updated_at, so the stale-claim grace window always measures active execution
// time rather than queue wait. Duplicate deliveries resolve against the stored
// receipt exactly as before; only the winner proceeds.
func (s *Server) beginExecution(item dispatchItem) *store.WebhookDelivery {
	ctx := context.Background()
	ep := item.ep

	receipt, err := s.deliveries.Get(ctx, ep.Name, item.deliveryID)
	if err != nil {
		slog.Error("webhook: receipt load failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", err)
		return nil
	}
	created := false
	if receipt == nil {
		created, err = s.deliveries.Create(ctx, store.WebhookDelivery{
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
		receipt, err = s.deliveries.Get(ctx, ep.Name, item.deliveryID)
		if err != nil {
			slog.Error("webhook: receipt load failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", err)
			return nil
		}
	}
	if receipt == nil {
		slog.Error("webhook: receipt missing after claim", "endpoint", ep.Name, "delivery_id", item.deliveryID)
		return nil
	}

	staleClaimed := false
	if !created {
		switch {
		case isReopenable(receipt.Status):
			ok, tErr := s.deliveries.Reopen(ctx, receipt.ID, []store.WebhookStatus{receipt.Status})
			if tErr != nil {
				slog.Error("webhook: reopen claim failed", "endpoint", ep.Name, "delivery_id", item.deliveryID, "error", tErr)
				return nil
			}
			if !ok {
				s.logDuplicate(item, receipt.Status)
				return nil
			}
			receipt.Status = store.WebhookStatusReceived
		case isTerminal(receipt.Status):
			s.logDuplicate(item, receipt.Status)
			s.settleReplay(ctx, item.replayID, store.WebhookReplaySuperseded)
			return nil
		case receipt.Status == store.WebhookStatusReceived:
		case time.Now().Unix()-receipt.UpdatedAt < s.claimGraceSeconds:
			s.logDuplicate(item, receipt.Status)
			return nil
		default:
			ok, tErr := s.deliveries.ClaimStale(ctx, receipt.ID, time.Now().Add(-s.claimGrace).Unix())
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
		envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, s.verdicts, true, reason, ep.CommentTrigger)
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
		s.emitAudit(ctx, ep, envelope, "SKIP", reason, "")
		s.settleReplay(ctx, item.replayID, store.WebhookReplayReplayed)
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

	s.settleReplay(ctx, item.replayID, store.WebhookReplayReplayed)
	slog.Info("webhook: delivery accepted", "endpoint", ep.Name, "delivery_id", item.deliveryID, "event_type", item.eventType, "status", store.WebhookStatusProcessing)
	return receipt
}

func (s *Server) settleReplay(ctx context.Context, id int64, status store.WebhookReplayStatus) {
	if s.replays == nil || id == 0 {
		return
	}
	if err := s.replays.Mark(ctx, id, status, 0); err != nil {
		slog.Warn("webhook: replay mark failed", "replay_id", id, "status", status, "error", err)
	}
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
	if strings.TrimSpace(item.prKey) != "" {
		defer s.repairPRQueueInvariants(ctx, item.prKey)
	}
	s.mirrorDelete(item.queueID)
	_, err := s.deliveries.Create(ctx, store.WebhookDelivery{
		Endpoint:    item.ep.Name,
		DeliveryID:  item.deliveryID,
		EventType:   item.eventType,
		PayloadHash: item.payloadHash,
		PRKey:       item.prKey,
		Attempt:     1,
	})
	if err != nil {
		slog.Error("webhook: abandoned delivery receipt failed", "endpoint", item.ep.Name, "delivery_id", item.deliveryID, "error", err)
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
// cleanup failure cannot change the delivery outcome. allowRetryIncomplete is
// the dispatcher's one-shot self-heal decision: when an execution fails with
// (a wrapped) ErrWebhookResponseIncomplete and the closure returns true, the
// receipt is deliberately left processing and the dispatcher re-executes once.
// heartbeat, when non-nil, is exposed on the work context so the executor can
// report observable progress; the dispatcher stamps last activity from it.
// The returned error is the executor's error, after the receipt's terminal
// transition (or retry grant) is recorded.
// ensureReviewDedupeHead resolves the PR head for comment-triggered review
// deliveries on non-git endpoints, whose payload carries no head SHA. Without
// it the synthetic internal re-review and its GitHub comment echo would key
// differently and both start a review on the same head.
func (s *Server) ensureReviewDedupeHead(envelope WebhookEnvelope) {
	if stringValue(envelope["event_type"]) != "issue_comment" {
		return
	}
	if strings.TrimSpace(stringValue(envelope["comment_trigger"])) == "" {
		return
	}
	if normalizeRevision(stringValue(envelope["head_sha"])) != "" {
		return
	}
	repo := strings.TrimSpace(stringValue(envelope["repository"]))
	number, err := strconv.Atoi(strings.TrimSpace(stringValue(envelope["pr_number"])))
	if repo == "" || err != nil || number <= 0 {
		return
	}
	resolver := s.prHeadResolver
	if resolver == nil {
		resolver = &ghPRHeadResolver{timeout: s.processingTimeout}
	}
	head, err := resolver.ResolvePRHead(context.Background(), repo, number)
	if err != nil {
		slog.Warn("webhook: review dedupe head lookup failed", "repo", repo, "pr_number", number, "error", err)
		return
	}
	sha := normalizeRevision(head.SHA)
	if sha == "" {
		return
	}
	envelope["head_sha"] = sha
}

func (s *Server) executeDelivery(ep config.EndpointConfig, body []byte, id int64, deliveryID, eventType string, attempt int, lease *WorkspaceLease, allowRetryIncomplete func() bool, heartbeat func()) error {
	key := ExtractExecutionKey(body)
	envelope := normalizeWebhook(body, eventType, deliveryID, s.verdicts, false, "", ep.CommentTrigger)
	injectTaskContext(envelope, key, s.dispatchTasks)

	workCtx := &WebhookWorkContext{
		Key:          key,
		DeliveryID:   deliveryID,
		Attempt:      attempt,
		Thread:       ep.Thread,
		ProgressCard: ep.ProgressCard,
		ThreadID:     ep.ThreadID,
		Workflow:     ep.Workflow,
		Envelope:     envelope,
		StartTime:    time.Now(),
		Heartbeat:    heartbeat,
	}
	prKey, hasPRKey := prKeyForEndpoint(ep, key, envelope)
	if hasPRKey {
		workCtx.PRKey = prKey
	}
	workdir := ""
	if lease != nil {
		workdir = lease.Path
		workCtx.Worktree = lease.Path
		defer func() {
			if rErr := lease.Release(context.Background()); rErr != nil {
				slog.Warn("webhook: workspace cleanup failed", "endpoint", ep.Name, "delivery_id", deliveryID, "workspace", lease.Path, "error", rErr)
			}
		}()
	}
	run := s.beginActiveRun(workCtx.PRKey, ep.Name, deliveryID, workdir)
	defer s.endActiveRun(run)
	if heartbeat != nil {
		progress := heartbeat
		workCtx.Heartbeat = func() {
			run.noteProgress()
			progress()
		}
	}
	workCtx.ReportWorkdir = func(workdir string) {
		run.setWorkdir(workdir)
	}
	boundThreadID := strings.TrimSpace(workCtx.ThreadID)
	binding := prBindingResolution{ThreadID: boundThreadID}
	if hasPRKey {
		binding = s.resolvePRBinding(context.Background(), prKey, eventType, envelope)
		if binding.ThreadID == "" {
			binding.ThreadID = boundThreadID
		}
		if binding.ThreadID != "" {
			workCtx.ThreadID = binding.ThreadID
		}
		if binding.RootMessageID != "" {
			workCtx.RootMessageID = binding.RootMessageID
		}
	}
	if !hasPRKey && strings.TrimSpace(workCtx.ThreadID) == "" {
		repo, branch := TaskRepoBranch(key, envelope)
		if repo == "" {
			repo = normalizeRepoKey(stringValue(envelope["repository"]))
		}
		if branch == "" && eventType == "issue_comment" {
			if routingRepo, routingBranch, ok := s.commentRoutingIdentity(body, eventType, ep, envelope); ok {
				repo, branch = routingRepo, routingBranch
			}
		}
		if repo != "" && branch != "" {
			if rec, ok := s.tasks.ResolveTaskThread(repo, branch, ep.Workflow); ok {
				workCtx.ThreadID = strings.TrimSpace(rec.ThreadID)
				workCtx.RootMessageID = strings.TrimSpace(rec.RootMessageID)
			}
		}
	}
	if hasPRKey && strings.TrimSpace(workCtx.ThreadID) == "" && s.threadStarter != nil &&
		isBindingRequiredWorkflow(ep.Workflow) {
		summary := prBindingRefusalSummary(prKey, binding)
		slog.Error("webhook: run refused, PR thread binding unresolved",
			"endpoint", ep.Name,
			"delivery_id", deliveryID,
			"event_type", eventType,
			"pr_key", prKey,
			"repo", binding.Repo,
			"branch", binding.Branch,
			"branch_source", binding.BranchSource,
		)
		s.failUnboundRun(ep, id, envelope, prKey, summary)
		return nil
	}
	workCtx.ReportSession = func(sessionID string) {
		workCtx.SessionID = strings.TrimSpace(sessionID)
		run.setSession(workCtx.SessionID)
		if s.sessions != nil && workCtx.SessionID != "" && strings.TrimSpace(workCtx.ThreadID) != "" {
			if err := s.sessions.SetActive(context.Background(), ep.Platform, ep.ChannelID, workCtx.ThreadID, "", workCtx.SessionID, workCtx.AgentPID); err != nil {
				slog.Warn("webhook: session binding persist failed", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "thread_id", workCtx.ThreadID, "error", err)
			}
		}
		if workCtx.PRKey != "" && s.prQueues != nil {
			if err := s.prQueues.UpdateSession(context.Background(), workCtx.PRKey, workCtx.SessionID); err != nil {
				slog.Warn("webhook: pr session stamp failed", "pr_key", workCtx.PRKey, "error", err)
			}
		}
	}
	defer s.endActiveRun(run)
	defer func() {
		if workCtx.SessionID != "" {
			s.deactivateSession(ep.Platform, ep.ChannelID, workCtx.ThreadID)
		}
	}()

	defer func() {
		if r := recover(); r != nil {
			slog.Error("webhook: panic recovered in delivery processing",
				append([]any{
					"endpoint", ep.Name,
					"delivery_id", deliveryID,
					"event_type", eventType,
					"execution_key", workCtx.Key.String(),
					"worktree", workCtx.Worktree,
					"panic", fmt.Sprint(r),
				}, sessionLogAttrs(workCtx)...)...)
			s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary(fmt.Sprintf("panic: %v", r), maxErrorSummaryRunes, ep.Secret), workCtx)
		}
	}()

	if workCtx.PRKey != "" {
		verdict := s.recordMergeGate(workCtx.PRKey, envelope)
		if verdict != "" && strings.EqualFold(strings.TrimSpace(ep.Workflow), "merge") &&
			strings.TrimSpace(stringValue(envelope["review_verdict"])) == "" {
			// The merge lane renders the verdict the gate holds, not the
			// one this delivery happened to carry.
			envelope["review_verdict"] = verdict
		}
	}
	if allowed, reason := admitDelivery(ep, s.policy, envelope); !allowed {
		s.markSkipped(id, ep, envelope, reason)
		return nil
	}

	reviewKey := ""
	if isReviewWorkflow(ep.Workflow) && !isOperatorRetryDelivery(deliveryID) {
		s.ensureReviewDedupeHead(envelope)
		reviewKey = reviewDedupeKey(envelope)
	}
	triggerPrefix := ""
	if stringValue(envelope["event_type"]) == "issue_comment" && stringValue(envelope["comment_trigger"]) != "" {
		triggerPrefix = reviewDedupePrefix(envelope)
	}
	if reviewKey != "" && id != 0 {
		prior, err := s.deliveries.FindReviewDuplicate(context.Background(), ep.Name, reviewKey, id, s.reviewDedupeCutoff())
		if err != nil {
			slog.Warn("webhook: review duplicate lookup failed", "endpoint", ep.Name, "delivery_id", deliveryID, "error", err)
		} else if prior != nil {
			slog.Info("webhook: duplicate review event skipped", "endpoint", ep.Name, "delivery_id", deliveryID, "review_key", reviewKey, "handled_by", prior.DeliveryID)
			s.markSkipped(id, ep, envelope, "duplicate review event (same head, already handled by delivery "+prior.DeliveryID+")")
			return nil
		}
	}

	if triggerPrefix != "" && id != 0 {
		limit, ok := s.triggerLimits[ep.Name]
		if !ok {
			slog.Error("webhook: comment trigger without configured limits", "endpoint", ep.Name, "delivery_id", deliveryID)
			s.markSkipped(id, ep, envelope, "trigger rate cap")
			return nil
		}
		windowCutoff := time.Now().Add(-limit.Window).Unix()
		count, err := s.deliveries.CountReviewDeliveries(context.Background(), ep.Name, triggerPrefix, windowCutoff)
		if err != nil {
			slog.Warn("webhook: review trigger count lookup failed", "endpoint", ep.Name, "delivery_id", deliveryID, "error", err)
		} else if count >= limit.MaxRunsPerPR {
			slog.Info("webhook: review delivery trigger rate cap reached", "endpoint", ep.Name, "delivery_id", deliveryID, "trigger_prefix", triggerPrefix, "count", count)
			s.markSkipped(id, ep, envelope, "trigger rate cap")
			return nil
		}
	}

	if id != 0 {
		storedKey := reviewKey
		if storedKey == "" {
			storedKey = triggerPrefix
		}
		if storedKey != "" {
			if err := s.deliveries.SetReviewKey(context.Background(), id, storedKey); err != nil {
				slog.Warn("webhook: review key persist failed", "endpoint", ep.Name, "delivery_id", deliveryID, "error", err)
			}
		}
	}

	if strings.TrimSpace(ep.Model) != "" {
		ref, err := relay.ParseModelRef(strings.TrimSpace(ep.Model))
		if err != nil {
			slog.Error("webhook: malformed endpoint model", "endpoint", ep.Name, "platform", ep.Platform, "channel_id", ep.ChannelID)
			s.failDelivery(ep, id, deliveryID, eventType, envelope, "invalid endpoint model", workCtx)
			return nil
		}
		workCtx.Model = &ref
		workCtx.ModelSource = "endpoint"
		envelope["model"] = relay.FormatModelRef(ref)
		envelope["model_source"] = "endpoint"
	} else if s.channels != nil {
		ch, err := s.channels.Get(context.Background(), ep.Platform, ep.ChannelID)
		if err != nil {
			slog.Error("webhook: channel repo get failed", "endpoint", ep.Name, "channel_id", ep.ChannelID, "error", err)
			s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary("channel configuration error: "+err.Error(), maxErrorSummaryRunes, ep.Secret), workCtx)
			return nil
		}
		if ch != nil && strings.TrimSpace(ch.Model) != "" {
			ref, err := relay.ParseModelRef(strings.TrimSpace(ch.Model))
			if err != nil {
				slog.Error("webhook: malformed channel model", "endpoint", ep.Name, "platform", ep.Platform, "channel_id", ep.ChannelID)
				s.failDelivery(ep, id, deliveryID, eventType, envelope, "invalid channel model", workCtx)
				return nil
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
		return nil
	}

	rendered = strings.ReplaceAll(rendered, "</untrusted_payload>", "&lt;/untrusted_payload&gt;")
	wrapped := fmt.Sprintf("<untrusted_payload>\n%s\n</untrusted_payload>", rendered)

	ctx, cancel := context.WithTimeout(context.Background(), s.processingTimeout)
	defer cancel()
	if workCtx.PRKey != "" {
		phase := PhaseForWorkflow(ep.Workflow)
		if rootID, _ := s.syncPhaseParent(ctx, ep, workCtx.PRKey, envelope, phase); rootID != "" {
			workCtx.RootMessageID = rootID
		}
	}
	if workCtx.PRKey != "" && workCtx.SessionID == "" && s.prQueues != nil {
		if row, rerr := s.prQueues.Get(context.Background(), workCtx.PRKey); rerr == nil && row != nil && strings.TrimSpace(row.ActiveSession) != "" {
			workCtx.SessionID = strings.TrimSpace(row.ActiveSession)
		}
	}
	fixHeadBefore := ""
	if isFixWorkflow(ep.Workflow) && strings.TrimSpace(workCtx.PRKey) != "" {
		fixHeadBefore = normalizeRevision(stringValue(envelope["head_sha"]))
	}
	err = s.executor(ctx, ep.Platform, ep.ChannelID, wrapped, workCtx)
	s.prMirrorSync(workCtx)
	if err == nil && fixHeadBefore != "" {
		s.queueReviewAfterFix(workCtx.PRKey, fixHeadBefore)
	}

	if run.watchdogTookOver() {
		if aborted, abortOK := run.abortedState(); aborted {
			slog.Error("webhook: delivery aborted by watchdog",
				"endpoint", ep.Name,
				"delivery_id", deliveryID,
				"session_id", workCtx.SessionID,
				"session_abort_ok", abortOK,
			)
		}
		if itemPRKey := strings.TrimSpace(workCtx.PRKey); itemPRKey != "" && err != nil && !errors.Is(err, relay.ErrWebhookResponseIncomplete) {
			s.planRetry(ep, itemPRKey, id, err, workCtx, false, true)
		}
		return err
	}

	if err == nil && strings.EqualFold(strings.TrimSpace(ep.Workflow), "merge") && strings.TrimSpace(workCtx.PRKey) != "" {
		if state, lookupErr := s.resolveLivePRState(ctx, workCtx.PRKey); lookupErr != nil || state != PRStateMerged {
			slog.Warn("webhook: merge not confirmed",
				"endpoint", ep.Name,
				"delivery_id", deliveryID,
				"pr_key", workCtx.PRKey,
				"state", state,
				"error", lookupErr,
			)
		} else {
			s.markMergeConfirmed(ctx, ep, envelope, workCtx)
		}
	}

	if itemPRKey := strings.TrimSpace(workCtx.PRKey); err != nil && itemPRKey != "" && !errors.Is(err, relay.ErrWebhookResponseIncomplete) {
		timedOut := errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded
		if d := s.dispatcherFor(itemPRKey); d != nil {
			if op := d.takeIntent(); op == wakeCancel {
				s.markSkipped(id, ep, envelope, "cancelled by operator")
				return errOpCancelled
			} else if op == wakeSkip {
				s.markSkipped(id, ep, envelope, "skipped by operator")
				return errOpSkipped
			}
		}
		if s.planRetry(ep, itemPRKey, id, err, workCtx, timedOut, false) {
			return errRetryScheduled
		}
	}

	switch {
	case err == nil:
		card := s.terminalCard(ep, envelope, "COMPLETED", "", workCtx)
		ok, tErr := s.deliveries.Transition(context.Background(), id, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusCompleted, "")
		if tErr != nil {
			slog.Error("webhook: completed transition failed", "endpoint", ep.Name, "delivery_id", deliveryID, "error", tErr)
		} else if ok {
			s.emitAudit(context.Background(), ep, envelope, "COMPLETED", "", card, workCtx)
		}
		slog.Info("webhook: delivery completed",
			append([]any{
				"endpoint", ep.Name,
				"delivery_id", deliveryID,
				"event_type", eventType,
				"attempt", attempt,
				"execution_key", workCtx.Key.String(),
				"worktree", workCtx.Worktree,
			}, sessionLogAttrs(workCtx)...)...)
		s.recordTaskThread(ep, envelope, workCtx, StatusForWorkflow(ep.Workflow))
		s.scheduleDeliveryFallback(ep, workCtx)
		if s.sessions != nil && workCtx.ThreadID != "" && workCtx.SessionID != "" {
			seed := redactAuditSummary(formatAuditSummary(envelope, ep.Workflow, "COMPLETED", ""), ep.Secret)
			if err := s.sessions.MarkTakeoverEligible(context.Background(), ep.Platform, ep.ChannelID, workCtx.ThreadID, workCtx.SessionID, workCtx.AgentPID, seed); err != nil {
				slog.Warn("webhook: takeover mark failed", "endpoint", ep.Name, "delivery_id", deliveryID, "error", err)
			}
		}
		s.linkRootCard(ep, workCtx, deliveryID, card)
		return nil
	case errors.Is(err, context.DeadlineExceeded), ctx.Err() == context.DeadlineExceeded:
		s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary(timeoutSummary(s.processingTimeout, s.stallFreshness, workCtx), maxErrorSummaryRunes, ep.Secret), workCtx)
	default:
		// Self-heal coordination: an incomplete response is not written
		// terminal yet. The dispatcher owns the one-shot decision; when it
		// grants the retry the receipt stays processing and this attempt
		// unwinds so the retry loop can re-execute with a fresh session.
		if errors.Is(err, relay.ErrWebhookResponseIncomplete) && allowRetryIncomplete != nil && allowRetryIncomplete() {
			summary := redactSummary(err.Error(), maxErrorSummaryRunes, ep.Secret)
			slog.Warn("webhook: incomplete response, retrying once",
				append([]any{
					"endpoint", ep.Name,
					"delivery_id", deliveryID,
					"event_type", eventType,
					"attempt", attempt,
					"execution_key", workCtx.Key.String(),
					"worktree", workCtx.Worktree,
					"error_summary", summary,
				}, sessionLogAttrs(workCtx)...)...)
			return err
		}
		s.failDelivery(ep, id, deliveryID, eventType, envelope, redactSummary(err.Error(), maxErrorSummaryRunes, ep.Secret), workCtx)
	}
	return err
}

func takeoverBannerTarget(ep config.EndpointConfig, workCtx *WebhookWorkContext) string {
	if workCtx == nil {
		return ep.ChannelID
	}
	if ep.Platform == "discord" && workCtx.ThreadID != "" {
		return workCtx.ThreadID
	}
	if ep.Platform == "telegram" && workCtx.ThreadID != "" && !strings.Contains(ep.ChannelID, ":") {
		return ep.ChannelID + ":" + workCtx.ThreadID
	}
	return ep.ChannelID
}

func (s *Server) terminalCard(ep config.EndpointConfig, envelope WebhookEnvelope, status, reason string, workCtx *WebhookWorkContext) string {
	elapsed := time.Duration(0)
	threadID := ""
	if workCtx != nil {
		if !workCtx.StartTime.IsZero() {
			elapsed = time.Since(workCtx.StartTime)
		}
		threadID = workCtx.ThreadID
	}
	return redactAuditSummary(terminalRootCard(envelope, ep.Workflow, status, reason, threadID, ep.Platform, elapsed), ep.Secret)
}

func (s *Server) linkRootCard(ep config.EndpointConfig, workCtx *WebhookWorkContext, deliveryID, card string) {
	if s.sessions == nil || workCtx == nil || workCtx.ThreadID == "" || workCtx.RootMessageID == "" {
		return
	}
	target := auditTargetChannel(ep.Platform, ep.ChannelID, workCtx.ThreadID)
	if err := s.sessions.LinkThreadRoot(context.Background(), ep.Platform, ep.ChannelID, workCtx.ThreadID, workCtx.RootMessageID, target, card); err != nil {
		slog.Warn("webhook: root card link failed", "endpoint", ep.Name, "delivery_id", deliveryID, "error", err)
	}
}

func (s *Server) failUnboundRun(ep config.EndpointConfig, id int64, envelope WebhookEnvelope, prKey, summary string) {
	ok, err := s.deliveries.Transition(context.Background(), id, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusFailed, summary)
	if err != nil {
		slog.Error("webhook: unbound run transition failed", "endpoint", ep.Name, "error", err)
	}
	if !ok {
		return
	}
	repo, number, found := splitPRKey(prKey)
	if !found {
		repo = strings.TrimSpace(stringValue(envelope["repository"]))
		number = strings.TrimSpace(stringValue(envelope["pr_number"]))
	}
	label := strings.TrimSpace(repo)
	if number != "" {
		label += "#" + number
	}
	text := "⚠️ RUN REFUSED — " + label + " · " + strings.TrimPrefix(summary, "run refused: ")
	if s.notifier != nil {
		if nErr := s.notifier(context.Background(), ep.Platform, ep.ChannelID, text); nErr != nil {
			slog.Warn("webhook: unbound run notice failed", "endpoint", ep.Name, "pr_key", prKey, "error", nErr)
		}
	}
}

func (s *Server) failDelivery(ep config.EndpointConfig, id int64, deliveryID, eventType string, envelope WebhookEnvelope, summary string, workCtx *WebhookWorkContext) {
	card := s.terminalCard(ep, envelope, "FAILED", summary, workCtx)
	s.recordTaskThread(ep, envelope, workCtx, TaskFailed)
	s.prMirrorSync(workCtx)
	s.scheduleDeliveryFallback(ep, workCtx)
	ok, err := s.deliveries.Transition(context.Background(), id, []store.WebhookStatus{store.WebhookStatusProcessing}, store.WebhookStatusFailed, summary)
	if err != nil {
		slog.Error("webhook: failed transition", "endpoint", ep.Name, "delivery_id", deliveryID, "error", err)
	} else if ok {
		s.emitAudit(context.Background(), ep, envelope, "FAILED", summary, card, workCtx)
	}
	if s.sessions != nil && workCtx != nil && workCtx.ThreadID != "" {
		seed := redactAuditSummary(formatAuditSummary(envelope, ep.Workflow, "FAILED", summary), ep.Secret)
		if err := s.sessions.MarkTakeoverEligible(context.Background(), ep.Platform, ep.ChannelID, workCtx.ThreadID, workCtx.SessionID, workCtx.AgentPID, seed); err != nil {
			slog.Warn("webhook: takeover mark failed", "endpoint", ep.Name, "delivery_id", deliveryID, "error", err)
		}
	}
	s.linkRootCard(ep, workCtx, deliveryID, card)
	if s.notifier != nil && workCtx != nil && workCtx.ThreadID != "" {
		target := takeoverBannerTarget(ep, workCtx)
		banner := "💬 Delivery " + deliveryID + " failed (" + summary + "). Continue in this thread to resume from the last context."
		if err := s.notifier(context.Background(), ep.Platform, target, banner); err != nil {
			slog.Warn("webhook: takeover banner failed", "endpoint", ep.Name, "delivery_id", deliveryID, "error", err)
		}
	}
	slog.Warn("webhook: delivery failed",
		append([]any{
			"endpoint", ep.Name,
			"delivery_id", deliveryID,
			"event_type", eventType,
			"attempt", workCtx.Attempt,
			"execution_key", workCtx.Key.String(),
			"worktree", workCtx.Worktree,
			"error_summary", summary,
		}, sessionLogAttrs(workCtx)...)...)
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
	return status == store.WebhookStatusCompleted
}

func isReopenable(status store.WebhookStatus) bool {
	switch status {
	case store.WebhookStatusSkipped, store.WebhookStatusRejected, store.WebhookStatusFailed:
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
