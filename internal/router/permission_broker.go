package router

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
)

const (
	permissionBatchWindow    = 1500 * time.Millisecond
	permissionTombstoneTTL   = 10 * time.Minute
	permissionRetryMessage   = "⚠️ Could not submit the permission choice. Try again."
	permissionExpiredMessage = "⌛ Permission request expired."
)

type permissionOwner struct{}

type permissionState uint8

const (
	permissionPending permissionState = iota
	permissionHandling
	permissionResolved
	permissionExpired
)

type permissionRecord struct {
	token      string
	owner      *permissionOwner
	client     relay.Client
	platform   string
	channelID  string
	sessionID  string
	requestIDs []string
	reply      channel.ReplyContext
	origin     channel.MessageRef
	buttons    []channel.Button
	state      permissionState
	terminal   string
	createdAt  time.Time
	expiresAt  time.Time
	attempt    uint64
}

type pendingBatch struct {
	owner    *permissionOwner
	handler  *permissionPromptHandler
	requests []relay.PermissionRequest
	timer    *time.Timer
}

type permissionBroker struct {
	mu      sync.Mutex
	records map[string]*permissionRecord
	batches map[*permissionOwner]*pendingBatch
}

type permissionPromptHandler struct {
	broker    *permissionBroker
	owner     *permissionOwner
	encode    func(string) string
	client    relay.Client
	platform  string
	channelID string
	sessionID string
	reply     channel.ReplyContext
}

func newPermissionBroker() *permissionBroker {
	return &permissionBroker{
		records: make(map[string]*permissionRecord),
		batches: make(map[*permissionOwner]*pendingBatch),
	}
}

func (h *permissionPromptHandler) Prompt(ctx context.Context, request relay.PermissionRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	h.broker.mu.Lock()
	batch, exists := h.broker.batches[h.owner]
	if exists {
		batch.requests = append(batch.requests, request)
		h.broker.mu.Unlock()
		return nil
	}

	batch = &pendingBatch{
		owner:    h.owner,
		handler:  h,
		requests: []relay.PermissionRequest{request},
	}
	batch.timer = time.AfterFunc(permissionBatchWindow, func() {
		h.broker.flushBatch(batch)
	})
	h.broker.batches[h.owner] = batch
	h.broker.mu.Unlock()

	return nil
}

func (b *permissionBroker) flushBatch(batch *pendingBatch) {
	b.mu.Lock()
	current, exists := b.batches[batch.owner]
	if !exists || current != batch {
		b.mu.Unlock()
		return
	}
	delete(b.batches, batch.owner)
	b.mu.Unlock()

	h := batch.handler
	requests := batch.requests
	if len(requests) == 0 {
		return
	}

	token, err := permissionToken()
	if err != nil {
		slog.Error("permission: generate token failed", "error", err)
		return
	}

	sessionID := requests[0].SessionID
	if sessionID == "" {
		sessionID = h.sessionID
	}

	requestIDs := make([]string, len(requests))
	for i, req := range requests {
		requestIDs[i] = req.ID
	}

	record := &permissionRecord{
		token:      token,
		owner:      h.owner,
		client:     h.client,
		platform:   h.platform,
		channelID:  h.channelID,
		sessionID:  sessionID,
		requestIDs: requestIDs,
		reply:      h.reply,
		buttons:    permissionButtons(token),
		state:      permissionPending,
		createdAt:  time.Now(),
	}

	b.mu.Lock()
	b.cleanupLocked(time.Now())
	b.records[token] = record
	b.mu.Unlock()

	ref, err := h.reply.SendWithButtons(h.promptText(requests...), record.buttons)
	if err != nil {
		b.removePending(record)
		slog.Warn("permission: send prompt failed", "error", err)
		return
	}
	if ref == nil || ref.ID() == "" {
		b.removePending(record)
		slog.Warn("permission: prompt has no origin reference")
		return
	}

	b.mu.Lock()
	if record.state == permissionPending {
		record.origin = ref
		b.mu.Unlock()
	} else {
		record.origin = ref
		b.mu.Unlock()
		_ = h.reply.EditWithButtons(ref, permissionExpiredMessage, nil)
	}

	slog.Info("permission prompt registered", "platform", record.platform, "channel_id", record.channelID, "count", len(requestIDs))
}

func (b *permissionBroker) handle(ctx context.Context, msg channel.IncomingMessage) error {
	token, decision, ok := parsePermissionCallback(msg.CallbackData)
	if !ok || msg.CallbackRef == nil {
		b.renderExpired(msg)
		return nil
	}

	b.mu.Lock()
	b.cleanupLocked(time.Now())
	record := b.records[token]
	if record == nil || record.origin == nil || record.origin.ID() != msg.CallbackRef.ID() || record.platform != msg.Platform || record.channelID != msg.ChannelID {
		if record != nil && record.origin == nil && record.state == permissionPending {
			record.state = permissionExpired
			record.terminal = permissionExpiredMessage
			record.client = nil
			record.expiresAt = time.Now().Add(permissionTombstoneTTL)
		}
		b.mu.Unlock()
		slog.Info("permission callback rejected", "platform", msg.Platform, "channel_id", msg.ChannelID, "outcome", "scope_mismatch")
		b.renderExpired(msg)
		return nil
	}

	switch record.state {
	case permissionHandling:
		b.mu.Unlock()
		slog.Info("permission callback duplicate", "platform", msg.Platform, "channel_id", msg.ChannelID, "outcome", "handling")
		return nil
	case permissionResolved, permissionExpired:
		reply := record.reply
		origin := record.origin
		terminal := record.terminal
		state := record.state
		b.mu.Unlock()
		slog.Info("permission callback duplicate", "platform", msg.Platform, "channel_id", msg.ChannelID, "outcome", stateName(state))
		if terminal != "" {
			if err := reply.EditWithButtons(origin, terminal, nil); err != nil {
				slog.Warn("permission: reapply terminal view failed", "platform", msg.Platform, "channel_id", msg.ChannelID, "error", err)
			}
		}
		return nil
	}

	record.state = permissionHandling
	record.attempt++
	attempt := record.attempt
	client := record.client
	requestIDs := append([]string(nil), record.requestIDs...)
	reply := record.reply
	origin := record.origin
	buttons := append([]channel.Button(nil), record.buttons...)
	b.mu.Unlock()

	var firstErr error
	var succeededCount int
	for _, reqID := range requestIDs {
		if err := client.ReplyPermission(ctx, reqID, decision); err != nil {
			firstErr = err
			break
		}
		succeededCount++
	}

	// A 404 after at least one successful reply in the same batch means the
	// agent already resolved the whole permission group on the first reply —
	// the remaining IDs are gone server-side and retrying them can never
	// succeed. Treat that as a resolved batch and render the terminal label;
	// retry only when nothing in the batch succeeded (agent gone / transient).
	batchResolved := firstErr == nil ||
		(succeededCount > 0 && errors.Is(firstErr, relay.ErrNotFound))
	if !batchResolved {
		if b.retry(record, attempt, requestIDs[succeededCount:]) {
			slog.Warn("permission callback retryable failure", "platform", msg.Platform, "channel_id", msg.ChannelID, "error", firstErr)
			if updateErr := reply.EditWithButtons(origin, permissionRetryMessage, buttons); updateErr != nil {
				slog.Warn("permission: retry view update failed", "platform", msg.Platform, "channel_id", msg.ChannelID, "error", updateErr)
			}
		}
		return nil
	}

	terminal := permissionTerminalLabel(decision)
	if b.resolve(record, attempt, terminal) {
		slog.Info("permission callback resolved", "platform", msg.Platform, "channel_id", msg.ChannelID, "decision", decision, "latency", time.Since(record.createdAt).Truncate(time.Millisecond))
		if err := reply.EditWithButtons(origin, terminal, nil); err != nil {
			slog.Warn("permission: terminal view update failed", "platform", msg.Platform, "channel_id", msg.ChannelID, "error", err)
		}
	}
	return nil
}

func (b *permissionBroker) expireOwner(owner *permissionOwner) {
	now := time.Now()
	var expired []*permissionRecord
	var origins []channel.MessageRef
	b.mu.Lock()
	b.cleanupLocked(now)

	if batch, ok := b.batches[owner]; ok {
		delete(b.batches, owner)
		if batch.timer != nil {
			batch.timer.Stop()
		}
	}

	for _, record := range b.records {
		if record.owner != owner || (record.state != permissionPending && record.state != permissionHandling) {
			continue
		}
		record.state = permissionExpired
		record.terminal = permissionExpiredMessage
		record.client = nil
		record.expiresAt = now.Add(permissionTombstoneTTL)
		expired = append(expired, record)
		origins = append(origins, record.origin)
	}
	b.mu.Unlock()

	for i, record := range expired {
		slog.Info("permission prompt expired", "platform", record.platform, "channel_id", record.channelID, "outcome", "expired")
		if origin := origins[i]; origin != nil {
			if err := record.reply.EditWithButtons(origin, permissionExpiredMessage, nil); err != nil {
				slog.Warn("permission: expired view update failed", "platform", record.platform, "channel_id", record.channelID, "error", err)
			}
		}
	}
}

func (b *permissionBroker) removePending(record *permissionRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if current := b.records[record.token]; current == record && record.state == permissionPending {
		delete(b.records, record.token)
	}
}

func (b *permissionBroker) retry(record *permissionRecord, attempt uint64, remainingIDs []string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if record.state != permissionHandling || record.attempt != attempt {
		return false
	}
	record.state = permissionPending
	record.requestIDs = append([]string(nil), remainingIDs...)
	record.expiresAt = time.Time{}
	return true
}

func (b *permissionBroker) resolve(record *permissionRecord, attempt uint64, terminal string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if record.state != permissionHandling || record.attempt != attempt {
		return false
	}
	record.state = permissionResolved
	record.terminal = terminal
	record.client = nil
	record.expiresAt = time.Now().Add(permissionTombstoneTTL)
	return true
}

func (b *permissionBroker) cleanupLocked(now time.Time) {
	for token, record := range b.records {
		if (record.state == permissionResolved || record.state == permissionExpired) && !record.expiresAt.After(now) {
			delete(b.records, token)
		}
	}
}

func (b *permissionBroker) renderExpired(msg channel.IncomingMessage) {
	if msg.CallbackRef == nil || msg.ReplyCtx == nil {
		return
	}
	if err := msg.ReplyCtx.EditWithButtons(msg.CallbackRef, permissionExpiredMessage, nil); err != nil {
		slog.Warn("permission: expired callback view failed", "platform", msg.Platform, "channel_id", msg.ChannelID, "error", err)
	}
}

func parsePermissionCallback(data string) (string, relay.PermissionReply, bool) {
	parts := strings.Split(data, ":")
	if len(parts) != 3 || parts[0] != "permission" || parts[1] == "" {
		return "", "", false
	}
	switch parts[2] {
	case "once":
		return parts[1], relay.PermissionOnce, true
	case "always":
		return parts[1], relay.PermissionAlways, true
	case "reject":
		return parts[1], relay.PermissionReject, true
	default:
		return "", "", false
	}
}

func permissionToken() (string, error) {
	buf := make([]byte, 18)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func permissionButtons(token string) []channel.Button {
	return []channel.Button{
		{Label: "✅ Allow once", Value: "permission:" + token + ":once"},
		{Label: "✅ Always allow", Value: "permission:" + token + ":always"},
		{Label: "❌ Deny", Value: "permission:" + token + ":reject"},
	}
}

func (h *permissionPromptHandler) promptText(requests ...relay.PermissionRequest) string {
	text := permissionPromptText(requests...)
	if h.encode == nil {
		return text
	}
	return h.encode(text)
}

func permissionPromptText(requests ...relay.PermissionRequest) string {
	var blocks []string
	for _, req := range requests {
		text := fmt.Sprintf("🔐 Permission requested: %s", req.Permission)
		if req.Tool != "" {
			text += fmt.Sprintf(" (%s)", req.Tool)
		}
		if len(req.Patterns) > 0 {
			text += fmt.Sprintf("\nPath: %s", strings.Join(req.Patterns, ", "))
		}
		blocks = append(blocks, text)
	}
	return strings.Join(blocks, "\n")
}

func permissionTerminalLabel(decision relay.PermissionReply) string {
	switch decision {
	case relay.PermissionOnce:
		return "✅ Allowed once"
	case relay.PermissionAlways:
		return "✅ Always allowed"
	default:
		return "❌ Denied"
	}
}

func stateName(state permissionState) string {
	switch state {
	case permissionResolved:
		return "resolved"
	case permissionExpired:
		return "expired"
	default:
		return "unknown"
	}
}
