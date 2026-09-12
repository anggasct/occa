package webhook

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"

	"github.com/anggasct/occa/internal/relay"
)

// permissionReplier is the narrow client surface the responder needs.
type permissionReplier interface {
	ReplyPermission(ctx context.Context, requestID string, reply relay.PermissionReply) error
}

// permissionResponder implements relay.PermissionPromptHandler for headless
// webhook deliveries. A webhook turn has no human approval surface, so every
// permission request resolves deterministically instead of waiting for a
// button that will never be tapped:
//
//   - external_directory requests whose patterns stay under the endpoint's
//     configured docs root are allowed (project docs are part of every
//     webhook contract: read for reviewer/fixer, read+write for post-merge
//     docs updates);
//   - every other permission request is rejected — the delivery then ends
//     with a real outcome (the agent sees the tool failure) instead of
//     hanging until the delivery timeout.
//
// The docs root comes from endpoint configuration, never from code, so the
// OSS core stays generic; an empty docs root denies everything.
//
// The responder is safe under duplicate delivery (webhook turns forward
// events to their streamer AND handle them in the turn loop): one request ID
// is answered at most once; later duplicate calls are skipped.
type permissionResponder struct {
	mu       sync.Mutex
	resolved map[string]bool
	client   permissionReplier
	docsRoot string
}

// NewPermissionResponder builds the deterministic webhook permission handler
// for one delivery. docsRoot is the canonical project-docs root for the
// hosted endpoint; empty means deny every permission request.
func NewPermissionResponder(client permissionReplier, docsRoot string) relay.PermissionPromptHandler {
	return &permissionResponder{
		client:   client,
		docsRoot: docsRoot,
		resolved: make(map[string]bool),
	}
}

func (h *permissionResponder) Prompt(ctx context.Context, req relay.PermissionRequest) error {
	if !h.markResolved(req.ID) {
		return nil
	}

	reply := relay.PermissionReject
	if h.allowed(req) {
		reply = relay.PermissionAlways
	}
	slog.Debug("webhook: permission resolved", "request_id", req.ID, "permission", req.Permission, "patterns", req.Patterns, "action", reply)
	if err := h.client.ReplyPermission(ctx, req.ID, reply); err != nil {
		return err
	}
	return nil
}

// markResolved returns false when the request ID was already answered (or is
// being answered) so concurrent owners never double-post a reply.
func (h *permissionResponder) markResolved(requestID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.resolved[requestID] {
		return false
	}
	h.resolved[requestID] = true
	return true
}

// allowed returns true only for external_directory requests whose patterns
// stay under the configured docs root. Everything else fails closed.
func (h *permissionResponder) allowed(req relay.PermissionRequest) bool {
	if h.docsRoot == "" || req.Permission != "external_directory" {
		return false
	}
	root := filepath.Clean(h.docsRoot)
	for _, pattern := range req.Patterns {
		if underRoot(pattern, root) {
			return true
		}
	}
	return false
}

// underRoot reports whether an opencode external_directory pattern refers to
// a path inside root. Patterns arrive as canonical paths, optionally with a
// glob suffix (/** or /*).
func underRoot(pattern, root string) bool {
	root = filepath.Clean(root)
	p := strings.TrimSuffix(pattern, "/**")
	p = strings.TrimSuffix(p, "/*")
	p = filepath.Clean(p)
	if p == root {
		return true
	}
	if !strings.HasSuffix(root, "/") {
		root += "/"
	}
	return strings.HasPrefix(p, root)
}
