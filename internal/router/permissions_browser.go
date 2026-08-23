package router

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

const (
	permCallbackPrefix  = "perm:"
	maxPermissionRules  = 30
	permissionsPerPage  = 6
	permissionsMaxPages = 5
	emptyPermissionsMsg = `Tidak ada rule tersimpan. Pakai tombol "Always allow" biar tersimpan.`
)

// registerPermissionCommand keeps the /permissions command registration with
// its browser implementation instead of the shared command table.
func (r *Router) registerPermissionCommand() {
	r.commands["permissions"] = Command{
		Name:    "permissions",
		Handler: r.handlePermissions,
	}
}

func (r *Router) handlePermissions(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	parts := strings.Fields(args)
	if len(parts) > 0 {
		switch parts[0] {
		case "delete":
			if len(parts) < 2 {
				return "Usage: /permissions delete <id>", nil
			}
			return r.permissionsDelete(ctx, msg, parts[1])
		case "clear":
			if err := r.renderPermissionClearConfirm(ctx, msg, 1, false); err != nil {
				return "", err
			}
			return "", errReplied
		}
	}
	return r.renderPermissionsPage(ctx, msg, 1)
}

// permissionOwnerFingerprint is a short, stable hash of the conversation key
// embedded in every permissions callback. A press from a different
// conversation carries a different fingerprint for the same rule id, so a
// stale or forwarded button can never delete (or reveal) another
// conversation's rules — the owner-scoped store queries are the backstop,
// this is the cheap first gate.
func permissionOwnerFingerprint(owner store.PermissionOwner) string {
	sum := sha256.Sum256([]byte(owner.Platform + "|" + owner.ChannelID + "|" + owner.ThreadID + "|" + owner.UserID))
	return hex.EncodeToString(sum[:8])
}

// permissionCallbackValid reports whether a callback action carries the
// fingerprint of the conversation it was rendered for.
func permissionCallbackValid(msg channel.IncomingMessage, action string) bool {
	idx := strings.LastIndex(action, ":")
	if idx <= 0 {
		return false
	}
	return action[idx+1:] == permissionOwnerFingerprint(permissionOwnerFromMsg(msg))
}

func (r *Router) handlePermissionCallback(ctx context.Context, msg channel.IncomingMessage) error {
	action := strings.TrimPrefix(msg.CallbackData, permCallbackPrefix)
	if !permissionCallbackValid(msg, action) {
		slog.Info("permissions callback rejected", "platform", msg.Platform, "channel_id", msg.ChannelID, "thread_id", msg.ThreadID, "user_id", msg.UserID, "outcome", "owner_mismatch")
		return nil
	}
	body := action[:strings.LastIndex(action, ":")]
	switch {
	case strings.HasPrefix(body, "del:"):
		return r.handlePermissionDeleteCallback(ctx, msg, strings.TrimPrefix(body, "del:"))
	case strings.HasPrefix(body, "det:"):
		return r.handlePermissionDetailsCallback(ctx, msg, body)
	case body == "clear:confirm":
		return r.handlePermissionCleared(ctx, msg)
	case strings.HasPrefix(body, "clear:"):
		page := 1
		if p, err := strconv.Atoi(strings.TrimPrefix(body, "clear:")); err == nil && p >= 1 {
			page = p
		}
		return r.renderPermissionClearConfirm(ctx, msg, page, true)
	case strings.HasPrefix(body, "page:"):
		return r.handlePermissionPageCallback(ctx, msg, strings.TrimPrefix(body, "page:"))
	}
	return nil
}

func (r *Router) renderPermissionsPage(ctx context.Context, msg channel.IncomingMessage, page int) (string, error) {
	text, buttons, err := r.buildPermissionsPage(ctx, msg, page)
	if err != nil {
		return "", err
	}
	if msg.ReplyCtx != nil {
		if _, err := msg.ReplyCtx.SendWithButtons(text, buttons); err != nil {
			return "", err
		}
		return "", errReplied
	}
	return text, nil
}

// permissionContextHeader renders the saved-count and the conversation's
// project/workdir context. The project is derived from the effective
// workdir; when the context is unavailable it reports "unknown" instead of
// guessing.
func (r *Router) permissionContextHeader(ctx context.Context, msg channel.IncomingMessage, ruleCount int) string {
	workdir, err := r.effectiveWorkdir(ctx, msg)
	project := "unknown"
	if err == nil && workdir != "" {
		project = filepath.Base(filepath.Clean(workdir))
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "🔐 Saved permissions: %d\n", ruleCount)
	fmt.Fprintf(&sb, "📦 Project: %s", project)
	if err == nil && workdir != "" {
		fmt.Fprintf(&sb, "\n📂 Workdir: %s", workdir)
	}
	return sb.String()
}

func (r *Router) buildPermissionsPage(ctx context.Context, msg channel.IncomingMessage, page int) (string, []channel.Button, error) {
	owner := permissionOwnerFromMsg(msg)
	rules, err := r.store.PermissionRuleRepo().ListByOwner(ctx, owner)
	if err != nil {
		return "", nil, fmt.Errorf("permissions list: %w", err)
	}
	if len(rules) == 0 {
		return emptyPermissionsMsg, nil, nil
	}
	if len(rules) > maxPermissionRules {
		rules = rules[:maxPermissionRules]
	}

	totalPages := permissionPagesTotal(len(rules))
	start, end, clampedPage := permissionPageBounds(len(rules), page)

	header := r.permissionContextHeader(ctx, msg, len(rules))
	header += fmt.Sprintf("\nPage %d/%d", clampedPage, totalPages)

	var sb strings.Builder
	sb.WriteString(header)
	sb.WriteString("\n")

	fp := permissionOwnerFingerprint(owner)
	count := end - start
	navRow := permissionNavRow(msg.Platform, count)
	var buttons []channel.Button
	for i := start; i < end; i++ {
		rule := rules[i]
		num := i + 1
		desc := permissionRuleDescription(rule)
		row := permissionRuleRow(msg.Platform, i-start)
		fmt.Fprintf(&sb, "%d · %s\n   🕒 %s\n", num, desc, permissionAgeLabel(rule.CreatedAt))
		buttons = append(buttons,
			channel.Button{Label: "🔍 Details", Value: fmt.Sprintf("%sdet:%d:%d:%s", permCallbackPrefix, rule.ID, clampedPage, fp), Row: row},
			channel.Button{Label: "🗑 Delete", Value: fmt.Sprintf("%sdel:%d:%s", permCallbackPrefix, rule.ID, fp), Row: row},
		)
	}

	if clampedPage > 1 {
		buttons = append(buttons, channel.Button{Label: "◀️ Prev", Value: fmt.Sprintf("%spage:%d:%s", permCallbackPrefix, clampedPage-1, fp), Row: navRow})
	}
	if clampedPage < totalPages {
		buttons = append(buttons, channel.Button{Label: "Next ▶️", Value: fmt.Sprintf("%spage:%d:%s", permCallbackPrefix, clampedPage+1, fp), Row: navRow})
	}
	buttons = append(buttons, channel.Button{Label: "❌ Clear all", Value: fmt.Sprintf("%sclear:%d:%s", permCallbackPrefix, clampedPage, fp), Row: navRow + 1})

	return strings.TrimRight(sb.String(), "\n"), buttons, nil
}

// permissionRuleRow maps a rule index to its button row so each rule's
// Details + Delete pair stays together. Discord caps messages at 5 action
// rows with 5 buttons per row, so two rule pairs (4 buttons) share a row
// there; Telegram has no row cap, so each rule gets its own row.
func permissionRuleRow(platform string, index int) int {
	if platform == "discord" {
		return index/2 + 1
	}
	return index + 1
}

// permissionNavRow places Prev/Next just below the last rule row; clear-all
// confirmation sits one row further down.
func permissionNavRow(platform string, ruleCount int) int {
	if ruleCount <= 0 {
		return 1
	}
	return permissionRuleRow(platform, ruleCount-1) + 1
}

// permissionAgeLabel renders a rule's age like the session picker's
// relativeAge, except fresh rules read "just now" per the manager UX.
func permissionAgeLabel(createdAt int64) string {
	if createdAt <= 0 {
		return "just now"
	}
	d := time.Since(time.Unix(createdAt, 0))
	if d < 0 {
		d = 0
	}
	if d < time.Minute {
		return "just now"
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	return fmt.Sprintf("%dd ago", int(d.Hours()/24))
}

func (r *Router) handlePermissionPageCallback(ctx context.Context, msg channel.IncomingMessage, pageStr string) error {
	page, err := strconv.Atoi(pageStr)
	if err != nil {
		page = 1
	}
	text, buttons, err := r.buildPermissionsPage(ctx, msg, page)
	if err != nil {
		return err
	}
	if msg.ReplyCtx != nil && msg.CallbackRef != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
	}
	r.reply(msg, text)
	return nil
}

func (r *Router) handlePermissionDetailsCallback(ctx context.Context, msg channel.IncomingMessage, body string) error {
	parts := strings.Split(body, ":")
	if len(parts) != 3 {
		return nil
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil
	}
	page, err := strconv.Atoi(parts[2])
	if err != nil || page < 1 {
		page = 1
	}

	owner := permissionOwnerFromMsg(msg)
	rules, err := r.store.PermissionRuleRepo().ListByOwner(ctx, owner)
	if err != nil {
		return err
	}
	var rule *store.PermissionRule
	for i := range rules {
		if rules[i].ID == id {
			rule = &rules[i]
			break
		}
	}
	if rule == nil {
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "Rule not found.", nil)
		}
		return nil
	}

	text := formatPermissionDetails(*rule)
	fp := permissionOwnerFingerprint(owner)
	buttons := []channel.Button{
		{Label: "⬅️ Back", Value: fmt.Sprintf("%spage:%d:%s", permCallbackPrefix, page, fp), Row: 1},
		{Label: "🗑 Delete", Value: fmt.Sprintf("%sdel:%d:%s", permCallbackPrefix, rule.ID, fp), Row: 1},
	}
	if msg.ReplyCtx != nil && msg.CallbackRef != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
	}
	return nil
}

// formatPermissionDetails reveals the full rule payload — raw tool name,
// every pattern, conversation scope, creation time, and the internal rule
// id — so the list itself can stay free of internal identifiers.
func formatPermissionDetails(rule store.PermissionRule) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "🔍 Rule #%d\n\n", rule.ID)
	fmt.Fprintf(&sb, "Tool: %s (%s)\n", displayTool(rule.Tool), rule.Tool)
	patterns := splitPatterns(rule.Patterns)
	if len(patterns) == 0 {
		sb.WriteString("Patterns: (none)\n")
	} else {
		sb.WriteString("Patterns:\n")
		for _, p := range patterns {
			fmt.Fprintf(&sb, "• %s\n", p)
		}
	}
	fmt.Fprintf(&sb, "Conversation: %s, channel %s", rule.Platform, rule.ChannelID)
	if rule.ThreadID != "" {
		fmt.Fprintf(&sb, ", thread %s", rule.ThreadID)
	}
	if rule.UserID != "" {
		fmt.Fprintf(&sb, ", user %s", rule.UserID)
	}
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "Created: %s UTC (%s)\n", time.Unix(rule.CreatedAt, 0).UTC().Format("2006-01-02 15:04"), permissionAgeLabel(rule.CreatedAt))
	fmt.Fprintf(&sb, "Rule ID: %d", rule.ID)
	return sb.String()
}

func (r *Router) permissionsDelete(ctx context.Context, msg channel.IncomingMessage, idStr string) (string, error) {
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return "Rule id must be a number.", nil
	}
	owner := permissionOwnerFromMsg(msg)
	rules, err := r.store.PermissionRuleRepo().ListByOwner(ctx, owner)
	if err != nil {
		return "", fmt.Errorf("permissions delete: %w", err)
	}
	for _, rule := range rules {
		if rule.ID == id {
			if err := r.store.PermissionRuleRepo().DeleteByID(ctx, owner, id); err != nil {
				return "", fmt.Errorf("permissions delete: %w", err)
			}
			return fmt.Sprintf("🗑 Rule #%d deleted: %s.", rule.ID, permissionRuleDescription(rule)), nil
		}
	}
	return "Rule not found.", nil
}

func (r *Router) handlePermissionDeleteCallback(ctx context.Context, msg channel.IncomingMessage, idStr string) error {
	text, err := r.permissionsDelete(ctx, msg, idStr)
	if err != nil {
		return err
	}
	if msg.ReplyCtx != nil && msg.CallbackRef != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, nil)
	}
	r.reply(msg, text)
	return nil
}

func (r *Router) renderPermissionClearConfirm(ctx context.Context, msg channel.IncomingMessage, page int, edit bool) error {
	fp := permissionOwnerFingerprint(permissionOwnerFromMsg(msg))
	buttons := []channel.Button{
		{Label: "✅ Ya, hapus", Value: permCallbackPrefix + "clear:confirm:" + fp, Row: 1},
		{Label: "⬅️ Batal", Value: fmt.Sprintf("%spage:%d:%s", permCallbackPrefix, page, fp), Row: 1},
	}
	text := "Hapus SEMUA rule?"
	if edit && msg.ReplyCtx != nil && msg.CallbackRef != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
	}
	if msg.ReplyCtx != nil {
		_, err := msg.ReplyCtx.SendWithButtons(text, buttons)
		return err
	}
	return nil
}

func (r *Router) handlePermissionCleared(ctx context.Context, msg channel.IncomingMessage) error {
	owner := permissionOwnerFromMsg(msg)
	if err := r.store.PermissionRuleRepo().ClearByOwner(ctx, owner); err != nil {
		return err
	}
	const text = "🗑 All always-allow rules cleared."
	if msg.ReplyCtx != nil && msg.CallbackRef != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, nil)
	}
	r.reply(msg, text)
	return nil
}

func permissionRuleDescription(rule store.PermissionRule) string {
	return fmt.Sprintf("%s — %s", displayTool(rule.Tool), describePatterns(splitPatterns(rule.Patterns)))
}

func splitPatterns(canonical string) []string {
	if canonical == "" {
		return nil
	}
	return strings.Split(canonical, "|")
}

func permissionOwnerFromMsg(msg channel.IncomingMessage) store.PermissionOwner {
	threadID, userID := conversationKey(msg)
	return store.PermissionOwner{Platform: msg.Platform, ChannelID: msg.ChannelID, ThreadID: threadID, UserID: userID}
}

func permissionPagesTotal(total int) int {
	if total <= 0 {
		return 1
	}
	pages := (total + permissionsPerPage - 1) / permissionsPerPage
	if pages > permissionsMaxPages {
		pages = permissionsMaxPages
	}
	return pages
}

func permissionPageBounds(total, page int) (start, end, clampedPage int) {
	totalPages := permissionPagesTotal(total)
	clampedPage = page
	if clampedPage < 1 {
		clampedPage = 1
	}
	if clampedPage > totalPages {
		clampedPage = totalPages
	}

	if total <= 0 {
		return 0, 0, clampedPage
	}
	start = (clampedPage - 1) * permissionsPerPage
	if start > total {
		start = total
	}
	end = start + permissionsPerPage
	if end > total {
		end = total
	}
	return start, end, clampedPage
}
