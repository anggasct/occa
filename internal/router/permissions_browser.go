package router

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

const (
	permCallbackPrefix    = "perm:"
	maxPermissionRules    = 30
	permissionsPerPage    = 6
	permissionsMaxPages   = 5
	permissionRowMaxRunes = 60
	emptyPermissionsMsg   = `Tidak ada rule tersimpan. Pakai tombol "Always allow" biar tersimpan.`
)

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

func (r *Router) handlePermissionCallback(ctx context.Context, msg channel.IncomingMessage) error {
	action := strings.TrimPrefix(msg.CallbackData, permCallbackPrefix)
	switch {
	case strings.HasPrefix(action, "del:"):
		return r.handlePermissionDeleteCallback(ctx, msg, strings.TrimPrefix(action, "del:"))
	case action == "clear:confirm":
		return r.handlePermissionCleared(ctx, msg)
	case action == "clear" || strings.HasPrefix(action, "clear:"):
		page := 1
		if p, err := strconv.Atoi(strings.TrimPrefix(action, "clear:")); err == nil && p >= 1 {
			page = p
		}
		return r.renderPermissionClearConfirm(ctx, msg, page, true)
	case strings.HasPrefix(action, "page:"):
		return r.handlePermissionPageCallback(ctx, msg, strings.TrimPrefix(action, "page:"))
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

	header := fmt.Sprintf("Saved always-allow rules · Page %d/%d", clampedPage, totalPages)

	var sb strings.Builder
	sb.WriteString(header)
	sb.WriteString("\n")

	var buttons []channel.Button
	for i := start; i < end; i++ {
		rule := rules[i]
		num := i + 1
		desc := permissionRuleDescription(rule)
		fmt.Fprintf(&sb, "%d · %s (%s)\n", num, desc, relativeAge(rule.CreatedAt))
		buttons = append(buttons, channel.Button{
			Label: truncateRunes(fmt.Sprintf("%d · %s", num, desc), permissionRowMaxRunes),
			Value: fmt.Sprintf("%sdel:%d", permCallbackPrefix, rule.ID),
			Row:   1,
		})
	}

	if clampedPage > 1 {
		buttons = append(buttons, channel.Button{Label: "◀️ Prev", Value: fmt.Sprintf("%spage:%d", permCallbackPrefix, clampedPage-1), Row: 2})
	}
	if clampedPage < totalPages {
		buttons = append(buttons, channel.Button{Label: "Next ▶️", Value: fmt.Sprintf("%spage:%d", permCallbackPrefix, clampedPage+1), Row: 2})
	}
	buttons = append(buttons, channel.Button{Label: "❌ Clear all", Value: fmt.Sprintf("%sclear:%d", permCallbackPrefix, clampedPage), Row: 3})

	return strings.TrimRight(sb.String(), "\n"), buttons, nil
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
	buttons := []channel.Button{
		{Label: "✅ Ya, hapus", Value: permCallbackPrefix + "clear:confirm", Row: 1},
		{Label: "⬅️ Batal", Value: fmt.Sprintf("%spage:%d", permCallbackPrefix, page), Row: 1},
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
