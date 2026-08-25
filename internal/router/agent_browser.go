package router

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

const (
	agentCallbackPrefix = "agent:"
	agentsPerPage       = 6
	agentsMaxPages      = 5
	maxTruncatedDescLen = 60
	agentBrowserTTL     = 30 * time.Minute
	agentBrowserCap     = 256
)

var hiddenInternalAgents = map[string]bool{
	"compaction": true,
	"summary":    true,
	"title":      true,
}

type agentActionKind string

const (
	agentActionSwitch        agentActionKind = "switch"
	agentActionPage          agentActionKind = "page"
	agentActionRefresh       agentActionKind = "refresh"
	agentActionDeleteConfirm agentActionKind = "del_confirm"
	agentActionDeleteCancel  agentActionKind = "del_cancel"
)

type agentBrowseAction struct {
	kind      agentActionKind
	page      int
	agentName string
	ownerFP   string
	createdAt time.Time
}

type agentBrowserBroker struct {
	mu          sync.Mutex
	tokens      map[string]agentBrowseAction
	ownerTokens map[string][]string
}

func newAgentBrowserBroker() *agentBrowserBroker {
	return &agentBrowserBroker{
		tokens:      make(map[string]agentBrowseAction),
		ownerTokens: make(map[string][]string),
	}
}

func (b *agentBrowserBroker) revokeOwner(ownerFP string) {
	if ownerFP == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, tok := range b.ownerTokens[ownerFP] {
		delete(b.tokens, tok)
	}
	delete(b.ownerTokens, ownerFP)
}

func (b *agentBrowserBroker) register(action agentBrowseAction) (string, error) {
	action.createdAt = time.Now()
	buf := make([]byte, 9)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(buf)

	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.tokens) >= agentBrowserCap {
		var oldest string
		var oldestAt time.Time
		for t, a := range b.tokens {
			if oldest == "" || a.createdAt.Before(oldestAt) {
				oldest, oldestAt = t, a.createdAt
			}
		}
		delete(b.tokens, oldest)
	}
	b.tokens[token] = action
	if action.ownerFP != "" {
		b.ownerTokens[action.ownerFP] = append(b.ownerTokens[action.ownerFP], token)
	}
	return token, nil
}

func (b *agentBrowserBroker) lookup(token string) (agentBrowseAction, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	action, ok := b.tokens[token]
	if !ok {
		return agentBrowseAction{}, false
	}
	if time.Since(action.createdAt) > agentBrowserTTL {
		delete(b.tokens, token)
		return agentBrowseAction{}, false
	}
	return action, true
}

type agentTracker struct {
	mu          sync.Mutex
	knownAgents map[string]map[string]bool
	retryDelay  time.Duration
}

func newAgentTracker() *agentTracker {
	return &agentTracker{
		knownAgents: make(map[string]map[string]bool),
		retryDelay:  2 * time.Second,
	}
}

func (t *agentTracker) snapshotDir(workdir string) []string {
	dir := filepath.Join(workdir, ".opencode", "agent")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".md") {
			agentName := strings.TrimSuffix(name, ".md")
			if isValidAgentName(agentName) {
				names = append(names, agentName)
			}
		}
	}
	return names
}

func (t *agentTracker) snapshotWorkdir(workdir string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.knownAgents[workdir]; exists {
		return
	}
	current := t.snapshotDir(workdir)
	known := make(map[string]bool)
	for _, name := range current {
		known[name] = true
	}
	t.knownAgents[workdir] = known
}

func (t *agentTracker) updateAndDiff(workdir string, current []string) []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	known, exists := t.knownAgents[workdir]
	if !exists {
		known = make(map[string]bool)
		t.knownAgents[workdir] = known
	}

	var added []string
	currMap := make(map[string]bool)
	for _, name := range current {
		currMap[name] = true
		if !known[name] {
			added = append(added, name)
		}
	}
	t.knownAgents[workdir] = currMap
	return added
}

func agentOwnerFingerprint(msg channel.IncomingMessage) string {
	threadID, userID := conversationKey(msg)
	sum := sha256.Sum256([]byte(msg.Platform + "|" + msg.ChannelID + "|" + threadID + "|" + userID))
	return hex.EncodeToString(sum[:8])
}

func isValidAgentName(name string) bool {
	if name == "" || strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
		return false
	}
	return filepath.Base(name) == name
}

func filterAgents(all []relay.AgentInfo) (switchable []relay.AgentInfo, subagents []string) {
	for _, a := range all {
		if hiddenInternalAgents[a.Name] {
			continue
		}
		switch a.Mode {
		case "primary":
			switchable = append(switchable, a)
		case "subagent":
			subagents = append(subagents, a.Name)
		default:
		}
	}
	return switchable, subagents
}

func validateAndRemoveProjectAgentFile(workdir, agentName string, remove bool) error {
	if !isValidAgentName(agentName) {
		return errors.New("invalid agent name")
	}

	workdirFd, err := syscall.Open(workdir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer func() { _ = syscall.Close(workdirFd) }()

	opencodeFd, err := syscall.Openat(workdirFd, ".opencode", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer func() { _ = syscall.Close(opencodeFd) }()

	agentFd, err := syscall.Openat(opencodeFd, "agent", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer func() { _ = syscall.Close(agentFd) }()

	fileName := agentName + ".md"
	fileFd, err := syscall.Openat(agentFd, fileName, syscall.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(fileFd, &st); err != nil {
		_ = syscall.Close(fileFd)
		return err
	}
	_ = syscall.Close(fileFd)

	if st.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return errors.New("not a regular file")
	}

	if remove {
		if err := syscall.Unlinkat(agentFd, fileName); err != nil {
			return err
		}
	}

	return nil
}

func (r *Router) handleAgent(ctx context.Context, msg channel.IncomingMessage, args string) (string, error) {
	parts := strings.Fields(args)
	if len(parts) == 0 || parts[0] == "list" || parts[0] == "refresh" {
		return r.renderAgentPicker(ctx, msg)
	}

	if page, err := strconv.Atoi(parts[0]); err == nil && page >= 1 {
		return r.renderAgentPickerPage(ctx, msg, page)
	}

	switch parts[0] {
	case "switch":
		if len(parts) < 2 {
			return "Usage: /agent switch <n|name>", nil
		}
		target := strings.Join(parts[1:], " ")
		return r.switchAgent(ctx, msg, target)
	case "delete":
		if len(parts) < 2 {
			return "Usage: /agent delete <n|name>", nil
		}
		target := strings.Join(parts[1:], " ")
		return r.handleAgentDeleteCommand(ctx, msg, target)
	default:
		return r.switchAgent(ctx, msg, strings.Join(parts, " "))
	}
}

func (r *Router) renderAgentPicker(ctx context.Context, msg channel.IncomingMessage, headerPrefix ...string) (string, error) {
	return r.renderAgentPickerPage(ctx, msg, 1, headerPrefix...)
}

func (r *Router) renderAgentPickerPage(ctx context.Context, msg channel.IncomingMessage, page int, headerPrefix ...string) (string, error) {
	text, buttons, err := r.buildAgentPickerPage(ctx, msg, page, headerPrefix...)
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

func (r *Router) buildAgentPickerPage(ctx context.Context, msg channel.IncomingMessage, page int, headerPrefix ...string) (string, []channel.Button, error) {
	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return "⚠️ Agent unreachable", nil, nil
	}
	defer inst.End()

	threadID, userID := conversationKey(msg)
	activeSession, err := r.findActiveSession(ctx, msg.Platform, msg.ChannelID, threadID, userID)
	if err != nil {
		return "", nil, fmt.Errorf("agent picker: %w", err)
	}
	if activeSession == nil {
		return "No active session yet. Send a message first to start a conversation, then use /agent to switch agents.", nil, nil
	}

	fp := agentOwnerFingerprint(msg)
	r.agentBrowser.revokeOwner(fp)

	allAgents, err := inst.Client().ListAgents(ctx)
	if err != nil {
		if errors.Is(err, relay.ErrUnsupported) {
			return "⚠️ Agent switching is not supported by the current agent backend.", nil, nil
		}
		tok, _ := r.agentBrowser.register(agentBrowseAction{
			kind:    agentActionRefresh,
			page:    1,
			ownerFP: fp,
		})
		retryButton := channel.Button{
			Label: "⟳ Refresh",
			Value: fmt.Sprintf("%s%s", agentCallbackPrefix, tok),
			Row:   1,
		}
		return "⚠️ Agents unavailable — agent server not responding", []channel.Button{retryButton}, nil
	}

	activeAgentName := "build"
	if sessInfo, err := inst.Client().GetSession(ctx, activeSession.AgentSessionID); err == nil && sessInfo != nil && sessInfo.Agent != "" {
		activeAgentName = sessInfo.Agent
	}

	providers, _ := inst.Client().Providers(ctx)

	switchable, subagents := filterAgents(allAgents)

	totalPages := (len(switchable) + agentsPerPage - 1) / agentsPerPage
	if totalPages < 1 {
		totalPages = 1
	}
	if totalPages > agentsMaxPages {
		totalPages = agentsMaxPages
	}

	clampedPage := page
	if clampedPage < 1 {
		clampedPage = 1
	}
	if clampedPage > totalPages {
		clampedPage = totalPages
	}

	start := (clampedPage - 1) * agentsPerPage
	end := start + agentsPerPage
	if start > len(switchable) {
		start = len(switchable)
	}
	if end > len(switchable) {
		end = len(switchable)
	}

	var sb strings.Builder
	if len(headerPrefix) > 0 && headerPrefix[0] != "" {
		sb.WriteString(headerPrefix[0])
		sb.WriteString("\n\n")
	}
	fmt.Fprintf(&sb, "Page %d/%d · Agents\n", clampedPage, totalPages)

	activeDisplay := activeAgentName
	if activeAgentName == "build" {
		activeDisplay = "build (default)"
	} else {
		for _, a := range switchable {
			if a.Name == activeAgentName && a.Model != nil && a.Model.ID != "" {
				if a.Model.ProviderID != "" {
					activeDisplay = fmt.Sprintf("%s (%s/%s)", a.Name, a.Model.ProviderID, a.Model.ID)
				} else {
					activeDisplay = fmt.Sprintf("%s (%s)", a.Name, a.Model.ID)
				}
				break
			}
		}
	}
	fmt.Fprintf(&sb, "Active: %s\n\n", activeDisplay)

	var buttons []channel.Button
	var anyUnknownModel bool
	for i := start; i < end; i++ {
		a := switchable[i]
		num := i + 1
		marker := "  "
		if a.Name == activeAgentName {
			marker = "→ "
		}

		nameLabel := a.Name
		if !a.Native {
			nameLabel += " [custom]"
		}

		desc := cleanOneLine(a.Description, maxTruncatedDescLen)

		modelSuffix := ""
		if a.Model != nil && a.Model.ID != "" {
			if !providers.HasConnectedModel(*a.Model) {
				anyUnknownModel = true
				modelSuffix = fmt.Sprintf(" — %s ⚠", a.Model.ID)
			} else {
				modelSuffix = fmt.Sprintf(" — %s", a.Model.ID)
			}
		}

		if desc != "" {
			fmt.Fprintf(&sb, "%d. %s%s — %s%s\n", num, marker, nameLabel, desc, modelSuffix)
		} else {
			fmt.Fprintf(&sb, "%d. %s%s%s\n", num, marker, nameLabel, modelSuffix)
		}

		tok, _ := r.agentBrowser.register(agentBrowseAction{
			kind:      agentActionSwitch,
			agentName: a.Name,
			ownerFP:   fp,
		})
		row := agentButtonRow(msg.Platform, i-start)
		buttons = append(buttons, channel.Button{
			Label: fmt.Sprintf("%d", num),
			Value: fmt.Sprintf("%s%s", agentCallbackPrefix, tok),
			Row:   row,
		})
	}

	if len(subagents) > 0 {
		fmt.Fprintf(&sb, "\nSubagents (info only): %s\n", strings.Join(subagents, ", "))
	}

	if anyUnknownModel {
		sb.WriteString("\n⚠ Pinned model not found in connected providers\n")
	}

	if len(switchable) > agentsMaxPages*agentsPerPage {
		fmt.Fprintf(&sb, "\nShowing %d of %d agents. Use /agent switch <name> for others.\n", agentsMaxPages*agentsPerPage, len(switchable))
	}

	sb.WriteString("\n💡 Ask in chat to create new agents (e.g. \"create an agent named reviewer that...\")")

	navRow := agentNavRow(msg.Platform, end-start)
	if clampedPage > 1 {
		tok, _ := r.agentBrowser.register(agentBrowseAction{
			kind:    agentActionPage,
			page:    clampedPage - 1,
			ownerFP: fp,
		})
		buttons = append(buttons, channel.Button{
			Label: "◀️ Prev",
			Value: fmt.Sprintf("%s%s", agentCallbackPrefix, tok),
			Row:   navRow,
		})
	}
	if clampedPage < totalPages {
		tok, _ := r.agentBrowser.register(agentBrowseAction{
			kind:    agentActionPage,
			page:    clampedPage + 1,
			ownerFP: fp,
		})
		buttons = append(buttons, channel.Button{
			Label: "Next ▶️",
			Value: fmt.Sprintf("%s%s", agentCallbackPrefix, tok),
			Row:   navRow,
		})
	}
	refreshTok, _ := r.agentBrowser.register(agentBrowseAction{
		kind:    agentActionRefresh,
		page:    clampedPage,
		ownerFP: fp,
	})
	buttons = append(buttons, channel.Button{
		Label: "⟳ Refresh",
		Value: fmt.Sprintf("%s%s", agentCallbackPrefix, refreshTok),
		Row:   navRow,
	})

	return strings.TrimRight(sb.String(), "\n"), buttons, nil
}

func agentButtonRow(platform string, index int) int {
	if platform == "discord" {
		return index/5 + 1
	}
	return index/2 + 1
}

func agentNavRow(platform string, count int) int {
	if count <= 0 {
		return 1
	}
	return agentButtonRow(platform, count-1) + 1
}

func cleanOneLine(s string, maxRunes int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	runes := []rune(s)
	if len(runes) > maxRunes {
		return string(runes[:maxRunes-1]) + "…"
	}
	return s
}

func (r *Router) findActiveSession(ctx context.Context, platform, channelID, threadID, userID string) (*store.Session, error) {
	sessions, err := r.store.SessionRepo().ListConversation(ctx, platform, channelID, threadID, userID)
	if err != nil {
		return nil, err
	}
	for i := range sessions {
		if sessions[i].Active {
			return &sessions[i], nil
		}
	}
	return nil, nil
}

func (r *Router) switchAgent(ctx context.Context, msg channel.IncomingMessage, target string) (string, error) {
	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return "⚠️ Agent unreachable", nil
	}
	defer inst.End()

	threadID, userID := conversationKey(msg)
	activeSession, err := r.findActiveSession(ctx, msg.Platform, msg.ChannelID, threadID, userID)
	if err != nil {
		return "", fmt.Errorf("switch agent: %w", err)
	}
	if activeSession == nil {
		return "No active session yet. Send a message first to start a conversation, then use /agent to switch agents.", nil
	}

	allAgents, err := inst.Client().ListAgents(ctx)
	if err != nil {
		if errors.Is(err, relay.ErrUnsupported) {
			return "⚠️ Agent switching is not supported by the current agent backend.", nil
		}
		return "⚠️ Agents unavailable — agent server not responding", nil
	}

	switchable, _ := filterAgents(allAgents)

	var matched *relay.AgentInfo
	if num, err := strconv.Atoi(target); err == nil && num >= 1 && num <= len(switchable) {
		matched = &switchable[num-1]
	}

	if matched == nil {
		for i := range switchable {
			if strings.EqualFold(switchable[i].Name, target) {
				matched = &switchable[i]
				break
			}
		}
	}

	if matched == nil {
		lowerTarget := strings.ToLower(target)
		var matches []relay.AgentInfo
		for _, a := range switchable {
			if strings.Contains(strings.ToLower(a.Name), lowerTarget) {
				matches = append(matches, a)
			}
		}
		if len(matches) == 1 {
			matched = &matches[0]
		} else if len(matches) > 1 {
			header := fmt.Sprintf("Ambiguous agent \"%s\". Pick from below:", target)
			return r.renderAgentPickerPage(ctx, msg, 1, header)
		}
	}

	if matched == nil {
		return "Agent not found — refresh with /agent", nil
	}

	if err := inst.Client().SwitchAgent(ctx, activeSession.AgentSessionID, matched.Name); err != nil {
		if errors.Is(err, relay.ErrNotFound) {
			return "Agent not found — refresh with /agent", nil
		}
		if errors.Is(err, relay.ErrUnsupported) {
			return "⚠️ Agent switching is not supported by the current agent backend.", nil
		}
		return "", fmt.Errorf("switch agent: %w", err)
	}

	if matched.Model != nil && matched.Model.ID != "" {
		return fmt.Sprintf("✅ Switched to agent %s (%s)", matched.Name, matched.Model.ID), nil
	}
	return fmt.Sprintf("✅ Switched to agent %s", matched.Name), nil
}

func (r *Router) handleAgentCallback(ctx context.Context, msg channel.IncomingMessage) error {
	token := strings.TrimPrefix(msg.CallbackData, agentCallbackPrefix)
	action, ok := r.agentBrowser.lookup(token)
	if !ok || action.ownerFP != agentOwnerFingerprint(msg) {
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Expired — use /agent again.", nil)
		}
		return nil
	}

	switch action.kind {
	case agentActionPage, agentActionRefresh:
		text, buttons, err := r.buildAgentPickerPage(ctx, msg, action.page)
		if err != nil {
			return err
		}
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
		}
		r.reply(msg, text)
		return nil

	case agentActionSwitch:
		r.agentBrowser.revokeOwner(action.ownerFP)
		replyText, err := r.switchAgent(ctx, msg, action.agentName)
		if err != nil {
			if msg.ReplyCtx != nil && msg.CallbackRef != nil {
				return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Error switching agent.", nil)
			}
			return err
		}
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, replyText, nil)
		}
		r.reply(msg, replyText)
		return nil

	case agentActionDeleteConfirm:
		r.agentBrowser.revokeOwner(action.ownerFP)
		return r.executeAgentDelete(ctx, msg, action.agentName)

	case agentActionDeleteCancel:
		r.agentBrowser.revokeOwner(action.ownerFP)
		text, buttons, err := r.buildAgentPickerPage(ctx, msg, 1, "Deletion canceled.")
		if err != nil {
			return err
		}
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
		}
		return nil
	}

	return nil
}

func (r *Router) handleAgentDeleteCommand(ctx context.Context, msg channel.IncomingMessage, target string) (string, error) {
	if !r.isAdmin(ctx, msg) {
		return accessDeniedMessage, nil
	}

	if !isValidAgentName(target) {
		return "Agent not found — refresh with /agent", nil
	}

	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		return "⚠️ Agent unreachable", nil
	}
	defer inst.End()

	allAgents, err := inst.Client().ListAgents(ctx)
	if err != nil {
		if errors.Is(err, relay.ErrUnsupported) {
			return "⚠️ Agent switching is not supported by the current agent backend.", nil
		}
		return "⚠️ Agents unavailable — agent server not responding", nil
	}

	switchable, _ := filterAgents(allAgents)

	var matched *relay.AgentInfo
	if num, err := strconv.Atoi(target); err == nil && num >= 1 && num <= len(switchable) {
		matched = &switchable[num-1]
	}

	if matched == nil {
		for i := range switchable {
			if strings.EqualFold(switchable[i].Name, target) {
				matched = &switchable[i]
				break
			}
		}
	}

	if matched == nil {
		lowerTarget := strings.ToLower(target)
		var matches []relay.AgentInfo
		for _, a := range switchable {
			if strings.Contains(strings.ToLower(a.Name), lowerTarget) {
				matches = append(matches, a)
			}
		}
		if len(matches) == 1 {
			matched = &matches[0]
		}
	}

	if matched == nil {
		return "Agent not found — refresh with /agent", nil
	}

	if matched.Native {
		return "⚠️ Built-in agents cannot be deleted.", nil
	}

	workdir, err := r.effectiveWorkdir(ctx, msg)
	if err != nil {
		return "", fmt.Errorf("effective workdir: %w", err)
	}

	if err := validateAndRemoveProjectAgentFile(workdir, matched.Name, false); err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) {
			return "⚠️ Global agents cannot be deleted from chat.", nil
		}
		return "⚠️ Invalid agent file path.", nil
	}

	fp := agentOwnerFingerprint(msg)
	r.agentBrowser.revokeOwner(fp)

	confirmTok, _ := r.agentBrowser.register(agentBrowseAction{
		kind:      agentActionDeleteConfirm,
		agentName: matched.Name,
		ownerFP:   fp,
	})
	cancelTok, _ := r.agentBrowser.register(agentBrowseAction{
		kind:      agentActionDeleteCancel,
		agentName: matched.Name,
		ownerFP:   fp,
	})

	text := fmt.Sprintf("Are you sure you want to delete custom agent **%s**?\nThis will remove `.opencode/agent/%s.md`.", matched.Name, matched.Name)
	buttons := []channel.Button{
		{
			Label: fmt.Sprintf("🗑 Delete %s", matched.Name),
			Value: fmt.Sprintf("%s%s", agentCallbackPrefix, confirmTok),
			Row:   1,
		},
		{
			Label: "Cancel",
			Value: fmt.Sprintf("%s%s", agentCallbackPrefix, cancelTok),
			Row:   1,
		},
	}

	if msg.ReplyCtx != nil {
		if _, err := msg.ReplyCtx.SendWithButtons(text, buttons); err != nil {
			return "", err
		}
		return "", errReplied
	}
	return text, nil
}

func (r *Router) executeAgentDelete(ctx context.Context, msg channel.IncomingMessage, agentName string) error {
	if !r.isAdmin(ctx, msg) {
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, accessDeniedMessage, nil)
		}
		r.reply(msg, accessDeniedMessage)
		return nil
	}

	if !isValidAgentName(agentName) {
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Invalid agent name.", nil)
		}
		r.reply(msg, "⚠️ Invalid agent name.")
		return nil
	}

	inst, err := r.clientFor(ctx, msg)
	if err != nil {
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Agent unreachable", nil)
		}
		r.reply(msg, "⚠️ Agent unreachable")
		return nil
	}
	defer inst.End()

	allAgents, err := inst.Client().ListAgents(ctx)
	if err != nil {
		if errors.Is(err, relay.ErrUnsupported) {
			if msg.ReplyCtx != nil && msg.CallbackRef != nil {
				return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Agent switching is not supported by the current agent backend.", nil)
			}
			r.reply(msg, "⚠️ Agent switching is not supported by the current agent backend.")
			return nil
		}
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Agents unavailable — agent server not responding", nil)
		}
		r.reply(msg, "⚠️ Agents unavailable — agent server not responding")
		return nil
	}

	switchable, _ := filterAgents(allAgents)

	var matched *relay.AgentInfo
	for i := range switchable {
		if switchable[i].Name == agentName {
			matched = &switchable[i]
			break
		}
	}
	if matched == nil {
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "Agent not found — refresh with /agent", nil)
		}
		r.reply(msg, "Agent not found — refresh with /agent")
		return nil
	}
	if matched.Native {
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Built-in agents cannot be deleted.", nil)
		}
		r.reply(msg, "⚠️ Built-in agents cannot be deleted.")
		return nil
	}

	workdir, err := r.effectiveWorkdir(ctx, msg)
	if err != nil {
		return fmt.Errorf("effective workdir: %w", err)
	}

	if err := validateAndRemoveProjectAgentFile(workdir, matched.Name, true); err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) {
			if msg.ReplyCtx != nil && msg.CallbackRef != nil {
				return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Global agents cannot be deleted from chat.", nil)
			}
			r.reply(msg, "⚠️ Global agents cannot be deleted from chat.")
			return nil
		}
		if msg.ReplyCtx != nil && msg.CallbackRef != nil {
			return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, "⚠️ Invalid agent file path.", nil)
		}
		r.reply(msg, "⚠️ Invalid agent file path.")
		return nil
	}

	slog.Info("agent deleted", "agent", matched.Name, "workdir", workdir, "user_id", msg.UserID, "platform", msg.Platform, "channel_id", msg.ChannelID)

	banner := fmt.Sprintf("🗑 Deleted custom agent %s.", matched.Name)
	text, buttons, err := r.buildAgentPickerPage(ctx, msg, 1, banner)
	if err != nil {
		return err
	}

	if msg.ReplyCtx != nil && msg.CallbackRef != nil {
		return msg.ReplyCtx.EditWithButtons(msg.CallbackRef, text, buttons)
	}
	if msg.ReplyCtx != nil {
		_, err := msg.ReplyCtx.SendWithButtons(text, buttons)
		return err
	}
	r.reply(msg, text)
	return nil
}

func (r *Router) detectNewAgents(_ context.Context, msg channel.IncomingMessage, inst AgentInstance) {
	if r.agentTracker == nil {
		return
	}
	workdir := inst.Workdir()

	diskAgents := r.agentTracker.snapshotDir(workdir)

	r.agentTracker.mu.Lock()
	known := r.agentTracker.knownAgents[workdir]
	var hasNewDiskFiles bool
	for _, name := range diskAgents {
		if known == nil || !known[name] {
			hasNewDiskFiles = true
			break
		}
	}
	r.agentTracker.mu.Unlock()

	if !hasNewDiskFiles {
		r.agentTracker.updateAndDiff(workdir, diskAgents)
		return
	}

	detectCtx, cancelDetect := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDetect()

	checkNew := func() []string {
		disk := r.agentTracker.snapshotDir(workdir)
		if len(disk) == 0 {
			return nil
		}
		liveAgents, err := inst.Client().ListAgents(detectCtx)
		if err != nil {
			return nil
		}
		liveSet := make(map[string]bool)
		for _, a := range liveAgents {
			if a.Mode == "primary" || a.Mode == "subagent" {
				liveSet[a.Name] = true
			}
		}
		var registeredDiskAgents []string
		for _, name := range disk {
			if liveSet[name] {
				registeredDiskAgents = append(registeredDiskAgents, name)
			}
		}
		return r.agentTracker.updateAndDiff(workdir, registeredDiskAgents)
	}

	newAgents := checkNew()
	if len(newAgents) == 0 && r.agentTracker.retryDelay > 0 {
		select {
		case <-detectCtx.Done():
			return
		case <-time.After(r.agentTracker.retryDelay):
		}
		newAgents = checkNew()
	}

	for _, agentName := range newAgents {
		r.reply(msg, fmt.Sprintf("🆕 Agent `%s` available — /agent to switch", agentName))
	}
}
