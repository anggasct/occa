package router

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

func permissionsOwner() store.PermissionOwner {
	return store.PermissionOwner{Platform: "telegram", ChannelID: "chat1"}
}

// permissionsMessageOwner is the conversation key the default test message
// resolves to: a group message from user1 (no thread).
func permissionsMessageOwner() store.PermissionOwner {
	return permissionOwnerFromMsg(permissionsMessage(&permissionReply{}))
}

func waitForSendCount(reply *permissionReply, n int) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		reply.mu.Lock()
		c := len(reply.sends)
		reply.mu.Unlock()
		if c >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func newPermissionPromptWithRules(t *testing.T, client relay.Client, rules store.PermissionRuleRepo) (*permissionBroker, *permissionPromptHandler, *permissionReply) {
	t.Helper()
	broker := newPermissionBroker(rules)
	owner := &permissionOwner{}
	reply := &permissionReply{}
	handler := &permissionPromptHandler{
		broker:    broker,
		owner:     owner,
		client:    client,
		platform:  "telegram",
		channelID: "chat1",
		sessionID: "session-1",
		reply:     reply,
	}
	return broker, handler, reply
}

func TestPermissionAutoApplyRepliesAlwaysWithoutButtons(t *testing.T) {
	client := &permissionClient{}
	rules := newFakePermissionRuleRepo()
	ctx := context.Background()
	if _, err := rules.Add(ctx, permissionsOwner(), "bash", []string{"git push origin main"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	_, handler, reply := newPermissionPromptWithRules(t, client, rules)
	if err := handler.Prompt(ctx, relay.PermissionRequest{
		ID:         "request-1",
		SessionID:  "session-1",
		Permission: "bash",
		Tool:       "bash",
		Patterns:   []string{"git push origin main"},
	}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	calls := client.callSnapshot()
	if len(calls) != 1 || calls[0].requestID != "request-1" || calls[0].decision != relay.PermissionAlways {
		t.Fatalf("backend calls = %+v, want one always reply", calls)
	}
	if len(reply.sends) != 1 {
		t.Fatalf("sends = %+v, want one notice", reply.sends)
	}
	if len(reply.sends[0].buttons) != 0 {
		t.Fatalf("auto-allowed notice carried buttons: %+v", reply.sends[0])
	}
	if !strings.Contains(reply.sends[0].text, "⚡ Auto-allowed") || !strings.Contains(reply.sends[0].text, `"git push origin main"`) {
		t.Fatalf("notice text = %q", reply.sends[0].text)
	}
}

func TestPermissionAutoApplyDifferentPatternPrompts(t *testing.T) {
	client := &permissionClient{}
	rules := newFakePermissionRuleRepo()
	ctx := context.Background()
	if _, err := rules.Add(ctx, permissionsOwner(), "bash", []string{"git push origin main"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	_, handler, reply := newPermissionPromptWithRules(t, client, rules)
	if err := handler.Prompt(ctx, relay.PermissionRequest{
		ID:         "request-1",
		SessionID:  "session-1",
		Permission: "bash",
		Tool:       "bash",
		Patterns:   []string{"git status"},
	}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	// The different pattern must fall through to the button prompt.
	waitForSends(reply)
	if len(reply.sends) != 1 || len(reply.sends[0].buttons) != 3 {
		t.Fatalf("prompt view = %+v", reply.sends)
	}
	if len(client.callSnapshot()) != 0 {
		t.Fatalf("different pattern auto-replied: %+v", client.callSnapshot())
	}
}

func TestPermissionAutoApplyDifferentToolPrompts(t *testing.T) {
	client := &permissionClient{}
	rules := newFakePermissionRuleRepo()
	ctx := context.Background()
	if _, err := rules.Add(ctx, permissionsOwner(), "bash", []string{"git push origin main"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	_, handler, reply := newPermissionPromptWithRules(t, client, rules)
	if err := handler.Prompt(ctx, relay.PermissionRequest{
		ID:         "request-1",
		SessionID:  "session-1",
		Permission: "write",
		Tool:       "write",
		Patterns:   []string{"git push origin main"},
	}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	waitForSends(reply)
	if len(reply.sends) != 1 || len(reply.sends[0].buttons) != 3 {
		t.Fatalf("prompt view = %+v", reply.sends)
	}
	if len(client.callSnapshot()) != 0 {
		t.Fatalf("different tool auto-replied: %+v", client.callSnapshot())
	}
}

func TestPermissionAutoApplyDifferentConversationPrompts(t *testing.T) {
	client := &permissionClient{}
	rules := newFakePermissionRuleRepo()
	ctx := context.Background()
	if _, err := rules.Add(ctx, permissionsOwner(), "bash", []string{"git push origin main"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	broker := newPermissionBroker(rules)
	owner := &permissionOwner{}
	reply := &permissionReply{}
	handler := &permissionPromptHandler{
		broker:    broker,
		owner:     owner,
		client:    client,
		platform:  "telegram",
		channelID: "chat1",
		threadID:  "thread-9",
		sessionID: "session-1",
		reply:     reply,
	}
	if err := handler.Prompt(ctx, relay.PermissionRequest{
		ID:         "request-1",
		SessionID:  "session-1",
		Permission: "bash",
		Tool:       "bash",
		Patterns:   []string{"git push origin main"},
	}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	waitForSends(reply)
	if len(reply.sends) != 1 || len(reply.sends[0].buttons) != 3 {
		t.Fatalf("prompt view = %+v", reply.sends)
	}
	if len(client.callSnapshot()) != 0 {
		t.Fatalf("rule leaked across conversations: %+v", client.callSnapshot())
	}
}

func TestPermissionAlwaysTapPersistsRule(t *testing.T) {
	client := &permissionClient{}
	rules := newFakePermissionRuleRepo()
	broker, _, reply, token, origin := newPermissionPrompt(t, client)
	broker.rules = rules

	callback := permissionCallback(token, origin, reply)
	callback.CallbackData = "permission:" + token + ":always"
	if err := broker.handle(context.Background(), callback); err != nil {
		t.Fatalf("handle: %v", err)
	}

	calls := client.callSnapshot()
	if len(calls) != 1 || calls[0].decision != relay.PermissionAlways {
		t.Fatalf("backend calls = %+v", calls)
	}
	got, err := rules.ListByOwner(context.Background(), permissionsOwner())
	if err != nil {
		t.Fatalf("ListByOwner: %v", err)
	}
	if len(got) != 1 || got[0].Tool != "bash" {
		t.Fatalf("persisted rules = %+v", got)
	}
	if view := reply.lastEdit(); view.text != "✅ Always allowed" || len(view.buttons) != 0 {
		t.Fatalf("terminal view = %+v", view)
	}
}

func TestPermissionAlwaysTapPersistFailureStillAllows(t *testing.T) {
	client := &permissionClient{}
	rules := newFakePermissionRuleRepo()
	rules.addErr = errors.New("db down")
	broker, _, reply, token, origin := newPermissionPrompt(t, client)
	broker.rules = rules

	callback := permissionCallback(token, origin, reply)
	callback.CallbackData = "permission:" + token + ":always"
	if err := broker.handle(context.Background(), callback); err != nil {
		t.Fatalf("handle: %v", err)
	}

	calls := client.callSnapshot()
	if len(calls) != 1 || calls[0].decision != relay.PermissionAlways {
		t.Fatalf("backend calls = %+v, want the always reply anyway", calls)
	}
	if view := reply.lastEdit(); view.text != permissionNotSavedLabel {
		t.Fatalf("terminal view = %+v, want not-saved variant", view)
	}
}

func TestPermissionDeleteStopsAutoApply(t *testing.T) {
	client := &permissionClient{}
	rules := newFakePermissionRuleRepo()
	ctx := context.Background()
	ruleID, err := rules.Add(ctx, permissionsOwner(), "bash", []string{"git push origin main"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	_, handler, reply := newPermissionPromptWithRules(t, client, rules)
	req := relay.PermissionRequest{
		ID:         "request-1",
		SessionID:  "session-1",
		Permission: "bash",
		Tool:       "bash",
		Patterns:   []string{"git push origin main"},
	}
	if err := handler.Prompt(ctx, req); err != nil {
		t.Fatalf("Prompt (before delete): %v", err)
	}
	if len(client.callSnapshot()) != 1 {
		t.Fatalf("auto-apply before delete failed: %+v", client.callSnapshot())
	}

	if err := rules.DeleteByID(ctx, permissionsOwner(), ruleID); err != nil {
		t.Fatalf("DeleteByID: %v", err)
	}
	if err := handler.Prompt(ctx, req); err != nil {
		t.Fatalf("Prompt (after delete): %v", err)
	}
	waitForSendCount(reply, 2)
	if len(client.callSnapshot()) != 1 {
		t.Fatalf("deleted rule still auto-applied: %+v", client.callSnapshot())
	}
	if len(reply.sends) < 2 || len(reply.sends[1].buttons) != 3 {
		t.Fatalf("expected a prompt after delete, sends = %+v", reply.sends)
	}
}

func permissionsTestRouter(t *testing.T) (*Router, *fakeStore) {
	t.Helper()
	r, st := newResponseRouter(&permissionClient{})
	return r, st
}

func permissionsMessage(reply channel.ReplyContext) channel.IncomingMessage {
	return responseMessage("user1", "chat1", "/permissions", reply)
}

func TestPermissionsCommandEmptyState(t *testing.T) {
	r, _ := permissionsTestRouter(t)
	reply := &permissionReply{}
	msg := permissionsMessage(reply)

	_, err := r.handlePermissions(context.Background(), msg, "")
	if !errors.Is(err, errReplied) {
		t.Fatalf("err = %v, want errReplied", err)
	}
	if len(reply.sends) != 1 || reply.sends[0].text != emptyPermissionsMsg || len(reply.sends[0].buttons) != 0 {
		t.Fatalf("empty state send = %+v", reply.sends)
	}
}

func TestPermissionsCommandListsRulesNewestFirst(t *testing.T) {
	r, st := permissionsTestRouter(t)
	ctx := context.Background()
	owner := permissionsMessageOwner()
	for _, p := range []string{"git status", "git push origin main", "ls -la"} {
		if _, err := st.PermissionRuleRepo().Add(ctx, owner, "bash", []string{p}); err != nil {
			t.Fatalf("Add %q: %v", p, err)
		}
	}

	reply := &permissionReply{}
	_, err := r.handlePermissions(ctx, permissionsMessage(reply), "")
	if !errors.Is(err, errReplied) {
		t.Fatalf("err = %v, want errReplied", err)
	}
	send := reply.sends[0]
	if !strings.Contains(send.text, "Saved always-allow rules · Page 1/1") {
		t.Fatalf("header = %q", send.text)
	}
	if len(send.buttons) != 4 {
		t.Fatalf("buttons = %+v, want 3 rows + clear", send.buttons)
	}
	// Newest first: the last added rule is the first row, with relative age.
	if !strings.Contains(send.text, `1 · Bash — "ls -la" (0m ago)`) {
		t.Fatalf("first row text = %q", send.text)
	}
	first := send.buttons[0]
	if !strings.Contains(first.Label, `"ls -la"`) || !strings.Contains(first.Label, "1 · Bash") {
		t.Fatalf("first row label = %q", first.Label)
	}
	if !strings.HasPrefix(first.Value, "perm:del:") {
		t.Fatalf("row value = %q", first.Value)
	}
}

func TestPermissionsDeleteRowCallback(t *testing.T) {
	r, st := permissionsTestRouter(t)
	ctx := context.Background()
	owner := permissionsMessageOwner()
	ruleID, err := st.PermissionRuleRepo().Add(ctx, owner, "bash", []string{"git push origin main"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	reply := &permissionReply{}
	msg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: "perm:del:" + strconv.FormatInt(ruleID, 10),
		CallbackRef:  permissionRef("perm-message"),
		ReplyCtx:     reply,
	}
	if err := r.handlePermissionCallback(ctx, msg); err != nil {
		t.Fatalf("handlePermissionCallback: %v", err)
	}

	edit := reply.lastEdit()
	if !strings.Contains(edit.text, "🗑 Rule #"+strconv.FormatInt(ruleID, 10)+" deleted") || !strings.Contains(edit.text, "Bash") {
		t.Fatalf("deleted reply = %q", edit.text)
	}
	rules, err := st.PermissionRuleRepo().ListByOwner(ctx, owner)
	if err != nil {
		t.Fatalf("ListByOwner: %v", err)
	}
	if len(rules) != 0 {
		t.Fatalf("rules after delete = %+v", rules)
	}
}

func TestPermissionsClearAllConfirmFlow(t *testing.T) {
	r, st := permissionsTestRouter(t)
	ctx := context.Background()
	owner := permissionsMessageOwner()
	for _, p := range []string{"git status", "git push origin main"} {
		if _, err := st.PermissionRuleRepo().Add(ctx, owner, "bash", []string{p}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	// /permissions clear shows the confirm view, not a direct clear.
	reply := &permissionReply{}
	_, err := r.handlePermissions(ctx, permissionsMessage(reply), "clear")
	if !errors.Is(err, errReplied) {
		t.Fatalf("err = %v, want errReplied", err)
	}
	confirm := reply.sends[0]
	if confirm.text != "Hapus SEMUA rule?" {
		t.Fatalf("confirm text = %q", confirm.text)
	}
	if len(confirm.buttons) != 2 || confirm.buttons[0].Value != "perm:clear:confirm" || confirm.buttons[1].Value != "perm:page:1" {
		t.Fatalf("confirm buttons = %+v", confirm.buttons)
	}
	rules, _ := st.PermissionRuleRepo().ListByOwner(ctx, owner)
	if len(rules) != 2 {
		t.Fatalf("rules before confirm = %d, want 2", len(rules))
	}

	// Confirming clears; cancelling (page callback) keeps them.
	cancelMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: "perm:page:1",
		CallbackRef:  permissionRef("perm-message"),
		ReplyCtx:     reply,
	}
	if err := r.handlePermissionCallback(ctx, cancelMsg); err != nil {
		t.Fatalf("cancel callback: %v", err)
	}
	rules, _ = st.PermissionRuleRepo().ListByOwner(ctx, owner)
	if len(rules) != 2 {
		t.Fatalf("rules after cancel = %d, want 2", len(rules))
	}

	clearMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: "perm:clear:confirm",
		CallbackRef:  permissionRef("perm-message-2"),
		ReplyCtx:     reply,
	}
	if err := r.handlePermissionCallback(ctx, clearMsg); err != nil {
		t.Fatalf("clear callback: %v", err)
	}
	rules, _ = st.PermissionRuleRepo().ListByOwner(ctx, owner)
	if len(rules) != 0 {
		t.Fatalf("rules after clear = %+v", rules)
	}
	if !strings.Contains(reply.lastEdit().text, "cleared") {
		t.Fatalf("clear reply = %q", reply.lastEdit().text)
	}
}

func TestPermissionsPagination(t *testing.T) {
	r, st := permissionsTestRouter(t)
	ctx := context.Background()
	owner := permissionsMessageOwner()
	for i := 0; i < 7; i++ {
		if _, err := st.PermissionRuleRepo().Add(ctx, owner, "bash", []string{"pattern-" + strconv.Itoa(i)}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}

	reply := &permissionReply{}
	_, err := r.handlePermissions(ctx, permissionsMessage(reply), "")
	if !errors.Is(err, errReplied) {
		t.Fatalf("err = %v, want errReplied", err)
	}
	page1 := reply.sends[0]
	if !strings.Contains(page1.text, "Page 1/2") {
		t.Fatalf("page 1 header = %q", page1.text)
	}
	if len(page1.buttons) != 8 { // 6 rows + Next + Clear all
		t.Fatalf("page 1 buttons = %d, want 8", len(page1.buttons))
	}

	next := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: "perm:page:2",
		CallbackRef:  permissionRef("perm-message"),
		ReplyCtx:     reply,
	}
	if err := r.handlePermissionCallback(ctx, next); err != nil {
		t.Fatalf("page 2 callback: %v", err)
	}
	page2Text, page2Buttons, _ := r.buildPermissionsPage(ctx, next, 2)
	if !strings.Contains(page2Text, "Page 2/2") {
		t.Fatalf("page 2 header = %q", page2Text)
	}
	if len(page2Buttons) != 3 { // 1 row + Prev + Clear all
		t.Fatalf("page 2 buttons = %d, want 3", len(page2Buttons))
	}
}

func TestPermissionRowLabelTruncatedAtCap(t *testing.T) {
	r, st := permissionsTestRouter(t)
	ctx := context.Background()
	owner := permissionsMessageOwner()
	long := strings.Repeat("a", 200)
	if _, err := st.PermissionRuleRepo().Add(ctx, owner, "bash", []string{long}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	text, buttons, err := r.buildPermissionsPage(ctx, permissionsMessage(&permissionReply{}), 1)
	if err != nil {
		t.Fatalf("buildPermissionsPage: %v", err)
	}
	if !strings.Contains(text, "Saved always-allow rules") {
		t.Fatalf("page text = %q", text)
	}
	if len(buttons) != 2 { // one row + clear
		t.Fatalf("buttons = %d, want 2", len(buttons))
	}
	if n := utf8.RuneCountInString(buttons[0].Label); n > permissionRowMaxRunes+1 {
		t.Fatalf("row label runes = %d, want <= %d", n, permissionRowMaxRunes+1)
	}
	if !strings.HasSuffix(buttons[0].Label, "…") {
		t.Fatalf("row label not truncated: %q", buttons[0].Label)
	}
}

func TestPermissionsCommandThroughRoute(t *testing.T) {
	r, st := permissionsTestRouter(t)
	ctx := context.Background()
	owner := permissionsMessageOwner()
	ruleID, err := st.PermissionRuleRepo().Add(ctx, owner, "bash", []string{"git push origin main"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	reply := &permissionReply{}
	msg := permissionsMessage(reply)
	msg.Text = "/permissions"
	if err := r.Route(ctx, msg); err != nil {
		t.Fatalf("Route /permissions: %v", err)
	}
	if len(reply.sends) != 1 || len(reply.sends[0].buttons) != 2 { // one row + clear
		t.Fatalf("list send = %+v", reply.sends)
	}

	delMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: "perm:del:" + strconv.FormatInt(ruleID, 10),
		CallbackRef:  permissionRef("perm-message"),
		ReplyCtx:     reply,
	}
	if err := r.Route(ctx, delMsg); err != nil {
		t.Fatalf("Route delete callback: %v", err)
	}
	rules, _ := st.PermissionRuleRepo().ListByOwner(ctx, owner)
	if len(rules) != 0 {
		t.Fatalf("rules after routed delete = %+v", rules)
	}
}

func TestPermissionRuleDescription(t *testing.T) {
	desc := permissionRuleDescription(store.PermissionRule{Tool: "bash", Patterns: "a|b|c"})
	if desc != `Bash — "a" (+2 more)` {
		t.Fatalf("description = %q", desc)
	}
}

func TestAutoAllowedNoticeFormat(t *testing.T) {
	notice := autoAllowedNotice(relay.PermissionRequest{Tool: "bash", Patterns: []string{"git status"}})
	if notice != `⚡ Auto-allowed (rule: Bash — "git status").` {
		t.Fatalf("notice = %q", notice)
	}
}
