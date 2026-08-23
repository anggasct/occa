package router

import (
	"context"
	"errors"
	"fmt"
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
	if !strings.Contains(send.text, "🔐 Saved permissions: 3") || !strings.Contains(send.text, "📦 Project:") {
		t.Fatalf("header = %q", send.text)
	}
	if !strings.Contains(send.text, "Page 1/1") {
		t.Fatalf("page indicator = %q", send.text)
	}
	// The list rows carry no internal rule id.
	if strings.Contains(send.text, "Rule ID") {
		t.Fatalf("list leaked internal id: %q", send.text)
	}
	// Newest first: the last added rule is the first row, with relative age.
	if !strings.Contains(send.text, `1 · Bash — "ls -la"`) {
		t.Fatalf("first row text = %q", send.text)
	}
	if !strings.Contains(send.text, "🕒 just now") {
		t.Fatalf("row age = %q", send.text)
	}
	if len(send.buttons) != 7 { // 3 rules x (Details + Delete) + clear
		t.Fatalf("buttons = %+v, want 7", send.buttons)
	}
	if send.buttons[0].Label != "🔍 Details" || send.buttons[1].Label != "🗑 Delete" {
		t.Fatalf("first rule buttons = %+v", send.buttons[:2])
	}
	if !strings.HasPrefix(send.buttons[0].Value, "perm:det:") || !strings.HasPrefix(send.buttons[1].Value, "perm:del:") {
		t.Fatalf("first rule values = %q, %q", send.buttons[0].Value, send.buttons[1].Value)
	}
	if send.buttons[0].Row != 1 || send.buttons[1].Row != 1 {
		t.Fatalf("first rule rows = %d, %d, want 1", send.buttons[0].Row, send.buttons[1].Row)
	}
	fp := permissionOwnerFingerprint(owner)
	if !strings.HasSuffix(send.buttons[0].Value, fp) || !strings.HasSuffix(send.buttons[1].Value, fp) {
		t.Fatalf("row values missing owner fingerprint: %q, %q", send.buttons[0].Value, send.buttons[1].Value)
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
		CallbackData: "perm:del:" + strconv.FormatInt(ruleID, 10) + ":" + permissionOwnerFingerprint(owner),
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

func TestPermissionsCallbackOwnerMismatch(t *testing.T) {
	r, st := permissionsTestRouter(t)
	ctx := context.Background()
	owner := permissionsMessageOwner()
	ruleID, err := st.PermissionRuleRepo().Add(ctx, owner, "bash", []string{"git push origin main"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	// A button rendered for this conversation, pressed from another
	// conversation: the embedded fingerprint no longer matches the presser,
	// so the delete (and the clear confirm) must be ignored entirely.
	wrongFp := permissionOwnerFingerprint(store.PermissionOwner{Platform: "telegram", ChannelID: "chat2", UserID: "user2"})
	delMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat2",
		UserID:       "user2",
		IsCallback:   true,
		CallbackData: "perm:del:" + strconv.FormatInt(ruleID, 10) + ":" + permissionOwnerFingerprint(owner),
		CallbackRef:  permissionRef("perm-message"),
		ReplyCtx:     &permissionReply{},
	}
	if err := r.handlePermissionCallback(ctx, delMsg); err != nil {
		t.Fatalf("foreign delete callback: %v", err)
	}
	clearMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat2",
		UserID:       "user2",
		IsCallback:   true,
		CallbackData: "perm:clear:confirm:" + permissionOwnerFingerprint(owner),
		CallbackRef:  permissionRef("perm-message"),
		ReplyCtx:     &permissionReply{},
	}
	if err := r.handlePermissionCallback(ctx, clearMsg); err != nil {
		t.Fatalf("foreign clear callback: %v", err)
	}
	rules, _ := st.PermissionRuleRepo().ListByOwner(ctx, owner)
	if len(rules) != 1 {
		t.Fatalf("foreign callbacks affected rules: %+v", rules)
	}
	malformedClearMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: "perm:clear:confirm-extra:" + permissionOwnerFingerprint(owner),
		CallbackRef:  permissionRef("perm-message"),
		ReplyCtx:     &permissionReply{},
	}
	if err := r.handlePermissionCallback(ctx, malformedClearMsg); err != nil {
		t.Fatalf("malformed clear callback: %v", err)
	}
	rules, _ = st.PermissionRuleRepo().ListByOwner(ctx, owner)
	if len(rules) != 1 {
		t.Fatalf("malformed callback affected rules: %+v", rules)
	}

	// A press from the same user, same conversation, but with a stale
	// payload (no fingerprint) is also ignored.
	staleMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: "perm:del:" + strconv.FormatInt(ruleID, 10),
		CallbackRef:  permissionRef("perm-message"),
		ReplyCtx:     &permissionReply{},
	}
	if err := r.handlePermissionCallback(ctx, staleMsg); err != nil {
		t.Fatalf("stale delete callback: %v", err)
	}
	rules, _ = st.PermissionRuleRepo().ListByOwner(ctx, owner)
	if len(rules) != 1 {
		t.Fatalf("stale callback affected rules: %+v", rules)
	}

	// Forging the presser's own fingerprint cannot delete another
	// conversation's rule either: the store keeps the delete owner-scoped.
	forgedMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat2",
		UserID:       "user2",
		IsCallback:   true,
		CallbackData: "perm:del:" + strconv.FormatInt(ruleID, 10) + ":" + wrongFp,
		CallbackRef:  permissionRef("perm-message"),
		ReplyCtx:     &permissionReply{},
	}
	if err := r.handlePermissionCallback(ctx, forgedMsg); err != nil {
		t.Fatalf("forged delete callback: %v", err)
	}
	rules, _ = st.PermissionRuleRepo().ListByOwner(ctx, owner)
	if len(rules) != 1 {
		t.Fatalf("forged callback deleted another conversation's rule: %+v", rules)
	}
}

func TestPermissionsDetailsView(t *testing.T) {
	r, st := permissionsTestRouter(t)
	ctx := context.Background()
	owner := permissionsMessageOwner()
	ruleID, err := st.PermissionRuleRepo().Add(ctx, owner, "bash", []string{"git push origin main", "git status"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	text, buttons, err := r.buildPermissionsPage(ctx, permissionsMessage(&permissionReply{}), 1)
	if err != nil {
		t.Fatalf("buildPermissionsPage: %v", err)
	}
	if !strings.Contains(text, `1 · Bash — "git push origin main" (+1 more)`) {
		t.Fatalf("row text = %q", text)
	}
	var detValue string
	for _, b := range buttons {
		if strings.HasPrefix(b.Value, "perm:det:") {
			detValue = b.Value
			break
		}
	}
	if detValue == "" {
		t.Fatalf("no details button in %+v", buttons)
	}

	reply := &permissionReply{}
	msg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: detValue,
		CallbackRef:  permissionRef("perm-message"),
		ReplyCtx:     reply,
	}
	if err := r.handlePermissionCallback(ctx, msg); err != nil {
		t.Fatalf("details callback: %v", err)
	}
	edit := reply.lastEdit()
	for _, want := range []string{
		"🔍 Rule #" + strconv.FormatInt(ruleID, 10),
		"Tool: Bash (bash)",
		"• git push origin main",
		"• git status",
		"Conversation: telegram, channel chat1, user user1",
		"Created:",
		"Rule ID: " + strconv.FormatInt(ruleID, 10),
	} {
		if !strings.Contains(edit.text, want) {
			t.Fatalf("details text missing %q: %q", want, edit.text)
		}
	}
	if len(edit.buttons) != 2 || edit.buttons[0].Label != "⬅️ Back" || edit.buttons[1].Label != "🗑 Delete" {
		t.Fatalf("details buttons = %+v", edit.buttons)
	}

	// Back returns to the owning conversation's list.
	backMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: edit.buttons[0].Value,
		CallbackRef:  permissionRef("perm-message"),
		ReplyCtx:     reply,
	}
	if err := r.handlePermissionCallback(ctx, backMsg); err != nil {
		t.Fatalf("back callback: %v", err)
	}
	if last := reply.lastEdit(); !strings.Contains(last.text, "🔐 Saved permissions: 1") {
		t.Fatalf("back returned to %q", last.text)
	}

	// Delete straight from the details view removes the rule.
	delMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: edit.buttons[1].Value,
		CallbackRef:  permissionRef("perm-message"),
		ReplyCtx:     reply,
	}
	if err := r.handlePermissionCallback(ctx, delMsg); err != nil {
		t.Fatalf("details delete callback: %v", err)
	}
	rules, _ := st.PermissionRuleRepo().ListByOwner(ctx, owner)
	if len(rules) != 0 {
		t.Fatalf("rules after details delete = %+v", rules)
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
	fp := permissionOwnerFingerprint(owner)
	if len(confirm.buttons) != 2 || confirm.buttons[0].Value != "perm:clear:confirm:"+fp || confirm.buttons[1].Value != "perm:page:1:"+fp {
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
		CallbackData: "perm:page:1:" + fp,
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
		CallbackData: "perm:clear:confirm:" + fp,
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
	fp := permissionOwnerFingerprint(owner)
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
	if len(page1.buttons) != 14 { // 6 rules x (Details + Delete) + Next + Clear all
		t.Fatalf("page 1 buttons = %d, want 14", len(page1.buttons))
	}

	next := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: "perm:page:2:" + fp,
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
	if len(page2Buttons) != 4 { // 1 rule x (Details + Delete) + Prev + Clear all
		t.Fatalf("page 2 buttons = %d, want 4", len(page2Buttons))
	}
}

// TestPermissionsBrowserScaleAndLimits runs the permission browser at 0, 1,
// 6, and 30 rules on both platform seams, asserting pagination structure and
// that every rendered button stays within the platform's label/callback
// limits (Telegram callback_data <= 64 bytes, Discord custom_id <= 100 chars,
// Discord <= 5 action rows with <= 5 buttons each, Telegram <= 8 per row).
func TestPermissionsBrowserScaleAndLimits(t *testing.T) {
	for _, platform := range []string{"telegram", "discord"} {
		for _, count := range []int{0, 1, 6, 30} {
			t.Run(fmt.Sprintf("%s-%d", platform, count), func(t *testing.T) {
				r, st := permissionsTestRouter(t)
				ctx := context.Background()
				owner := store.PermissionOwner{Platform: platform, ChannelID: "chat1", UserID: "user1"}
				for i := 0; i < count; i++ {
					if _, err := st.PermissionRuleRepo().Add(ctx, owner, "bash", []string{"pattern-" + strconv.Itoa(i)}); err != nil {
						t.Fatalf("Add %d: %v", i, err)
					}
				}
				msg := channel.IncomingMessage{Platform: platform, ChannelID: "chat1", UserID: "user1", ReplyCtx: &permissionReply{}}

				text, buttons, err := r.buildPermissionsPage(ctx, msg, 1)
				if err != nil {
					t.Fatalf("buildPermissionsPage: %v", err)
				}
				if count == 0 {
					if text != emptyPermissionsMsg || len(buttons) != 0 {
						t.Fatalf("empty state = %q / %+v", text, buttons)
					}
					return
				}

				totalPages := permissionPagesTotal(count)
				if !strings.Contains(text, fmt.Sprintf("Page 1/%d", totalPages)) {
					t.Fatalf("page 1 header = %q, want Page 1/%d", text, totalPages)
				}
				for page := 1; page <= totalPages; page++ {
					_, pageButtons, err := r.buildPermissionsPage(ctx, msg, page)
					if err != nil {
						t.Fatalf("page %d: %v", page, err)
					}
					if len(pageButtons) == 0 {
						t.Fatalf("page %d has no buttons", page)
					}
					byRow := make(map[int]int)
					for _, b := range pageButtons {
						if platform == "telegram" {
							if len([]byte(b.Value)) > 64 {
								t.Fatalf("telegram callback exceeds 64 bytes: %q", b.Value)
							}
						} else {
							if utf8.RuneCountInString(b.Value) > 100 {
								t.Fatalf("discord custom_id exceeds 100 chars: %q", b.Value)
							}
						}
						if utf8.RuneCountInString(b.Label) > 80 {
							t.Fatalf("button label exceeds 80 chars: %q", b.Label)
						}
						byRow[b.Row]++
					}
					if platform == "discord" {
						for row, n := range byRow {
							if row > 5 {
								t.Fatalf("discord action row %d exceeds the 5-row cap", row)
							}
							if n > 5 {
								t.Fatalf("discord row %d has %d buttons, cap is 5", row, n)
							}
						}
					} else {
						for row, n := range byRow {
							if n > 8 {
								t.Fatalf("telegram row %d has %d buttons, cap is 8", row, n)
							}
						}
					}
				}
			})
		}
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
	if len(reply.sends) != 1 || len(reply.sends[0].buttons) != 3 { // 1 rule x (Details + Delete) + clear
		t.Fatalf("list send = %+v", reply.sends)
	}

	delMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: "perm:del:" + strconv.FormatInt(ruleID, 10) + ":" + permissionOwnerFingerprint(owner),
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

func TestDisplayToolReadableLabels(t *testing.T) {
	for tool, want := range map[string]string{
		"bash":                       "Bash",
		"webfetch":                   "Web fetch",
		"mcp__filesystem__read_file": "Filesystem Read File",
		"my-tool":                    "My Tool",
		"":                           "tool",
	} {
		if got := displayTool(tool); got != want {
			t.Fatalf("displayTool(%q) = %q, want %q", tool, got, want)
		}
	}
}

func TestPermissionAgeLabel(t *testing.T) {
	if got := permissionAgeLabel(0); got != "just now" {
		t.Fatalf("age(0) = %q", got)
	}
	if got := permissionAgeLabel(time.Now().Unix() - 30); got != "just now" {
		t.Fatalf("age(30s) = %q", got)
	}
	if got := permissionAgeLabel(time.Now().Unix() - 5*60); got != "5m ago" {
		t.Fatalf("age(5m) = %q", got)
	}
	if got := permissionAgeLabel(time.Now().Unix() - 2*3600); got != "2h ago" {
		t.Fatalf("age(2h) = %q", got)
	}
	if got := permissionAgeLabel(time.Now().Unix() - 3*86400); got != "3d ago" {
		t.Fatalf("age(3d) = %q", got)
	}
}

func TestPermissionContextHeader(t *testing.T) {
	r, st := permissionsTestRouter(t)
	ctx := context.Background()
	if err := st.ChannelRepo().UpsertWorkdir(ctx, "telegram", "chat1", "/home/ubuntu/projects/aura"); err != nil {
		t.Fatalf("UpsertWorkdir: %v", err)
	}
	header := r.permissionContextHeader(ctx, permissionsMessage(&permissionReply{}), 1)
	for _, want := range []string{"🔐 Saved permissions: 1", "📦 Project: aura", "📂 Workdir: /home/ubuntu/projects/aura"} {
		if !strings.Contains(header, want) {
			t.Fatalf("header missing %q: %q", want, header)
		}
	}

	// Unknown project when no workdir is resolvable (no channel row and no
	// default workdir configured).
	unknownRouter, _ := permissionsTestRouter(t)
	unknownRouter.defaultWorkdir = ""
	unknown := unknownRouter.permissionContextHeader(ctx, channel.IncomingMessage{Platform: "telegram", ChannelID: "missing-chat", UserID: "user1"}, 2)
	if !strings.Contains(unknown, "📦 Project: unknown") || strings.Contains(unknown, "📂 Workdir:") {
		t.Fatalf("unknown project header = %q", unknown)
	}
}
