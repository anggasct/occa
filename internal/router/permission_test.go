package router

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
)

type permissionView struct {
	ref     channel.MessageRef
	text    string
	buttons []channel.Button
}

type permissionReply struct {
	mu     sync.Mutex
	nextID int
	sends  []permissionView
	edits  []permissionView
}

type permissionRef string

func (r permissionRef) ID() string { return string(r) }

func (r *permissionReply) SendTyping() error { return nil }

func (r *permissionReply) Send(text string) (channel.MessageRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	ref := permissionRef("message-" + string(rune('0'+r.nextID)))
	r.sends = append(r.sends, permissionView{ref: ref, text: text})
	return ref, nil
}

func (r *permissionReply) SendWithButtons(text string, buttons []channel.Button) (channel.MessageRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	ref := permissionRef("message-" + string(rune('0'+r.nextID)))
	r.sends = append(r.sends, permissionView{ref: ref, text: text, buttons: append([]channel.Button(nil), buttons...)})
	return ref, nil
}

func (r *permissionReply) Edit(_ channel.MessageRef, text string) error {
	return r.EditWithButtons(nil, text, nil)
}

func (r *permissionReply) EditWithButtons(ref channel.MessageRef, text string, buttons []channel.Button) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.edits = append(r.edits, permissionView{ref: ref, text: text, buttons: append([]channel.Button(nil), buttons...)})
	return nil
}

func (r *permissionReply) lastEdit() permissionView {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.edits) == 0 {
		return permissionView{}
	}
	view := r.edits[len(r.edits)-1]
	view.buttons = append([]channel.Button(nil), view.buttons...)
	return view
}

type permissionCall struct {
	requestID string
	decision  relay.PermissionReply
}

type permissionClient struct {
	mu             sync.Mutex
	calls          []permissionCall
	errors         []error
	started        chan struct{}
	startOne       sync.Once
	release        chan struct{}
	blockFirstCall bool
}

func (c *permissionClient) CreateSession(_ context.Context) (string, error) { return "session", nil }
func (c *permissionClient) GetSession(_ context.Context, _ string) (*relay.SessionInfo, error) {
	return &relay.SessionInfo{}, nil
}
func (c *permissionClient) SessionExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}
func (c *permissionClient) SendMessage(_ context.Context, _ string, _ string, _ *relay.ModelRef, _ []relay.Attachment) error {
	return nil
}
func (c *permissionClient) Providers(_ context.Context) (relay.Providers, error) {
	return relay.Providers{}, nil
}
func (c *permissionClient) RunCommand(_ context.Context, _ string, _ string) error { return nil }
func (c *permissionClient) ListCommands(_ context.Context) ([]relay.CommandInfo, error) {
	return nil, nil
}
func (c *permissionClient) Events(_ context.Context, _ string) (<-chan relay.Event, error) {
	return nil, nil
}

func (c *permissionClient) AnswerQuestion(_ context.Context, _ string, _ [][]string) error {
	return nil
}
func (c *permissionClient) RejectQuestion(_ context.Context, _ string) error         { return nil }
func (c *permissionClient) AbortSession(_ context.Context, _ string) error           { return nil }
func (c *permissionClient) SummarizeSession(_ context.Context, _, _, _ string) error { return nil }
func (c *permissionClient) RevertMessage(_ context.Context, _, _ string) error       { return nil }
func (c *permissionClient) UnrevertSession(_ context.Context, _ string) error        { return nil }
func (c *permissionClient) ListMessages(_ context.Context, _ string) ([]relay.MessageInfo, error) {
	return nil, nil
}
func (c *permissionClient) ReplyPermission(ctx context.Context, requestID string, decision relay.PermissionReply) error {
	c.mu.Lock()
	callIndex := len(c.calls)
	c.calls = append(c.calls, permissionCall{requestID: requestID, decision: decision})
	var err error
	if callIndex < len(c.errors) {
		err = c.errors[callIndex]
	}
	release := c.release
	blockFirst := c.blockFirstCall
	if blockFirst {
		c.blockFirstCall = false
	}
	c.mu.Unlock()
	if c.started != nil {
		c.startOne.Do(func() { close(c.started) })
	}
	if release != nil && blockFirst {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (c *permissionClient) callSnapshot() []permissionCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]permissionCall(nil), c.calls...)
}

func waitForSends(reply *permissionReply) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		reply.mu.Lock()
		n := len(reply.sends)
		reply.mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func newPermissionPrompt(t *testing.T, client relay.Client) (*permissionBroker, *permissionOwner, *permissionReply, string, channel.MessageRef) {
	t.Helper()
	broker := newPermissionBroker()
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
	if err := handler.Prompt(context.Background(), relay.PermissionRequest{
		ID:         "request-1",
		SessionID:  "session-1",
		Permission: "external_directory",
		Tool:       "bash",
	}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	waitForSends(reply)

	if len(reply.sends) != 1 || len(reply.sends[0].buttons) != 3 {
		t.Fatalf("prompt view = %+v", reply.sends)
	}
	token, _, ok := parsePermissionCallback(reply.sends[0].buttons[0].Value)
	if !ok {
		t.Fatalf("invalid callback value: %q", reply.sends[0].buttons[0].Value)
	}
	return broker, owner, reply, token, reply.sends[0].ref
}

func permissionCallback(token string, ref channel.MessageRef, reply channel.ReplyContext) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "allowed-user",
		IsCallback:   true,
		CallbackData: "permission:" + token + ":once",
		CallbackRef:  ref,
		ReplyCtx:     reply,
	}
}

func TestPermissionPromptUsesOpaqueTokenAndOrigin(t *testing.T) {
	client := &permissionClient{}
	_, _, reply, token, origin := newPermissionPrompt(t, client)

	if token == "request-1" || token == "session-1" || strings.Contains(token, "/") {
		t.Fatalf("token is not opaque: %q", token)
	}
	for _, button := range reply.sends[0].buttons {
		if strings.Contains(button.Value, "request-1") || strings.Contains(button.Value, "session-1") {
			t.Fatalf("callback leaks backend identity: %q", button.Value)
		}
	}
	if origin == nil || origin.ID() == "" {
		t.Fatal("prompt did not capture origin reference")
	}
}

func TestPermissionBrokerUsesCapturedClientAndClearsButtons(t *testing.T) {
	client := &permissionClient{}
	broker, _, reply, token, origin := newPermissionPrompt(t, client)

	if err := broker.handle(context.Background(), permissionCallback(token, origin, reply)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	calls := client.callSnapshot()
	if len(calls) != 1 || calls[0].requestID != "request-1" || calls[0].decision != relay.PermissionOnce {
		t.Fatalf("permission calls = %+v", calls)
	}
	view := reply.lastEdit()
	if view.text != "✅ Allowed once" || len(view.buttons) != 0 || view.ref.ID() != origin.ID() {
		t.Fatalf("terminal view = %+v", view)
	}
}

func TestRoutePermissionCallbackUsesGenericCapturedClient(t *testing.T) {
	client := &permissionClient{}
	r, _ := newResponseRouter(client)
	provider := r.instances.(*fakeInstanceProvider)
	owner := &permissionOwner{}
	reply := &permissionReply{}
	handler := &permissionPromptHandler{
		broker:    r.permissions,
		owner:     owner,
		client:    client,
		platform:  "telegram",
		channelID: "chat1",
		sessionID: "session-1",
		reply:     reply,
	}
	if err := handler.Prompt(context.Background(), relay.PermissionRequest{ID: "request-1", Permission: "bash"}); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	waitForSends(reply)
	token, _, ok := parsePermissionCallback(reply.sends[0].buttons[0].Value)
	if !ok {
		t.Fatal("prompt token did not parse")
	}

	callback := permissionCallback(token, reply.sends[0].ref, reply)
	callback.UserID = "user1"
	if err := r.Route(context.Background(), callback); err != nil {
		t.Fatalf("Route: %v", err)
	}
	calls := client.callSnapshot()
	if len(calls) != 1 || calls[0].requestID != "request-1" {
		t.Fatalf("permission calls = %+v", calls)
	}
	if provider.calls != 0 {
		t.Fatalf("callback resolved a fresh instance: %d", provider.calls)
	}
}

func TestPermissionBrokerConcurrentDuplicateCallsOnce(t *testing.T) {
	client := &permissionClient{started: make(chan struct{}), release: make(chan struct{})}
	broker, _, reply, token, origin := newPermissionPrompt(t, client)
	callback := permissionCallback(token, origin, reply)

	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			if err := broker.handle(context.Background(), callback); err != nil {
				t.Errorf("handle: %v", err)
			}
		}()
	}
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("backend call did not start")
	}
	close(client.release)
	wg.Wait()

	calls := client.callSnapshot()
	if len(calls) != 1 {
		t.Fatalf("permission calls = %d, want 1", len(calls))
	}
	if got := reply.lastEdit(); got.text != "✅ Allowed once" || len(got.buttons) != 0 {
		t.Fatalf("duplicate terminal view = %+v", got)
	}
}

func TestPermissionBrokerFailureKeepsButtonsForRetry(t *testing.T) {
	client := &permissionClient{errors: []error{errors.New("backend unavailable"), nil}}
	broker, _, reply, token, origin := newPermissionPrompt(t, client)
	callback := permissionCallback(token, origin, reply)

	if err := broker.handle(context.Background(), callback); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	failed := reply.lastEdit()
	if failed.text != permissionRetryMessage || len(failed.buttons) != 3 || strings.Contains(failed.text, "backend unavailable") {
		t.Fatalf("retry view = %+v", failed)
	}
	if err := broker.handle(context.Background(), callback); err != nil {
		t.Fatalf("retry handle: %v", err)
	}
	if len(client.callSnapshot()) != 2 {
		t.Fatalf("permission calls = %d, want 2", len(client.callSnapshot()))
	}
	if got := reply.lastEdit(); got.text != "✅ Allowed once" || len(got.buttons) != 0 {
		t.Fatalf("resolved retry view = %+v", got)
	}
}

func TestPermissionBrokerRejectsScopeAndOriginMismatch(t *testing.T) {
	client := &permissionClient{}
	broker, _, reply, token, origin := newPermissionPrompt(t, client)

	wrongScope := permissionCallback(token, origin, reply)
	wrongScope.ChannelID = "other-chat"
	if err := broker.handle(context.Background(), wrongScope); err != nil {
		t.Fatalf("wrong scope handle: %v", err)
	}
	wrongOrigin := permissionCallback(token, permissionRef("other-message"), reply)
	if err := broker.handle(context.Background(), wrongOrigin); err != nil {
		t.Fatalf("wrong origin handle: %v", err)
	}
	if len(client.callSnapshot()) != 0 {
		t.Fatalf("mismatched callbacks called backend: %+v", client.callSnapshot())
	}
	if got := reply.lastEdit(); got.text != permissionExpiredMessage || len(got.buttons) != 0 {
		t.Fatalf("mismatch view = %+v", got)
	}
}

func TestPermissionBrokerMissingOriginDoesNotMutate(t *testing.T) {
	client := &permissionClient{}
	broker, _, reply, token, _ := newPermissionPrompt(t, client)
	callback := permissionCallback(token, nil, reply)

	if err := broker.handle(context.Background(), callback); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(client.callSnapshot()) != 0 || len(reply.edits) != 0 {
		t.Fatalf("missing origin changed state: calls=%v edits=%v", client.callSnapshot(), reply.edits)
	}
}

func TestPermissionBrokerExpiresOwnerAndUnknownTokens(t *testing.T) {
	client := &permissionClient{}
	broker, owner, reply, token, origin := newPermissionPrompt(t, client)
	broker.expireOwner(owner)
	if got := reply.lastEdit(); got.text != permissionExpiredMessage || len(got.buttons) != 0 {
		t.Fatalf("expired view = %+v", got)
	}
	if err := broker.handle(context.Background(), permissionCallback(token, origin, reply)); err != nil {
		t.Fatalf("expired handle: %v", err)
	}
	if len(client.callSnapshot()) != 0 {
		t.Fatal("expired callback called backend")
	}

	newBroker := newPermissionBroker()
	unknown := permissionCallback("after-restart", origin, reply)
	if err := newBroker.handle(context.Background(), unknown); err != nil {
		t.Fatalf("unknown handle: %v", err)
	}
	if got := reply.lastEdit(); got.text != permissionExpiredMessage || len(got.buttons) != 0 {
		t.Fatalf("unknown token view = %+v", got)
	}
}

func TestExpireInterleavedWithDuplicateCallbackNoRace(t *testing.T) {
	client := &permissionClient{}
	broker, owner, reply, token, origin := newPermissionPrompt(t, client)

	done := make(chan struct{})
	go func() {
		defer close(done)
		broker.expireOwner(owner)
	}()

	// The duplicate callback may land before or after the expiry; both orders
	// must be race-free on the broker record.
	_ = broker.handle(context.Background(), permissionCallback(token, origin, reply))
	<-done
}

func TestExpireWhileHandlingThenDuplicateCallback(t *testing.T) {
	client := &permissionClient{}
	broker, owner, reply, token, origin := newPermissionPrompt(t, client)

	// Resolve the record first so the duplicate branch reports "resolved".
	if err := broker.handle(context.Background(), permissionCallback(token, origin, reply)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := broker.handle(context.Background(), permissionCallback(token, origin, reply)); err != nil {
		t.Fatalf("duplicate handle: %v", err)
	}

	// Expire an unrelated pending record concurrently with a resolved duplicate.
	otherClient := &permissionClient{}
	_, _, otherReply, otherToken, otherOrigin := newPermissionPrompt(t, otherClient)
	_ = otherReply

	done := make(chan struct{})
	go func() {
		defer close(done)
		broker.expireOwner(owner)
	}()
	_ = broker.handle(context.Background(), permissionCallback(otherToken, otherOrigin, otherReply))
	<-done
}

func TestCallbackNotSerializedBehindBlockedBackend(t *testing.T) {
	client := &permissionClient{
		started:        make(chan struct{}),
		release:        make(chan struct{}),
		blockFirstCall: true,
	}
	broker := newPermissionBroker()

	newPrompt := func(requestID string) (channel.MessageRef, string, channel.ReplyContext) {
		reply := &permissionReply{}
		owner := &permissionOwner{}
		handler := &permissionPromptHandler{
			broker:    broker,
			owner:     owner,
			client:    client,
			platform:  "telegram",
			channelID: "chat1",
			sessionID: "session-1",
			reply:     reply,
		}
		if err := handler.Prompt(context.Background(), relay.PermissionRequest{
			ID:         requestID,
			SessionID:  "session-1",
			Permission: "external_directory",
			Tool:       "bash",
		}); err != nil {
			t.Fatalf("Prompt: %v", err)
		}
		waitForSends(reply)
		token, _, ok := parsePermissionCallback(reply.sends[0].buttons[0].Value)
		if !ok {
			t.Fatalf("invalid callback value: %q", reply.sends[0].buttons[0].Value)
		}
		return reply.sends[0].ref, token, reply
	}

	origin1, token1, reply1 := newPrompt("request-1")
	origin2, token2, reply2 := newPrompt("request-2")

	// First callback blocks inside the backend call.
	firstDone := make(chan error, 1)
	go func() { firstDone <- broker.handle(context.Background(), permissionCallback(token1, origin1, reply1)) }()
	<-client.started

	// Second callback for a different record must reach the backend while the
	// first is still blocked — the broker lock is not held across the call.
	if err := broker.handle(context.Background(), permissionCallback(token2, origin2, reply2)); err != nil {
		t.Fatalf("second handle: %v", err)
	}
	calls := client.callSnapshot()
	if len(calls) != 2 {
		t.Fatalf("second callback did not reach the backend: %d calls", len(calls))
	}

	close(client.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first handle: %v", err)
	}
}

func TestPermissionPromptTextWithPatterns(t *testing.T) {
	req := relay.PermissionRequest{
		Permission: "external_directory",
		Tool:       "bash",
		Patterns:   []string{"/tmp/*"},
	}
	got := permissionPromptText(req)
	want := "🔐 Permission requested: external_directory (bash)\nPath: /tmp/*"
	if got != want {
		t.Fatalf("permissionPromptText = %q, want %q", got, want)
	}
}

func TestPermissionBatchingTwoRequestsWithinWindow(t *testing.T) {
	client := &permissionClient{}
	broker := newPermissionBroker()
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

	req1 := relay.PermissionRequest{
		ID:         "request-1",
		SessionID:  "session-1",
		Permission: "external_directory",
		Tool:       "bash",
		Patterns:   []string{"/path/api/*"},
	}
	req2 := relay.PermissionRequest{
		ID:         "request-2",
		SessionID:  "session-1",
		Permission: "external_directory",
		Tool:       "bash",
		Patterns:   []string{"/path/database/*"},
	}

	if err := handler.Prompt(context.Background(), req1); err != nil {
		t.Fatalf("Prompt 1: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := handler.Prompt(context.Background(), req2); err != nil {
		t.Fatalf("Prompt 2: %v", err)
	}

	waitForSends(reply)

	reply.mu.Lock()
	sendsCount := len(reply.sends)
	var view permissionView
	if sendsCount == 1 {
		view = reply.sends[0]
	}
	reply.mu.Unlock()

	if sendsCount != 1 {
		t.Fatalf("send count = %d, want 1", sendsCount)
	}

	if !strings.Contains(view.text, "/path/api/*") || !strings.Contains(view.text, "/path/database/*") {
		t.Fatalf("prompt text does not contain both paths: %q", view.text)
	}

	if len(view.buttons) != 3 {
		t.Fatalf("button count = %d, want 3", len(view.buttons))
	}

	token, decision, ok := parsePermissionCallback(view.buttons[0].Value)
	if !ok {
		t.Fatalf("invalid callback value: %q", view.buttons[0].Value)
	}

	// Verify resolving the single token replies to BOTH request IDs
	callback := permissionCallback(token, view.ref, reply)
	if err := broker.handle(context.Background(), callback); err != nil {
		t.Fatalf("handle: %v", err)
	}

	calls := client.callSnapshot()
	if len(calls) != 2 {
		t.Fatalf("client calls count = %d, want 2", len(calls))
	}
	if calls[0].requestID != "request-1" || calls[0].decision != decision {
		t.Errorf("call 0 = %+v, want request-1", calls[0])
	}
	if calls[1].requestID != "request-2" || calls[1].decision != decision {
		t.Errorf("call 1 = %+v, want request-2", calls[1])
	}
}

func TestPermissionBatchingRequestsSeparatedBeyondWindow(t *testing.T) {
	client := &permissionClient{}
	broker := newPermissionBroker()
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

	req1 := relay.PermissionRequest{
		ID:         "request-1",
		Permission: "external_directory",
		Patterns:   []string{"/path/1/*"},
	}
	req2 := relay.PermissionRequest{
		ID:         "request-2",
		Permission: "external_directory",
		Patterns:   []string{"/path/2/*"},
	}

	if err := handler.Prompt(context.Background(), req1); err != nil {
		t.Fatalf("Prompt 1: %v", err)
	}

	waitForSends(reply)

	reply.mu.Lock()
	count1 := len(reply.sends)
	reply.mu.Unlock()
	if count1 != 1 {
		t.Fatalf("sends after req1 = %d, want 1", count1)
	}

	if err := handler.Prompt(context.Background(), req2); err != nil {
		t.Fatalf("Prompt 2: %v", err)
	}

	// Wait for second batch window
	time.Sleep(1600 * time.Millisecond)

	reply.mu.Lock()
	count2 := len(reply.sends)
	reply.mu.Unlock()
	if count2 != 2 {
		t.Fatalf("sends after req2 = %d, want 2", count2)
	}
}

func TestPermissionBatchingExpireBeforeWindowElapses(t *testing.T) {
	client := &permissionClient{}
	broker := newPermissionBroker()
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

	req := relay.PermissionRequest{
		ID:         "request-1",
		Permission: "external_directory",
		Patterns:   []string{"/path/1/*"},
	}

	if err := handler.Prompt(context.Background(), req); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	// Sleep briefly while batch is pending
	time.Sleep(200 * time.Millisecond)

	broker.expireOwner(owner)

	// Sleep past the batch window
	time.Sleep(1600 * time.Millisecond)

	reply.mu.Lock()
	count := len(reply.sends)
	reply.mu.Unlock()

	if count != 0 {
		t.Fatalf("sends after expire = %d, want 0 (prompt should not have been sent)", count)
	}
}

func TestPermissionBrokerBatchRetryOnlyUnresolvedIDs(t *testing.T) {
	client := &permissionClient{
		errors: []error{nil, errors.New("backend error 2nd id")},
	}
	broker := newPermissionBroker()
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

	req1 := relay.PermissionRequest{
		ID:         "request-1",
		SessionID:  "session-1",
		Permission: "external_directory",
		Tool:       "bash",
		Patterns:   []string{"/path/1/*"},
	}
	req2 := relay.PermissionRequest{
		ID:         "request-2",
		SessionID:  "session-1",
		Permission: "external_directory",
		Tool:       "bash",
		Patterns:   []string{"/path/2/*"},
	}

	if err := handler.Prompt(context.Background(), req1); err != nil {
		t.Fatalf("Prompt 1: %v", err)
	}
	if err := handler.Prompt(context.Background(), req2); err != nil {
		t.Fatalf("Prompt 2: %v", err)
	}

	waitForSends(reply)

	reply.mu.Lock()
	if len(reply.sends) != 1 {
		reply.mu.Unlock()
		t.Fatalf("send count = %d, want 1", len(reply.sends))
	}
	view := reply.sends[0]
	reply.mu.Unlock()

	token, decision, ok := parsePermissionCallback(view.buttons[0].Value)
	if !ok {
		t.Fatalf("invalid callback value: %q", view.buttons[0].Value)
	}

	callback := permissionCallback(token, view.ref, reply)

	// First click: request-1 succeeds, request-2 fails.
	if err := broker.handle(context.Background(), callback); err != nil {
		t.Fatalf("first handle: %v", err)
	}

	callsAfterFirst := client.callSnapshot()
	if len(callsAfterFirst) != 2 {
		t.Fatalf("calls after first attempt = %d, want 2", len(callsAfterFirst))
	}
	if callsAfterFirst[0].requestID != "request-1" || callsAfterFirst[1].requestID != "request-2" {
		t.Fatalf("calls after first attempt = %+v", callsAfterFirst)
	}

	failedView := reply.lastEdit()
	if failedView.text != permissionRetryMessage || len(failedView.buttons) != 3 {
		t.Fatalf("retry view after partial failure = %+v", failedView)
	}

	// Second click (retry): only request-2 should be attempted (request-1 must NOT be re-replied).
	if err := broker.handle(context.Background(), callback); err != nil {
		t.Fatalf("retry handle: %v", err)
	}

	allCalls := client.callSnapshot()
	if len(allCalls) != 3 {
		t.Fatalf("total calls = %d, want 3: %+v", len(allCalls), allCalls)
	}

	req1Count := 0
	req2Count := 0
	for _, call := range allCalls {
		switch call.requestID {
		case "request-1":
			req1Count++
		case "request-2":
			req2Count++
		}
	}

	if req1Count != 1 {
		t.Errorf("request-1 call count = %d, want 1 (must not be re-replied on retry)", req1Count)
	}
	if req2Count != 2 {
		t.Errorf("request-2 call count = %d, want 2", req2Count)
	}

	resolvedView := reply.lastEdit()
	if resolvedView.text != permissionTerminalLabel(decision) || len(resolvedView.buttons) != 0 {
		t.Fatalf("resolved view = %+v", resolvedView)
	}
}

func newPermissionBatchPrompt(t *testing.T, client relay.Client, ids ...string) (*permissionBroker, *permissionReply, string, channel.MessageRef) {
	t.Helper()
	broker := newPermissionBroker()
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
	for _, id := range ids {
		if err := handler.Prompt(context.Background(), relay.PermissionRequest{
			ID:         id,
			SessionID:  "session-1",
			Permission: "external_directory",
			Tool:       "bash",
			Patterns:   []string{"/path/*"},
		}); err != nil {
			t.Fatalf("Prompt %s: %v", id, err)
		}
	}

	waitForSends(reply)

	if len(reply.sends) != 1 || len(reply.sends[0].buttons) != 3 {
		t.Fatalf("prompt view = %+v", reply.sends)
	}
	token, _, ok := parsePermissionCallback(reply.sends[0].buttons[0].Value)
	if !ok {
		t.Fatalf("invalid callback value: %q", reply.sends[0].buttons[0].Value)
	}
	return broker, reply, token, reply.sends[0].ref
}

func TestPermissionBrokerBatchSecondReplyNotFoundResolves(t *testing.T) {
	client := &permissionClient{errors: []error{nil, relay.ErrNotFound}}
	broker, reply, token, origin := newPermissionBatchPrompt(t, client, "request-1", "request-2")

	// One tap: request-1 succeeds, request-2 404s because opencode already
	// resolved the whole group on the first reply. The batch must resolve
	// with the terminal label instead of falling into the retry view.
	if err := broker.handle(context.Background(), permissionCallback(token, origin, reply)); err != nil {
		t.Fatalf("handle: %v", err)
	}

	calls := client.callSnapshot()
	if len(calls) != 2 {
		t.Fatalf("client calls = %d, want 2: %+v", len(calls), calls)
	}
	if calls[0].requestID != "request-1" || calls[1].requestID != "request-2" {
		t.Fatalf("client calls = %+v, want request-1 then request-2", calls)
	}

	view := reply.lastEdit()
	if view.text != "✅ Allowed once" || len(view.buttons) != 0 {
		t.Fatalf("terminal view = %+v", view)
	}

	reply.mu.Lock()
	var renderedRetry bool
	retryEditIndex := -1
	for i, edit := range reply.edits {
		if edit.text == permissionRetryMessage {
			renderedRetry = true
			retryEditIndex = i
		}
	}
	reply.mu.Unlock()
	if renderedRetry {
		t.Fatalf("retry message rendered after partial 404 (edit %d): %+v", retryEditIndex, reply.edits)
	}

	broker.mu.Lock()
	record := broker.records[token]
	broker.mu.Unlock()
	if record == nil {
		t.Fatal("record missing after resolve")
	}
	if record.state != permissionResolved {
		t.Fatalf("record state = %v, want resolved", stateName(record.state))
	}
}

func TestPermissionBrokerBatchFirstReplyNotFoundRetries(t *testing.T) {
	client := &permissionClient{errors: []error{relay.ErrNotFound}}
	broker, reply, token, origin := newPermissionBatchPrompt(t, client, "request-1", "request-2")

	// Zero successes: the first reply 404s, so retry behavior is unchanged —
	// retry view with buttons retained and all IDs kept for re-tap.
	if err := broker.handle(context.Background(), permissionCallback(token, origin, reply)); err != nil {
		t.Fatalf("handle: %v", err)
	}

	calls := client.callSnapshot()
	if len(calls) != 1 {
		t.Fatalf("client calls = %d, want 1 (first 404 breaks the loop)", len(calls))
	}

	view := reply.lastEdit()
	if view.text != permissionRetryMessage || len(view.buttons) != 3 {
		t.Fatalf("retry view = %+v, want retry message with buttons retained", view)
	}

	broker.mu.Lock()
	record := broker.records[token]
	broker.mu.Unlock()
	if record == nil {
		t.Fatal("record missing after retry")
	}
	if record.state != permissionPending {
		t.Fatalf("record state = %v, want pending", stateName(record.state))
	}
	if len(record.requestIDs) != 2 || record.requestIDs[0] != "request-1" || record.requestIDs[1] != "request-2" {
		t.Fatalf("remaining request IDs = %v, want both kept for re-tap", record.requestIDs)
	}
}

func TestPermissionBrokerBatchAllRepliesSucceedResolves(t *testing.T) {
	client := &permissionClient{}
	broker, reply, token, origin := newPermissionBatchPrompt(t, client, "request-1", "request-2")

	// Full success batch keeps the existing behavior: terminal resolve.
	if err := broker.handle(context.Background(), permissionCallback(token, origin, reply)); err != nil {
		t.Fatalf("handle: %v", err)
	}

	calls := client.callSnapshot()
	if len(calls) != 2 {
		t.Fatalf("client calls = %d, want 2", len(calls))
	}

	view := reply.lastEdit()
	if view.text != "✅ Allowed once" || len(view.buttons) != 0 {
		t.Fatalf("terminal view = %+v", view)
	}

	broker.mu.Lock()
	record := broker.records[token]
	broker.mu.Unlock()
	if record == nil {
		t.Fatal("record missing after resolve")
	}
	if record.state != permissionResolved {
		t.Fatalf("record state = %v, want resolved", stateName(record.state))
	}
}
