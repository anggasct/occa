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
	mu       sync.Mutex
	calls    []permissionCall
	errors   []error
	started  chan struct{}
	startOne sync.Once
	release  chan struct{}
}

func (c *permissionClient) CreateSession(_ context.Context) (string, error) { return "session", nil }
func (c *permissionClient) SendMessage(_ context.Context, _ string, _ string, _ *relay.ModelRef, _ []relay.Attachment) error {
	return nil
}
func (c *permissionClient) Providers(_ context.Context) (relay.Providers, error) {
	return relay.Providers{}, nil
}
func (c *permissionClient) RunCommand(_ context.Context, _ string, _ string) error { return nil }
func (c *permissionClient) Events(_ context.Context, _ string) (<-chan relay.Event, error) {
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
	c.mu.Unlock()
	if c.started != nil {
		c.startOne.Do(func() { close(c.started) })
	}
	if release != nil {
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
