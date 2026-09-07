package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/router"
	"github.com/anggasct/occa/internal/store"
	"github.com/anggasct/occa/internal/webhook"
)

type stubChannel struct {
	name    string
	start   func(context.Context, func(channel.IncomingMessage)) error
	started chan struct{}
}

func (s *stubChannel) Name() string { return s.name }

func (s *stubChannel) Start(ctx context.Context, handler func(channel.IncomingMessage)) error {
	if s.started != nil {
		close(s.started)
	}
	return s.start(ctx, handler)
}

func (s *stubChannel) Stop() error                     { return nil }
func (s *stubChannel) Notify(_ string, _ string) error { return nil }

type stubRouter struct{ err error }

func (s stubRouter) Route(context.Context, channel.IncomingMessage) error { return s.err }

func TestRunChannelContainsPanic(t *testing.T) {
	panicking := &stubChannel{
		name: "discord",
		start: func(context.Context, func(channel.IncomingMessage)) error {
			var nilUser *struct{ ID string }
			_ = nilUser.ID
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		runChannel(context.Background(), panicking, stubRouter{})
	}()

	<-done
}

func TestRunChannelPanicDoesNotStopOtherChannels(t *testing.T) {
	ctx := context.Background()
	survivor := &stubChannel{
		name:    "telegram",
		started: make(chan struct{}),
		start: func(c context.Context, _ func(channel.IncomingMessage)) error {
			<-c.Done()
			return nil
		},
	}
	go runChannel(ctx, &stubChannel{
		name:  "discord",
		start: func(context.Context, func(channel.IncomingMessage)) error { panic("gateway identity") },
	}, stubRouter{})

	runCtx, cancel := context.WithCancel(ctx)
	go runChannel(runCtx, survivor, stubRouter{})

	<-survivor.started
	cancel()
}

func TestRunChannelReportsStartError(t *testing.T) {
	failing := &stubChannel{
		name: "telegram",
		start: func(context.Context, func(channel.IncomingMessage)) error {
			return errors.New("init failed")
		},
	}

	runChannel(context.Background(), failing, stubRouter{})
}

type captureChannel struct {
	name string
	send func(channelID, text string)
}

func (c *captureChannel) Name() string { return c.name }

func (c *captureChannel) Start(context.Context, func(channel.IncomingMessage)) error { return nil }
func (c *captureChannel) Stop() error                                                { return nil }
func (c *captureChannel) Notify(channelID, text string) error {
	if c.send != nil {
		c.send(channelID, text)
	}
	return nil
}

func TestNotifyEscapesMarkupForTelegram(t *testing.T) {
	var got string
	notify(&captureChannel{
		name: "telegram",
		send: func(_, text string) { got = text },
	}, "chat1", "scheduled run failed: <boom> & more")

	if strings.Contains(got, "<boom>") || !strings.Contains(got, "&lt;boom&gt;") || !strings.Contains(got, "&amp; more") {
		t.Fatalf("notify text not escaped for telegram: %q", got)
	}
}

func TestNotifyPassesThroughDiscord(t *testing.T) {
	var got string
	notify(&captureChannel{
		name: "discord",
		send: func(_, text string) { got = text },
	}, "123", "value <x> & more")

	if got == "" || !strings.Contains(got, "<x>") {
		t.Fatalf("discord content altered: %q", got)
	}
}

func TestNotifyWebhookAddsSeparatorToLifecycleMessage(t *testing.T) {
	var got string
	notifyWebhook(&captureChannel{
		name: "telegram",
		send: func(_, text string) { got = text },
	}, "chat1", "📨 Webhook: analyzing...")

	if want := webhook.FormatWebhookMessage("📨 Webhook: analyzing..."); got != want {
		t.Fatalf("webhook notification = %q, want %q", got, want)
	}
}

func TestNotifyLeavesOrdinaryChatUnchanged(t *testing.T) {
	var got string
	notify(&captureChannel{
		name: "telegram",
		send: func(_, text string) { got = text },
	}, "chat1", "ordinary chat")

	if got != "ordinary chat" {
		t.Fatalf("ordinary notification = %q, want ordinary chat", got)
	}
	if strings.Contains(got, "━━━━━━━━━━━━━━━━━━━━━━━━") {
		t.Fatalf("ordinary chat received webhook separator: %q", got)
	}
}

func TestDBSubcommandHelpExitsSuccessfully(t *testing.T) {
	for _, command := range []string{"backup", "restore"} {
		t.Run(command, func(t *testing.T) {
			if got := runDBCommand([]string{command, "--help"}); got != 0 {
				t.Fatalf("runDBCommand(%q, --help) = %d, want 0", command, got)
			}
		})
	}
}

func TestOpenStoreWithLockSerializesStartupAndRestore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "occa.db")

	first, firstLock, err := openStoreWithLock(dbPath, "")
	if err != nil {
		t.Fatalf("first startup: %v", err)
	}
	defer func() {
		_ = first.Close()
		_ = firstLock.Unlock()
	}()

	backupPath := filepath.Join(dir, "backup.db")
	if _, err := store.BackupFile(dbPath, backupPath, false); err != nil {
		t.Fatalf("backup during startup: %v", err)
	}
	if _, err := store.RestoreFile(dbPath, backupPath, false); !errors.Is(err, store.ErrDBInUse) {
		t.Fatalf("restore during initialized service = %v, want ErrDBInUse", err)
	}

	second, secondLock, err := openStoreWithLock(dbPath, "")
	if second != nil || secondLock != nil {
		if second != nil {
			_ = second.Close()
		}
		if secondLock != nil {
			_ = secondLock.Unlock()
		}
		t.Fatal("second startup returned initialized store while first startup was active")
	}
	if !errors.Is(err, store.ErrDBInUse) {
		t.Fatalf("second startup = %v, want ErrDBInUse", err)
	}
}

func TestOpenStoreWithLockReleasesAfterInitializationFailure(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "occa.db")
	if err := os.Mkdir(dbPath, 0o755); err != nil {
		t.Fatalf("create invalid database path: %v", err)
	}

	db, lock, err := openStoreWithLock(dbPath, "")
	if db != nil || lock != nil {
		t.Fatal("failed startup returned resources")
	}
	if err == nil {
		t.Fatal("startup unexpectedly succeeded against a directory")
	}

	lock, err = store.LockDB(dbPath)
	if err != nil {
		t.Fatalf("lock after failed startup: %v", err)
	}
	if err := lock.Unlock(); err != nil {
		t.Fatalf("unlock after failed startup: %v", err)
	}
}

type mockDiscordChannel struct {
	name           string
	mu             sync.Mutex
	sentMessages   []mockMessage
	editedMessages []mockEdit
	threadStarts   []mockThread
	startThreadErr error
	nextMsgID      string
	nextThreadID   string
}

type mockMessage struct {
	channelID string
	text      string
}

type mockEdit struct {
	channelID string
	messageID string
	text      string
}

type mockThread struct {
	channelID string
	messageID string
	name      string
}

func (m *mockDiscordChannel) Name() string                                               { return m.name }
func (m *mockDiscordChannel) Start(context.Context, func(channel.IncomingMessage)) error { return nil }
func (m *mockDiscordChannel) Stop() error                                                { return nil }
func (m *mockDiscordChannel) Notify(channelID, text string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentMessages = append(m.sentMessages, mockMessage{channelID, text})
	return nil
}
func (m *mockDiscordChannel) SendNotification(channelID, text string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentMessages = append(m.sentMessages, mockMessage{channelID, text})
	if m.nextMsgID != "" {
		return m.nextMsgID, nil
	}
	return fmt.Sprintf("msg-%d", len(m.sentMessages)), nil
}
func (m *mockDiscordChannel) EditNotification(channelID, messageID, text string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.editedMessages = append(m.editedMessages, mockEdit{channelID, messageID, text})
	return nil
}
func (m *mockDiscordChannel) StartThread(channelID, messageID, name string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.threadStarts = append(m.threadStarts, mockThread{channelID, messageID, name})
	if m.startThreadErr != nil {
		return "", m.startThreadErr
	}
	if m.nextThreadID != "" {
		return m.nextThreadID, nil
	}
	return "thread-999", nil
}

type mockAgentManager struct {
	client relay.Client
}

func (m *mockAgentManager) Instance(ctx context.Context, workdir string) (router.AgentInstance, error) {
	return &mockAgentInstance{client: m.client}, nil
}

type mockAgentInstance struct {
	client relay.Client
}

func (i *mockAgentInstance) Client() relay.Client { return i.client }
func (i *mockAgentInstance) End()                 {}
func (i *mockAgentInstance) PID() int             { return 1234 }
func (i *mockAgentInstance) Workdir() string      { return "/tmp" }

type mockRelayClient struct {
	relay.Client
	events []relay.Event
}

func (c *mockRelayClient) CreateSession(ctx context.Context) (string, error) {
	return "session-1", nil
}

func (c *mockRelayClient) Events(ctx context.Context, sessionID string) (<-chan relay.Event, error) {
	ch := make(chan relay.Event, len(c.events)+1)
	for _, ev := range c.events {
		ch <- ev
	}
	ch <- relay.Event{Type: "done"}
	return ch, nil
}

func (c *mockRelayClient) SendMessage(ctx context.Context, sessionID, text string, model *relay.ModelRef, attachments []relay.Attachment) error {
	return nil
}

func (c *mockRelayClient) ListMessages(ctx context.Context, sessionID string) ([]relay.MessageInfo, error) {
	return []relay.MessageInfo{{ID: "msg-1", Role: "assistant", Completed: 1}}, nil
}

func (c *mockRelayClient) AbortSession(ctx context.Context, sessionID string) error {
	return nil
}

type mockChannelStore struct{}

func (s mockChannelStore) Get(ctx context.Context, platform, channelID string) (*store.Channel, error) {
	return nil, nil
}

func TestNotifySendAndNotifyEdit(t *testing.T) {
	discordCh := &mockDiscordChannel{name: "discord", nextMsgID: "msg-test-1"}
	plainCh := &captureChannel{name: "telegram"}

	msgID, err := notifySend(discordCh, "chan-1", "hello")
	if err != nil {
		t.Fatalf("notifySend failed on discord: %v", err)
	}
	if msgID != "msg-test-1" {
		t.Fatalf("msgID = %q, want msg-test-1", msgID)
	}

	plainID, err := notifySend(plainCh, "chan-2", "hello")
	if err != nil {
		t.Fatalf("notifySend failed on plain channel: %v", err)
	}
	if plainID != "" {
		t.Fatalf("plainID = %q, want empty string", plainID)
	}

	if err := notifyEdit(discordCh, "chan-1", "msg-test-1", "edited"); err != nil {
		t.Fatalf("notifyEdit failed on discord: %v", err)
	}
	if len(discordCh.editedMessages) != 1 || discordCh.editedMessages[0].text != "edited" {
		t.Fatalf("discord editedMessages = %+v, want 1 edit", discordCh.editedMessages)
	}

	if err := notifyEdit(plainCh, "chan-2", "msg-1", "edited"); err == nil {
		t.Fatal("notifyEdit on plain channel should return error")
	}
}

func TestWebhookExecutorWithThread(t *testing.T) {
	discordCh := &mockDiscordChannel{
		name:         "discord",
		nextMsgID:    "msg-root-1",
		nextThreadID: "thread-999",
	}

	events := []relay.Event{
		{Type: relay.EventTool, Delta: "bash", ToolContext: "git status"},
		{Type: relay.EventTool, Delta: "bash", ToolContext: "git status -s", ToolSamePart: true},
		{Type: relay.EventTool, Delta: "read_file", ToolContext: "main.go"},
		{Type: relay.EventTool, Delta: "bash", ToolContext: "go build"},
		{Type: relay.EventTool, Delta: "write_to_file", ToolContext: "main.go"},
		{Type: relay.EventTool, Delta: "bash", ToolContext: "go test"},
		{Type: relay.EventTool, Delta: "bash", ToolContext: "echo suppressed"},
		{Type: relay.EventDelta, Delta: "Analysis output line."},
	}

	agentClient := &mockRelayClient{events: events}
	manager := &mockAgentManager{client: agentClient}
	exec := newWebhookExecutor([]channel.Channel{discordCh}, manager, mockChannelStore{}, "/tmp")

	workCtx := &webhook.WebhookWorkContext{
		Thread:     true,
		Workflow:   "github_reviewer",
		Envelope:   webhook.WebhookEnvelope{"pr_number": "42", "head_branch": "feat/thread"},
		DeliveryID: "del-123",
	}

	err := exec(context.Background(), "discord", "chan-main", "prompt text", workCtx)
	if err != nil {
		t.Fatalf("executor returned error: %v", err)
	}

	if len(discordCh.threadStarts) != 1 {
		t.Fatalf("threadStarts count = %d, want 1", len(discordCh.threadStarts))
	}
	th := discordCh.threadStarts[0]
	if th.channelID != "chan-main" || th.messageID != "msg-root-1" {
		t.Fatalf("thread start target = %+v, want chan-main / msg-root-1", th)
	}
	if th.name != "PR #42: github_reviewer (feat/thread)" {
		t.Fatalf("thread name = %q, want PR #42: github_reviewer (feat/thread)", th.name)
	}

	if workCtx.RootMessageID != "msg-root-1" {
		t.Errorf("workCtx.RootMessageID = %q, want msg-root-1", workCtx.RootMessageID)
	}
	if workCtx.ThreadID != "thread-999" {
		t.Errorf("workCtx.ThreadID = %q, want thread-999", workCtx.ThreadID)
	}

	var mainMsgs, threadMsgs []string
	for _, m := range discordCh.sentMessages {
		switch m.channelID {
		case "chan-main":
			mainMsgs = append(mainMsgs, m.text)
		case "thread-999":
			threadMsgs = append(threadMsgs, m.text)
		}
	}

	if len(mainMsgs) != 1 || !strings.Contains(mainMsgs[0], "Status: RUNNING") {
		t.Fatalf("mainMsgs = %+v, want single RUNNING root card", mainMsgs)
	}

	var foundGreeting, foundWorking, foundOutput bool
	var toolBubbleCount int
	for _, text := range threadMsgs {
		if strings.Contains(text, "🚀 Starting task:") {
			foundGreeting = true
		}
		if strings.Contains(text, "⚙️") {
			toolBubbleCount++
		}
		if strings.Contains(text, "🔄 Working...") {
			foundWorking = true
		}
		if strings.Contains(text, "Analysis output line.") {
			foundOutput = true
		}
		if strings.Contains(text, "echo suppressed") {
			t.Errorf("found 6th tool bubble that should have been suppressed: %q", text)
		}
	}

	if !foundGreeting {
		t.Error("missing greeting message in thread")
	}
	if toolBubbleCount != 5 {
		t.Errorf("toolBubbleCount = %d, want 5", toolBubbleCount)
	}
	if !foundWorking {
		t.Error("missing 🔄 Working... message on 5th tool bubble")
	}
	if !foundOutput {
		t.Error("missing final agent output in thread")
	}

	if len(discordCh.editedMessages) == 0 {
		t.Error("expected ToolSamePart to edit previous tool message with updated context")
	} else if !strings.Contains(discordCh.editedMessages[0].text, "git status -s") {
		t.Errorf("edited message does not contain updated context: %+v", discordCh.editedMessages[0])
	}
}

func TestWebhookExecutorThreadCreationFallback(t *testing.T) {
	discordCh := &mockDiscordChannel{
		name:           "discord",
		nextMsgID:      "msg-root-fallback",
		startThreadErr: errors.New("Missing Permissions: CreatePublicThreads"),
	}

	agentClient := &mockRelayClient{
		events: []relay.Event{
			{Type: relay.EventDelta, Delta: "Fallback completed."},
		},
	}
	manager := &mockAgentManager{client: agentClient}
	exec := newWebhookExecutor([]channel.Channel{discordCh}, manager, mockChannelStore{}, "/tmp")

	workCtx := &webhook.WebhookWorkContext{
		Thread:     true,
		Workflow:   "github_reviewer",
		Envelope:   webhook.WebhookEnvelope{"pr_number": "10"},
		DeliveryID: "del-fallback",
	}

	err := exec(context.Background(), "discord", "chan-main", "prompt", workCtx)
	if err != nil {
		t.Fatalf("executor should not fail when thread creation fails: %v", err)
	}

	var mainMsgs []string
	for _, m := range discordCh.sentMessages {
		if m.channelID == "chan-main" {
			mainMsgs = append(mainMsgs, m.text)
		}
	}

	var foundFallbackOutput bool
	for _, text := range mainMsgs {
		if strings.Contains(text, "Fallback completed.") {
			foundFallbackOutput = true
		}
	}
	if !foundFallbackOutput {
		t.Fatalf("expected output delivered to chan-main on fallback, got mainMsgs: %+v", mainMsgs)
	}
}

func TestWebhookExecutorWithoutThread(t *testing.T) {
	discordCh := &mockDiscordChannel{name: "discord"}
	agentClient := &mockRelayClient{
		events: []relay.Event{
			{Type: relay.EventDelta, Delta: "Channel output."},
		},
	}
	manager := &mockAgentManager{client: agentClient}
	exec := newWebhookExecutor([]channel.Channel{discordCh}, manager, mockChannelStore{}, "/tmp")

	workCtx := &webhook.WebhookWorkContext{
		Thread:     false,
		Workflow:   "github_reviewer",
		DeliveryID: "del-nothread",
	}

	err := exec(context.Background(), "discord", "chan-main", "prompt", workCtx)
	if err != nil {
		t.Fatalf("executor returned error: %v", err)
	}

	if len(discordCh.threadStarts) != 0 {
		t.Fatalf("threadStarts = %d, want 0 when Thread: false", len(discordCh.threadStarts))
	}

	var foundAnalyzing, foundOutput bool
	for _, m := range discordCh.sentMessages {
		if strings.Contains(m.text, "📨 Webhook: analyzing...") {
			foundAnalyzing = true
		}
		if strings.Contains(m.text, "Channel output.") {
			foundOutput = true
		}
	}
	if !foundAnalyzing {
		t.Error("missing 📨 Webhook: analyzing... for non-thread execution")
	}
	if !foundOutput {
		t.Error("missing output for non-thread execution")
	}
}
