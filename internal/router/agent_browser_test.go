package router

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

func TestAgentPicker_NoSession(t *testing.T) {
	r, _, reply := newTestRouter()
	msg := msgFrom("user1", "/agent", reply)
	if err := r.Route(context.Background(), msg); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "No active session") {
		t.Fatalf("expected no active session reply, got %v", reply.sends)
	}
}

func TestAgentPicker_RenderPage_TelegramAndDiscord(t *testing.T) {
	r, client, reply := newTestRouter()
	if err := r.store.SessionRepo().SetActive(context.Background(), "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatal(err)
	}
	if err := r.store.SessionRepo().SetActive(context.Background(), "discord", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatal(err)
	}

	client.agents = []relay.AgentInfo{
		{Name: "build", Description: "General software development agent", Mode: "primary", Native: true},
		{Name: "plan", Description: "Architectural planning and review", Mode: "primary", Native: true, Model: &relay.ModelRef{ProviderID: "anthropic", ID: "claude-3-5-sonnet"}},
		{Name: "reviewer", Description: "Code reviewer with custom prompt", Mode: "primary", Native: false, Model: &relay.ModelRef{ProviderID: "openrouter", ID: "glm-5.2"}},
		{Name: "tester", Description: "Test runner and assertion writer", Mode: "primary", Native: false},
		{Name: "docwriter", Description: "Documentation writer", Mode: "primary", Native: false},
		{Name: "secops", Description: "Security scanner", Mode: "primary", Native: false},
		{Name: "performance", Description: "Performance profiler", Mode: "primary", Native: false},
		{Name: "compaction", Description: "Internal compaction", Mode: "primary", Native: true},
		{Name: "summary", Description: "Internal summary", Mode: "primary", Native: true},
		{Name: "title", Description: "Internal title", Mode: "primary", Native: true},
		{Name: "general", Description: "General subagent", Mode: "subagent", Native: true},
		{Name: "explore", Description: "Explore subagent", Mode: "subagent", Native: true},
	}
	client.providers = relay.Providers{
		All: []relay.Provider{
			{ID: "anthropic", Models: map[string]json.RawMessage{"claude-3-5-sonnet": json.RawMessage(`{}`)}},
			{ID: "openrouter", Models: map[string]json.RawMessage{"glm-5.2": json.RawMessage(`{}`)}},
		},
		Connected: []string{"anthropic", "openrouter"},
	}

	t.Run("telegram layout and filtering", func(t *testing.T) {
		msg := msgFrom("user1", "/agent", reply)
		msg.Platform = "telegram"
		text, buttons, err := r.buildAgentPickerPage(context.Background(), msg, 1)
		if err != nil {
			t.Fatalf("buildAgentPickerPage: %v", err)
		}

		if !strings.Contains(text, "Page 1/2 · Agents") {
			t.Fatalf("expected page indicator, got:\n%s", text)
		}
		if !strings.Contains(text, "Active: build (default)") {
			t.Fatalf("expected active build line, got:\n%s", text)
		}
		if !strings.Contains(text, "1. → build — General software development agent") {
			t.Fatalf("expected build row with marker, got:\n%s", text)
		}
		if !strings.Contains(text, "2.   plan — Architectural planning and review — claude-3-5-sonnet") {
			t.Fatalf("expected plan row with model, got:\n%s", text)
		}
		if !strings.Contains(text, "3.   reviewer [custom] — Code reviewer with custom prompt — glm-5.2") {
			t.Fatalf("expected reviewer row marked [custom], got:\n%s", text)
		}
		if strings.Contains(text, "compaction") || strings.Contains(text, "summary") || strings.Contains(text, "title") {
			t.Fatalf("internal native agents must be hidden, got:\n%s", text)
		}
		if !strings.Contains(text, "Subagents (info only): general, explore") {
			t.Fatalf("expected subagents info line, got:\n%s", text)
		}
		if !strings.Contains(text, "Ask in chat to create new agents") {
			t.Fatalf("expected create hint footer, got:\n%s", text)
		}

		if len(buttons) < 7 {
			t.Fatalf("expected at least 7 buttons (6 items + nav), got %d", len(buttons))
		}
		if buttons[0].Row != 1 || buttons[1].Row != 1 {
			t.Fatalf("expected buttons 1 and 2 in row 1, got %d, %d", buttons[0].Row, buttons[1].Row)
		}
		if buttons[2].Row != 2 || buttons[3].Row != 2 {
			t.Fatalf("expected buttons 3 and 4 in row 2, got %d, %d", buttons[2].Row, buttons[3].Row)
		}
		if buttons[4].Row != 3 || buttons[5].Row != 3 {
			t.Fatalf("expected buttons 5 and 6 in row 3, got %d, %d", buttons[4].Row, buttons[5].Row)
		}
	})

	t.Run("discord layout", func(t *testing.T) {
		msg := msgFrom("user1", "/agent", reply)
		msg.Platform = "discord"
		_, buttons, err := r.buildAgentPickerPage(context.Background(), msg, 1)
		if err != nil {
			t.Fatalf("buildAgentPickerPage: %v", err)
		}

		for i := 0; i < 5; i++ {
			if buttons[i].Row != 1 {
				t.Fatalf("expected button %d in row 1, got row %d", i+1, buttons[i].Row)
			}
		}
		if buttons[5].Row != 2 {
			t.Fatalf("expected button 6 in row 2, got row %d", buttons[5].Row)
		}
	})

	t.Run("page 2 prev button", func(t *testing.T) {
		msg := msgFrom("user1", "/agent 2", reply)
		text, buttons, err := r.buildAgentPickerPage(context.Background(), msg, 2)
		if err != nil {
			t.Fatalf("buildAgentPickerPage: %v", err)
		}
		if !strings.Contains(text, "Page 2/2 · Agents") {
			t.Fatalf("expected page 2/2, got:\n%s", text)
		}
		if !strings.Contains(text, "7.   performance [custom]") {
			t.Fatalf("expected 7. performance row, got:\n%s", text)
		}

		var hasPrev bool
		for _, b := range buttons {
			if strings.Contains(b.Label, "Prev") {
				hasPrev = true
				break
			}
		}
		if !hasPrev {
			t.Fatal("expected Prev button on page 2")
		}
	})
}

func TestAgentPicker_UnknownModelWarning(t *testing.T) {
	r, client, reply := newTestRouter()
	if err := r.store.SessionRepo().SetActive(context.Background(), "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatal(err)
	}

	client.agents = []relay.AgentInfo{
		{Name: "build", Mode: "primary", Native: true},
		{Name: "custom-agent", Description: "Unconnected model agent", Mode: "primary", Native: false, Model: &relay.ModelRef{ProviderID: "fakeprovider", ID: "fake-model"}},
	}
	client.providers = relay.Providers{
		All: []relay.Provider{
			{ID: "openai", Models: map[string]json.RawMessage{"gpt-4o": json.RawMessage(`{}`)}},
		},
	}

	msg := msgFrom("user1", "/agent", reply)
	text, _, err := r.buildAgentPickerPage(context.Background(), msg, 1)
	if err != nil {
		t.Fatalf("buildAgentPickerPage: %v", err)
	}

	if !strings.Contains(text, "fake-model ⚠") {
		t.Fatalf("expected warning marker on unknown model, got:\n%s", text)
	}
	if !strings.Contains(text, "⚠ Pinned model not found in connected providers") {
		t.Fatalf("expected footnote warning, got:\n%s", text)
	}
}

func TestAgentSwitch_NumberNameSubstring(t *testing.T) {
	r, client, reply := newTestRouter()
	if err := r.store.SessionRepo().SetActive(context.Background(), "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatal(err)
	}

	client.agents = []relay.AgentInfo{
		{Name: "build", Mode: "primary", Native: true},
		{Name: "plan", Mode: "primary", Native: true},
		{Name: "reviewer", Mode: "primary", Native: false, Model: &relay.ModelRef{ProviderID: "openrouter", ID: "glm-5.2"}},
		{Name: "review-bot", Mode: "primary", Native: false},
	}

	t.Run("switch by number", func(t *testing.T) {
		msg := msgFrom("user1", "/agent switch 3", reply)
		out, err := r.handleAgent(context.Background(), msg, "switch 3")
		if err != nil {
			t.Fatalf("handleAgent: %v", err)
		}
		if !strings.Contains(out, "Switched to agent reviewer (glm-5.2)") {
			t.Fatalf("unexpected switch output: %s", out)
		}
		if len(client.switchAgentCalls) == 0 || client.switchAgentCalls[len(client.switchAgentCalls)-1].name != "reviewer" {
			t.Fatalf("unexpected switch call: %+v", client.switchAgentCalls)
		}
	})

	t.Run("switch by exact name", func(t *testing.T) {
		msg := msgFrom("user1", "/agent switch plan", reply)
		out, err := r.handleAgent(context.Background(), msg, "switch plan")
		if err != nil {
			t.Fatalf("handleAgent: %v", err)
		}
		if !strings.Contains(out, "Switched to agent plan") {
			t.Fatalf("unexpected switch output: %s", out)
		}
	})

	t.Run("switch by unique substring", func(t *testing.T) {
		msg := msgFrom("user1", "/agent switch plan", reply)
		out, err := r.handleAgent(context.Background(), msg, "switch pla")
		if err != nil {
			t.Fatalf("handleAgent: %v", err)
		}
		if !strings.Contains(out, "Switched to agent plan") {
			t.Fatalf("unexpected switch output: %s", out)
		}
	})

	t.Run("switch by ambiguous substring re-renders picker", func(t *testing.T) {
		msg := msgFrom("user1", "/agent switch review", reply)
		_, err := r.handleAgent(context.Background(), msg, "switch review")
		if err != nil && err != errReplied {
			t.Fatalf("handleAgent: %v", err)
		}
		if len(reply.sends) == 0 || !strings.Contains(reply.sends[len(reply.sends)-1], "Ambiguous agent \"review\"") {
			t.Fatalf("expected ambiguous picker re-render, got sends: %v", reply.sends)
		}
	})

	t.Run("switch unknown agent", func(t *testing.T) {
		msg := msgFrom("user1", "/agent switch non-existent", reply)
		out, err := r.handleAgent(context.Background(), msg, "switch non-existent")
		if err != nil {
			t.Fatalf("handleAgent: %v", err)
		}
		if !strings.Contains(out, "Agent not found — refresh with /agent") {
			t.Fatalf("expected agent not found, got %s", out)
		}
	})
}

func TestAgentCallback_SwitchAndExpired(t *testing.T) {
	r, client, reply := newTestRouter()
	if err := r.store.SessionRepo().SetActive(context.Background(), "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatal(err)
	}

	client.agents = []relay.AgentInfo{
		{Name: "build", Mode: "primary", Native: true},
		{Name: "reviewer", Mode: "primary", Native: false},
	}

	t.Run("valid switch callback clears buttons", func(t *testing.T) {
		ref := fakeRef{id: "msg-1"}
		fp := agentOwnerFingerprint(channel.IncomingMessage{Platform: "telegram", ChannelID: "chat1", UserID: "user1"})
		tok, err := r.agentBrowser.register(agentBrowseAction{
			kind:      agentActionSwitch,
			agentName: "reviewer",
			ownerFP:   fp,
		})
		if err != nil {
			t.Fatal(err)
		}

		msg := channel.IncomingMessage{
			Platform:     "telegram",
			ChannelID:    "chat1",
			UserID:       "user1",
			IsCallback:   true,
			CallbackData: fmt.Sprintf("agent:%s", tok),
			CallbackRef:  ref,
			ReplyCtx:     reply,
		}
		if err := r.handleCallback(context.Background(), msg); err != nil {
			t.Fatalf("handleCallback: %v", err)
		}
		if len(reply.edits) == 0 {
			t.Fatal("expected button edit")
		}
		edit := reply.edits[len(reply.edits)-1]
		if !strings.Contains(edit, "Switched to agent reviewer") {
			t.Fatalf("unexpected edit text: %s", edit)
		}
		if len(reply.buttons) == 0 || len(reply.buttons[len(reply.buttons)-1]) != 0 {
			t.Fatalf("expected buttons cleared on terminal switch, got: %v", reply.buttons)
		}
	})

	t.Run("expired callback renders expired notice", func(t *testing.T) {
		ref := fakeRef{id: "msg-2"}
		msg := channel.IncomingMessage{
			Platform:     "telegram",
			ChannelID:    "chat1",
			UserID:       "user1",
			IsCallback:   true,
			CallbackData: "agent:unregistered-token",
			CallbackRef:  ref,
			ReplyCtx:     reply,
		}
		if err := r.handleCallback(context.Background(), msg); err != nil {
			t.Fatalf("handleCallback: %v", err)
		}
		if len(reply.edits) == 0 {
			t.Fatal("expected expired button edit")
		}
		edit := reply.edits[len(reply.edits)-1]
		if !strings.Contains(edit, "Expired — use /agent again") {
			t.Fatalf("unexpected expired text: %s", edit)
		}
		if len(reply.buttons) == 0 || len(reply.buttons[len(reply.buttons)-1]) != 0 {
			t.Fatal("expected buttons cleared on expired notice")
		}
	})
}

func TestAgentDelete_SecurityAndFlow(t *testing.T) {
	r, client, reply, overrideRepo := newTestRouterWithAccess()
	overrideRepo.overrides["telegram:chat1:admin1"] = &store.UserOverride{
		ChannelID: "chat1",
		Platform:  "telegram",
		UserID:    "admin1",
		Role:      "admin",
	}
	overrideRepo.overrides["telegram:chat1:user1"] = &store.UserOverride{
		ChannelID: "chat1",
		Platform:  "telegram",
		UserID:    "user1",
		Role:      "user",
	}

	tmpDir := t.TempDir()
	r.defaultWorkdir = tmpDir
	agentDir := filepath.Join(tmpDir, ".opencode", "agent")
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		t.Fatal(err)
	}
	customFile := filepath.Join(agentDir, "mycustom.md")
	if err := os.WriteFile(customFile, []byte("# custom"), 0644); err != nil {
		t.Fatal(err)
	}

	client.agents = []relay.AgentInfo{
		{Name: "build", Mode: "primary", Native: true},
		{Name: "global-agent", Mode: "primary", Native: false},
		{Name: "mycustom", Mode: "primary", Native: false},
	}

	t.Run("non-admin rejected", func(t *testing.T) {
		msg := msgFrom("user1", "/agent delete mycustom", reply)
		out, err := r.handleAgent(context.Background(), msg, "delete mycustom")
		if err != nil {
			t.Fatalf("handleAgent: %v", err)
		}
		if !strings.Contains(out, "Access denied") {
			t.Fatalf("expected access denied, got %s", out)
		}
	})

	t.Run("native agent deletion refused", func(t *testing.T) {
		msg := msgFrom("admin1", "/agent delete build", reply)
		out, err := r.handleAgent(context.Background(), msg, "delete build")
		if err != nil {
			t.Fatalf("handleAgent: %v", err)
		}
		if !strings.Contains(out, "Built-in agents cannot be deleted") {
			t.Fatalf("expected built-in refusal, got %s", out)
		}
	})

	t.Run("global agent deletion refused", func(t *testing.T) {
		msg := msgFrom("admin1", "/agent delete global-agent", reply)
		out, err := r.handleAgent(context.Background(), msg, "delete global-agent")
		if err != nil {
			t.Fatalf("handleAgent: %v", err)
		}
		if !strings.Contains(out, "Global agents cannot be deleted") {
			t.Fatalf("expected global refusal, got %s", out)
		}
	})

	t.Run("traversal target refused in delete command", func(t *testing.T) {
		msg := msgFrom("admin1", "/agent delete ../../secret", reply)
		out, err := r.handleAgent(context.Background(), msg, "delete ../../secret")
		if err != nil {
			t.Fatalf("handleAgent: %v", err)
		}
		if !strings.Contains(out, "Agent not found — refresh with /agent") {
			t.Fatalf("expected agent not found for traversal name, got %s", out)
		}
	})

	t.Run("spoofed traversal callback refused", func(t *testing.T) {
		fp := agentOwnerFingerprint(channel.IncomingMessage{Platform: "telegram", ChannelID: "chat1", UserID: "admin1"})
		tok, _ := r.agentBrowser.register(agentBrowseAction{
			kind:      agentActionDeleteConfirm,
			agentName: "../../etc/passwd",
			ownerFP:   fp,
		})
		cbMsg := channel.IncomingMessage{
			Platform:     "telegram",
			ChannelID:    "chat1",
			UserID:       "admin1",
			IsCallback:   true,
			CallbackData: fmt.Sprintf("agent:%s", tok),
			CallbackRef:  fakeRef{id: "del-msg"},
			ReplyCtx:     reply,
		}
		if err := r.handleCallback(context.Background(), cbMsg); err != nil {
			t.Fatalf("handleCallback: %v", err)
		}
		edit := reply.edits[len(reply.edits)-1]
		if !strings.Contains(edit, "Invalid agent name") {
			t.Fatalf("expected invalid agent name for traversal callback, got: %s", edit)
		}
	})

	t.Run("spoofed unlisted agent callback refused", func(t *testing.T) {
		fp := agentOwnerFingerprint(channel.IncomingMessage{Platform: "telegram", ChannelID: "chat1", UserID: "admin1"})
		tok, _ := r.agentBrowser.register(agentBrowseAction{
			kind:      agentActionDeleteConfirm,
			agentName: "unlisted_agent",
			ownerFP:   fp,
		})
		cbMsg := channel.IncomingMessage{
			Platform:     "telegram",
			ChannelID:    "chat1",
			UserID:       "admin1",
			IsCallback:   true,
			CallbackData: fmt.Sprintf("agent:%s", tok),
			CallbackRef:  fakeRef{id: "del-msg"},
			ReplyCtx:     reply,
		}
		if err := r.handleCallback(context.Background(), cbMsg); err != nil {
			t.Fatalf("handleCallback: %v", err)
		}
		edit := reply.edits[len(reply.edits)-1]
		if !strings.Contains(edit, "Agent not found — refresh with /agent") {
			t.Fatalf("expected agent not found for unlisted callback, got: %s", edit)
		}
	})

	t.Run("project custom agent renders confirm buttons and executes deletion", func(t *testing.T) {
		msg := msgFrom("admin1", "/agent delete mycustom", reply)
		_, err := r.handleAgent(context.Background(), msg, "delete mycustom")
		if err != nil && err != errReplied {
			t.Fatalf("handleAgent: %v", err)
		}
		if len(reply.sends) == 0 {
			t.Fatal("expected confirm button prompt in sends")
		}
		prompt := reply.sends[len(reply.sends)-1]
		if !strings.Contains(prompt, "Are you sure you want to delete custom agent **mycustom**?") {
			t.Fatalf("unexpected prompt text: %s", prompt)
		}

		// Find the confirm button from reply.buttons
		if len(reply.buttons) == 0 || len(reply.buttons[len(reply.buttons)-1]) == 0 {
			t.Fatal("expected buttons in reply")
		}
		confirmBtn := reply.buttons[len(reply.buttons)-1][0]
		cbMsg := channel.IncomingMessage{
			Platform:     "telegram",
			ChannelID:    "chat1",
			UserID:       "admin1",
			IsCallback:   true,
			CallbackData: confirmBtn.Value,
			CallbackRef:  fakeRef{id: "del-msg"},
			ReplyCtx:     reply,
		}
		if err := r.handleCallback(context.Background(), cbMsg); err != nil {
			t.Fatalf("handleCallback: %v", err)
		}
		if _, err := os.Stat(customFile); !os.IsNotExist(err) {
			t.Fatalf("expected file deleted, stat err: %v", err)
		}
		edit := reply.edits[len(reply.edits)-1]
		if !strings.Contains(edit, "Deleted custom agent mycustom") {
			t.Fatalf("unexpected edit text: %s", edit)
		}
	})
}

func TestStatus_IncludesAgent(t *testing.T) {
	r, client, reply := newTestRouter()
	if err := r.store.SessionRepo().SetActive(context.Background(), "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatal(err)
	}

	client.sessionInfo = &relay.SessionInfo{
		Agent: "reviewer",
		Tokens: relay.SessionTokens{
			Input: 12000,
		},
	}
	client.agents = []relay.AgentInfo{
		{Name: "reviewer", Mode: "primary", Native: false, Model: &relay.ModelRef{ProviderID: "openrouter", ID: "glm-5.2"}},
	}

	msg := msgFrom("user1", "/status", reply)
	if err := r.Route(context.Background(), msg); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 {
		t.Fatal("expected status reply")
	}
	statusText := reply.sends[0]
	if !strings.Contains(statusText, "Agent: reviewer (openrouter/glm-5.2)") {
		t.Fatalf("expected Agent line in /status, got:\n%s", statusText)
	}
}

func TestNewAgentDetectionOnTurnEnd(t *testing.T) {
	r, _, reply := newTestRouter()
	tmpDir := t.TempDir()
	r.defaultWorkdir = tmpDir
	agentDir := filepath.Join(tmpDir, ".opencode", "agent")
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		t.Fatal(err)
	}

	r.agentTracker.retryDelay = 10 * time.Millisecond
	inst := &fakeAgentInstance{workdir: tmpDir}
	msg := msgFrom("user1", "hello", reply)

	t.Run("custom agent created during first turn is announced", func(t *testing.T) {
		reply.sends = nil
		// Pre-turn baseline snapshot taken before prompt execution
		r.agentTracker.snapshotWorkdir(tmpDir)

		// File created during first turn
		if err := os.WriteFile(filepath.Join(agentDir, "first_turn_agent.md"), []byte("# agent"), 0644); err != nil {
			t.Fatal(err)
		}

		r.detectNewAgents(context.Background(), msg, inst)
		if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "first_turn_agent") || !strings.Contains(reply.sends[0], "available — /agent to switch") {
			t.Fatalf("expected new agent notice on first turn, got: %v", reply.sends)
		}
	})

	t.Run("delayed file appearance via bounded retry announces agent", func(t *testing.T) {
		reply.sends = nil
		// Pre-turn snapshot already has first_turn_agent.md
		r.agentTracker.snapshotWorkdir(tmpDir)

		// Start a goroutine that writes the file after 5ms (during retry delay)
		go func() {
			time.Sleep(5 * time.Millisecond)
			_ = os.WriteFile(filepath.Join(agentDir, "delayed_agent.md"), []byte("# delayed"), 0644)
		}()

		r.detectNewAgents(context.Background(), msg, inst)
		if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "delayed_agent") {
			t.Fatalf("expected delayed agent notice via retry, got: %v", reply.sends)
		}
	})

	t.Run("turn where agent is deleted produces no notice", func(t *testing.T) {
		reply.sends = nil
		if err := os.Remove(filepath.Join(agentDir, "delayed_agent.md")); err != nil {
			t.Fatal(err)
		}
		r.detectNewAgents(context.Background(), msg, inst)
		if len(reply.sends) != 0 {
			t.Fatalf("expected no notice on agent deletion, got: %v", reply.sends)
		}
	})
}

type fakeAgentInstance struct {
	workdir string
	client  relay.Client
}

func (f *fakeAgentInstance) Client() relay.Client { return f.client }
func (f *fakeAgentInstance) End()                 {}
func (f *fakeAgentInstance) PID() int             { return 100 }
func (f *fakeAgentInstance) Workdir() string      { return f.workdir }
