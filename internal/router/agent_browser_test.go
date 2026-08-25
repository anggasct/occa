package router

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
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
		{Name: "experimental", Description: "Experimental agent", Mode: "unknown_mode", Native: false},
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
		if strings.Contains(text, "experimental") {
			t.Fatalf("unknown mode must be excluded, got:\n%s", text)
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

	t.Run("unconnected provider model triggers warning", func(t *testing.T) {
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
	})

	t.Run("model in catalog but provider not in connected triggers warning", func(t *testing.T) {
		client.agents = []relay.AgentInfo{
			{Name: "build", Mode: "primary", Native: true},
			{Name: "claude-agent", Description: "Agent with disconnected provider model", Mode: "primary", Native: false, Model: &relay.ModelRef{ProviderID: "anthropic", ID: "claude-3-5-sonnet"}},
		}
		client.providers = relay.Providers{
			All: []relay.Provider{
				{ID: "anthropic", Models: map[string]json.RawMessage{"claude-3-5-sonnet": json.RawMessage(`{}`)}},
				{ID: "openai", Models: map[string]json.RawMessage{"gpt-4o": json.RawMessage(`{}`)}},
			},
			Connected: []string{"openai"}, // anthropic is not connected!
		}

		msg := msgFrom("user1", "/agent", reply)
		text, _, err := r.buildAgentPickerPage(context.Background(), msg, 1)
		if err != nil {
			t.Fatalf("buildAgentPickerPage: %v", err)
		}

		if !strings.Contains(text, "claude-3-5-sonnet ⚠") {
			t.Fatalf("expected warning marker for disconnected provider, got:\n%s", text)
		}
		if !strings.Contains(text, "⚠ Pinned model not found in connected providers") {
			t.Fatalf("expected footnote warning, got:\n%s", text)
		}
	})
}

func TestAgentSwitch_NumberNameSubstringAndModeFilter(t *testing.T) {
	r, client, reply := newTestRouter()
	if err := r.store.SessionRepo().SetActive(context.Background(), "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatal(err)
	}

	client.agents = []relay.AgentInfo{
		{Name: "build", Mode: "primary", Native: true},
		{Name: "plan", Mode: "primary", Native: true},
		{Name: "reviewer", Mode: "primary", Native: false, Model: &relay.ModelRef{ProviderID: "openrouter", ID: "glm-5.2"}},
		{Name: "review-bot", Mode: "primary", Native: false},
		{Name: "general", Mode: "subagent", Native: true},
		{Name: "unknown-moded", Mode: "custom_mode", Native: false},
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

	t.Run("switch subagent refused", func(t *testing.T) {
		msg := msgFrom("user1", "/agent switch general", reply)
		out, err := r.handleAgent(context.Background(), msg, "switch general")
		if err != nil {
			t.Fatalf("handleAgent: %v", err)
		}
		if !strings.Contains(out, "Agent not found — refresh with /agent") {
			t.Fatalf("subagent switch should not be found, got %s", out)
		}
	})

	t.Run("switch unknown mode refused", func(t *testing.T) {
		msg := msgFrom("user1", "/agent switch unknown-moded", reply)
		out, err := r.handleAgent(context.Background(), msg, "switch unknown-moded")
		if err != nil {
			t.Fatalf("handleAgent: %v", err)
		}
		if !strings.Contains(out, "Agent not found — refresh with /agent") {
			t.Fatalf("unknown mode switch should not be found, got %s", out)
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

func TestAgentCallback_TokenRevocationOnReplacement(t *testing.T) {
	r, client, reply := newTestRouter()
	if err := r.store.SessionRepo().SetActive(context.Background(), "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatal(err)
	}

	client.agents = []relay.AgentInfo{
		{Name: "build", Mode: "primary", Native: true},
		{Name: "reviewer", Mode: "primary", Native: false},
	}

	msg := channel.IncomingMessage{Platform: "telegram", ChannelID: "chat1", UserID: "user1", ReplyCtx: reply}

	_, buttons1, err := r.buildAgentPickerPage(context.Background(), msg, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(buttons1) == 0 {
		t.Fatal("expected buttons in picker 1")
	}
	oldSwitchBtn := buttons1[0].Value

	_, buttons2, err := r.buildAgentPickerPage(context.Background(), msg, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(buttons2) == 0 {
		t.Fatal("expected buttons in picker 2")
	}

	ref := fakeRef{id: "msg-old"}
	oldMsg := channel.IncomingMessage{
		Platform:     "telegram",
		ChannelID:    "chat1",
		UserID:       "user1",
		IsCallback:   true,
		CallbackData: oldSwitchBtn,
		CallbackRef:  ref,
		ReplyCtx:     reply,
	}
	if err := r.handleCallback(context.Background(), oldMsg); err != nil {
		t.Fatalf("handleCallback: %v", err)
	}
	if len(reply.edits) == 0 {
		t.Fatal("expected expired button edit")
	}
	edit := reply.edits[len(reply.edits)-1]
	if !strings.Contains(edit, "Expired — use /agent again") {
		t.Fatalf("expected expired text for revoked picker token, got: %s", edit)
	}
}

func TestAgentDelete_SecuritySymlinksAndModeFilter(t *testing.T) {
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
		{Name: "subagent-to-delete", Mode: "subagent", Native: false},
		{Name: "unknown-mode-agent", Mode: "other_mode", Native: false},
	}

	t.Run("non-primary modes excluded from delete", func(t *testing.T) {
		msg := msgFrom("admin1", "/agent delete subagent-to-delete", reply)
		out, err := r.handleAgent(context.Background(), msg, "delete subagent-to-delete")
		if err != nil {
			t.Fatalf("handleAgent: %v", err)
		}
		if !strings.Contains(out, "Agent not found — refresh with /agent") {
			t.Fatalf("expected agent not found for subagent delete, got %s", out)
		}

		msg2 := msgFrom("admin1", "/agent delete unknown-mode-agent", reply)
		out2, err2 := r.handleAgent(context.Background(), msg2, "delete unknown-mode-agent")
		if err2 != nil {
			t.Fatalf("handleAgent: %v", err2)
		}
		if !strings.Contains(out2, "Agent not found — refresh with /agent") {
			t.Fatalf("expected agent not found for unknown mode delete, got %s", out2)
		}
	})

	t.Run("symlinked agent file or dir refused and external target preserved", func(t *testing.T) {
		extDir := t.TempDir()
		extFile := filepath.Join(extDir, "external_secret.md")
		if err := os.WriteFile(extFile, []byte("secret content"), 0644); err != nil {
			t.Fatal(err)
		}

		symFile := filepath.Join(agentDir, "symlinkagent.md")
		if err := os.Symlink(extFile, symFile); err != nil {
			t.Fatal(err)
		}

		client.agents = append(client.agents, relay.AgentInfo{Name: "symlinkagent", Mode: "primary", Native: false})

		msg := msgFrom("admin1", "/agent delete symlinkagent", reply)
		out, err := r.handleAgent(context.Background(), msg, "delete symlinkagent")
		if err != nil {
			t.Fatalf("handleAgent: %v", err)
		}
		if !strings.Contains(out, "Invalid agent file path") {
			t.Fatalf("expected invalid agent file path for symlink delete, got: %s", out)
		}

		if _, err := os.Stat(extFile); err != nil {
			t.Fatalf("external file should be intact, stat err: %v", err)
		}

		symWorkdir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(symWorkdir, ".opencode"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(extDir, filepath.Join(symWorkdir, ".opencode", "agent")); err != nil {
			t.Fatal(err)
		}
		if err := validateAndRemoveProjectAgentFile(symWorkdir, "external_secret", true); err == nil {
			t.Fatal("expected error for symlinked agent directory")
		}
		if _, err := os.Stat(extFile); err != nil {
			t.Fatalf("external file should be intact after symlinked dir delete attempt, stat err: %v", err)
		}
	})

	t.Run("race confirmed delete against symlink directory swap preserves external file", func(t *testing.T) {
		extDir := t.TempDir()
		extSecret := filepath.Join(extDir, "target_agent.md")
		if err := os.WriteFile(extSecret, []byte("protected"), 0644); err != nil {
			t.Fatal(err)
		}

		raceWorkdir := t.TempDir()
		opencodeDir := filepath.Join(raceWorkdir, ".opencode")
		realAgentDir := filepath.Join(opencodeDir, "agent")
		if err := os.MkdirAll(realAgentDir, 0755); err != nil {
			t.Fatal(err)
		}
		raceFile := filepath.Join(realAgentDir, "target_agent.md")
		if err := os.WriteFile(raceFile, []byte("victim"), 0644); err != nil {
			t.Fatal(err)
		}

		// Concurrently swap .opencode/agent with symlink to extDir while deleting
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = validateAndRemoveProjectAgentFile(raceWorkdir, "target_agent", true)
		}()
		go func() {
			defer wg.Done()
			_ = os.Rename(realAgentDir, filepath.Join(opencodeDir, "agent_old"))
			_ = os.Symlink(extDir, realAgentDir)
		}()
		wg.Wait()

		// Target file in external directory must NEVER be deleted
		if _, err := os.Stat(extSecret); err != nil {
			t.Fatalf("external secret file was deleted during race! err: %v", err)
		}
	})

	t.Run("fifo non-regular file delete promptly rejected without hanging", func(t *testing.T) {
		fifoWorkdir := t.TempDir()
		fifoDir := filepath.Join(fifoWorkdir, ".opencode", "agent")
		if err := os.MkdirAll(fifoDir, 0755); err != nil {
			t.Fatal(err)
		}
		fifoPath := filepath.Join(fifoDir, "fifo_agent.md")
		if err := syscall.Mkfifo(fifoPath, 0666); err != nil {
			t.Fatalf("Mkfifo: %v", err)
		}

		done := make(chan error, 1)
		go func() {
			done <- validateAndRemoveProjectAgentFile(fifoWorkdir, "fifo_agent", true)
		}()

		select {
		case err := <-done:
			if err == nil {
				t.Fatal("expected error rejecting FIFO non-regular file")
			}
		case <-time.After(1 * time.Second):
			t.Fatal("delete hung on FIFO file open!")
		}

		// FIFO must not be removed
		if _, err := os.Stat(fifoPath); err != nil {
			t.Fatalf("FIFO should be intact, stat err: %v", err)
		}
	})

	t.Run("valid custom agent deletion succeeds and refreshes picker", func(t *testing.T) {
		if err := r.store.SessionRepo().SetActive(context.Background(), "telegram", "chat1", "", "admin1", "sess-1", 100); err != nil {
			t.Fatal(err)
		}

		msg := msgFrom("admin1", "/agent delete mycustom", reply)
		_, err := r.handleAgent(context.Background(), msg, "delete mycustom")
		if err != nil && err != errReplied {
			t.Fatalf("handleAgent: %v", err)
		}
		if len(reply.sends) == 0 {
			t.Fatal("expected confirm button prompt in sends")
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
			t.Fatalf("expected delete confirmation banner, got: %s", edit)
		}
		if !strings.Contains(edit, "Page 1/1 · Agents") || !strings.Contains(edit, "Active: build (default)") {
			t.Fatalf("expected refreshed picker content, got:\n%s", edit)
		}
		if strings.Contains(edit, "mycustom") && !strings.Contains(edit, "Deleted custom agent mycustom") {
			t.Fatalf("refreshed picker must not contain deleted agent, got:\n%s", edit)
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
	client := &fakeRelayClient{}
	inst := &fakeAgentInstance{workdir: tmpDir, client: client}
	msg := msgFrom("user1", "hello", reply)

	t.Run("custom agent created and registered during first turn is announced", func(t *testing.T) {
		reply.sends = nil
		// Pre-turn baseline snapshot taken before prompt execution
		r.agentTracker.snapshotWorkdir(tmpDir)

		// File created on disk and registered in live client
		if err := os.WriteFile(filepath.Join(agentDir, "first_turn_agent.md"), []byte("# agent"), 0644); err != nil {
			t.Fatal(err)
		}
		client.mu.Lock()
		client.agents = []relay.AgentInfo{
			{Name: "first_turn_agent", Mode: "primary", Native: false},
		}
		client.mu.Unlock()

		r.detectNewAgents(context.Background(), msg, inst)
		if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "first_turn_agent") || !strings.Contains(reply.sends[0], "available — /agent to switch") {
			t.Fatalf("expected new agent notice on first turn, got: %v", reply.sends)
		}
	})

	t.Run("unregistered disk file produces no notice", func(t *testing.T) {
		reply.sends = nil
		// File created on disk but not in client ListAgents
		if err := os.WriteFile(filepath.Join(agentDir, "unregistered_file.md"), []byte("# unregistered"), 0644); err != nil {
			t.Fatal(err)
		}

		r.detectNewAgents(context.Background(), msg, inst)
		if len(reply.sends) != 0 {
			t.Fatalf("expected no notice for unregistered disk file, got: %v", reply.sends)
		}
	})

	t.Run("delayed live registry registration announces agent after retry", func(t *testing.T) {
		reply.sends = nil
		if err := os.WriteFile(filepath.Join(agentDir, "delayed_reg.md"), []byte("# delayed"), 0644); err != nil {
			t.Fatal(err)
		}

		// Client registers agent after 5ms (during retry delay)
		go func() {
			time.Sleep(5 * time.Millisecond)
			client.mu.Lock()
			client.agents = append(client.agents, relay.AgentInfo{Name: "delayed_reg", Mode: "primary", Native: false})
			client.mu.Unlock()
		}()

		r.detectNewAgents(context.Background(), msg, inst)
		if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "delayed_reg") {
			t.Fatalf("expected delayed agent notice via retry, got: %v", reply.sends)
		}
	})

	t.Run("turn where agent is deleted produces no notice", func(t *testing.T) {
		reply.sends = nil
		if err := os.Remove(filepath.Join(agentDir, "delayed_reg.md")); err != nil {
			t.Fatal(err)
		}
		r.detectNewAgents(context.Background(), msg, inst)
		if len(reply.sends) != 0 {
			t.Fatalf("expected no notice on agent deletion, got: %v", reply.sends)
		}
	})

	t.Run("canceled turn context does not abort detector registry fetch", func(t *testing.T) {
		reply.sends = nil
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel() // Simulate canceled context from runResponse completion

		if err := os.WriteFile(filepath.Join(agentDir, "turn_end_agent.md"), []byte("# end"), 0644); err != nil {
			t.Fatal(err)
		}
		client.mu.Lock()
		client.agents = append(client.agents, relay.AgentInfo{Name: "turn_end_agent", Mode: "primary", Native: false})
		client.mu.Unlock()

		r.detectNewAgents(canceledCtx, msg, inst)
		if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "turn_end_agent") {
			t.Fatalf("expected new agent notice despite canceled turn context, got: %v", reply.sends)
		}
	})
}

func TestAgentPicker_CLIBackendUnsupported(t *testing.T) {
	r, client, reply := newTestRouter()
	if err := r.store.SessionRepo().SetActive(context.Background(), "telegram", "chat1", "", "user1", "sess-1", 100); err != nil {
		t.Fatal(err)
	}

	client.mu.Lock()
	client.agentsErr = relay.ErrUnsupported
	client.switchAgentErr = relay.ErrUnsupported
	client.mu.Unlock()

	msg := msgFrom("user1", "/agent", reply)
	if err := r.Route(context.Background(), msg); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) == 0 || !strings.Contains(reply.sends[0], "Agent switching is not supported by the current agent backend") {
		t.Fatalf("expected unsupported backend message, got: %v", reply.sends)
	}

	msgSwitch := msgFrom("user1", "/agent switch reviewer", reply)
	out, err := r.handleAgent(context.Background(), msgSwitch, "switch reviewer")
	if err != nil {
		t.Fatalf("handleAgent: %v", err)
	}
	if !strings.Contains(out, "Agent switching is not supported by the current agent backend") {
		t.Fatalf("expected unsupported backend message on switch, got: %s", out)
	}
}

type fakeAgentInstance struct {
	workdir string
	client  relay.Client
}

func (f *fakeAgentInstance) Client() relay.Client { return f.client }
func (f *fakeAgentInstance) End()                 {}
func (f *fakeAgentInstance) PID() int             { return 100 }
func (f *fakeAgentInstance) Workdir() string      { return f.workdir }
