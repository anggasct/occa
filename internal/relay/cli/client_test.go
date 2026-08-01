package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/relay"
)

type fakeRunner struct {
	mu       sync.Mutex
	calls    [][]string
	outputs  []string
	waits    []error
	stderrs  []string
	spawnErr error
}

func (f *fakeRunner) runner() runner {
	return func(ctx context.Context, binary string, args []string) (*runResult, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls = append(f.calls, append([]string(nil), args...))
		if f.spawnErr != nil {
			return nil, f.spawnErr
		}
		i := len(f.calls) - 1
		var out string
		if i < len(f.outputs) {
			out = f.outputs[i]
		}
		var waitErr error
		if i < len(f.waits) {
			waitErr = f.waits[i]
		}
		var stderr string
		if i < len(f.stderrs) {
			stderr = f.stderrs[i]
		}
		wait := waitErr
		if wait != nil && stderr != "" {
			wait = fmt.Errorf("%w: %s", wait, stderr)
		}
		return &runResult{
			stdout: io.NopCloser(strings.NewReader(out)),
			wait:   func() error { return wait },
			stderr: func() string { return stderr },
		}, nil
	}
}

func (f *fakeRunner) callArgs(i int) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls[i]...)
}

func drainEvents(t *testing.T, ch <-chan relay.Event) []relay.Event {
	t.Helper()
	var events []relay.Event
	timeout := time.After(3 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return events
			}
			events = append(events, ev)
		case <-timeout:
			t.Fatal("event stream did not close")
		}
	}
}

func TestCreateSessionMintsPlaceholderWithoutSpawning(t *testing.T) {
	runner := &fakeRunner{}
	c := New("claude")
	c.run = runner.runner()

	sid, err := c.CreateSession(context.Background())
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if sid == "" {
		t.Fatal("placeholder session id is empty")
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if len(runner.calls) != 0 {
		t.Fatalf("CreateSession spawned a subprocess: %d calls", len(runner.calls))
	}
}

func TestSendMessageParsesJSONLAndResumes(t *testing.T) {
	runner := &fakeRunner{
		outputs: []string{
			`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"text":"hello "}}}
{"type":"stream_event","event":{"type":"content_block_delta","delta":{"text":"world"}}}
{"type":"result","subtype":"success","result":"hello world","session_id":"real-1"}`,
			`{"type":"assistant","message":{"content":[{"type":"text","text":"second turn"}]}}
{"type":"result","subtype":"success","result":"second turn","session_id":"real-1"}`,
		},
	}
	c := New("claude")
	c.run = runner.runner()

	events, err := c.Events(context.Background(), "sess-a")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if err := c.SendMessage(context.Background(), "sess-a", "first prompt", nil, nil); err != nil {
		t.Fatalf("SendMessage 1: %v", err)
	}
	got := drainEvents(t, events)
	if len(got) != 3 || got[0].Type != "delta" || got[0].Delta != "hello " || got[1].Delta != "world" || got[2].Type != "done" {
		t.Fatalf("unexpected first-turn events: %+v", got)
	}
	args1 := runner.callArgs(0)
	if contains(args1, "--resume") {
		t.Fatalf("first turn must not resume: %v", args1)
	}

	events2, err := c.Events(context.Background(), "sess-a")
	if err != nil {
		t.Fatalf("Events 2: %v", err)
	}
	if err := c.SendMessage(context.Background(), "sess-a", "second prompt", nil, nil); err != nil {
		t.Fatalf("SendMessage 2: %v", err)
	}
	got2 := drainEvents(t, events2)
	if len(got2) != 2 || got2[0].Delta != "second turn" || got2[1].Type != "done" {
		t.Fatalf("unexpected second-turn events: %+v", got2)
	}
	args2 := runner.callArgs(1)
	idx := indexOf(args2, "--resume")
	if idx < 0 || idx+1 >= len(args2) || args2[idx+1] != "real-1" {
		t.Fatalf("second turn must resume with real-1: %v", args2)
	}
}

func TestSendMessageModelFlag(t *testing.T) {
	runner := &fakeRunner{outputs: []string{`{"type":"result","session_id":"r1"}`}}
	c := New("claude")
	c.run = runner.runner()

	events, _ := c.Events(context.Background(), "sess")
	if err := c.SendMessage(context.Background(), "sess", "hi", &relay.ModelRef{ProviderID: "claude", ID: "opus-4"}, nil); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	drainEvents(t, events)
	args := runner.callArgs(0)
	idx := indexOf(args, "--model")
	if idx < 0 || args[idx+1] != "opus-4" {
		t.Fatalf("model flag not threaded: %v", args)
	}
}

func TestSendMessageRejectsAttachments(t *testing.T) {
	runner := &fakeRunner{}
	c := New("claude")
	c.run = runner.runner()

	err := c.SendMessage(context.Background(), "sess", "hi", nil, []relay.Attachment{{Filename: "a.txt"}})
	if err == nil {
		t.Fatal("expected error for attachments")
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if len(runner.calls) != 0 {
		t.Fatal("subprocess spawned despite attachment rejection")
	}
}

func TestNonZeroExitSurfacesStderrEvent(t *testing.T) {
	runner := &fakeRunner{
		outputs: []string{`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"text":"partial"}}}`},
		waits:   []error{errors.New("exit status 1")},
		stderrs: []string{"tool denied: write /tmp/x"},
	}
	c := New("claude")
	c.run = runner.runner()

	events, _ := c.Events(context.Background(), "sess")
	if err := c.SendMessage(context.Background(), "sess", "hi", nil, nil); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	got := drainEvents(t, events)
	if len(got) != 2 || got[0].Delta != "partial" || got[1].Type != "error" {
		t.Fatalf("unexpected events: %+v", got)
	}
	if !strings.Contains(got[1].Delta, "tool denied: write /tmp/x") {
		t.Fatalf("stderr not carried: %q", got[1].Delta)
	}
}

func TestEmptyOutputIsErrorNotSilence(t *testing.T) {
	runner := &fakeRunner{outputs: []string{""}}
	c := New("claude")
	c.run = runner.runner()

	events, _ := c.Events(context.Background(), "sess")
	if err := c.SendMessage(context.Background(), "sess", "hi", nil, nil); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	got := drainEvents(t, events)
	if len(got) != 1 || got[0].Type != "error" {
		t.Fatalf("expected a single error event, got %+v", got)
	}
	if !strings.Contains(got[0].Delta, "no output produced") {
		t.Fatalf("unexpected error text: %q", got[0].Delta)
	}
}

func TestEmptyOutputCarriesStderrNotice(t *testing.T) {
	runner := &fakeRunner{outputs: []string{""}, stderrs: []string{"tool soft-denied: read /etc/passwd"}}
	c := New("claude")
	c.run = runner.runner()

	events, _ := c.Events(context.Background(), "sess")
	if err := c.SendMessage(context.Background(), "sess", "hi", nil, nil); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	got := drainEvents(t, events)
	if len(got) != 1 || !strings.Contains(got[0].Delta, "tool soft-denied: read /etc/passwd") {
		t.Fatalf("stderr notice lost: %+v", got)
	}
}

func TestRunCommandPassesThroughAsPrompt(t *testing.T) {
	runner := &fakeRunner{outputs: []string{`{"type":"result","session_id":"r1"}`}}
	c := New("claude")
	c.run = runner.runner()

	events, _ := c.Events(context.Background(), "sess")
	if err := c.RunCommand(context.Background(), "sess", "run the tests"); err != nil {
		t.Fatalf("RunCommand: %v", err)
	}
	drainEvents(t, events)
	args := runner.callArgs(0)
	idx := indexOf(args, "-p")
	if idx < 0 || idx+1 >= len(args) || args[idx+1] != "run the tests" {
		t.Fatalf("command not passed through as prompt: %v", args)
	}
}

func TestSpawnFailureReturnsErrorAndClosesStream(t *testing.T) {
	runner := &fakeRunner{spawnErr: errors.New("exec: claude not found")}
	c := New("claude")
	c.run = runner.runner()

	events, _ := c.Events(context.Background(), "sess")
	err := c.SendMessage(context.Background(), "sess", "hi", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected spawn error, got %v", err)
	}
	got := drainEvents(t, events)
	if len(got) != 0 {
		t.Fatalf("expected no events on spawn failure, got %+v", got)
	}
}

func TestReplyPermissionUnsupported(t *testing.T) {
	c := New("claude")
	if err := c.ReplyPermission(context.Background(), "req", relay.PermissionOnce); err == nil {
		t.Fatal("expected unsupported error")
	}
}

func contains(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func indexOf(args []string, want string) int {
	for i, a := range args {
		if a == want {
			return i
		}
	}
	return -1
}

func TestLargeLineDeliveredUpToCeiling(t *testing.T) {
	big := strings.Repeat("x", 100*1024)
	line := `{"type":"stream_event","event":{"type":"content_block_delta","delta":{"text":"` + big + `"}}}`
	runner := &fakeRunner{outputs: []string{line + "\n" + `{"type":"result","session_id":"r1"}` + "\n"}}
	c := New("claude")
	c.run = runner.runner()

	events, _ := c.Events(context.Background(), "sess")
	if err := c.SendMessage(context.Background(), "sess", "hi", nil, nil); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	got := drainEvents(t, events)
	if len(got) != 2 || got[0].Type != "delta" || got[0].Delta != big || got[1].Type != "done" {
		t.Fatalf("large line not delivered intact: %d events, first=%q", len(got), got[0].Delta[:min(20, len(got[0].Delta))])
	}
}

func TestOverCeilingLineSurfacesErrorEvent(t *testing.T) {
	huge := strings.Repeat("y", relay.MaxEventLineBytes+1)
	line := `{"type":"stream_event","event":{"type":"content_block_delta","delta":{"text":"` + huge + `"}}}`
	runner := &fakeRunner{outputs: []string{line + "\n"}}
	c := New("claude")
	c.run = runner.runner()

	events, _ := c.Events(context.Background(), "sess")
	if err := c.SendMessage(context.Background(), "sess", "hi", nil, nil); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	got := drainEvents(t, events)
	if len(got) != 1 || got[0].Type != "error" {
		t.Fatalf("expected a truncation error event, got %+v", got)
	}
	if !strings.Contains(got[0].Delta, "truncated") {
		t.Fatalf("unexpected error text: %q", got[0].Delta)
	}
}

func TestNonSuccessResultSubtypeIsError(t *testing.T) {
	runner := &fakeRunner{outputs: []string{`{"type":"result","subtype":"error_limit","session_id":"r1"}` + "\n"}}
	c := New("claude")
	c.run = runner.runner()

	events, _ := c.Events(context.Background(), "sess")
	if err := c.SendMessage(context.Background(), "sess", "hi", nil, nil); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	got := drainEvents(t, events)
	if len(got) != 1 || got[0].Type != "error" || !strings.Contains(got[0].Delta, "error_limit") {
		t.Fatalf("expected error event for non-success subtype, got %+v", got)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestListCommandsReturnsEmpty(t *testing.T) {
	c := New("claude")
	commands, err := c.ListCommands(context.Background())
	if err != nil {
		t.Fatalf("ListCommands: %v", err)
	}
	if commands != nil {
		t.Fatalf("expected nil commands, got %v", commands)
	}
}
