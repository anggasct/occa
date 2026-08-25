package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"

	"github.com/anggasct/occa/internal/relay"
)

type runResult struct {
	stdout io.ReadCloser
	wait   func() error
	stderr func() string
}

type runner func(ctx context.Context, binary string, args []string) (*runResult, error)

type Client struct {
	binary  string
	run     runner
	mu      sync.Mutex
	realIDs map[string]string
	streams map[string]chan relay.Event
}

func New(binary string) *Client {
	return &Client{
		binary:  binary,
		run:     execRunner,
		realIDs: make(map[string]string),
		streams: make(map[string]chan relay.Event),
	}
}

func execRunner(ctx context.Context, binary string, args []string) (*runResult, error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("cli: stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &runResult{
		stdout: stdout,
		wait: func() error {
			err := cmd.Wait()
			if err != nil {
				return fmt.Errorf("cli: %w: %s", err, strings.TrimSpace(stderr.String()))
			}
			return nil
		},
		stderr: func() string { return strings.TrimSpace(stderr.String()) },
	}, nil
}

var _ relay.Client = (*Client)(nil)

func (c *Client) CreateSession(ctx context.Context) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("cli: session id: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func (c *Client) GetSession(ctx context.Context, sessionID string) (*relay.SessionInfo, error) {
	return &relay.SessionInfo{}, nil
}

func (c *Client) SessionExists(ctx context.Context, sessionID string) (bool, error) {
	return sessionID != "", nil
}

func (c *Client) AbortSession(ctx context.Context, sessionID string) error {
	return nil
}

func (c *Client) SummarizeSession(ctx context.Context, sessionID, providerID, modelID string) error {
	return fmt.Errorf("cli: session state commands are not supported by the CLI backend")
}

func (c *Client) RevertMessage(ctx context.Context, sessionID, messageID string) error {
	return fmt.Errorf("cli: session state commands are not supported by the CLI backend")
}

func (c *Client) UnrevertSession(ctx context.Context, sessionID string) error {
	return fmt.Errorf("cli: session state commands are not supported by the CLI backend")
}

func (c *Client) ListMessages(ctx context.Context, sessionID string) ([]relay.MessageInfo, error) {
	return nil, fmt.Errorf("cli: list messages is not supported by the CLI backend")
}

func (c *Client) Providers(ctx context.Context) (relay.Providers, error) {
	return relay.Providers{}, nil
}

func (c *Client) ListCommands(ctx context.Context) ([]relay.CommandInfo, error) {
	return nil, nil
}

func (c *Client) ListAgents(ctx context.Context) ([]relay.AgentInfo, error) {
	return nil, nil
}

func (c *Client) SwitchAgent(ctx context.Context, sessionID, name string) error {
	return fmt.Errorf("cli: agent switching is not supported by the CLI backend")
}

func (c *Client) Events(ctx context.Context, sessionID string) (<-chan relay.Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan relay.Event, 64)
	c.streams[sessionID] = ch
	return ch, nil
}

func (c *Client) SendMessage(ctx context.Context, sessionID, text string, model *relay.ModelRef, attachments []relay.Attachment) error {
	if len(attachments) > 0 {
		return fmt.Errorf("cli: attachments are not supported by the CLI backend")
	}

	c.mu.Lock()
	realID := c.realIDs[sessionID]
	ch, ok := c.streams[sessionID]
	if !ok {
		ch = make(chan relay.Event, 64)
		c.streams[sessionID] = ch
	}
	c.mu.Unlock()

	args := []string{"-p", text, "--output-format", "stream-json", "--verbose"}
	if realID != "" {
		args = append(args, "--resume", realID)
	}
	if model != nil {
		args = append(args, "--model", model.ID)
	}

	res, err := c.run(ctx, c.binary, args)
	if err != nil {
		c.closeStream(sessionID)
		return fmt.Errorf("cli: spawn %s: %w", c.binary, err)
	}

	go c.stream(ctx, sessionID, ch, res)
	return nil
}

func (c *Client) stream(ctx context.Context, sessionID string, ch chan<- relay.Event, res *runResult) {
	defer c.closeStream(sessionID)
	defer func() { _ = res.stdout.Close() }()

	p := &parser{}
	events := 0
	scanner := bufio.NewScanner(res.stdout)
	scanner.Buffer(make([]byte, 64*1024), relay.MaxEventLineBytes+1)
	for scanner.Scan() {
		if ev := p.parseLine(scanner.Bytes()); ev != nil {
			events++
			select {
			case ch <- *ev:
			case <-ctx.Done():
				return
			}
		}
	}

	if p.realID != "" {
		c.mu.Lock()
		c.realIDs[sessionID] = p.realID
		c.mu.Unlock()
	}

	if scanErr := scanner.Err(); scanErr != nil {
		// Pipe unblocking before reaping
		_ = res.stdout.Close()
		_ = res.wait()
		select {
		case ch <- relay.Event{Type: "error", Delta: "cli: stream truncated: " + scanErr.Error()}:
		case <-ctx.Done():
		}
		return
	}

	if waitErr := res.wait(); waitErr != nil {
		select {
		case ch <- relay.Event{Type: "error", Delta: waitErr.Error()}:
		case <-ctx.Done():
		}
		return
	}
	if events == 0 {
		message := "no output produced by " + c.binary
		if stderr := res.stderr(); stderr != "" {
			message += ": " + stderr
		}
		select {
		case ch <- relay.Event{Type: "error", Delta: message}:
		case <-ctx.Done():
		}
	}
}

func (c *Client) RunCommand(ctx context.Context, sessionID, command string) error {
	return c.SendMessage(ctx, sessionID, command, nil, nil)
}

func (c *Client) ReplyPermission(ctx context.Context, requestID string, reply relay.PermissionReply) error {
	return fmt.Errorf("cli: permission replies are not supported by the CLI backend")
}

func (c *Client) AnswerQuestion(ctx context.Context, requestID string, answers [][]string) error {
	return fmt.Errorf("cli: question replies are not supported by the CLI backend")
}

func (c *Client) RejectQuestion(ctx context.Context, requestID string) error {
	return fmt.Errorf("cli: question replies are not supported by the CLI backend")
}

func (c *Client) closeStream(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch, ok := c.streams[sessionID]; ok {
		close(ch)
		delete(c.streams, sessionID)
	}
}
