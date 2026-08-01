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
	wait   func() error // blocks until exit; wraps exit errors with captured stderr
	stderr func() string
}

type runner func(ctx context.Context, binary string, args []string) (*runResult, error)

// Client implements relay.Client against a subprocess-invoked CLI agent: the
// CLI couples send and stream, so each SendMessage spawns one subprocess and
// pushes parsed stdout events onto a per-session channel Events returns.
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

// CreateSession returns a local placeholder id; the tool creates nothing
// until the first message is sent.
func (c *Client) CreateSession(ctx context.Context) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("cli: session id: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func (c *Client) Providers(ctx context.Context) (relay.Providers, error) {
	return relay.Providers{}, nil
}

// Events mints a fresh channel per call, mirroring one event stream per turn.
func (c *Client) Events(ctx context.Context, sessionID string) (<-chan relay.Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan relay.Event, 64)
	c.streams[sessionID] = ch
	return ch, nil
}

// SendMessage spawns one CLI invocation per call. The first call for a
// session runs without --resume; later calls resume with the tool's real
// session id captured from the previous result line.
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

// stream surfaces a run that produced no parsed events as an error event,
// never a silent empty response.
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
		// The child may still be writing to a full pipe; closing the read side
		// unblocks it before reaping.
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

// RunCommand is best-effort: CLI backends have no discrete command endpoint.
func (c *Client) RunCommand(ctx context.Context, sessionID, command string) error {
	return c.SendMessage(ctx, sessionID, command, nil, nil)
}

// ReplyPermission is unsupported: tools are pre-approved for unattended runs,
// so permission prompts never surface through this backend.
func (c *Client) ReplyPermission(ctx context.Context, requestID string, reply relay.PermissionReply) error {
	return fmt.Errorf("cli: permission replies are not supported by the CLI backend")
}

func (c *Client) closeStream(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch, ok := c.streams[sessionID]; ok {
		close(ch)
		delete(c.streams, sessionID)
	}
}
