package relay

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrUnreachable        = errors.New("agent unreachable")
	ErrNotFound           = errors.New("agent resource not found")
	ErrTimeout            = errors.New("agent request timed out")
	ErrAttachmentTooLarge = errors.New("attachment exceeds size limit")
)

const maxAttachmentSize = 10 * 1024 * 1024

type Attachment struct {
	Filename string
	MimeType string
	Data     []byte
}

type ModelRef struct {
	ProviderID string `json:"providerID"`
	ID         string `json:"modelID"`
}

type Provider struct {
	ID     string                     `json:"id"`
	Models map[string]json.RawMessage `json:"models"`
}

type Providers struct {
	All []Provider `json:"all"`
	// Connected lists the provider ids with working credentials, as
	// reported by the agent; empty when the backend does not report it.
	Connected []string `json:"connected"`
}

func (p Providers) HasProvider(id string) bool {
	for _, provider := range p.All {
		if provider.ID == id {
			return true
		}
	}
	return false
}

func (p Providers) HasModel(ref ModelRef) bool {
	for _, provider := range p.All {
		if provider.ID == ref.ProviderID {
			_, ok := provider.Models[ref.ID]
			return ok
		}
	}
	return false
}

// Relay event types delivered on the stream channel.
const (
	EventDelta   = "delta"
	EventDone    = "done"
	EventError   = "error"
	EventSegment = "segment" // tool/part boundary — finalize preview, next delta = new message
	EventTool    = "tool"    // a tool part started — emit ⚙️ notice once per part, then segment
)

type Event struct {
	Type       string
	Delta      string
	Err        error
	Permission *PermissionRequest
}

type PermissionRequest struct {
	ID         string
	SessionID  string
	Permission string
	Tool       string
}

type PermissionReply string

const (
	PermissionOnce   PermissionReply = "once"
	PermissionAlways PermissionReply = "always"
	PermissionReject PermissionReply = "reject"
)

type Client interface {
	CreateSession(ctx context.Context) (string, error)
	SendMessage(ctx context.Context, sessionID, text string, model *ModelRef, attachments []Attachment) error
	Providers(ctx context.Context) (Providers, error)
	RunCommand(ctx context.Context, sessionID, command string) error
	Events(ctx context.Context, sessionID string) (<-chan Event, error)
	ReplyPermission(ctx context.Context, requestID string, reply PermissionReply) error
	ListCommands(ctx context.Context) ([]CommandInfo, error)
}

// CommandInfo describes one command the agent backend can invoke, used to
// enrich the chat-facing help text with the agent's own commands.
type CommandInfo struct {
	Name        string
	Description string
	Source      string
}

type HTTPClient struct {
	base string
	http *http.Client
}

func NewHTTPClient(base string) *HTTPClient {
	return &HTTPClient{
		base: base,
		http: &http.Client{Timeout: 15 * time.Minute},
	}
}

func (c *HTTPClient) CreateSession(ctx context.Context) (string, error) {
	resp, err := c.post(ctx, "/session", nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", ErrNotFound
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("relay: create session: unexpected status %d", resp.StatusCode)
	}

	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("relay: create session: decode response: %w", err)
	}
	return body.ID, nil
}

func (c *HTTPClient) SendMessage(ctx context.Context, sessionID, text string, model *ModelRef, attachments []Attachment) error {
	for _, a := range attachments {
		if len(a.Data) > maxAttachmentSize {
			return fmt.Errorf("relay: %w: %s (%d bytes)", ErrAttachmentTooLarge, a.Filename, len(a.Data))
		}
	}

	payload := buildMessagePayload(text, model, attachments)
	resp, err := c.post(ctx, "/session/"+sessionID+"/message", payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return fmt.Errorf("relay: send message: drain body: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("relay: send message: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (c *HTTPClient) Providers(ctx context.Context) (Providers, error) {
	resp, err := c.get(ctx, "/provider")
	if err != nil {
		return Providers{}, fmt.Errorf("relay: providers: request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return Providers{}, fmt.Errorf("relay: providers: %w", ErrNotFound)
	}
	if resp.StatusCode != http.StatusOK {
		return Providers{}, fmt.Errorf("relay: providers: unexpected status %d", resp.StatusCode)
	}

	var providers Providers
	if err := json.NewDecoder(resp.Body).Decode(&providers); err != nil {
		return Providers{}, fmt.Errorf("relay: providers: decode response: %w", err)
	}
	return providers, nil
}

func (c *HTTPClient) ListCommands(ctx context.Context) ([]CommandInfo, error) {
	resp, err := c.get(ctx, "/command")
	if err != nil {
		return nil, fmt.Errorf("relay: list commands: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("relay: list commands: unexpected status %d", resp.StatusCode)
	}

	var raw []struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Source      string `json:"source"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("relay: list commands: decode response: %w", err)
	}

	commands := make([]CommandInfo, len(raw))
	for i, r := range raw {
		commands[i] = CommandInfo{Name: r.Name, Description: r.Description, Source: r.Source}
	}
	return commands, nil
}

// splitCommand separates a forwarded "/name args" text into the agent's
// command name (without the leading slash) and its arguments.
func splitCommand(command string) (name, args string) {
	text := strings.TrimPrefix(command, "/")
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return text, ""
	}
	rest := strings.TrimSpace(strings.TrimPrefix(text, fields[0]))
	return fields[0], rest
}

func (c *HTTPClient) RunCommand(ctx context.Context, sessionID, command string) error {
	name, args := splitCommand(command)
	payload := map[string]string{"command": name, "arguments": args}
	resp, err := c.post(ctx, "/session/"+sessionID+"/command", payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return fmt.Errorf("relay: run command: drain body: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("relay: run command: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (c *HTTPClient) ReplyPermission(ctx context.Context, requestID string, reply PermissionReply) error {
	payload := map[string]string{"reply": string(reply)}
	resp, err := c.post(ctx, "/permission/"+requestID+"/reply", payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return fmt.Errorf("relay: reply permission: drain body: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("relay: reply permission: unexpected status %d", resp.StatusCode)
	}
	return nil
}

type McpConfig struct {
	Type string `json:"type"`
	URL  string `json:"url"`
}

func (c *HTTPClient) RegisterMCP(ctx context.Context, name string, config McpConfig) error {
	payload := map[string]any{"name": name, "config": config}
	resp, err := c.post(ctx, "/mcp", payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return fmt.Errorf("relay: register mcp: drain body: %w", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("relay: register mcp: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (c *HTTPClient) Events(ctx context.Context, sessionID string) (<-chan Event, error) {
	url := c.base + "/event?session_id=" + sessionID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("relay: events: build request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	httpClient := *c.http
	httpClient.Timeout = 0
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, c.wrapTransportErr(err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("relay: events: unexpected status %d", resp.StatusCode)
	}

	ch := make(chan Event, 64)
	go func() {
		defer close(ch)
		defer resp.Body.Close()
		err := readSSE(ctx, resp.Body, ch)
		if err != nil && ctx.Err() == nil {
			slog.Warn("relay: event stream read failed", "session_id", sessionID, "error", err)
		}
	}()
	return ch, nil
}

func (c *HTTPClient) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, fmt.Errorf("relay: build request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, c.wrapTransportErr(err)
	}
	return resp, nil
}

func (c *HTTPClient) post(ctx context.Context, path string, payload any) (*http.Response, error) {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("relay: marshal request: %w", err)
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, body)
	if err != nil {
		return nil, fmt.Errorf("relay: build request: %w", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, c.wrapTransportErr(err)
	}
	return resp, nil
}

func (c *HTTPClient) wrapTransportErr(err error) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("relay: %w", err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("relay: %w: %v", ErrTimeout, err)
	}
	if isConnectionError(err) {
		return fmt.Errorf("relay: %w: %v", ErrUnreachable, err)
	}
	return fmt.Errorf("relay: %w: %v", ErrUnreachable, err)
}

func isConnectionError(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op == "dial"
	}
	return errors.Is(err, net.ErrClosed)
}

type textPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type filePart struct {
	Type     string `json:"type"`
	Filename string `json:"filename"`
	MimeType string `json:"mime_type"`
	Data     string `json:"data"`
}

func buildMessagePayload(text string, model *ModelRef, attachments []Attachment) map[string]any {
	tools := map[string]bool{"schedule_task": true}
	payload := map[string]any{"tools": tools}
	if model != nil {
		payload["model"] = model
	}

	var parts []any
	if text != "" {
		parts = append(parts, textPart{Type: "text", Text: text})
	}
	for _, a := range attachments {
		if isTextLike(a.MimeType, a.Data) {
			content := fmt.Sprintf("<attached_file name=%q>\n%s\n</attached_file>", a.Filename, string(a.Data))
			parts = append(parts, textPart{Type: "text", Text: content})
		} else {
			dataURL := fmt.Sprintf("data:%s;base64,%s", a.MimeType, base64.StdEncoding.EncodeToString(a.Data))
			parts = append(parts, filePart{Type: "file", Filename: a.Filename, MimeType: a.MimeType, Data: dataURL})
		}
	}
	payload["parts"] = parts
	return payload
}

var textMimeAllowlist = map[string]bool{
	"application/json":   true,
	"application/xml":    true,
	"application/x-yaml": true,
}

func isTextLike(mime string, data []byte) bool {
	if strings.HasPrefix(mime, "text/") || textMimeAllowlist[mime] {
		return utf8.Valid(data)
	}
	return false
}
