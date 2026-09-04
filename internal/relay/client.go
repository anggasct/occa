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
	"mime"
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
	ErrUnsupported        = errors.New("operation not supported by agent backend")
)

const maxAttachmentSize = 10 * 1024 * 1024

type Attachment struct {
	Filename string
	MimeType string
	Data     []byte
}

var ErrInvalidVariant = errors.New("invalid variant")

type ModelRef struct {
	ProviderID string `json:"providerID"`
	ID         string `json:"modelID"`
	Variant    string `json:"variant,omitempty"`
}

func ParseModelRef(value string) (ModelRef, error) {
	parts := strings.SplitN(value, "/", 2)
	if len(parts) != 2 {
		return ModelRef{}, fmt.Errorf("invalid model %q; use provider/model-id[@variant]", value)
	}
	providerID, modelIDPart := parts[0], parts[1]
	if providerID == "" || modelIDPart == "" {
		return ModelRef{}, fmt.Errorf("invalid model %q; use provider/model-id[@variant]", value)
	}

	var variant string
	if modelID, v, hasAt := strings.Cut(modelIDPart, "@"); hasAt {
		if v == "" {
			return ModelRef{}, ErrInvalidVariant
		}
		if modelID == "" {
			return ModelRef{}, fmt.Errorf("invalid model %q; use provider/model-id[@variant]", value)
		}
		modelIDPart = modelID
		variant = v
	}

	return ModelRef{ProviderID: providerID, ID: modelIDPart, Variant: variant}, nil
}

func FormatModelRef(ref ModelRef) string {
	if ref.Variant != "" {
		return ref.ProviderID + "/" + ref.ID + "@" + ref.Variant
	}
	return ref.ProviderID + "/" + ref.ID
}

func (m ModelRef) String() string {
	return FormatModelRef(m)
}

type Provider struct {
	ID     string                     `json:"id"`
	Models map[string]json.RawMessage `json:"models"`
}

type Providers struct {
	All       []Provider `json:"all"`
	Connected []string   `json:"connected"`
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

func (p Providers) HasConnectedModel(ref ModelRef) bool {
	if len(p.Connected) > 0 {
		var connected bool
		for _, c := range p.Connected {
			if c == ref.ProviderID {
				connected = true
				break
			}
		}
		if !connected {
			return false
		}
	}
	return p.HasModel(ref)
}

func (p Providers) Variants(providerID, modelID string) (map[string]json.RawMessage, bool) {
	for _, provider := range p.All {
		if provider.ID == providerID {
			raw, ok := provider.Models[modelID]
			if !ok || len(raw) == 0 {
				return nil, false
			}
			var cfg struct {
				Variants map[string]json.RawMessage `json:"variants"`
			}
			if err := json.Unmarshal(raw, &cfg); err != nil || cfg.Variants == nil {
				return nil, false
			}
			return cfg.Variants, true
		}
	}
	return nil, false
}

func (p Providers) HasVariant(ref ModelRef) bool {
	if ref.Variant == "" {
		return true
	}
	variants, ok := p.Variants(ref.ProviderID, ref.ID)
	if !ok || variants == nil {
		return true
	}
	_, has := variants[ref.Variant]
	return has
}

func (p Providers) ContextLimit(providerID, modelID string) (int64, bool) {
	for _, provider := range p.All {
		if provider.ID == providerID {
			raw, ok := provider.Models[modelID]
			if !ok || len(raw) == 0 {
				return 0, false
			}
			var cfg struct {
				Limit struct {
					Context int64 `json:"context"`
				} `json:"limit"`
			}
			if err := json.Unmarshal(raw, &cfg); err != nil || cfg.Limit.Context <= 0 {
				return 0, false
			}
			return cfg.Limit.Context, true
		}
	}
	return 0, false
}

type SessionTokens struct {
	Input      int64
	Output     int64
	Reasoning  int64
	CacheRead  int64
	CacheWrite int64
}

// ContextSource identifies where the current-window occupancy value came from.
// An empty ContextSource means no verified current-window value is available.
type ContextSource string

const (
	// ContextSourceMessageTail is the per-message usage of the most recent
	// completed assistant request in the session message tail: the current
	// window occupancy, as opposed to the cumulative Tokens counters.
	ContextSourceMessageTail ContextSource = "message-tail"
)

type SessionInfo struct {
	Tokens SessionTokens
	Cost   float64
	// CostKnown is false when the backend cannot provide provider pricing.
	CostKnown bool
	Model     ModelRef
	Agent     string
	// ContextTokens holds the prompt size (input + cache read) of the most
	// recent completed assistant request: the current window occupancy,
	// as opposed to the cumulative Tokens counters.
	ContextTokens int64
	// ContextSource records which verified source produced ContextTokens, and
	// ContextUpdatedAt when that occupancy last changed (message completion
	// time). Both are zero when no current-window value is available; the
	// renderer must never present unverified or stale data as live.
	ContextSource    ContextSource
	ContextUpdatedAt time.Time
}

const (
	EventDelta   = "delta"
	EventDone    = "done"
	EventError   = "error"
	EventSegment = "segment"
	EventTool    = "tool"
)

type Event struct {
	Type         string
	Delta        string
	ToolContext  string
	ToolInput    json.RawMessage
	ToolSamePart bool
	Err          error
	Permission   *PermissionRequest
	Question     *QuestionRequest
}

type PermissionRequest struct {
	ID         string
	SessionID  string
	Permission string
	Tool       string
	Patterns   []string
}

type QuestionOption struct {
	Label       string
	Description string
}

type QuestionInfo struct {
	Question string
	Header   string
	Options  []QuestionOption
}

type QuestionRequest struct {
	ID        string
	SessionID string
	Questions []QuestionInfo
}

type PermissionReply string

const (
	PermissionOnce   PermissionReply = "once"
	PermissionAlways PermissionReply = "always"
	PermissionReject PermissionReply = "reject"
)

type MessageInfo struct {
	ID        string
	Role      string
	Created   int64
	Completed int64
}

type AgentInfo struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Mode        string    `json:"mode"`
	Native      bool      `json:"native"`
	Model       *ModelRef `json:"model,omitempty"`
}

type Client interface {
	CreateSession(ctx context.Context) (string, error)
	GetSession(ctx context.Context, sessionID string) (*SessionInfo, error)
	SessionExists(ctx context.Context, sessionID string) (bool, error)
	SendMessage(ctx context.Context, sessionID, text string, model *ModelRef, attachments []Attachment) error
	Providers(ctx context.Context) (Providers, error)
	RunCommand(ctx context.Context, sessionID, command string) error
	Events(ctx context.Context, sessionID string) (<-chan Event, error)
	ReplyPermission(ctx context.Context, requestID string, reply PermissionReply) error
	AnswerQuestion(ctx context.Context, requestID string, answers [][]string) error
	RejectQuestion(ctx context.Context, requestID string) error
	ListCommands(ctx context.Context) ([]CommandInfo, error)
	ListAgents(ctx context.Context) ([]AgentInfo, error)
	SwitchAgent(ctx context.Context, sessionID, name string) error
	AbortSession(ctx context.Context, sessionID string) error
	SummarizeSession(ctx context.Context, sessionID, providerID, modelID string) error
	RevertMessage(ctx context.Context, sessionID, messageID string) error
	UnrevertSession(ctx context.Context, sessionID string) error
	ListMessages(ctx context.Context, sessionID string) ([]MessageInfo, error)
}

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
		http: &http.Client{Timeout: 3 * time.Minute},
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

func (c *HTTPClient) GetSession(ctx context.Context, sessionID string) (*SessionInfo, error) {
	resp, err := c.get(ctx, "/session/"+sessionID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("relay: get session: unexpected status %d", resp.StatusCode)
	}

	var raw struct {
		Agent string          `json:"agent"`
		Cost  json.RawMessage `json:"cost"`
		Model struct {
			ID         string `json:"id"`
			ProviderID string `json:"providerID"`
			Variant    string `json:"variant"`
		} `json:"model"`
		Tokens struct {
			Input     int64 `json:"input"`
			Output    int64 `json:"output"`
			Reasoning int64 `json:"reasoning"`
			Cache     struct {
				Read  int64 `json:"read"`
				Write int64 `json:"write"`
			} `json:"cache"`
		} `json:"tokens"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("relay: get session: decode response: %w", err)
	}
	var cost float64
	if len(raw.Cost) > 0 && string(raw.Cost) != "null" {
		if err := json.Unmarshal(raw.Cost, &cost); err != nil {
			return nil, fmt.Errorf("relay: get session: decode cost: %w", err)
		}
	}

	var contextTokens int64
	var contextSource ContextSource
	var contextUpdatedAt time.Time
	if msgs, mErr := c.get(ctx, "/session/"+sessionID+"/message?limit=20"); mErr == nil {
		if msgs.StatusCode != http.StatusOK {
			// Non-200 tail: the fetch failed, so the current-window occupancy is
			// unavailable. Never decode an error body — it may still contain a
			// valid-looking message array that would render wrong data.
			_ = msgs.Body.Close()
		} else {
			var list []struct {
				Info struct {
					Role   string `json:"role"`
					Tokens struct {
						Input int64 `json:"input"`
						Cache struct {
							Read int64 `json:"read"`
						} `json:"cache"`
					} `json:"tokens"`
					Time struct {
						Created   int64 `json:"created"`
						Completed int64 `json:"completed"`
					} `json:"time"`
				} `json:"info"`
			}
			decodeErr := json.NewDecoder(msgs.Body).Decode(&list)
			_ = msgs.Body.Close()
			if decodeErr == nil {
				for i := len(list) - 1; i >= 0; i-- {
					if list[i].Info.Role != "assistant" {
						continue
					}
					if occupancy := list[i].Info.Tokens.Input + list[i].Info.Tokens.Cache.Read; occupancy > 0 {
						if ms := list[i].Info.Time.Completed; ms > 0 {
							contextTokens = occupancy
							contextSource = ContextSourceMessageTail
							contextUpdatedAt = time.UnixMilli(ms)
							break
						}
					}
				}
			}
		}
	}

	return &SessionInfo{
		Tokens: SessionTokens{
			Input:      raw.Tokens.Input,
			Output:     raw.Tokens.Output,
			Reasoning:  raw.Tokens.Reasoning,
			CacheRead:  raw.Tokens.Cache.Read,
			CacheWrite: raw.Tokens.Cache.Write,
		},
		Cost:             cost,
		CostKnown:        cost > 0,
		ContextTokens:    contextTokens,
		ContextSource:    contextSource,
		ContextUpdatedAt: contextUpdatedAt,
		Model: ModelRef{
			ProviderID: raw.Model.ProviderID,
			ID:         raw.Model.ID,
			Variant:    raw.Model.Variant,
		},
		Agent: raw.Agent,
	}, nil
}

func (c *HTTPClient) SessionExists(ctx context.Context, sessionID string) (bool, error) {
	resp, err := c.get(ctx, "/session/"+sessionID)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return false, fmt.Errorf("relay: session exists: drain body: %w", err)
	}

	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, fmt.Errorf("relay: session exists: unexpected status %d", resp.StatusCode)
}

func (c *HTTPClient) AbortSession(ctx context.Context, sessionID string) error {
	resp, err := c.post(ctx, "/session/"+sessionID+"/abort", nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return fmt.Errorf("relay: abort session: drain body: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("relay: abort session: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (c *HTTPClient) SummarizeSession(ctx context.Context, sessionID, providerID, modelID string) error {
	payload := map[string]string{
		"providerID": providerID,
		"modelID":    modelID,
	}
	resp, err := c.post(ctx, "/session/"+sessionID+"/summarize", payload)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		if len(body) > 0 {
			return fmt.Errorf("relay: summarize session: unexpected status %d: %s", resp.StatusCode, truncateBody(body))
		}
		return fmt.Errorf("relay: summarize session: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (c *HTTPClient) RevertMessage(ctx context.Context, sessionID, messageID string) error {
	payload := map[string]string{"messageID": messageID}
	resp, err := c.post(ctx, "/session/"+sessionID+"/revert", payload)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		if len(body) > 0 {
			return fmt.Errorf("relay: revert message: unexpected status %d: %s", resp.StatusCode, truncateBody(body))
		}
		return fmt.Errorf("relay: revert message: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (c *HTTPClient) UnrevertSession(ctx context.Context, sessionID string) error {
	resp, err := c.post(ctx, "/session/"+sessionID+"/unrevert", map[string]any{})
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		if len(body) > 0 {
			return fmt.Errorf("relay: unrevert session: unexpected status %d: %s", resp.StatusCode, truncateBody(body))
		}
		return fmt.Errorf("relay: unrevert session: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (c *HTTPClient) ListMessages(ctx context.Context, sessionID string) ([]MessageInfo, error) {
	resp, err := c.get(ctx, "/session/"+sessionID+"/message?limit=50")
	if err != nil {
		return nil, fmt.Errorf("relay: list messages: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("relay: list messages: unexpected status %d", resp.StatusCode)
	}

	var raw []struct {
		Info struct {
			ID   string `json:"id"`
			Role string `json:"role"`
			Time struct {
				Created   int64 `json:"created"`
				Completed int64 `json:"completed"`
			} `json:"time"`
		} `json:"info"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("relay: list messages: decode response: %w", err)
	}

	messages := make([]MessageInfo, len(raw))
	for i, r := range raw {
		messages[i] = MessageInfo{
			ID:        r.Info.ID,
			Role:      r.Info.Role,
			Created:   r.Info.Time.Created,
			Completed: r.Info.Time.Completed,
		}
	}
	return messages, nil
}

func (c *HTTPClient) SendMessage(ctx context.Context, sessionID, text string, model *ModelRef, attachments []Attachment) error {
	for _, a := range attachments {
		if len(a.Data) > maxAttachmentSize {
			return fmt.Errorf("relay: %w: %s (%d bytes)", ErrAttachmentTooLarge, a.Filename, len(a.Data))
		}
	}

	payload := buildMessagePayload(text, model, attachments)
	resp, err := c.post(ctx, "/session/"+sessionID+"/prompt_async", payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= 300 {
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

func (c *HTTPClient) ListAgents(ctx context.Context) ([]AgentInfo, error) {
	resp, err := c.get(ctx, "/agent")
	if err != nil {
		return nil, fmt.Errorf("relay: list agents: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("relay: list agents: unexpected status %d", resp.StatusCode)
	}

	var raw []struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Mode        string `json:"mode"`
		Native      bool   `json:"native"`
		Model       *struct {
			ProviderID string `json:"providerID"`
			ModelID    string `json:"modelID"`
		} `json:"model"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("relay: list agents: decode response: %w", err)
	}

	agents := make([]AgentInfo, len(raw))
	for i, r := range raw {
		agents[i] = AgentInfo{
			Name:        r.Name,
			Description: r.Description,
			Mode:        r.Mode,
			Native:      r.Native,
		}
		if r.Model != nil && (r.Model.ProviderID != "" || r.Model.ModelID != "") {
			agents[i].Model = &ModelRef{
				ProviderID: r.Model.ProviderID,
				ID:         r.Model.ModelID,
			}
		}
	}
	return agents, nil
}

func (c *HTTPClient) SwitchAgent(ctx context.Context, sessionID, name string) error {
	payload := struct {
		Agent string `json:"agent"`
	}{
		Agent: name,
	}
	resp, err := c.post(ctx, "/api/session/"+sessionID+"/agent", payload)
	if err != nil {
		return fmt.Errorf("relay: switch agent: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= 300 {
		return fmt.Errorf("relay: switch agent: unexpected status %d", resp.StatusCode)
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("relay: switch agent: read response: %w", readErr)
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil
	}
	mediaType, _, parseErr := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if parseErr != nil || mediaType != "application/json" {
		return fmt.Errorf("relay: switch agent: unexpected content type %q", resp.Header.Get("Content-Type"))
	}
	if bytes.Equal(trimmed, []byte("null")) || !json.Valid(trimmed) {
		return errors.New("relay: switch agent: invalid JSON response")
	}
	return nil
}

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

func (c *HTTPClient) AnswerQuestion(ctx context.Context, requestID string, answers [][]string) error {
	payload := map[string]any{"answers": answers}
	resp, err := c.post(ctx, "/question/"+requestID+"/reply", payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		slog.Warn("relay: answer question rejected", "request_id", requestID, "status", resp.StatusCode, "body", truncateBody(body))
		return fmt.Errorf("relay: answer question: unexpected status %d: %s", resp.StatusCode, truncateBody(body))
	}
	return nil
}

func (c *HTTPClient) RejectQuestion(ctx context.Context, requestID string) error {
	resp, err := c.post(ctx, "/question/"+requestID+"/reject", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		slog.Warn("relay: reject question rejected", "request_id", requestID, "status", resp.StatusCode, "body", truncateBody(body))
		return fmt.Errorf("relay: reject question: unexpected status %d: %s", resp.StatusCode, truncateBody(body))
	}
	return nil
}

func truncateBody(b []byte) string {
	const max = 512
	s := string(b)
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	var count int
	var byteIdx int
	for idx := range s {
		if count == max {
			byteIdx = idx
			break
		}
		count++
	}
	return s[:byteIdx] + fmt.Sprintf("… (%d bytes total)", len(b))
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
		// The agent's /event stream is a global bus: it delivers every
		// session's events regardless of the session_id query param. The
		// decoder filters by sessionID client-side so concurrent sessions
		// cannot complete or pollute this turn.
		err := readSSE(ctx, resp.Body, ch, sessionID)
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
		// opencode reads the reasoning-effort variant from the TOP-LEVEL
		// `variant` field (PromptInput schema), not from inside `model`
		// (ModelRef = {providerID, modelID}). Sending model.variant is
		// silently dropped by the agent, so keep the wire shape aligned.
		payload["model"] = map[string]string{
			"providerID": model.ProviderID,
			"modelID":    model.ID,
		}
		if model.Variant != "" {
			payload["variant"] = model.Variant
		}
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
