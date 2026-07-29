package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

var (
	ErrUnreachable = errors.New("agent unreachable")
	ErrNotFound    = errors.New("agent resource not found")
	ErrTimeout     = errors.New("agent request timed out")
)

type Event struct {
	Type  string
	Delta string
}

type Client interface {
	CreateSession(ctx context.Context) (string, error)
	SendMessage(ctx context.Context, sessionID, text string) error
	RunCommand(ctx context.Context, sessionID, command string) error
	Events(ctx context.Context, sessionID string) (<-chan Event, error)
}

type HTTPClient struct {
	base string
	http *http.Client
}

func NewHTTPClient(base string) *HTTPClient {
	return &HTTPClient{
		base: base,
		http: &http.Client{Timeout: 5 * time.Minute},
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

func (c *HTTPClient) SendMessage(ctx context.Context, sessionID, text string) error {
	payload := map[string]string{"content": text}
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

func (c *HTTPClient) RunCommand(ctx context.Context, sessionID, command string) error {
	payload := map[string]string{"command": command}
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

func (c *HTTPClient) Events(ctx context.Context, sessionID string) (<-chan Event, error) {
	url := c.base + "/event?session_id=" + sessionID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("relay: events: build request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.http.Do(req)
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
		readSSE(ctx, resp.Body, ch)
	}()
	return ch, nil
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
