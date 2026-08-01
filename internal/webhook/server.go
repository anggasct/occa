package webhook

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"text/template"
	"time"

	"github.com/anggasct/occa/internal/config"
)

const (
	maxWebhookBodySize         int64 = 10 * 1024 * 1024
	maxConcurrentWebhookEvents       = 16

	readHeaderTimeout = 10 * time.Second
	readTimeout       = 30 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 2 * time.Minute
)

type Executor func(ctx context.Context, platform, channelID, prompt string)

type Server struct {
	bind              string
	bindAddr          string
	endpoints         map[string]config.EndpointConfig
	executor          Executor
	httpSrv           *http.Server
	eventSlots        chan struct{}
	readHeaderTimeout time.Duration
	readTimeout       time.Duration
	writeTimeout      time.Duration
	idleTimeout       time.Duration
}

func New(cfg config.WebhookConfig, executor Executor) *Server {
	endpoints := make(map[string]config.EndpointConfig)
	for _, ep := range cfg.Endpoints {
		endpoints[ep.Path] = ep
	}
	return &Server{
		bind:              cfg.Bind,
		endpoints:         endpoints,
		executor:          executor,
		eventSlots:        make(chan struct{}, maxConcurrentWebhookEvents),
		readHeaderTimeout: readHeaderTimeout,
		readTimeout:       readTimeout,
		writeTimeout:      writeTimeout,
		idleTimeout:       idleTimeout,
	}
}

func (s *Server) tryAcquireEvent() bool {
	select {
	case s.eventSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) releaseEvent() {
	<-s.eventSlots
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRequest)

	ln, err := net.Listen("tcp", s.bind)
	if err != nil {
		return fmt.Errorf("webhook: listen %s: %w", s.bind, err)
	}
	s.bindAddr = ln.Addr().String()

	s.httpSrv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: s.readHeaderTimeout,
		ReadTimeout:       s.readTimeout,
		WriteTimeout:      s.writeTimeout,
		IdleTimeout:       s.idleTimeout,
	}

	go func() {
		<-ctx.Done()
		s.httpSrv.Close()
	}()

	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("webhook: serve error", "error", err)
		}
	}()

	slog.Info("webhook server started", "bind", s.bind, "endpoints", len(s.endpoints))
	return nil
}

// Addr returns the bound listen address; valid after Start succeeds.
func (s *Server) Addr() string { return s.bindAddr }

func (s *Server) Stop(ctx context.Context) error {
	if s.httpSrv == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	ep, ok := s.endpoints[r.URL.Path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if strings.TrimSpace(ep.Secret) == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	secret := r.Header.Get("X-Webhook-Secret")
	if secret == "" {
		secret = r.URL.Query().Get("secret")
	}
	if subtle.ConstantTimeCompare([]byte(secret), []byte(ep.Secret)) != 1 {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.ContentLength > maxWebhookBodySize {
		http.Error(w, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
		return
	}
	if !s.tryAcquireEvent() {
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBodySize))
	if err != nil {
		s.releaseEvent()
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "Bad Request", http.StatusBadRequest)
		}
		return
	}

	w.WriteHeader(http.StatusOK)

	go func() {
		defer s.releaseEvent()
		s.processAsync(ep, body)
	}()
}

func (s *Server) processAsync(ep config.EndpointConfig, body []byte) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		payload = map[string]any{"raw": string(body)}
	}

	tmplData := map[string]any{
		"payload": payload,
		"json":    string(body),
	}

	rendered, err := renderTemplate(ep.Prompt, tmplData)
	if err != nil {
		slog.Error("webhook: template render failed", "endpoint", ep.Name, "payload_bytes", len(body), "error", err)
		rendered = ep.Prompt + "\n\nRaw webhook payload:\n" + string(body)
	}

	rendered = strings.ReplaceAll(rendered, "</untrusted_payload>", "&lt;/untrusted_payload&gt;")
	wrapped := fmt.Sprintf("<untrusted_payload>\n%s\n</untrusted_payload>", rendered)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s.executor(ctx, ep.Platform, ep.ChannelID, wrapped)
}

func renderTemplate(prompt string, data any) (string, error) {
	if !strings.Contains(prompt, "{{") {
		return prompt, nil
	}

	tmpl, err := template.New("webhook").Parse(prompt)
	if err != nil {
		return "", fmt.Errorf("webhook: parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("webhook: execute template: %w", err)
	}
	return buf.String(), nil
}
