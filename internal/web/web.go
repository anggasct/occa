package web

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anggasct/occa/internal/webhook"
)

type Control interface {
	FireAction(ctx context.Context, actor string, req webhook.FireActionRequest) (webhook.ActionOutcome, error)
	ListInbox(ctx context.Context, repo string, limit int, cursor string) ([]webhook.InboxRow, string, error)
	GetWorkItemDetail(ctx context.Context, id string) (*webhook.WorkItemDetailView, error)
	GetActionOutcome(ctx context.Context, id int64) (*webhook.ActionOutcome, error)
}

type Server struct {
	bind    string
	token   string
	control Control
	httpSrv *http.Server
	limiter *rateLimiter

	listening atomic.Bool
	addr      string

	sessMu   sync.Mutex
	sessions map[string]*session
}

func New(bind, token string, control Control) *Server {
	s := &Server{
		bind:     bind,
		token:    token,
		control:  control,
		limiter:  newRateLimiter(),
		sessions: make(map[string]*session),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/inbox", s.handleInbox)
	mux.HandleFunc("GET /api/v1/work-items/{id}", s.handleGetWorkItem)
	mux.HandleFunc("POST /api/v1/work-items/{id}/actions/{action}", s.handleFireAction)
	mux.HandleFunc("GET /api/v1/actions/{id}", s.handleGetAction)
	mux.HandleFunc("POST /api/v1/pair", s.handlePair)
	mux.HandleFunc("POST /api/v1/logout", s.handleLogout)
	s.httpSrv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return s
}

func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.bind)
	if err != nil {
		return fmt.Errorf("web: listen %s: %w", s.bind, err)
	}
	s.addr = ln.Addr().String()
	s.listening.Store(true)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.Stop(shutdownCtx)
	}()

	go func() {
		defer s.listening.Store(false)
		if err := s.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("web: serve error", "error", err)
		}
	}()

	slog.Info("web: control API started", "bind", s.addr)
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.listening.Store(false)
	if err := s.httpSrv.Shutdown(ctx); err != nil {
		return fmt.Errorf("web: shutdown: %w", err)
	}
	return nil
}

func (s *Server) Addr() string { return s.addr }

func (s *Server) Healthy() bool { return s.listening.Load() }
