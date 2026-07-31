package mcpserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/anggasct/occa/internal/scheduler"
	"github.com/anggasct/occa/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Server struct {
	sched     *scheduler.Scheduler
	mcpServer *mcp.Server
	httpSrv   *http.Server
	port      int

	mu       sync.RWMutex
	contexts map[string]contextEntry
}

type contextEntry struct {
	platform  string
	channelID string
	ts        int64
}

func contextKey(platform, channelID string) string {
	return platform + ":" + channelID
}

func New(sched *scheduler.Scheduler) *Server {
	s := &Server{
		sched: sched,
	}

	mcpServer := mcp.NewServer(&mcp.Implementation{
		Name:    "occa",
		Version: "1.0.0",
	}, nil)

	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        "schedule_task",
		Description: "Schedule a recurring background task. The prompt will be executed automatically at the specified cron schedule and results pushed to the chat.",
	}, s.handleScheduleTask)

	s.mcpServer = mcpServer
	return s
}

type scheduleTaskInput struct {
	CronExpression string `json:"cron_expression" jsonschema:"the 5-field cron expression (e.g. '0 9 * * 1-5' for weekdays at 9 AM)"`
	Prompt         string `json:"prompt" jsonschema:"the prompt or instruction to execute at each scheduled run"`
	HumanSchedule  string `json:"human_schedule" jsonschema:"human-readable description of the schedule (e.g. 'every weekday at 9 AM')"`
}

func (s *Server) handleScheduleTask(ctx context.Context, req *mcp.CallToolRequest, input scheduleTaskInput) (*mcp.CallToolResult, scheduleTaskInput, error) {
	platform, channelID := s.mostRecentContext()
	if platform == "" || channelID == "" {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "Error: no active channel context"}},
			IsError: true,
		}, input, nil
	}

	sched := store.Schedule{
		Platform:       platform,
		ChannelID:      channelID,
		CronExpression: input.CronExpression,
		HumanSchedule:  input.HumanSchedule,
		Prompt:         input.Prompt,
		Enabled:        true,
	}

	id, err := s.sched.AddSchedule(ctx, sched)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error creating schedule: %v", err)}},
			IsError: true,
		}, input, nil
	}

	result := fmt.Sprintf("✅ Scheduled (ID: %d): %s\nPrompt: %s\nCron: %s", id, input.HumanSchedule, input.Prompt, input.CronExpression)
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: result}},
	}, input, nil
}

func (s *Server) SetContext(platform, channelID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.contexts == nil {
		s.contexts = make(map[string]contextEntry)
	}
	s.contexts[contextKey(platform, channelID)] = contextEntry{
		platform:  platform,
		channelID: channelID,
		ts:        time.Now().UnixNano(),
	}
}

func (s *Server) mostRecentContext() (platform, channelID string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var bestTS int64
	for _, entry := range s.contexts {
		if entry.ts > bestTS {
			bestTS = entry.ts
			platform = entry.platform
			channelID = entry.channelID
		}
	}
	return
}

func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("mcpserver: listen: %w", err)
	}
	s.port = ln.Addr().(*net.TCPAddr).Port

	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s.mcpServer }, nil)
	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)

	s.httpSrv = &http.Server{Handler: mux}

	go func() {
		<-ctx.Done()
		s.httpSrv.Close()
	}()

	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("mcpserver: serve error", "error", err)
		}
	}()

	slog.Info("mcpserver started", "port", s.port)
	return nil
}

func (s *Server) URL() string {
	return fmt.Sprintf("http://127.0.0.1:%d/mcp", s.port)
}

func (s *Server) Stop() {
	if s.httpSrv != nil {
		s.httpSrv.Close()
	}
}
