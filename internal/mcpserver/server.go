package mcpserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/attribution"
	"github.com/anggasct/occa/internal/scheduler"
	"github.com/anggasct/occa/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/robfig/cron/v3"
)

type Server struct {
	sched     *scheduler.Scheduler
	attrib    *attribution.Store
	mcpServer *mcp.Server
	httpSrv   *http.Server
	port      int
}

func New(sched *scheduler.Scheduler, attrib *attribution.Store) *Server {
	s := &Server{sched: sched, attrib: attrib}

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
	if _, err := cron.ParseStandard(input.CronExpression); err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error: invalid cron expression %q: %v", input.CronExpression, err)}},
			IsError: true,
		}, input, nil
	}

	if strings.TrimSpace(input.Prompt) == "" {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "Error: prompt cannot be empty"}},
			IsError: true,
		}, input, nil
	}

	pending := store.Schedule{
		Platform:       "",
		ChannelID:      "",
		CronExpression: input.CronExpression,
		HumanSchedule:  input.HumanSchedule,
		Prompt:         input.Prompt,
		Enabled:        false,
	}

	id, err := s.sched.AddSchedule(ctx, pending)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error creating schedule: %v", err)}},
			IsError: true,
		}, input, nil
	}

	// Wait for the relay to observe this tool call and record the
	// originating conversation. The FIFO pop consumes exactly one entry, so
	// identical concurrent calls pair one-to-one by tool-execution order.
	fp := attribution.Fingerprint(input.CronExpression, input.Prompt, input.HumanSchedule)
	var platform, channelID string
	var attributed bool
	for i := 0; i < 10; i++ {
		if p, c, ok := s.attrib.Pop(fp); ok {
			platform = p
			channelID = c
			attributed = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if !attributed {
		if err := s.sched.RemoveSchedule(ctx, "", "", id); err != nil {
			slog.Warn("schedule attribution: timeout cleanup failed", "id", id, "error", err)
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "Error: could not attribute schedule — please try again"}},
			IsError: true,
		}, input, nil
	}

	if err := s.sched.AttributeSchedule(ctx, id, platform, channelID); err != nil {
		_ = s.sched.RemoveSchedule(ctx, "", "", id)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error attributing schedule: %v", err)}},
			IsError: true,
		}, input, nil
	}

	result := fmt.Sprintf("Scheduled (ID: %d): %s\nPrompt: %s\nCron: %s", id, input.HumanSchedule, input.Prompt, input.CronExpression)
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: result}},
	}, input, nil
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

	// WriteTimeout is generous because streamable HTTP keeps responses open
	// for server-to-client messages over SSE.
	s.httpSrv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}

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
