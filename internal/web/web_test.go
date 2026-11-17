package web

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/webhook"
)

type fakeControl struct {
	fireOutcome webhook.ActionOutcome
	fireErr     error
	fireCalls   []webhook.FireActionRequest
	fireActor   string

	inboxRows   []webhook.InboxRow
	inboxCursor string
	inboxErr    error
	listRepo    string
	listLimit   int
	listCursor  string

	detail    *webhook.WorkItemDetailView
	detailErr error
	detailID  string

	outcome    *webhook.ActionOutcome
	outcomeErr error
	outcomeID  int64
}

func (f *fakeControl) FireAction(_ context.Context, actor string, req webhook.FireActionRequest) (webhook.ActionOutcome, error) {
	f.fireActor = actor
	f.fireCalls = append(f.fireCalls, req)
	return f.fireOutcome, f.fireErr
}

func (f *fakeControl) ListInbox(_ context.Context, repo string, limit int, cursor string) ([]webhook.InboxRow, string, error) {
	f.listRepo, f.listLimit, f.listCursor = repo, limit, cursor
	return f.inboxRows, f.inboxCursor, f.inboxErr
}

func (f *fakeControl) GetWorkItemDetail(_ context.Context, id string) (*webhook.WorkItemDetailView, error) {
	f.detailID = id
	return f.detail, f.detailErr
}

func (f *fakeControl) GetActionOutcome(_ context.Context, id int64) (*webhook.ActionOutcome, error) {
	f.outcomeID = id
	return f.outcome, f.outcomeErr
}

func newTestServer(fc *fakeControl) *Server {
	return New("127.0.0.1:0", "secret-token", fc)
}

func doReq(t *testing.T, s *Server, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rdr)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	s.httpSrv.Handler.ServeHTTP(rec, req)
	return rec
}

func bearerHeaders() map[string]string {
	return map[string]string{"Authorization": "Bearer secret-token"}
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var env errorEnvelope
	decodeBody(t, rec, &env)
	return env.Error.Code
}

func pair(t *testing.T, s *Server) (cookie *http.Cookie, csrf string) {
	t.Helper()
	rec := doReq(t, s, "POST", "/api/v1/pair", `{"code":"secret-token"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("pair status %d body %s", rec.Code, rec.Body.String())
	}
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			cookie = c
		}
	}
	if cookie == nil {
		t.Fatal("pair did not set session cookie")
	}
	var resp pairResponse
	decodeBody(t, rec, &resp)
	if resp.CSRFToken == "" {
		t.Fatal("pair response missing csrf_token")
	}
	return cookie, resp.CSRFToken
}

func cookieHeaders(cookie *http.Cookie, csrf string) map[string]string {
	return map[string]string{
		"Cookie":   cookie.Name + "=" + cookie.Value,
		csrfHeader: csrf,
	}
}

func TestStartBindsAndStops(t *testing.T) {
	fc := &fakeControl{inboxRows: []webhook.InboxRow{{WorkItemID: "o/r#1"}}}
	s := newTestServer(fc)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !s.Healthy() {
		t.Fatal("server not healthy after Start")
	}
	req, err := http.NewRequest("GET", "http://"+s.Addr()+"/api/v1/inbox", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer secret-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET inbox: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if s.Healthy() {
		t.Fatal("server still healthy after Stop")
	}
}

func TestStartFailsOnBadBind(t *testing.T) {
	s := New("not-an-addr", "tok", &fakeControl{})
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("expected listen error")
	}
}
