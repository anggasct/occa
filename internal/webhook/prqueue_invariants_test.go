package webhook

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

type lockedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestSkippedDeliveryLeavesNoActiveQueueRow(t *testing.T) {
	ep := config.EndpointConfig{
		Name: "github-widget-merged", Path: "/merged", Secret: "s3cret",
		Platform: "telegram", ChannelID: "chat1", Prompt: "merged",
		Workflow: "merged", Thread: true, Repository: "github.com/acme/widget",
		Admit:      []config.AdmitRule{{Event: "pull_request", Actions: []string{"closed"}}},
		Automation: &config.RepoAutomation{Default: config.AutomationFull},
	}
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{ep})
	srv.SetPRQueueRepo(st.PRQueueRepo())

	ctx := context.Background()
	repo := st.PRQueueRepo()
	for _, key := range []string{"acme/widget#7", "acme/widget#9"} {
		if _, err := repo.EnsurePR(ctx, key, "", ""); err != nil {
			t.Fatalf("seed ghost %s: %v", key, err)
		}
	}

	var logs lockedLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := `{"action":"opened","pull_request":{"number":7,"state":"open",` +
		`"head":{"ref":"feat/x","sha":"abc1234def5678","repo":{"full_name":"acme/widget"}},` +
		`"base":{"ref":"main","repo":{"full_name":"acme/widget"}}},` +
		`"repository":{"full_name":"acme/widget"}}`
	if resp := post(t, ts.URL+"/merged?secret=s3cret", "del-ghost", "pull_request", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("opened delivery returned %d", resp.StatusCode)
	}
	waitForReceiptDeliveryID(t, st, "del-ghost", store.WebhookStatusSkipped)

	deadline := time.Now().Add(5 * time.Second)
	for {
		row, err := repo.Get(ctx, "acme/widget#7")
		if err != nil {
			t.Fatalf("get skipped PR row: %v", err)
		}
		if row != nil && row.Status != store.PRStatusActive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("skipped delivery left an active queue row: %+v", row)
		}
		time.Sleep(10 * time.Millisecond)
	}

	other, err := repo.Get(ctx, "acme/widget#9")
	if err != nil || other == nil {
		t.Fatalf("get untouched row: %v", err)
	}
	if other.Status != store.PRStatusActive {
		t.Fatalf("repair touched another PR: %+v", other)
	}

	for {
		got := logs.String()
		if strings.Contains(got, "webhook: pr queue invariant repaired") && strings.Contains(got, "pr_key=acme/widget#7") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("missing invariant repair log for acme/widget#7: %s", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
