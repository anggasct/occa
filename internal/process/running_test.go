package process

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/relay"
)

func pidSpawner(pid int) instanceFactory {
	return func(_ context.Context, workdir string, port int) (*Instance, error) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		return &Instance{
			workdir: workdir,
			addr:    srv.URL,
			port:    port,
			pid:     pid,
			client:  relay.NewHTTPClient(srv.URL),
			stop:    func() { srv.Close() },
		}, nil
	}
}

func TestManagerRunning(t *testing.T) {
	m, err := NewManager(testConfig(5, "4100-4110", time.Minute), pidSpawner(4242))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	if pid, ok := m.Running("/repo/new"); pid != 0 || ok {
		t.Fatalf("Running(unknown) = %d,%v, want 0,false", pid, ok)
	}

	inst, err := m.Instance(context.Background(), "/repo/web")
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	if pid, ok := m.Running("/repo/web"); !ok || pid != 4242 {
		t.Fatalf("Running(live) = %d,%v, want 4242,true", pid, ok)
	}
	inst.End()

	m.ForceStop("/repo/web")
	if pid, ok := m.Running("/repo/web"); pid != 0 || ok {
		t.Fatalf("Running(after stop) = %d,%v, want 0,false", pid, ok)
	}
}
