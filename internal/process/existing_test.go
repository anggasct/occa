package process

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/relay"
)

// countingSpawner counts the spawned agents and reports a live pid, so the
// pool holds a running instance.
type countingSpawner struct {
	mu     sync.Mutex
	spawns int
}

func (f *countingSpawner) factory() instanceFactory {
	return func(_ context.Context, workdir string, port int) (*Instance, error) {
		f.mu.Lock()
		f.spawns++
		f.mu.Unlock()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		return &Instance{
			workdir: workdir,
			addr:    srv.URL,
			port:    port,
			pid:     4242,
			client:  relay.NewHTTPClient(srv.URL, relay.Config{ClientTimeout: 3 * time.Minute, MaxAttachmentBytes: 10 * 1024 * 1024, MaxEventLineBytes: 1024*1024 + 64*1024}),
			stop:    func() { srv.Close() },
		}, nil
	}
}

func (f *countingSpawner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.spawns
}

// TestManagerExistingNeverSpawns asserts the lookup an abort uses never
// starts an agent: an abort must not spawn a process to kill it.
func TestManagerExistingNeverSpawns(t *testing.T) {
	sp := &countingSpawner{}
	m, err := NewManager(testConfig(5, "4300-4310", time.Minute), sp.factory())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	if _, err := m.Existing("/repo/unknown"); !errors.Is(err, ErrNoLiveAgent) {
		t.Fatalf("Existing(unknown) error = %v, want ErrNoLiveAgent", err)
	}
	if sp.count() != 0 {
		t.Fatalf("Existing spawned %d agents, want 0", sp.count())
	}

	inst, err := m.Instance(context.Background(), "/repo/web")
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	inst.End()
	found, err := m.Existing("/repo/web")
	if err != nil {
		t.Fatalf("Existing(live): %v", err)
	}
	if found != inst {
		t.Fatal("Existing must return the running instance")
	}
	if sp.count() != 1 {
		t.Fatalf("spawns = %d, want 1: the lookup must not spawn", sp.count())
	}
}
