package process

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
)

func testConfig(maxInstances int, portRange string, idle time.Duration) config.AgentConfig {
	return config.AgentConfig{
		Binary:         "opencode",
		PortRange:      portRange,
		MaxInstances:   maxInstances,
		IdleTimeout:    idle,
		DefaultWorkdir: "~",
	}
}

// fakeSpawner is a thread-safe test double that backs instances with httptest
// servers instead of real OpenCode subprocesses.
type fakeSpawner struct {
	mu           sync.Mutex
	spawns       int
	stopped      map[string]int
	failWorkdir  string
	spawnGate    chan struct{} // when non-nil, each spawn waits here before returning
	spawnStarted chan struct{} // closed on first factory entry when non-nil
}

func newFakeSpawner() *fakeSpawner {
	return &fakeSpawner{stopped: make(map[string]int)}
}

func (f *fakeSpawner) factory() instanceFactory {
	return func(ctx context.Context, workdir string, port int) (*Instance, error) {
		f.mu.Lock()
		f.spawns++
		fail := f.failWorkdir != "" && workdir == NormalizeWorkdir(f.failWorkdir)
		started := f.spawnStarted
		f.mu.Unlock()
		if fail {
			return nil, fmt.Errorf("spawn failed for %s", workdir)
		}
		if started != nil {
			close(started)
		}
		if f.spawnGate != nil {
			<-f.spawnGate
		}

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		inst := &Instance{
			workdir: workdir,
			addr:    srv.URL,
			port:    port,
			client:  relay.NewHTTPClient(srv.URL),
			stop: func() {
				srv.Close()
				f.mu.Lock()
				f.stopped[workdir]++
				f.mu.Unlock()
			},
		}
		inst.Touch()
		return inst, nil
	}
}

func (f *fakeSpawner) spawnCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.spawns
}

func (f *fakeSpawner) stopCount(workdir string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped[NormalizeWorkdir(workdir)]
}

func newTestManager(t *testing.T, cfg config.AgentConfig, sp *fakeSpawner) *Manager {
	t.Helper()
	m, err := NewManager(cfg, sp.factory())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func TestInstanceLazySpawnAndSharing(t *testing.T) {
	sp := newFakeSpawner()
	m := newTestManager(t, testConfig(5, "4096-4100", time.Minute), sp)

	inst1, err := m.Instance(context.Background(), "/repo/web")
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	inst1.End()

	inst2, err := m.Instance(context.Background(), "/repo/web")
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	inst2.End()

	if inst1 != inst2 {
		t.Fatal("same workdir should share one instance")
	}
	if sp.spawnCount() != 1 {
		t.Fatalf("spawns = %d, want 1 (lazy, shared)", sp.spawnCount())
	}
}

func TestInstanceSeparateWorkdirs(t *testing.T) {
	sp := newFakeSpawner()
	m := newTestManager(t, testConfig(5, "4096-4100", time.Minute), sp)

	a, err := m.Instance(context.Background(), "/repo/a")
	if err != nil {
		t.Fatalf("Instance a: %v", err)
	}
	defer a.End()
	b, err := m.Instance(context.Background(), "/repo/b")
	if err != nil {
		t.Fatalf("Instance b: %v", err)
	}
	defer b.End()

	if a == b {
		t.Fatal("different workdirs should be separate instances")
	}
	if a.port == b.port {
		t.Fatalf("instances should use different ports, both %d", a.port)
	}
	if sp.spawnCount() != 2 {
		t.Fatalf("spawns = %d, want 2", sp.spawnCount())
	}
}

func TestInstanceSpawnError(t *testing.T) {
	sp := newFakeSpawner()
	sp.failWorkdir = "/repo/bad"
	m := newTestManager(t, testConfig(5, "4096-4100", time.Minute), sp)

	if _, err := m.Instance(context.Background(), "/repo/bad"); err == nil {
		t.Fatal("expected spawn error")
	}
}

func TestPortAllocationNoDoubleAssign(t *testing.T) {
	sp := newFakeSpawner()
	m := newTestManager(t, testConfig(5, "4096-4098", time.Minute), sp)

	seen := make(map[int]bool)
	var insts []*Instance
	for _, wd := range []string{"/a", "/b", "/c"} {
		inst, err := m.Instance(context.Background(), wd)
		if err != nil {
			t.Fatalf("Instance %s: %v", wd, err)
		}
		insts = append(insts, inst)
		if seen[inst.port] {
			t.Fatalf("port %d double-assigned", inst.port)
		}
		seen[inst.port] = true
	}
	for _, inst := range insts {
		inst.End()
	}
}

func TestPortExhaustion(t *testing.T) {
	sp := newFakeSpawner()
	// Cap high so port exhaustion (range of 2) is the limiting factor.
	m := newTestManager(t, testConfig(10, "4096-4097", time.Minute), sp)

	var insts []*Instance
	for _, wd := range []string{"/a", "/b"} {
		inst, err := m.Instance(context.Background(), wd)
		if err != nil {
			t.Fatalf("Instance %s: %v", wd, err)
		}
		insts = append(insts, inst)
	}
	if _, err := m.Instance(context.Background(), "/c"); err == nil {
		t.Fatal("expected port exhaustion error")
	}
	for _, inst := range insts {
		inst.End()
	}
}

func TestIdleReap(t *testing.T) {
	sp := newFakeSpawner()
	m := newTestManager(t, testConfig(5, "4096-4100", time.Minute), sp)

	inst, err := m.Instance(context.Background(), "/repo/a")
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	inst.End() // now idle

	inst.lastUsed.Store(time.Now().Add(-2 * time.Minute).Unix())
	m.reapOnce(time.Now())

	if sp.stopCount("/repo/a") != 1 {
		t.Fatalf("expected idle instance reaped, stopCount = %d", sp.stopCount("/repo/a"))
	}

	// Next use respawns.
	inst2, err := m.Instance(context.Background(), "/repo/a")
	if err != nil {
		t.Fatalf("Instance after reap: %v", err)
	}
	inst2.End()
	if sp.spawnCount() != 2 {
		t.Fatalf("spawns = %d, want 2 (respawn after reap)", sp.spawnCount())
	}
}

func TestReapSkipsInFlight(t *testing.T) {
	sp := newFakeSpawner()
	m := newTestManager(t, testConfig(5, "4096-4100", time.Minute), sp)

	inst, err := m.Instance(context.Background(), "/repo/a")
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	// Not Ended → in-flight.
	inst.lastUsed.Store(time.Now().Add(-2 * time.Minute).Unix())
	m.reapOnce(time.Now())

	if sp.stopCount("/repo/a") != 0 {
		t.Fatal("in-flight instance must not be reaped")
	}
	inst.End()
}

func TestCapEvictionLRU(t *testing.T) {
	sp := newFakeSpawner()
	m := newTestManager(t, testConfig(2, "4096-4100", time.Minute), sp)

	a, _ := m.Instance(context.Background(), "/a")
	a.End()
	a.lastUsed.Store(time.Now().Add(-10 * time.Minute).Unix()) // oldest

	b, _ := m.Instance(context.Background(), "/b")
	b.End()
	b.lastUsed.Store(time.Now().Unix()) // recent

	// /c forces eviction of the LRU idle instance (/a).
	c, err := m.Instance(context.Background(), "/c")
	if err != nil {
		t.Fatalf("Instance /c: %v", err)
	}
	c.End()

	if sp.stopCount("/a") != 1 {
		t.Fatalf("expected /a evicted, stopCount = %d", sp.stopCount("/a"))
	}
	if sp.stopCount("/b") != 0 {
		t.Fatal("/b (more recent) should not be evicted")
	}
}

func TestCapNoneIdleFails(t *testing.T) {
	sp := newFakeSpawner()
	m := newTestManager(t, testConfig(1, "4096-4100", time.Minute), sp)

	a, _ := m.Instance(context.Background(), "/a")
	// Not Ended → in-flight, cannot be evicted.
	if _, err := m.Instance(context.Background(), "/b"); err == nil {
		t.Fatal("expected limit-reached error when no idle instance to evict")
	}
	a.End()
}

func TestRestartDeadInstance(t *testing.T) {
	sp := newFakeSpawner()
	m := newTestManager(t, testConfig(5, "4096-4100", time.Minute), sp)

	inst, _ := m.Instance(context.Background(), "/a")
	inst.End()
	inst.dead.Store(true) // simulate process death

	inst2, err := m.Instance(context.Background(), "/a")
	if err != nil {
		t.Fatalf("Instance after death: %v", err)
	}
	inst2.End()

	if inst2 == inst {
		t.Fatal("dead instance should be respawned as a new instance")
	}
	if sp.spawnCount() != 2 {
		t.Fatalf("spawns = %d, want 2", sp.spawnCount())
	}
}

func TestCloseStopsAll(t *testing.T) {
	sp := newFakeSpawner()
	m, err := NewManager(testConfig(5, "4096-4100", time.Minute), sp.factory())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	a, _ := m.Instance(context.Background(), "/a")
	a.End()
	b, _ := m.Instance(context.Background(), "/b")
	b.End()

	m.Close()

	if sp.stopCount("/a") != 1 || sp.stopCount("/b") != 1 {
		t.Fatalf("expected both stopped on Close: a=%d b=%d", sp.stopCount("/a"), sp.stopCount("/b"))
	}
	if _, err := m.Instance(context.Background(), "/c"); err == nil {
		t.Fatal("expected error after Close")
	}
}

func TestNormalizeWorkdir(t *testing.T) {
	if NormalizeWorkdir("/repo/x") != NormalizeWorkdir("/repo/x/") {
		t.Fatal("trailing slash should normalize to the same path")
	}
	abs, _ := filepath.Abs("/repo/x")
	if NormalizeWorkdir("/repo/x") != abs {
		t.Fatalf("expected absolute path %q, got %q", abs, NormalizeWorkdir("/repo/x"))
	}
}

func TestCloseDuringSpawnDoesNotLeak(t *testing.T) {
	sp := newFakeSpawner()
	gate := make(chan struct{})
	spawnStarted := make(chan struct{})
	sp.spawnGate = gate
	sp.spawnStarted = spawnStarted
	m := newTestManager(t, testConfig(5, "4096-4100", time.Minute), sp)

	instanceErr := make(chan error)
	go func() {
		_, err := m.Instance(context.Background(), "/repo/a")
		instanceErr <- err
	}()

	<-spawnStarted // the spawn is now inside the factory

	closeDone := make(chan struct{})
	go func() { _ = m.Close(); close(closeDone) }()

	select {
	case <-closeDone:
		t.Fatal("Close returned while a spawn was in flight")
	case <-time.After(200 * time.Millisecond):
	}

	close(gate)
	<-closeDone

	if err := <-instanceErr; err == nil {
		t.Fatal("Instance must fail when the manager closes mid-spawn")
	}
	if sp.stopCount("/repo/a") != 1 {
		t.Fatalf("spawned instance was not stopped: %d", sp.stopCount("/repo/a"))
	}
}

func TestCloseIdempotent(t *testing.T) {
	sp := newFakeSpawner()
	m := newTestManager(t, testConfig(5, "4096-4100", time.Minute), sp)

	inst, err := m.Instance(context.Background(), "/a")
	if err != nil {
		t.Fatalf("Instance: %v", err)
	}
	inst.End()

	if err := m.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
