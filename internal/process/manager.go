package process

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anggasct/occa/internal/config"
)

// entry is a pool slot: a ready instance or an in-progress spawn.
type entry struct {
	inst  *Instance
	err   error
	ready chan struct{} // closed once inst/err is set
}

// Manager supervises one agent instance per distinct working directory.
type Manager struct {
	cfg     config.AgentConfig
	mu      sync.Mutex
	pool    map[string]*entry
	ports   *portAllocator
	newInst instanceFactory
	stop    chan struct{}
	wg      sync.WaitGroup
	closed  atomic.Bool
}

// NewManager creates a Manager and starts its idle reaper.
func NewManager(cfg config.AgentConfig, factory instanceFactory) (*Manager, error) {
	lo, hi, err := parsePortRange(cfg.PortRange)
	if err != nil {
		return nil, err
	}
	if cfg.MaxInstances <= 0 {
		return nil, fmt.Errorf("process: max_instances must be > 0")
	}
	m := &Manager{
		cfg:     cfg,
		pool:    make(map[string]*entry),
		ports:   newPortAllocator(lo, hi),
		newInst: factory,
		stop:    make(chan struct{}),
	}
	m.startReaper(reapInterval(cfg.IdleTimeout))
	return m, nil
}

// DefaultManager builds a Manager that spawns real `binary serve` subprocesses.
func DefaultManager(cfg config.AgentConfig) (*Manager, error) {
	return NewManager(cfg, productionFactory(cfg.Binary, defaultReadinessTimeout))
}

func reapInterval(idle time.Duration) time.Duration {
	d := idle / 4
	switch {
	case d < time.Second:
		return time.Second
	case d > time.Minute:
		return time.Minute
	default:
		return d
	}
}

// Instance returns a ready instance for workdir (spawning if needed), already
// marked in-flight. The caller must call End() on the returned Instance.
func (m *Manager) Instance(ctx context.Context, workdir string) (*Instance, error) {
	workdir = NormalizeWorkdir(workdir)
	for {
		if m.closed.Load() {
			return nil, fmt.Errorf("process: manager closed")
		}

		m.mu.Lock()
		e, ok := m.pool[workdir]
		if ok {
			select {
			case <-e.ready:
				if e.err == nil && e.inst != nil && !e.inst.dead.Load() {
					e.inst.begin()
					m.mu.Unlock()
					return e.inst, nil
				}
				// Dead or errored: discard and respawn below.
				m.discardLocked(workdir, e)
				m.mu.Unlock()
				continue
			default:
				// Spawn in progress: wait for it, then re-check.
				m.mu.Unlock()
				select {
				case <-e.ready:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			}
		}

		// Spawn a new instance. Make room if at capacity.
		if len(m.pool) >= m.cfg.MaxInstances && !m.evictLRULocked() {
			m.mu.Unlock()
			return nil, fmt.Errorf("process: agent instance limit reached (%d in use)", m.cfg.MaxInstances)
		}
		port, err := m.ports.Acquire()
		if err != nil {
			m.mu.Unlock()
			return nil, err
		}
		e = &entry{ready: make(chan struct{})}
		m.pool[workdir] = e
		m.mu.Unlock()

		// Spawn outside the lock (readiness can take seconds).
		inst, spawnErr := m.newInst(ctx, workdir, port)

		m.mu.Lock()
		if spawnErr != nil {
			m.ports.Release(port)
			delete(m.pool, workdir)
			e.err = spawnErr
			close(e.ready)
			m.mu.Unlock()
			return nil, spawnErr
		}
		e.inst = inst
		inst.begin()
		close(e.ready)
		m.mu.Unlock()
		return inst, nil
	}
}

// discardLocked removes a dead/errored entry, releasing its resources.
func (m *Manager) discardLocked(workdir string, e *entry) {
	if e.inst != nil {
		e.inst.stop()
		m.ports.Release(e.inst.port)
	}
	delete(m.pool, workdir)
}

// evictLRULocked stops the least-recently-used idle instance to free a slot.
// Returns false if no idle instance can be evicted.
func (m *Manager) evictLRULocked() bool {
	var oldestKey string
	oldestTime := int64(math.MaxInt64)
	found := false
	for k, e := range m.pool {
		select {
		case <-e.ready:
			if e.err == nil && e.inst != nil && e.inst.isIdle() {
				if t := e.inst.lastUsed.Load(); t < oldestTime {
					oldestTime = t
					oldestKey = k
					found = true
				}
			}
		default:
			// Pending spawn: not evictable.
		}
	}
	if !found {
		return false
	}
	e := m.pool[oldestKey]
	e.inst.stop()
	m.ports.Release(e.inst.port)
	delete(m.pool, oldestKey)
	return true
}

// reapOnce stops idle instances whose last use is older than the idle timeout.
func (m *Manager) reapOnce(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := now.Add(-m.cfg.IdleTimeout).Unix()
	for k, e := range m.pool {
		select {
		case <-e.ready:
			if e.err == nil && e.inst != nil && e.inst.isIdle() && e.inst.lastUsed.Load() < cutoff {
				e.inst.stop()
				m.ports.Release(e.inst.port)
				delete(m.pool, k)
			}
		default:
		}
	}
}

func (m *Manager) startReaper(interval time.Duration) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-m.stop:
				return
			case <-ticker.C:
				m.reapOnce(time.Now())
			}
		}
	}()
}

// Close stops the reaper and all managed instances (no orphans).
func (m *Manager) Close() error {
	if m.closed.Swap(true) {
		return nil
	}
	close(m.stop)
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()
	for k, e := range m.pool {
		select {
		case <-e.ready:
			if e.inst != nil {
				e.inst.stop()
				m.ports.Release(e.inst.port)
			}
		default:
		}
		delete(m.pool, k)
	}
	return nil
}
