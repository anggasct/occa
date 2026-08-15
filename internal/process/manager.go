package process

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anggasct/occa/internal/config"
)

var errClosed = errors.New("process: manager closed")

type entry struct {
	inst  *Instance
	err   error
	ready chan struct{}
}

type Manager struct {
	cfg     config.AgentConfig
	mu      sync.Mutex
	pool    map[string]*entry
	ports   *portAllocator
	newInst instanceFactory
	stop    chan struct{}
	wg      sync.WaitGroup
	spawnWG sync.WaitGroup
	closed  atomic.Bool
}

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

func DefaultManager(cfg config.AgentConfig) (*Manager, error) {
	factory := productionFactory(cfg.Binary, defaultReadinessTimeout, defaultStopGrace)
	factory = wrapWithAutoInstall(factory, cfg.Binary, cfg.AutoInstall, func(ctx context.Context, _ string) error {
		return installOpenCode(ctx)
	})
	return NewManager(cfg, factory)
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

func (m *Manager) Instance(ctx context.Context, workdir string) (*Instance, error) {
	workdir = NormalizeWorkdir(workdir)
	for {
		if m.closed.Load() {
			return nil, errClosed
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
				m.discardLocked(workdir, e)
				m.mu.Unlock()
				continue
			default:
				m.mu.Unlock()
				select {
				case <-e.ready:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			}
		}

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

		m.spawnWG.Add(1)
		inst, spawnErr := m.newInst(ctx, workdir, port)
		defer m.spawnWG.Done()

		m.mu.Lock()
		if spawnErr != nil {
			m.ports.Release(port)
			delete(m.pool, workdir)
			e.err = spawnErr
			close(e.ready)
			m.mu.Unlock()
			return nil, spawnErr
		}
		if m.closed.Load() {
			// In-flight spawn handling under closed manager (race prevention)
			inst.stop()
			m.ports.Release(port)
			delete(m.pool, workdir)
			e.err = errClosed
			close(e.ready)
			m.mu.Unlock()
			return nil, errClosed
		}
		e.inst = inst
		inst.begin()
		close(e.ready)
		m.mu.Unlock()
		return inst, nil
	}
}

func (m *Manager) discardLocked(workdir string, e *entry) {
	if e.inst != nil {
		e.inst.stop()
		m.ports.Release(e.inst.port)
	}
	delete(m.pool, workdir)
}

func (m *Manager) ForceStop(workdir string) {
	workdir = NormalizeWorkdir(workdir)
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.pool[workdir]
	if !ok {
		return
	}
	select {
	case <-e.ready:
		if e.inst != nil {
			e.inst.stop()
			m.ports.Release(e.inst.port)
		}
		delete(m.pool, workdir)
	default:
	}
}

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

func (m *Manager) Close() error {
	if m.closed.Swap(true) {
		return nil
	}
	close(m.stop)
	m.wg.Wait()

	m.mu.Lock()
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
	m.mu.Unlock()

	m.spawnWG.Wait()
	return nil
}
