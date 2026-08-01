package process

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
)

// installAttempt tracks one in-flight (or just-finished) installer run so
// concurrent callers share its result instead of installing in parallel.
type installAttempt struct {
	done chan struct{}
	err  error
}

// wrapWithAutoInstall wraps factory: on a binary-not-found spawn error, if
// autoInstall is enabled, runs installer once (shared across concurrent
// callers via a mutex) and retries the spawn exactly once.
func wrapWithAutoInstall(factory instanceFactory, binary string, autoInstall bool, installer func(ctx context.Context, binary string) error) instanceFactory {
	if !autoInstall {
		return factory
	}

	var mu sync.Mutex
	var attempt *installAttempt

	ensureInstalled := func(ctx context.Context) error {
		mu.Lock()
		if a := attempt; a != nil {
			mu.Unlock()
			select {
			case <-a.done:
				return a.err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		a := &installAttempt{done: make(chan struct{})}
		attempt = a
		mu.Unlock()

		err := installer(ctx, binary)

		mu.Lock()
		attempt = nil
		mu.Unlock()
		a.err = err
		close(a.done)
		return err
	}

	return func(ctx context.Context, workdir string, port int) (*Instance, error) {
		inst, spawnErr := factory(ctx, workdir, port)
		if spawnErr == nil || !isBinaryNotFound(spawnErr) {
			return inst, spawnErr
		}
		// Install failure surfaces the original not-found error unchanged —
		// same as the disabled path, no separate error shape to handle.
		if err := ensureInstalled(ctx); err != nil {
			return nil, spawnErr
		}
		return factory(ctx, workdir, port)
	}
}

// isBinaryNotFound reports whether err is (or wraps) exec's not-found error,
// e.g. from a failed cmd.Start() when the binary isn't on PATH.
func isBinaryNotFound(err error) bool {
	return errors.Is(err, exec.ErrNotFound)
}

// installOpenCode runs OpenCode's official installer.
func installOpenCode(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "sh", "-c", "curl -fsSL https://opencode.ai/install | bash")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("process: install opencode: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}
