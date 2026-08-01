package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// wrapWithAutoInstall wraps factory: on a binary-not-found spawn error, if
// autoInstall is enabled, runs installer at most once for the life of the
// wrapped factory — shared across concurrent callers and cached on both
// success and failure — then retries the spawn exactly once per call.
func wrapWithAutoInstall(factory instanceFactory, binary string, autoInstall bool, installer func(ctx context.Context, binary string) error) instanceFactory {
	if !autoInstall {
		return factory
	}

	var once sync.Once
	var installErr error
	ensureInstalled := func(ctx context.Context) error {
		once.Do(func() {
			installErr = installer(ctx, binary)
		})
		return installErr
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
	addOpenCodeBinToPath()
	return nil
}

// addOpenCodeBinToPath prepends OpenCode's fixed install directory
// ($HOME/.opencode/bin) to this process's PATH so a freshly installed binary
// resolves on retry. The official installer only appends to shell rc files
// for future shells — it cannot update the already-running daemon's PATH.
func addOpenCodeBinToPath() {
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	dir := filepath.Join(home, ".opencode", "bin")
	path := os.Getenv("PATH")
	for _, p := range filepath.SplitList(path) {
		if p == dir {
			return
		}
	}
	_ = os.Setenv("PATH", dir+string(os.PathListSeparator)+path)
}
