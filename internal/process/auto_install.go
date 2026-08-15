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
		if err := ensureInstalled(ctx); err != nil {
			return nil, spawnErr
		}
		return factory(ctx, workdir, port)
	}
}

func isBinaryNotFound(err error) bool {
	return errors.Is(err, exec.ErrNotFound)
}

func installOpenCode(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "sh", "-c", "curl -fsSL https://opencode.ai/install | bash")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("process: install opencode: %w: %s", err, strings.TrimSpace(string(out)))
	}
	addOpenCodeBinToPath()
	return nil
}

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
