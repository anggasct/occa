package process

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func notFoundFactory(binary string) instanceFactory {
	return func(ctx context.Context, workdir string, port int) (*Instance, error) {
		return nil, fmt.Errorf("process: spawn %q in %q: %w", binary, workdir, &exec.Error{Name: binary, Err: exec.ErrNotFound})
	}
}

func TestWrapWithAutoInstallDisabledLeavesFactoryUnchanged(t *testing.T) {
	sp := newFakeSpawner()
	factory := wrapWithAutoInstall(sp.factory(), "opencode", false, func(ctx context.Context, binary string) error {
		t.Fatal("installer must not run when auto-install is disabled")
		return nil
	})

	if _, err := factory(context.Background(), "/repo/a", 4096); err != nil {
		t.Fatalf("factory: %v", err)
	}
	if sp.spawnCount() != 1 {
		t.Fatalf("spawns = %d, want 1", sp.spawnCount())
	}
}

func TestWrapWithAutoInstallInstallsAndRetriesOnce(t *testing.T) {
	sp := newFakeSpawner()
	var installs int
	base := notFoundFactory("opencode")
	calls := 0
	factory := wrapWithAutoInstall(func(ctx context.Context, workdir string, port int) (*Instance, error) {
		calls++
		if calls == 1 {
			return base(ctx, workdir, port)
		}
		return sp.factory()(ctx, workdir, port)
	}, "opencode", true, func(ctx context.Context, binary string) error {
		installs++
		return nil
	})

	inst, err := factory(context.Background(), "/repo/a", 4096)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if inst == nil {
		t.Fatal("expected instance after successful install + retry")
	}
	if installs != 1 {
		t.Fatalf("installs = %d, want 1", installs)
	}
	if calls != 2 {
		t.Fatalf("spawn attempts = %d, want 2 (initial + retry)", calls)
	}
}

func TestWrapWithAutoInstallNoRetryLoopAfterSecondFailure(t *testing.T) {
	var installs int
	factory := wrapWithAutoInstall(notFoundFactory("opencode"), "opencode", true, func(ctx context.Context, binary string) error {
		installs++
		return nil
	})

	if _, err := factory(context.Background(), "/repo/a", 4096); err == nil {
		t.Fatal("expected spawn failure to surface after the single retry still fails")
	}
	if installs != 1 {
		t.Fatalf("installs = %d, want 1 (no retry loop)", installs)
	}
}

func TestWrapWithAutoInstallInstallFailureSurfacesOriginalSpawnError(t *testing.T) {
	factory := wrapWithAutoInstall(notFoundFactory("opencode"), "opencode", true, func(ctx context.Context, binary string) error {
		return errors.New("curl: connection refused")
	})

	_, err := factory(context.Background(), "/repo/a", 4096)
	if err == nil || !isBinaryNotFound(err) {
		t.Fatalf("expected the original not-found spawn error to surface unchanged, got %v", err)
	}
}

func TestWrapWithAutoInstallConcurrentSpawnsShareOneInstall(t *testing.T) {
	var installs int32
	release := make(chan struct{})

	factory := wrapWithAutoInstall(notFoundFactory("opencode"), "opencode", true, func(ctx context.Context, binary string) error {
		atomic.AddInt32(&installs, 1)
		<-release
		return nil
	})

	var wg sync.WaitGroup
	for _, wd := range []string{"/repo/a", "/repo/b"} {
		wg.Add(1)
		go func(workdir string) {
			defer wg.Done()
			_, _ = factory(context.Background(), workdir, 4096)
		}(wd)
	}

	time.Sleep(100 * time.Millisecond) // let both goroutines enter the wrapper concurrently
	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&installs); got != 1 {
		t.Fatalf("installs = %d, want 1 for two concurrent workdirs racing on the same missing binary", got)
	}
}
