package boxmgr

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
)

func TestDetachForCloseReleasesManagerLocks(t *testing.T) {
	manager := New(&config.Config{}, monitor.Config{})
	_ = manager.detachForClose()

	if !manager.reloadMu.TryLock() {
		t.Fatal("detachForClose retained reloadMu")
	}
	manager.reloadMu.Unlock()
	if !manager.mu.TryLock() {
		t.Fatal("detachForClose retained manager mu")
	}
	manager.mu.Unlock()
}

func TestShutdownWithTimeoutSuppliesBoundedContext(t *testing.T) {
	const timeout = 25 * time.Millisecond
	started := time.Now()
	shutdownWithTimeout(timeout, func(ctx context.Context) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("shutdown context has no deadline")
			return
		}
		if remaining := time.Until(deadline); remaining <= 0 || remaining > timeout+50*time.Millisecond {
			t.Errorf("unexpected shutdown deadline: remaining=%s", remaining)
		}
		<-ctx.Done()
	})
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded shutdown took too long: %s", elapsed)
	}
}

func TestCloseIsIdempotentForConcurrentCallers(t *testing.T) {
	manager := New(&config.Config{}, monitor.Config{})
	const callers = 8
	var wg sync.WaitGroup
	wg.Add(callers)
	errorsByCaller := make(chan error, callers)
	for range callers {
		go func() {
			defer wg.Done()
			errorsByCaller <- manager.Close()
		}()
	}
	wg.Wait()
	close(errorsByCaller)
	for err := range errorsByCaller {
		if err != nil {
			t.Fatalf("concurrent close failed: %v", err)
		}
	}
}

func TestCloseWaitsForStartupTransactionAndLeavesNoRuntime(t *testing.T) {
	manager := New(newDrainTestConfig(t, time.Second), monitor.Config{Enabled: false})
	reachedPublish := make(chan struct{})
	releasePublish := make(chan struct{})
	manager.beforeStartPublish = func() {
		close(reachedPublish)
		<-releasePublish
	}
	startResult := make(chan error, 1)
	go func() { startResult <- manager.Start(context.Background()) }()
	<-reachedPublish
	closeResult := make(chan error, 1)
	go func() { closeResult <- manager.Close() }()
	select {
	case err := <-closeResult:
		t.Fatalf("Close returned before startup transaction completed: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	close(releasePublish)
	if err := <-startResult; err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := <-closeResult; err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	manager.mu.RLock()
	running := manager.currentBox != nil
	closed := manager.closed
	manager.mu.RUnlock()
	if running || !closed {
		t.Fatalf("manager state after Start/Close: running=%t closed=%t", running, closed)
	}
}

func TestClosedManagerRejectsMutatingConfigurationAPIs(t *testing.T) {
	manager := New(&config.Config{}, monitor.Config{})
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateNode(context.Background(), config.NodeConfig{URI: "socks5://127.0.0.1:1080"}); !errors.Is(err, errManagerClosed) {
		t.Fatalf("CreateNode error=%v, want manager closed", err)
	}
	persistCalled := false
	if err := manager.CommitConfig(context.Background(), 0, &config.Config{}, func(*config.Config) (func() error, error) {
		persistCalled = true
		return nil, nil
	}); !errors.Is(err, errManagerClosed) {
		t.Fatalf("CommitConfig error=%v, want manager closed", err)
	}
	if persistCalled {
		t.Fatal("closed CommitConfig invoked persistence callback")
	}
}

func TestRunPreflightProbeCoalescesHungStableTag(t *testing.T) {
	manager := New(&config.Config{}, monitor.Config{})
	blocked := make(chan struct{})
	var calls atomic.Int32

	run := func(timeout time.Duration, probe func() error) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return manager.runPreflightProbe(ctx, "node-stable", probe)
	}
	probe := func() error {
		calls.Add(1)
		<-blocked
		return nil
	}

	if err := run(20*time.Millisecond, probe); err == nil {
		t.Fatal("expected first caller to time out")
	}
	if err := run(20*time.Millisecond, func() error {
		calls.Add(1)
		return nil
	}); err == nil {
		t.Fatal("expected second caller to join the hung flight and time out")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("hung stable tag launched %d probes, want 1", got)
	}

	close(blocked)
	deadline := time.Now().Add(time.Second)
	for {
		manager.preflightMu.Lock()
		_, running := manager.preflightCalls["node-stable"]
		manager.preflightMu.Unlock()
		if !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("completed preflight call was not removed")
		}
		time.Sleep(time.Millisecond)
	}

	if err := run(time.Second, func() error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("new probe after completed flight failed: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("probe was not restarted after prior flight completed: calls=%d", got)
	}
}

func TestRunPreflightProbeBoundsHungDistinctTags(t *testing.T) {
	manager := New(&config.Config{}, monitor.Config{})
	blocked := make(chan struct{})
	var calls atomic.Int32
	probe := func() error {
		calls.Add(1)
		<-blocked
		return nil
	}
	for index := 0; index < maxHungPreflightProbes; index++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		err := manager.runPreflightProbe(ctx, fmt.Sprintf("hung-%d", index), probe)
		cancel()
		if err == nil {
			t.Fatalf("hung probe %d unexpectedly completed", index)
		}
	}
	if got := calls.Load(); got != maxHungPreflightProbes {
		t.Fatalf("started probes=%d, want %d", got, maxHungPreflightProbes)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err := manager.runPreflightProbe(ctx, "over-limit", probe)
	cancel()
	if err == nil {
		t.Fatal("over-limit probe unexpectedly completed")
	}
	if got := calls.Load(); got != maxHungPreflightProbes {
		t.Fatalf("over-limit probe started: calls=%d", got)
	}

	close(blocked)
	deadline := time.Now().Add(time.Second)
	for {
		manager.preflightMu.Lock()
		remaining := len(manager.preflightCalls)
		manager.preflightMu.Unlock()
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("preflight calls did not drain: %d", remaining)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReassignConflictingPortDoesNotWrapPastUint16(t *testing.T) {
	cfg := &config.Config{
		Mode:      "multi-port",
		MultiPort: config.MultiPortConfig{Address: "127.0.0.1"},
		Nodes: []config.NodeConfig{{
			Name: "last-port",
			Port: 65535,
		}},
	}

	if reassignConflictingPort(cfg, 65535) {
		t.Fatal("expected reassignment past port 65535 to fail")
	}
	if got := cfg.Nodes[0].Port; got != 65535 {
		t.Fatalf("conflicting port wrapped to %d", got)
	}
}
