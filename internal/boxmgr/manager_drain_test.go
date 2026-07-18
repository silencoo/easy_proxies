package boxmgr

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"easy_proxies/internal/builder"
	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"

	"github.com/sagernet/sing-box"
)

func TestNodeLevelReloadReclaimsReaddedDrainingOutbound(t *testing.T) {
	const drainTimeout = 180 * time.Millisecond
	cfg := newDrainTestConfig(t, drainTimeout)
	reduced := cfg.Clone()
	reduced.Nodes = cloneNodes(cfg.Nodes[1:])
	tag := removedBaseTag(t, cfg, reduced)

	manager := New(cfg, monitor.Config{Enabled: false})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer manager.Close()

	manager.mu.RLock()
	instance := manager.currentBox
	manager.mu.RUnlock()
	if err := normalizeDrainAndReload(t, manager, reduced); err != nil {
		t.Fatalf("remove node: %v", err)
	}

	manager.drainMu.Lock()
	target, draining := manager.drainPending[tag]
	manager.drainMu.Unlock()
	if !draining {
		t.Fatalf("outbound %q was not scheduled for draining", tag)
	}

	readded := cfg.Clone()
	if err := normalizeDrainAndReload(t, manager, readded); err != nil {
		t.Fatalf("re-add node during drain window: %v", err)
	}

	manager.mu.RLock()
	currentInstance := manager.currentBox
	manager.mu.RUnlock()
	if currentInstance != instance {
		t.Fatal("node-only re-add unexpectedly replaced the sing-box instance")
	}
	current, ok := currentInstance.Outbound().Outbound(tag)
	if !ok || current != target.expected {
		t.Fatalf("re-added outbound was not reclaimed: exists=%t same=%t", ok, current == target.expected)
	}

	// Cross the original retirement deadline. A stale drain entry must not
	// delete the now-live outbound after the second reload commits.
	time.Sleep(time.Until(target.due) + 120*time.Millisecond)
	current, ok = currentInstance.Outbound().Outbound(tag)
	if !ok || current != target.expected {
		t.Fatalf("reclaimed outbound disappeared after its old deadline: exists=%t same=%t", ok, current == target.expected)
	}
	manager.drainMu.Lock()
	_, stillPending := manager.drainPending[tag]
	manager.drainMu.Unlock()
	if stillPending {
		t.Fatalf("reclaimed outbound %q retained a stale drain target", tag)
	}
}

func TestRuntimeDrainRetriesFailedRemoval(t *testing.T) {
	cfg := newDrainTestConfig(t, 10*time.Millisecond)
	reduced := cfg.Clone()
	reduced.Nodes = cloneNodes(cfg.Nodes[1:])
	tag := removedBaseTag(t, cfg, reduced)

	manager := New(cfg, monitor.Config{Enabled: false})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer manager.Close()

	firstAttempt := make(chan struct{}, 1)
	var attempts atomic.Int32
	manager.removeDrainedOutbound = func(instance *box.Box, gotTag string) error {
		if gotTag != tag {
			return instance.Outbound().Remove(gotTag)
		}
		if attempts.Add(1) == 1 {
			firstAttempt <- struct{}{}
			return errors.New("injected remove failure")
		}
		return instance.Outbound().Remove(gotTag)
	}

	if err := normalizeDrainAndReload(t, manager, reduced); err != nil {
		t.Fatalf("remove node: %v", err)
	}
	select {
	case <-firstAttempt:
	case <-time.After(2 * time.Second):
		t.Fatal("drain removal was not attempted")
	}

	// Verify that failure scheduled a real backoff, then bring its deadline
	// forward so the test exercises the retry without sleeping a full second.
	manager.drainMu.Lock()
	target, pending := manager.drainPending[tag]
	if !pending || target.attempts != 1 || !target.due.After(time.Now()) {
		manager.drainMu.Unlock()
		t.Fatalf("failed removal did not schedule backoff: pending=%t target=%+v", pending, target)
	}
	target.due = time.Now()
	manager.drainPending[tag] = target
	manager.drainMu.Unlock()
	manager.wakeRuntimeDrainWorker()

	eventuallyBoxManager(t, 2*time.Second, func() bool {
		manager.drainMu.Lock()
		_, pending := manager.drainPending[tag]
		manager.drainMu.Unlock()
		_, exists := manager.currentBox.Outbound().Outbound(tag)
		return attempts.Load() == 2 && !pending && !exists
	}, "failed drain removal was not retried to completion")
}

func TestRuntimeDrainRetryIsDiscardedByFullSwitch(t *testing.T) {
	cfg := newDrainTestConfig(t, 10*time.Millisecond)
	reduced := cfg.Clone()
	reduced.Nodes = cloneNodes(cfg.Nodes[1:])
	tag := removedBaseTag(t, cfg, reduced)

	manager := New(cfg, monitor.Config{Enabled: false})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer manager.Close()

	firstAttempt := make(chan struct{}, 1)
	var attempts atomic.Int32
	manager.removeDrainedOutbound = func(instance *box.Box, gotTag string) error {
		if gotTag == tag {
			if attempts.Add(1) == 1 {
				firstAttempt <- struct{}{}
				return errors.New("injected remove failure")
			}
		}
		return instance.Outbound().Remove(gotTag)
	}

	if err := normalizeDrainAndReload(t, manager, reduced); err != nil {
		t.Fatalf("remove node: %v", err)
	}
	select {
	case <-firstAttempt:
	case <-time.After(2 * time.Second):
		t.Fatal("drain removal was not attempted")
	}

	manager.mu.RLock()
	oldInstance := manager.currentBox
	manager.mu.RUnlock()
	manager.drainMu.Lock()
	target := manager.drainPending[tag]
	target.due = time.Now().Add(200 * time.Millisecond)
	manager.drainPending[tag] = target
	manager.drainMu.Unlock()
	manager.wakeRuntimeDrainWorker()

	// SkipCertVerify is immutable at the Box level, so this forces a full
	// validated handoff. Re-add A in the new instance to catch any stale retry
	// that is accidentally redirected at the new runtime by its stable tag.
	replacement := cfg.Clone()
	replacement.SkipCertVerify = !cfg.SkipCertVerify
	if err := normalizeDrainAndReload(t, manager, replacement); err != nil {
		t.Fatalf("full switch: %v", err)
	}
	manager.mu.RLock()
	newInstance := manager.currentBox
	manager.mu.RUnlock()
	if newInstance == oldInstance {
		t.Fatal("expected immutable setting change to replace the sing-box instance")
	}

	time.Sleep(350 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("discarded old drain retried after full switch: attempts=%d", got)
	}
	if _, ok := newInstance.Outbound().Outbound(tag); !ok {
		t.Fatalf("old drain retry removed live outbound %q from new instance", tag)
	}
}

func TestRuntimeDrainRetryIsCancelledByClose(t *testing.T) {
	cfg := newDrainTestConfig(t, 10*time.Millisecond)
	reduced := cfg.Clone()
	reduced.Nodes = cloneNodes(cfg.Nodes[1:])
	tag := removedBaseTag(t, cfg, reduced)

	manager := New(cfg, monitor.Config{Enabled: false})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start manager: %v", err)
	}

	firstAttempt := make(chan struct{}, 1)
	var attempts atomic.Int32
	manager.removeDrainedOutbound = func(instance *box.Box, gotTag string) error {
		if gotTag == tag && attempts.Add(1) == 1 {
			firstAttempt <- struct{}{}
			return errors.New("injected remove failure")
		}
		return instance.Outbound().Remove(gotTag)
	}
	if err := normalizeDrainAndReload(t, manager, reduced); err != nil {
		t.Fatalf("remove node: %v", err)
	}
	select {
	case <-firstAttempt:
	case <-time.After(2 * time.Second):
		t.Fatal("drain removal was not attempted")
	}

	manager.drainMu.Lock()
	target := manager.drainPending[tag]
	target.due = time.Now().Add(100 * time.Millisecond)
	manager.drainPending[tag] = target
	manager.drainMu.Unlock()
	manager.wakeRuntimeDrainWorker()
	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}
	time.Sleep(180 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("drain retried after Close: attempts=%d", got)
	}
}

func TestCloseForcesRuntimeShutdownWithinBoundWhenDrainRemovalHangs(t *testing.T) {
	cfg := newDrainTestConfig(t, 5*time.Millisecond)
	reduced := cfg.Clone()
	reduced.Nodes = cloneNodes(cfg.Nodes[1:])
	tag := removedBaseTag(t, cfg, reduced)

	manager := New(cfg, monitor.Config{Enabled: false})
	manager.drainOperationWait = 30 * time.Millisecond
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	manager.removeDrainedOutbound = func(_ *box.Box, gotTag string) error {
		if gotTag == tag {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
		return nil
	}
	if err := normalizeDrainAndReload(t, manager, reduced); err != nil {
		t.Fatalf("remove node: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("drain removal did not enter injected hang")
	}

	started := time.Now()
	err := manager.Close()
	if err == nil {
		t.Fatal("Close unexpectedly ignored the hung drain")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Close exceeded its drain bound: %s", elapsed)
	}
	if !config.IsPortAvailable(reduced.MultiPort.Address, reduced.Nodes[0].Port) {
		t.Fatal("Close returned while the detached proxy listener was still bound")
	}

	close(release)
	drained := make(chan struct{})
	go func() {
		manager.drainWG.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("drain worker did not exit after releasing injected hang")
	}
}

func TestFullReloadPreservesLiveRuntimeWhenDrainDiscardTimesOut(t *testing.T) {
	cfg := newDrainTestConfig(t, 5*time.Millisecond)
	reduced := cfg.Clone()
	reduced.Nodes = cloneNodes(cfg.Nodes[1:])
	tag := removedBaseTag(t, cfg, reduced)

	manager := New(cfg, monitor.Config{Enabled: false})
	manager.drainOperationWait = 30 * time.Millisecond
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer manager.Close()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	manager.removeDrainedOutbound = func(_ *box.Box, gotTag string) error {
		if gotTag == tag {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
		return nil
	}
	if err := normalizeDrainAndReload(t, manager, reduced); err != nil {
		t.Fatalf("remove node: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("drain removal did not enter injected hang")
	}
	manager.mu.RLock()
	oldInstance := manager.currentBox
	oldRevision := manager.revision
	manager.mu.RUnlock()

	replacement := reduced.Clone()
	replacement.SkipCertVerify = !replacement.SkipCertVerify
	started := time.Now()
	err := normalizeDrainAndReload(t, manager, replacement)
	if err == nil {
		t.Fatal("full reload unexpectedly raced a hung drain")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("full reload exceeded its drain bound: %s", elapsed)
	}
	manager.mu.RLock()
	currentInstance := manager.currentBox
	currentRevision := manager.revision
	manager.mu.RUnlock()
	if currentInstance != oldInstance || currentRevision != oldRevision {
		t.Fatal("failed handoff replaced the live runtime or configuration revision")
	}

	close(release)
}

func newDrainTestConfig(t *testing.T, drainTimeout time.Duration) *config.Config {
	t.Helper()
	firstPort := findManagerTestPort(t)
	secondPort := findManagerTestPort(t)
	for secondPort == firstPort {
		secondPort = findManagerTestPort(t)
	}
	cfg := &config.Config{
		Mode: "multi-port",
		MultiPort: config.MultiPortConfig{
			Address:        "127.0.0.1",
			BasePort:       firstPort,
			PortReuseDelay: time.Hour,
		},
		Nodes: []config.NodeConfig{
			{Name: "drain-a", URI: "socks5://127.0.0.1:9#drain-a", Port: firstPort},
			{Name: "drain-b", URI: "socks5://127.0.0.1:10#drain-b", Port: secondPort},
		},
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			MinAvailableNodes: 0,
			DrainTimeout:      drainTimeout,
		},
	}
	cfg.SetFilePath(filepath.Join(t.TempDir(), "config.yaml"))
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize config: %v", err)
	}
	cfg.SubscriptionRefresh.MinAvailableNodes = 0
	return cfg
}

func removedBaseTag(t *testing.T, full, reduced *config.Config) string {
	t.Helper()
	fullOptions, err := builder.Build(full)
	if err != nil {
		t.Fatalf("build full runtime options: %v", err)
	}
	reducedOptions, err := builder.Build(reduced)
	if err != nil {
		t.Fatalf("build reduced runtime options: %v", err)
	}
	fullBase, _ := splitRuntimeOutbounds(fullOptions)
	reducedBase, _ := splitRuntimeOutbounds(reducedOptions)
	removed := mapDifferenceKeys(fullBase, reducedBase)
	if len(removed) != 1 {
		t.Fatalf("removed base tags=%v, want exactly one", removed)
	}
	return removed[0]
}

func normalizeAndReload(t *testing.T, manager *Manager, cfg *config.Config) error {
	t.Helper()
	if err := cfg.NormalizeWithPortMap(manager.CurrentPortMap()); err != nil {
		t.Fatalf("normalize replacement: %v", err)
	}
	return manager.Reload(cfg)
}

func normalizeDrainAndReload(t *testing.T, manager *Manager, cfg *config.Config) error {
	t.Helper()
	if err := cfg.NormalizeWithPortMap(manager.CurrentPortMap()); err != nil {
		t.Fatalf("normalize replacement: %v", err)
	}
	cfg.SubscriptionRefresh.MinAvailableNodes = 0
	return manager.Reload(cfg)
}

func eventuallyBoxManager(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
