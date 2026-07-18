package monitor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProbeConcurrencyClamp(t *testing.T) {
	manager, err := NewManager(Config{ProbeTarget: "example.com:80"})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	if got := manager.ProbeConcurrency(); got != defaultProbeConcurrency {
		t.Fatalf("default concurrency = %d, want %d", got, defaultProbeConcurrency)
	}
	manager.SetProbeConcurrency(1)
	if got := manager.ProbeConcurrency(); got != 1 {
		t.Fatalf("explicit concurrency = %d, want 1", got)
	}
	manager.SetProbeConcurrency(maxProbeConcurrency + 1)
	if got := manager.ProbeConcurrency(); got != maxProbeConcurrency {
		t.Fatalf("clamped concurrency = %d, want %d", got, maxProbeConcurrency)
	}
}

func TestHTTPSProbeTargetKeepsTLSWhenVerificationSkipped(t *testing.T) {
	target, ready, err := resolveProbeTarget("https://example.com/check", true)
	if err != nil {
		t.Fatal(err)
	}
	if !ready || !target.TLS || !target.SkipCertVerify {
		t.Fatalf("unexpected HTTPS target: ready=%v target=%+v", ready, target)
	}
	if target.Host != "example.com" || target.Destination.Port != 443 {
		t.Fatalf("unexpected HTTPS destination: %+v", target)
	}
}

func TestProbeTargetRejectsUnsupportedScheme(t *testing.T) {
	if _, ready, err := resolveProbeTarget("ftp://example.com/file", false); err == nil || ready {
		t.Fatalf("unsupported probe scheme result: ready=%v err=%v", ready, err)
	}
}

func TestProbeTargetRejectsInvalidPorts(t *testing.T) {
	for _, value := range []string{"example.com:abc", "example.com:0", "https://example.com:65536/check"} {
		if _, ready, err := resolveProbeTarget(value, false); err == nil || ready {
			t.Errorf("invalid target %q result: ready=%v err=%v", value, ready, err)
		}
	}
}

func TestInvalidLiveProbeTargetPreservesPreviousValue(t *testing.T) {
	manager, err := NewManager(Config{ProbeTarget: "example.com:80"})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	before, ready := manager.DestinationForProbe()
	if !ready {
		t.Fatal("initial target is not ready")
	}
	if err := manager.SetProbeTarget("example.com:not-a-port", true); err == nil {
		t.Fatal("invalid live target was accepted")
	}
	after, ready := manager.DestinationForProbe()
	if !ready || after != before {
		t.Fatalf("invalid update changed target: before=%+v after=%+v ready=%v", before, after, ready)
	}
}

func TestEntryProbeDeadlineDeduplicatesHungCallback(t *testing.T) {
	entry := &entry{}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	entry.setProbe(func(context.Context) (time.Duration, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release // deliberately ignore context
		return time.Millisecond, nil
	})

	ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel1()
	done1 := make(chan struct{})
	go func() {
		_, _ = entry.executeProbe(ctx1, context.Background(), time.Second)
		close(done1)
	}()
	<-started

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	if _, err := entry.executeProbe(ctx2, context.Background(), time.Second); err == nil {
		t.Fatal("joined probe unexpectedly succeeded before release")
	}
	<-done1
	if got := calls.Load(); got != 1 {
		t.Fatalf("underlying callback ran %d times, want 1", got)
	}
	close(release)
}

func TestProbeCallbacksAreGloballyBoundedAcrossRotatingTags(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	manager.probeSlots = make(chan struct{}, 2)

	release := make(chan struct{})
	var started atomic.Int32
	for index := 0; index < 8; index++ {
		tag := fmt.Sprintf("hung-%d", index)
		entry := manager.Register(NodeInfo{Tag: tag})
		entry.SetProbe(func(context.Context) (time.Duration, error) {
			started.Add(1)
			<-release
			return time.Millisecond, nil
		})
	}

	var callers sync.WaitGroup
	for index := 0; index < 8; index++ {
		callers.Add(1)
		go func(tag string) {
			defer callers.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			_, _ = manager.Probe(ctx, tag)
		}(fmt.Sprintf("hung-%d", index))
	}
	callers.Wait()
	if got := started.Load(); got != 2 {
		t.Fatalf("underlying callbacks=%d, want global cap 2", got)
	}
	close(release)
}

func TestSupersededProbeResultDoesNotMarkReplacementGeneration(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	entry := manager.Register(NodeInfo{Tag: "replace-result"})
	started := make(chan struct{})
	release := make(chan struct{})
	entry.SetProbe(func(context.Context) (time.Duration, error) {
		close(started)
		<-release
		return time.Millisecond, nil
	})
	result := make(chan error, 1)
	go func() {
		_, err := manager.Probe(context.Background(), "replace-result")
		result <- err
	}()
	<-started
	entry.SetProbe(func(context.Context) (time.Duration, error) { return 2 * time.Millisecond, nil })
	close(release)
	if err := <-result; !errors.Is(err, errProbeSuperseded) {
		t.Fatalf("old generation result=%v, want superseded", err)
	}
	before := manager.Snapshot()[0]
	if before.InitialCheckDone || before.Available {
		t.Fatalf("old generation mutated replacement state: %+v", before)
	}
	if _, err := manager.Probe(context.Background(), "replace-result"); err != nil {
		t.Fatal(err)
	}
	after := manager.Snapshot()[0]
	if !after.InitialCheckDone || !after.Available || after.LastProbeLatency != 2*time.Millisecond {
		t.Fatalf("replacement generation was not recorded: %+v", after)
	}
}

func TestSetProbeInvalidatesPreviouslyHealthyGeneration(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()

	entry := manager.Register(NodeInfo{Tag: "generation-reset"})
	entry.MarkInitialCheckDone(true)
	entry.SetProbe(func(context.Context) (time.Duration, error) {
		return time.Millisecond, nil
	})

	snapshot := manager.Snapshot()[0]
	if snapshot.InitialCheckDone || snapshot.Available {
		t.Fatalf("replacement probe inherited stale health: %+v", snapshot)
	}
}

func TestProbeSweepCoalescesOverlappingTriggers(t *testing.T) {
	manager, err := NewManager(Config{ProbeTarget: "example.com:80", ProbeConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	entry := manager.Register(NodeInfo{Tag: "node-1", URI: "socks5://127.0.0.1:1080"})
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	entry.SetProbe(func(context.Context) (time.Duration, error) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		return time.Millisecond, nil
	})

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); manager.ProbeAllNow(time.Second) }()
	<-firstStarted
	go func() { defer wg.Done(); manager.ProbeAllNow(time.Second) }()
	go func() { defer wg.Done(); manager.ProbeAllNow(time.Second) }()
	waitForRequestedGeneration(t, manager, 3)
	close(releaseFirst)
	wg.Wait()
	if got := calls.Load(); got != 2 {
		t.Fatalf("underlying callback ran %d times, want initial + one coalesced rerun", got)
	}
}

func waitForRequestedGeneration(t *testing.T, manager *Manager, want uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.probeGate.Lock()
		generation := manager.requestedGeneration
		manager.probeGate.Unlock()
		if generation >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("probe generation did not reach %d", want)
}

func TestProbeSweepRunsFollowupRequestedDuringFollowup(t *testing.T) {
	manager, err := NewManager(Config{ProbeTarget: "example.com:80", ProbeConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	entry := manager.Register(NodeInfo{Tag: "node-1"})
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	var calls atomic.Int32
	entry.SetProbe(func(context.Context) (time.Duration, error) {
		switch calls.Add(1) {
		case 1:
			close(firstStarted)
			<-releaseFirst
		case 2:
			close(secondStarted)
			<-releaseSecond
		}
		return time.Millisecond, nil
	})

	allDone := make(chan struct{})
	go func() { manager.ProbeAllNow(time.Second); close(allDone) }()
	<-firstStarted
	secondDone := make(chan struct{})
	go func() { manager.ProbeAllNow(time.Second); close(secondDone) }()
	waitForRequestedGeneration(t, manager, 2)
	close(releaseFirst)
	<-secondStarted
	thirdDone := make(chan struct{})
	go func() { manager.ProbeAllNow(time.Second); close(thirdDone) }()
	waitForRequestedGeneration(t, manager, 3)
	close(releaseSecond)
	<-allDone
	<-secondDone
	<-thirdDone
	if got := calls.Load(); got != 3 {
		t.Fatalf("underlying callback ran %d times, want initial plus two generation followups", got)
	}
}

func TestProbeSweepDeadlineAndProgress(t *testing.T) {
	manager, err := NewManager(Config{ProbeTarget: "example.com:80", ProbeConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	entry := manager.Register(NodeInfo{Tag: "hung", URI: "vless://secret@example.com:443"})
	release := make(chan struct{})
	entry.SetProbe(func(context.Context) (time.Duration, error) {
		<-release // deliberately ignore context
		return 0, nil
	})

	start := time.Now()
	manager.ProbeAllNow(30 * time.Millisecond)
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("bounded sweep took %s", elapsed)
	}
	active, done, total, okCount, failed := manager.ProbeSweepProgress()
	if active || done != 1 || total != 1 || okCount != 0 || failed != 1 {
		t.Fatalf("unexpected progress: active=%v done=%d total=%d ok=%d failed=%d", active, done, total, okCount, failed)
	}
	close(release)
}

func TestProbeAllNowContextCancelsUnderlyingSweep(t *testing.T) {
	manager, err := NewManager(Config{ProbeTarget: "example.com:80", ProbeConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()

	entry := manager.Register(NodeInfo{Tag: "cancel-sweep"})
	started := make(chan struct{})
	callbackDone := make(chan struct{})
	entry.SetProbe(func(ctx context.Context) (time.Duration, error) {
		close(started)
		<-ctx.Done()
		close(callbackDone)
		return 0, ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- manager.ProbeAllNowContext(ctx, time.Second)
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("ProbeAllNowContext error = %v, want context canceled", err)
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("operation cancellation did not reach probe callback")
	}

	deadline := time.Now().Add(time.Second)
	for {
		active, _, _, _, _ := manager.ProbeSweepProgress()
		if !active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("canceled sweep coordinator remained active")
		}
		time.Sleep(time.Millisecond)
	}
	snapshot := manager.Snapshot()[0]
	if snapshot.InitialCheckDone || snapshot.Available {
		t.Fatalf("canceled sweep committed a health result: %+v", snapshot)
	}
}

func TestProbeCallbackReplacementDoesNotLeakAnotherHungGeneration(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	entry := manager.Register(NodeInfo{Tag: "generation-bound"})
	release := make(chan struct{})
	started := make(chan struct{})
	entry.SetProbe(func(context.Context) (time.Duration, error) {
		close(started)
		<-release
		return time.Millisecond, nil
	})

	firstCtx, firstCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer firstCancel()
	if _, err := manager.Probe(firstCtx, "generation-bound"); err == nil {
		t.Fatal("hung first generation unexpectedly completed")
	}
	<-started
	var replacementCalls atomic.Int32
	entry.SetProbe(func(context.Context) (time.Duration, error) {
		replacementCalls.Add(1)
		return time.Millisecond, nil
	})
	secondCtx, secondCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer secondCancel()
	if _, err := manager.Probe(secondCtx, "generation-bound"); err == nil {
		t.Fatal("second caller did not join the older hung generation")
	}
	if replacementCalls.Load() != 0 {
		t.Fatal("replacement callback leaked a concurrent generation")
	}

	close(release)
	deadline := time.Now().Add(time.Second)
	for {
		entry.ref.probeMu.Lock()
		running := entry.ref.probeCall != nil
		entry.ref.probeMu.Unlock()
		if !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("released generation did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := manager.Probe(context.Background(), "generation-bound"); err != nil {
		t.Fatal(err)
	}
	if replacementCalls.Load() != 1 {
		t.Fatalf("replacement callback calls=%d, want 1", replacementCalls.Load())
	}
}

func TestSuccessfulProbeDoesNotBypassCooldown(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()

	entry := manager.Register(NodeInfo{Tag: "cooling"})
	entry.MarkInitialCheckDone(true)
	entry.Cooldown(time.Now().Add(time.Minute))
	entry.SetProbe(func(context.Context) (time.Duration, error) {
		return time.Millisecond, nil
	})

	if _, err := manager.Probe(context.Background(), "cooling"); err != nil {
		t.Fatalf("probe failed: %v", err)
	}
	snapshot := manager.Snapshot()[0]
	if !snapshot.CoolingDown || snapshot.Available {
		t.Fatalf("successful probe bypassed cooldown: %+v", snapshot)
	}
}

func TestStopAndWaitWaitsForInflightProbe(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	entry := manager.Register(NodeInfo{Tag: "stopping"})
	started := make(chan struct{})
	release := make(chan struct{})
	entry.SetProbe(func(context.Context) (time.Duration, error) {
		close(started)
		<-release // Deliberately ignore cancellation to exercise the wait bound.
		return time.Millisecond, nil
	})

	probeDone := make(chan error, 1)
	go func() {
		_, probeErr := manager.Probe(context.Background(), "stopping")
		probeDone <- probeErr
	}()
	<-started

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer shortCancel()
	if err := manager.StopAndWait(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatalf("StopAndWait error = %v, want deadline exceeded", err)
	}
	select {
	case err := <-probeDone:
		close(release)
		t.Fatalf("in-flight probe returned before release: %v", err)
	default:
	}

	close(release)
	finalCtx, finalCancel := context.WithTimeout(context.Background(), time.Second)
	defer finalCancel()
	if err := manager.StopAndWait(finalCtx); err != nil {
		t.Fatalf("StopAndWait after release: %v", err)
	}
	if err := <-probeDone; err != nil {
		t.Fatalf("in-flight probe result: %v", err)
	}
}

func TestStopPreventsNewProbeCallbacks(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	entry := manager.Register(NodeInfo{Tag: "stopped"})
	var calls atomic.Int32
	entry.SetProbe(func(context.Context) (time.Duration, error) {
		calls.Add(1)
		return time.Millisecond, nil
	})

	manager.Stop()
	if _, err := manager.Probe(context.Background(), "stopped"); !errors.Is(err, errProbeManagerStopped) {
		t.Fatalf("Probe error = %v, want manager stopped", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("probe callback started %d times after Stop, want 0", got)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.StopAndWait(waitCtx); err != nil {
		t.Fatalf("StopAndWait without callbacks: %v", err)
	}
}
