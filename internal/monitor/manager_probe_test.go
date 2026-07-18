package monitor

import (
	"context"
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
	target, ready := resolveProbeTarget("https://example.com/check", true)
	if !ready || !target.TLS || !target.SkipCertVerify {
		t.Fatalf("unexpected HTTPS target: ready=%v target=%+v", ready, target)
	}
	if target.Host != "example.com" || target.Destination.Port != 443 {
		t.Fatalf("unexpected HTTPS destination: %+v", target)
	}
}

func TestProbeTargetRejectsUnsupportedScheme(t *testing.T) {
	if _, ready := resolveProbeTarget("ftp://example.com/file", false); ready {
		t.Fatal("unsupported probe scheme was accepted")
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
		_, _ = entry.executeProbe(ctx1)
		close(done1)
	}()
	<-started

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	if _, err := entry.executeProbe(ctx2); err == nil {
		t.Fatal("joined probe unexpectedly succeeded before release")
	}
	<-done1
	if got := calls.Load(); got != 1 {
		t.Fatalf("underlying callback ran %d times, want 1", got)
	}
	close(release)
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
	waitForProbeWaiters(t, manager, 2)
	close(releaseFirst)
	wg.Wait()
	if got := calls.Load(); got != 2 {
		t.Fatalf("underlying callback ran %d times, want initial + one coalesced rerun", got)
	}
}

func waitForProbeWaiters(t *testing.T, manager *Manager, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.probeGate.Lock()
		waiters := manager.sweepWaiters
		manager.probeGate.Unlock()
		if waiters >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("probe sweep has fewer than %d overlapping waiters", want)
}

func TestProbeSweepDoesNotQueueMoreThanOneFollowup(t *testing.T) {
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
	waitForRerunRequest(t, manager)
	close(releaseFirst)
	<-secondStarted
	thirdDone := make(chan struct{})
	go func() { manager.ProbeAllNow(time.Second); close(thirdDone) }()
	waitForRerunRequest(t, manager)
	close(releaseSecond)
	<-allDone
	<-secondDone
	<-thirdDone
	if got := calls.Load(); got != 2 {
		t.Fatalf("underlying callback ran %d times, want at most one followup", got)
	}
}

func waitForRerunRequest(t *testing.T, manager *Manager) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.probeGate.Lock()
		requested := manager.rerunRequested
		manager.probeGate.Unlock()
		if requested {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("overlapping sweep did not request a followup")
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
