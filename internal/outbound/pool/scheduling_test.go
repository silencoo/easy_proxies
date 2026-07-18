package pool

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"easy_proxies/internal/monitor"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestTransientFailureUsesCooldownWithoutBlacklistStrike(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)

	manager, err := monitor.NewManager(monitor.Config{})
	if err != nil {
		t.Fatal(err)
	}
	state := acquireSharedState("transient")
	state.attachEntry(manager.Register(monitor.NodeInfo{Tag: "transient"}))
	decision := state.recordFailure(context.DeadlineExceeded, 3, time.Hour, time.Minute)
	if !decision.Cooldown || decision.Blacklisted || decision.Failures != 0 {
		t.Fatalf("unexpected transient decision: %#v", decision)
	}
	if state.isBlacklisted(time.Now()) || !state.isCoolingDown(time.Now()) || !state.blacklistedFast.Load() {
		t.Fatal("transient failure was not represented as an active routing cooldown")
	}
	snapshot := manager.Snapshot()[0]
	if !snapshot.CoolingDown || snapshot.Blacklisted || snapshot.Available {
		t.Fatalf("monitor did not distinguish cooldown from blacklist: %#v", snapshot)
	}

	state.mu.Lock()
	state.cooldownUntil = time.Now().Add(-time.Second)
	state.mu.Unlock()
	if state.isCoolingDown(time.Now()) || state.blacklistedFast.Load() {
		t.Fatal("expired cooldown did not release the node")
	}
}

func TestPermanentFailuresStillReachBlacklistThreshold(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)
	state := acquireSharedState("permanent")
	first := state.recordFailure(errors.New("invalid protocol handshake"), 2, time.Hour, time.Minute)
	second := state.recordFailure(errors.New("invalid protocol handshake"), 2, time.Hour, time.Minute)
	if first.Cooldown || first.Blacklisted || first.Failures != 1 {
		t.Fatalf("unexpected first decision: %#v", first)
	}
	if !second.Blacklisted || second.Cooldown || !state.isBlacklisted(time.Now()) {
		t.Fatalf("permanent failure did not trigger blacklist: %#v", second)
	}
}

func TestSuccessfulProbePreservesManualBlacklist(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)
	state := acquireSharedState("manual-probe")
	blacklistSharedMember("manual-probe", time.Hour)

	state.releaseAfterProbe()

	if !state.isBlacklisted(time.Now()) || !state.blacklistedFast.Load() {
		t.Fatal("successful probe cleared an administrator blacklist")
	}
	state.mu.Lock()
	manual := state.manualBlacklist
	state.mu.Unlock()
	if !manual {
		t.Fatal("manual blacklist origin was lost")
	}
}

func TestSuccessfulProbeReleasesAutomaticBlacklist(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)
	state := acquireSharedState("automatic-probe")
	state.recordFailure(errors.New("protocol failure"), 1, time.Hour, time.Minute)

	state.releaseAfterProbe()

	if state.isBlacklisted(time.Now()) || state.blacklistedFast.Load() {
		t.Fatal("successful probe did not recover an automatic blacklist")
	}
}

func TestSharedStateTransitionsPublishInStateOrder(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)
	state := acquireSharedState("linearized")

	trueEntered := make(chan struct{}, 1)
	releaseTrue := make(chan struct{})
	falseEntered := make(chan struct{}, 1)
	var observeTransitions atomic.Bool
	var appliedBlocked atomic.Bool
	unwatch := state.subscribeBlacklist(func(blocked bool) {
		if !observeTransitions.Load() {
			appliedBlocked.Store(blocked)
			return
		}
		if blocked {
			trueEntered <- struct{}{}
			<-releaseTrue
			appliedBlocked.Store(true)
			return
		}
		appliedBlocked.Store(false)
		falseEntered <- struct{}{}
	})
	t.Cleanup(unwatch)
	observeTransitions.Store(true)

	failureDone := make(chan struct{})
	go func() {
		state.recordFailure(context.DeadlineExceeded, 3, time.Hour, time.Minute)
		close(failureDone)
	}()
	<-trueEntered

	successDone := make(chan struct{})
	go func() {
		state.recordSuccess()
		close(successDone)
	}()

	falsePublishedEarly := false
	select {
	case <-falseEntered:
		falsePublishedEarly = true
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseTrue)
	<-failureDone
	<-successDone
	if falsePublishedEarly {
		t.Fatal("success notification overtook an in-flight failure notification")
	}
	select {
	case <-falseEntered:
	case <-time.After(time.Second):
		t.Fatal("final success notification was not published")
	}
	if appliedBlocked.Load() || state.blacklistedFast.Load() || state.isBlocked(time.Now()) {
		t.Fatal("notification order left a healthy node excluded")
	}
}

func TestStaleTimerScheduleCannotReplaceExtendedDeadline(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)
	state := acquireSharedState("extended")
	later := time.Now().Add(time.Hour)
	earlier := time.Now().Add(time.Minute)

	state.transitionMu.Lock()
	state.mu.Lock()
	state.cooldownUntil = later
	state.blacklistedFast.Store(true)
	state.mu.Unlock()
	state.scheduleCooldownExpiry(later)
	state.mu.Lock()
	cooldownTimer := state.cooldownTimer
	state.mu.Unlock()
	state.scheduleCooldownExpiry(earlier)
	state.mu.Lock()
	if state.cooldownTimer != cooldownTimer {
		state.mu.Unlock()
		state.transitionMu.Unlock()
		t.Fatal("stale cooldown schedule replaced the timer for the extended deadline")
	}
	state.blacklisted = true
	state.blacklistedUntil = later
	state.mu.Unlock()
	state.scheduleBlacklistExpiry(later)
	state.mu.Lock()
	blacklistTimer := state.blacklistTimer
	state.mu.Unlock()
	state.scheduleBlacklistExpiry(earlier)
	state.mu.Lock()
	blacklistChanged := state.blacklistTimer != blacklistTimer
	state.mu.Unlock()
	state.transitionMu.Unlock()
	if blacklistChanged {
		t.Fatal("stale blacklist schedule replaced the timer for the extended deadline")
	}
}

func TestRecordFailureLogsSanitizedSingleLineError(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)
	var output bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	previousPrefix := log.Prefix()
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})

	proxyPool := &poolOutbound{options: normalizeOptions(Options{FailureThreshold: 3})}
	member := &memberState{tag: "safe-tag", shared: acquireSharedState("safe-tag")}
	proxyPool.recordFailure(member, errors.New("dial vless://user:super-secret@example.com:443/path?token=hidden\r\nforged-line"))
	logged := output.String()
	for _, secret := range []string{"super-secret", "token=hidden"} {
		if strings.Contains(logged, secret) {
			t.Fatalf("pool log leaked unsafe content %q: %q", secret, logged)
		}
	}
	if strings.Count(strings.TrimSuffix(logged, "\n"), "\n") != 0 {
		t.Fatalf("pool failure produced a multi-line log entry: %q", logged)
	}
}

func TestStickyCacheIsBoundedLRUAndExpires(t *testing.T) {
	base := time.Unix(1_000, 0)
	cache := newStickyCache(time.Minute, 2)
	cache.set("a", "node-a", base)
	cache.set("b", "node-b", base)
	if _, ok := cache.get("a", base.Add(time.Second)); !ok {
		t.Fatal("expected a binding")
	}
	cache.set("c", "node-c", base.Add(2*time.Second))
	if cache.len() != 2 {
		t.Fatalf("cache exceeded capacity: %d", cache.len())
	}
	if _, ok := cache.get("b", base.Add(2*time.Second)); ok {
		t.Fatal("least-recently-used binding was not evicted")
	}
	if _, ok := cache.get("a", base.Add(2*time.Minute)); ok {
		t.Fatal("expired binding remained sticky")
	}
}

func TestLatencyCandidateBalancesWithinTolerance(t *testing.T) {
	manager, err := monitor.NewManager(monitor.Config{})
	if err != nil {
		t.Fatal(err)
	}
	low := &memberState{tag: "low", shared: &sharedMemberState{}}
	near := &memberState{tag: "near", shared: &sharedMemberState{}}
	far := &memberState{tag: "far", shared: &sharedMemberState{}}
	low.entry = manager.Register(monitor.NodeInfo{Tag: low.tag})
	near.entry = manager.Register(monitor.NodeInfo{Tag: near.tag})
	far.entry = manager.Register(monitor.NodeInfo{Tag: far.tag})
	low.entry.RecordSuccessWithLatency(20 * time.Millisecond)
	near.entry.RecordSuccessWithLatency(40 * time.Millisecond)
	far.entry.RecordSuccessWithLatency(200 * time.Millisecond)
	for range 10 {
		low.shared.incActive()
	}
	if !betterLatencyCandidate(near, low, 50*time.Millisecond) {
		t.Fatal("latency mode did not balance connections inside tolerance")
	}
	if betterLatencyCandidate(far, near, 50*time.Millisecond) {
		t.Fatal("latency mode preferred a materially slower node")
	}
}

type scriptedOutbound struct {
	adapter.Outbound
	fail  bool
	calls atomic.Int32
}

func (o *scriptedOutbound) Network() []string {
	return []string{N.NetworkTCP, N.NetworkUDP}
}

func (o *scriptedOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.calls.Add(1)
	if o.fail {
		return nil, errors.New("dial rejected")
	}
	client, server := net.Pipe()
	go server.Close()
	return client, nil
}

func TestPoolRetryChangesMemberButDedicatedPortDoesNot(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)
	failing := &scriptedOutbound{fail: true}
	succeeding := &scriptedOutbound{}
	first := &memberState{tag: "first", outbound: failing, shared: acquireSharedState("first")}
	second := &memberState{tag: "second", outbound: succeeding, shared: acquireSharedState("second")}
	proxyPool := &poolOutbound{
		options: normalizeOptions(Options{
			RetryEnabled:      true,
			RetryAttempts:     2,
			FailureThreshold:  10,
			TransientCooldown: time.Minute,
			DedicatedMembers:  map[string]string{"in-first": "first"},
		}),
		mode:        modeSequential,
		members:     []*memberState{first, second},
		memberByTag: map[string]*memberState{"first": first, "second": second},
		eligibleTCP: newMemberSet(2),
		eligibleUDP: newMemberSet(2),
	}
	proxyPool.eligibleTCP.add(first)
	proxyPool.eligibleTCP.add(second)
	proxyPool.eligibleUDP.add(first)
	proxyPool.eligibleUDP.add(second)
	proxyPool.initialized.Store(true)

	conn, err := proxyPool.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.com:80"))
	if err != nil {
		t.Fatalf("pooled retry failed: %v", err)
	}
	_ = conn.Close()
	if failing.calls.Load() != 1 || succeeding.calls.Load() != 1 {
		t.Fatalf("retry did not change member: failing=%d succeeding=%d", failing.calls.Load(), succeeding.calls.Load())
	}

	dedicatedCtx := adapter.WithContext(context.Background(), &adapter.InboundContext{Inbound: "in-first"})
	if _, err := proxyPool.DialContext(dedicatedCtx, N.NetworkTCP, M.ParseSocksaddr("example.com:80")); err == nil {
		t.Fatal("dedicated failing node unexpectedly succeeded")
	}
	if failing.calls.Load() != 2 || succeeding.calls.Load() != 1 {
		t.Fatal("dedicated port failed over to another node")
	}
}
