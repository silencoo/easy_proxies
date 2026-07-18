package pool

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"easy_proxies/internal/monitor"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

func TestDedicatedDispatchAttemptsExactNodeWithoutClearingSharedBlacklist(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)

	state := acquireSharedState("node-a")
	state.recordFailure(errors.New("down"), 1, time.Hour, time.Minute)
	member := &memberState{tag: "node-a", shared: state}
	proxyPool := newIndexedTestPool(member, Options{
		DedicatedMembers: map[string]string{"in-node-a": "node-a"},
	})
	t.Cleanup(func() { _ = proxyPool.Close() })

	if got := proxyPool.selectEligibleMember(""); got != nil {
		t.Fatal("public pool selected a blacklisted node")
	}
	ctx := adapter.WithContext(context.Background(), &adapter.InboundContext{Inbound: "in-node-a"})
	selected, err := proxyPool.pickMember(ctx, "")
	if err != nil || selected != member {
		t.Fatalf("dedicated dispatch did not retain its exact node: selected=%v err=%v", selected, err)
	}
	if state.isBlacklisted(time.Now()) == false {
		t.Fatal("dedicated access cleared the public pool blacklist")
	}
	if proxyPool.releaseIfAllBlacklisted() {
		t.Fatal("pool failed open without explicit configuration")
	}

	failOpenMember := &memberState{tag: "node-a", shared: state}
	failOpenPool := newIndexedTestPool(failOpenMember, Options{FailOpen: true})
	t.Cleanup(func() { _ = failOpenPool.Close() })
	if !failOpenPool.releaseIfAllBlacklisted() {
		t.Fatal("pool did not honor explicit fail_open")
	}
	if state.isBlacklisted(time.Now()) {
		t.Fatal("fail-open pool did not release blacklist")
	}
	if got := failOpenPool.selectEligibleMember(""); got != failOpenMember {
		t.Fatal("released member was not restored to the O(1) eligible index")
	}
}

func TestBlacklistIndexUpdatesAndExpires(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)

	state := acquireSharedState("node-a")
	member := &memberState{tag: "node-a", shared: state}
	proxyPool := newIndexedTestPool(member, Options{})
	t.Cleanup(func() { _ = proxyPool.Close() })
	if got := proxyPool.selectEligibleMember(""); got != member {
		t.Fatal("healthy member missing from eligible index")
	}

	blacklistSharedMember("node-a", 30*time.Millisecond)
	if got := proxyPool.selectEligibleMember(""); got != nil {
		t.Fatal("blacklisted member remained in eligible index")
	}
	deadline := time.Now().Add(time.Second)
	for proxyPool.selectEligibleMember("") == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := proxyPool.selectEligibleMember(""); got != member {
		t.Fatal("expired blacklist did not restore member to eligible index")
	}
}

func TestActiveProbeFailureImmediatelyUpdatesEligibleIndex(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)

	state := acquireSharedState("node-a")
	member := &memberState{tag: "node-a", shared: state}
	proxyPool := newIndexedTestPool(member, Options{BlacklistDuration: time.Hour})
	t.Cleanup(func() { _ = proxyPool.Close() })
	proxyPool.recordProbeFailure(member, errors.New("probe failed"))

	if !state.isBlacklisted(time.Now()) {
		t.Fatal("active probe failure did not update shared routing blacklist")
	}
	if got := proxyPool.selectEligibleMember(""); got != nil {
		t.Fatal("active probe failure did not remove member from eligible index")
	}
}

func TestMemberSetSwapDeleteKeepsConstantTimeIndexValid(t *testing.T) {
	set := newMemberSet(3)
	first := &memberState{tag: "first"}
	second := &memberState{tag: "second"}
	third := &memberState{tag: "third"}
	set.add(first)
	set.add(second)
	set.add(third)
	set.remove(second)
	_, firstExists := set.index[first]
	_, thirdExists := set.index[third]
	if len(set.items) != 2 || !firstExists || !thirdExists {
		t.Fatalf("swap-delete corrupted member index: %#v", set)
	}
	set.remove(first)
	set.remove(third)
	if len(set.items) != 0 || len(set.index) != 0 {
		t.Fatalf("member index did not empty cleanly: %#v", set)
	}
}

func TestClosedSharedStateIgnoresLateProbeTransitions(t *testing.T) {
	state := &sharedMemberState{tag: "closed-node"}
	state.close()
	decision := state.recordFailure(errors.New("late failure"), 1, time.Hour, time.Minute)
	state.releaseAfterProbe()
	state.recordSuccess()
	if decision != (failureDecision{}) {
		t.Fatalf("closed state returned a transition: %+v", decision)
	}
	state.mu.Lock()
	failures := state.failures
	blacklisted := state.blacklisted
	blacklistTimer := state.blacklistTimer
	cooldownTimer := state.cooldownTimer
	state.mu.Unlock()
	if failures != 0 || blacklisted || blacklistTimer != nil || cooldownTimer != nil {
		t.Fatalf("late transition revived closed state: failures=%d blacklisted=%t", failures, blacklisted)
	}
}

func TestClosedPoolDoesNotApplyLateProbeFailure(t *testing.T) {
	state := &sharedMemberState{tag: "late-probe"}
	member := &memberState{tag: "late-probe", shared: state}
	proxyPool := newIndexedTestPool(member, Options{FailureThreshold: 1, BlacklistDuration: time.Hour})
	if err := proxyPool.Close(); err != nil {
		t.Fatal(err)
	}
	proxyPool.recordProbeFailure(member, errors.New("late failure"))
	state.mu.Lock()
	failures := state.failures
	blacklisted := state.blacklisted
	state.mu.Unlock()
	if failures != 0 || blacklisted {
		t.Fatalf("closed pool polluted shared health: failures=%d blacklisted=%t", failures, blacklisted)
	}
}

func TestClosedPoolRejectsProbeBeforeOutboundDial(t *testing.T) {
	outbound := &scriptedOutbound{}
	member := &memberState{tag: "closed-probe", outbound: outbound}
	proxyPool := &poolOutbound{}
	if err := proxyPool.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := proxyPool.probeMember(context.Background(), member, monitor.ProbeTarget{}); err == nil {
		t.Fatal("closed pool unexpectedly accepted a probe")
	}
	if calls := outbound.calls.Load(); calls != 0 {
		t.Fatalf("closed pool started %d outbound dial(s), want 0", calls)
	}
}

func TestCloseDoesNotWaitForUncooperativeHealthProbeDial(t *testing.T) {
	outbound := &gatedProbeOutbound{started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-outbound.release:
		default:
			close(outbound.release)
		}
	})
	member := &memberState{tag: "blocked-health-probe", outbound: outbound}
	proxyPool := &poolOutbound{}

	probeResult := make(chan error, 1)
	go func() {
		_, err := proxyPool.probeMember(context.Background(), member, monitor.ProbeTarget{})
		probeResult <- err
	}()
	select {
	case <-outbound.started:
	case <-time.After(time.Second):
		t.Fatal("health probe dial did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- proxyPool.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Close waited for an uncooperative health probe dial")
	}

	close(outbound.release)
	select {
	case err := <-probeResult:
		if !errors.Is(err, errPoolClosed) {
			t.Fatalf("late health probe error = %v, want errPoolClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("health probe did not exit after release")
	}
}

type gatedProbeOutbound struct {
	adapter.Outbound
	started chan struct{}
	release chan struct{}
}

func (o *gatedProbeOutbound) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	close(o.started)
	select {
	case <-o.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		reader := bufio.NewReader(server)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if line == "\r\n" {
				break
			}
		}
		_, _ = server.Write([]byte("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"))
	}()
	return client, nil
}

func TestSupersededPoolProbeCannotMarkReplacementHealthy(t *testing.T) {
	monitorManager, err := monitor.NewManager(monitor.Config{ProbeTarget: "example.com:80"})
	if err != nil {
		t.Fatal(err)
	}
	defer monitorManager.Stop()

	entry := monitorManager.Register(monitor.NodeInfo{Tag: "replacement"})
	outbound := &gatedProbeOutbound{started: make(chan struct{}), release: make(chan struct{})}
	member := &memberState{tag: "replacement", outbound: outbound, entry: entry}
	proxyPool := &poolOutbound{monitor: monitorManager}
	entry.SetProbe(proxyPool.makeProbeFunc(member))

	probeResult := make(chan error, 1)
	go func() {
		_, probeErr := monitorManager.Probe(context.Background(), "replacement")
		probeResult <- probeErr
	}()
	<-outbound.started
	entry.SetProbe(func(context.Context) (time.Duration, error) {
		return 2 * time.Millisecond, nil
	})
	close(outbound.release)
	if err := <-probeResult; err == nil {
		t.Fatal("superseded pool probe unexpectedly committed")
	}

	snapshot := monitorManager.Snapshot()[0]
	if snapshot.InitialCheckDone || snapshot.Available {
		t.Fatalf("superseded pool callback marked replacement healthy: %+v", snapshot)
	}
}

func BenchmarkEligibleSelectionConstantTime(b *testing.B) {
	for _, size := range []int{1, 100, 10_000} {
		b.Run(fmt.Sprintf("members_%d", size), func(b *testing.B) {
			proxyPool := &poolOutbound{
				mode:        modeSequential,
				eligibleTCP: newMemberSet(size),
				eligibleUDP: newMemberSet(size),
			}
			for idx := 0; idx < size; idx++ {
				proxyPool.eligibleTCP.add(&memberState{tag: fmt.Sprint(idx), shared: &sharedMemberState{}})
			}
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				_ = proxyPool.selectHealthyMember("")
			}
		})
	}
}

func newIndexedTestPool(member *memberState, options Options) *poolOutbound {
	p := &poolOutbound{
		options:     options,
		mode:        modeSequential,
		members:     []*memberState{member},
		memberByTag: map[string]*memberState{member.tag: member},
		eligibleTCP: newMemberSet(1),
		eligibleUDP: newMemberSet(1),
	}
	member.unwatch = member.shared.subscribeBlacklist(func(blacklisted bool) {
		p.setMemberEligible(member, options.Dedicated || !blacklisted)
	})
	p.initialized.Store(true)
	return p
}
