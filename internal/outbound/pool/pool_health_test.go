package pool

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
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
