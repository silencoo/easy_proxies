package pool

import (
	"errors"
	"testing"
	"time"
)

func TestDedicatedPoolAttemptsExactNodeWithoutClearingSharedBlacklist(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)

	state := acquireSharedState("node-a")
	state.recordFailure(errors.New("down"), 1, time.Hour)
	member := &memberState{tag: "node-a", shared: state}
	dedicated := &poolOutbound{
		options: Options{Dedicated: true},
		members: []*memberState{member},
	}

	candidates := dedicated.availableMembersLocked(time.Now(), "", nil)
	if len(candidates) != 1 || candidates[0] != member {
		t.Fatal("dedicated pool did not retain its exact node")
	}
	if dedicated.releaseIfAllBlacklistedLocked(time.Now()) {
		t.Fatal("dedicated pool force-released shared blacklist")
	}
	if !state.isBlacklisted(time.Now()) {
		t.Fatal("dedicated access cleared the global pool blacklist")
	}

	sharedPool := &poolOutbound{members: []*memberState{member}}
	if candidates := sharedPool.availableMembersLocked(time.Now(), "", nil); len(candidates) != 0 {
		t.Fatal("shared pool selected a blacklisted node")
	}
	if sharedPool.releaseIfAllBlacklistedLocked(time.Now()) {
		t.Fatal("shared pool failed open without explicit configuration")
	}
	if !state.isBlacklisted(time.Now()) {
		t.Fatal("shared pool cleared blacklist while fail_open was disabled")
	}

	failOpenPool := &poolOutbound{options: Options{FailOpen: true}, members: []*memberState{member}}
	if !failOpenPool.releaseIfAllBlacklistedLocked(time.Now()) {
		t.Fatal("shared pool did not honor explicit fail_open")
	}
	if state.isBlacklisted(time.Now()) {
		t.Fatal("fail-open pool did not release blacklist")
	}
}

func TestActiveProbeFailureImmediatelyBlacklistsRoutingState(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)

	state := acquireSharedState("node-a")
	member := &memberState{tag: "node-a", shared: state}
	proxyPool := &poolOutbound{options: Options{BlacklistDuration: time.Hour}}
	proxyPool.recordProbeFailure(member, errors.New("probe failed"))

	if !state.isBlacklisted(time.Now()) {
		t.Fatal("active probe failure did not update shared routing blacklist")
	}
}
