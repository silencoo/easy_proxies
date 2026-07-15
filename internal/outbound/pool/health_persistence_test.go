package pool

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"easy_proxies/internal/monitor"

	"gopkg.in/yaml.v3"
)

func TestHealthStatePersistsAcrossRuntimeReset(t *testing.T) {
	ResetSharedStateStore()
	resetHealthPersistenceForTest()
	t.Cleanup(func() {
		ResetSharedStateStore()
		resetHealthPersistenceForTest()
	})

	path := filepath.Join(t.TempDir(), "health-state.yaml")
	if err := ConfigureHealthPersistence(path); err != nil {
		t.Fatalf("configure persistence: %v", err)
	}
	monitorManager, err := monitor.NewManager(monitor.Config{})
	if err != nil {
		t.Fatal(err)
	}
	entry := monitorManager.Register(monitor.NodeInfo{Tag: "stable-node", Name: "node"})
	state := acquireSharedState("stable-node")
	state.attachEntry(entry)
	state.recordFailure(errors.New("first failure"), 3, time.Hour)
	blacklistSharedMember("stable-node", time.Hour)
	state.recordFailure(errors.New("second failure"), 3, time.Hour)
	entry.RecordSuccessWithLatency(125 * time.Millisecond)
	entry.MarkInitialCheckDone(false)
	state.incActive()
	state.persist()
	if err := FlushHealthState(); err != nil {
		t.Fatalf("flush health state: %v", err)
	}

	ResetSharedStateStore()
	resetHealthPersistenceForTest()
	if err := ConfigureHealthPersistence(path); err != nil {
		t.Fatalf("reload persistence: %v", err)
	}
	restored := acquireSharedState("stable-node")
	restored.mu.Lock()
	failures := restored.failures
	blacklisted := restored.blacklisted
	until := restored.blacklistedUntil
	restored.mu.Unlock()
	if failures != 1 {
		t.Fatalf("failure streak was not restored: got %d want 1", failures)
	}
	if !blacklisted || !until.After(time.Now()) {
		t.Fatalf("active blacklist was not restored: blacklisted=%v until=%v", blacklisted, until)
	}
	if restored.activeCount() != 0 {
		t.Fatalf("process-local active connections were restored: %d", restored.activeCount())
	}

	newMonitor, err := monitor.NewManager(monitor.Config{})
	if err != nil {
		t.Fatal(err)
	}
	restoredEntry := newMonitor.Register(monitor.NodeInfo{Tag: "stable-node", Name: "renamed"})
	restored.attachEntry(restoredEntry)
	snapshot := newMonitor.Snapshot()[0]
	if snapshot.FailureCount != 2 || snapshot.SuccessCount != 1 {
		t.Fatalf("monitor counters were not restored: failures=%d successes=%d", snapshot.FailureCount, snapshot.SuccessCount)
	}
	if snapshot.LastError != "second failure" || snapshot.LastProbeLatency != 125*time.Millisecond {
		t.Fatalf("monitor details were not restored: %#v", snapshot)
	}
	if !snapshot.Blacklisted || snapshot.Available {
		t.Fatalf("monitor blacklist/availability was not restored: %#v", snapshot)
	}
	restoredEntry.RecordSuccess()
	restored.attachEntry(restoredEntry)
	if got := newMonitor.Snapshot()[0].SuccessCount; got != 2 {
		t.Fatalf("a second pool attachment overwrote live restored state: successes=%d", got)
	}
}

func TestExpiredBlacklistIsNotRestored(t *testing.T) {
	ResetSharedStateStore()
	resetHealthPersistenceForTest()
	t.Cleanup(func() {
		ResetSharedStateStore()
		resetHealthPersistenceForTest()
	})

	path := filepath.Join(t.TempDir(), "health-state.yaml")
	data, err := yaml.Marshal(persistedHealthFile{
		Version: healthStateVersion,
		Nodes: map[string]persistedMemberHealth{
			"expired": {
				Failures:         2,
				BlacklistedUntil: time.Now().Add(-time.Minute),
				Monitor: monitor.PersistedHealthState{
					BlacklistedUntil: time.Now().Add(-time.Minute),
					InitialCheckDone: true,
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ConfigureHealthPersistence(path); err != nil {
		t.Fatalf("configure persistence: %v", err)
	}
	state := acquireSharedState("expired")
	if state.isBlacklisted(time.Now()) {
		t.Fatal("expired blacklist was restored as active")
	}
	state.mu.Lock()
	failures := state.failures
	state.mu.Unlock()
	if failures != 2 {
		t.Fatalf("non-expiring failure streak was lost: %d", failures)
	}
}
