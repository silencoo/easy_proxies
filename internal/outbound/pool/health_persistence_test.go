package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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
	state.recordFailure(errors.New("first failure"), 3, time.Hour, time.Minute)
	blacklistSharedMember("stable-node", time.Hour)
	state.recordFailure(errors.New("second failure"), 3, time.Hour, time.Minute)
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

func TestTransientCooldownPersistsAcrossRestart(t *testing.T) {
	ResetSharedStateStore()
	resetHealthPersistenceForTest()
	t.Cleanup(func() {
		ResetSharedStateStore()
		resetHealthPersistenceForTest()
	})

	path := filepath.Join(t.TempDir(), "health-state.yaml")
	if err := ConfigureHealthPersistence(path); err != nil {
		t.Fatal(err)
	}
	state := acquireSharedState("cooling")
	decision := state.recordFailure(context.DeadlineExceeded, 3, time.Hour, 10*time.Minute)
	if !decision.Cooldown {
		t.Fatalf("expected cooldown decision: %#v", decision)
	}
	if err := PersistHealthStateNow(); err != nil {
		t.Fatal(err)
	}

	ResetSharedStateStore()
	resetHealthPersistenceForTest()
	if err := ConfigureHealthPersistence(path); err != nil {
		t.Fatal(err)
	}
	restored := acquireSharedState("cooling")
	if !restored.isCoolingDown(time.Now()) || restored.isBlacklisted(time.Now()) {
		t.Fatal("transient cooldown was not restored independently")
	}
}

func TestConcurrentFlushCannotOverwriteNewerSnapshot(t *testing.T) {
	ResetSharedStateStore()
	resetHealthPersistenceForTest()
	t.Cleanup(func() {
		ResetSharedStateStore()
		resetHealthPersistenceForTest()
	})

	path := filepath.Join(t.TempDir(), "health-state.yaml")
	if err := ConfigureHealthPersistence(path); err != nil {
		t.Fatal(err)
	}
	originalWriter := writeHealthStateFile
	defer func() { writeHealthStateFile = originalWriter }()
	firstWriteStarted := make(chan struct{})
	secondWriteStarted := make(chan struct{})
	releaseFirstWrite := make(chan struct{})
	var calls atomic.Int32
	writeHealthStateFile = func(path string, data []byte, mode os.FileMode) error {
		switch calls.Add(1) {
		case 1:
			close(firstWriteStarted)
			<-releaseFirstWrite
		case 2:
			close(secondWriteStarted)
		}
		return originalWriter(path, data, mode)
	}

	healthPersistence.mu.Lock()
	healthPersistence.records["node"] = persistedMemberHealth{Failures: 1}
	healthPersistence.dirty = true
	healthPersistence.mu.Unlock()
	firstDone := make(chan error, 1)
	go func() { firstDone <- FlushHealthState() }()
	<-firstWriteStarted

	// The first flush has already captured its old snapshot. Publish a newer
	// record while its physical write is deliberately stalled.
	healthPersistence.mu.Lock()
	healthPersistence.records["node"] = persistedMemberHealth{Failures: 2}
	healthPersistence.dirty = true
	healthPersistence.mu.Unlock()
	secondDone := make(chan error, 1)
	go func() { secondDone <- FlushHealthState() }()

	secondEnteredEarly := false
	select {
	case <-secondWriteStarted:
		secondEnteredEarly = true
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirstWrite)
	if err := <-firstDone; err != nil {
		t.Fatalf("first flush: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if secondEnteredEarly {
		t.Fatal("newer disk write overtook an older in-flight flush")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var persisted persistedHealthFile
	if err := yaml.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if got := persisted.Nodes["node"].Failures; got != 2 {
		t.Fatalf("older flush overwrote newer snapshot: failures=%d", got)
	}
}

func TestConfigureHealthPersistenceKeepsStoreDuringPathSwitch(t *testing.T) {
	ResetSharedStateStore()
	resetHealthPersistenceForTest()
	originalReader := readHealthStateFile
	originalWriter := writeHealthStateFile
	t.Cleanup(func() {
		readHealthStateFile = originalReader
		writeHealthStateFile = originalWriter
		ResetSharedStateStore()
		resetHealthPersistenceForTest()
	})

	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old-health.yaml")
	newPath := filepath.Join(dir, "new-health.yaml")
	if err := ConfigureHealthPersistence(oldPath); err != nil {
		t.Fatal(err)
	}
	newData, err := yaml.Marshal(persistedHealthFile{
		Version: healthStateVersion,
		Nodes: map[string]persistedMemberHealth{
			"node": {Failures: 10},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newPath, newData, 0o600); err != nil {
		t.Fatal(err)
	}

	healthPersistence.mu.Lock()
	healthPersistence.records["node"] = persistedMemberHealth{Failures: 1}
	healthPersistence.dirty = true
	healthPersistence.mu.Unlock()

	var oldWriteComplete atomic.Bool
	writeHealthStateFile = func(path string, data []byte, mode os.FileMode) error {
		err := originalWriter(path, data, mode)
		if path == oldPath && err == nil {
			oldWriteComplete.Store(true)
		}
		return err
	}
	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	var readOnce sync.Once
	readHealthStateFile = func(path string) ([]byte, error) {
		if path == newPath {
			readOnce.Do(func() { close(readStarted) })
			<-releaseRead
		}
		return originalReader(path)
	}

	configureDone := make(chan error, 1)
	go func() { configureDone <- ConfigureHealthPersistence(newPath) }()
	<-readStarted
	if !oldWriteComplete.Load() {
		t.Fatal("new path was read before pending old-path state reached disk")
	}
	storeDone := make(chan struct{})
	go func() {
		storeMemberHealth("node", persistedMemberHealth{Failures: 22})
		close(storeDone)
	}()
	close(releaseRead)
	if err := <-configureDone; err != nil {
		t.Fatalf("switch persistence path: %v", err)
	}
	<-storeDone

	if err := FlushHealthState(); err != nil {
		t.Fatal(err)
	}
	readHealthStateFile = originalReader
	writeHealthStateFile = originalWriter
	if got := readPersistedFailures(t, oldPath, "node"); got != 1 {
		t.Fatalf("old path did not receive its pending snapshot: failures=%d", got)
	}
	if got := readPersistedFailures(t, newPath, "node"); got != 22 {
		t.Fatalf("store during path switch was lost: failures=%d", got)
	}
}

func TestConfigureHealthPersistenceFailureKeepsOldStateAndConcurrentStore(t *testing.T) {
	ResetSharedStateStore()
	resetHealthPersistenceForTest()
	originalReader := readHealthStateFile
	originalWriter := writeHealthStateFile
	t.Cleanup(func() {
		readHealthStateFile = originalReader
		writeHealthStateFile = originalWriter
		ResetSharedStateStore()
		resetHealthPersistenceForTest()
	})

	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old-health.yaml")
	newPath := filepath.Join(dir, "unreadable-health.yaml")
	if err := ConfigureHealthPersistence(oldPath); err != nil {
		t.Fatal(err)
	}
	healthPersistence.mu.Lock()
	healthPersistence.records["node"] = persistedMemberHealth{Failures: 3}
	healthPersistence.dirty = true
	healthPersistence.mu.Unlock()

	var oldWriteComplete atomic.Bool
	writeHealthStateFile = func(path string, data []byte, mode os.FileMode) error {
		err := originalWriter(path, data, mode)
		if path == oldPath && err == nil {
			oldWriteComplete.Store(true)
		}
		return err
	}
	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	wantErr := errors.New("injected new-path read failure")
	readHealthStateFile = func(path string) ([]byte, error) {
		if path != newPath {
			return originalReader(path)
		}
		close(readStarted)
		<-releaseRead
		return nil, wantErr
	}

	configureDone := make(chan error, 1)
	go func() { configureDone <- ConfigureHealthPersistence(newPath) }()
	<-readStarted
	if !oldWriteComplete.Load() {
		t.Fatal("new path was read before pending old-path state reached disk")
	}
	storeDone := make(chan struct{})
	go func() {
		storeMemberHealth("node", persistedMemberHealth{Failures: 4})
		close(storeDone)
	}()
	close(releaseRead)
	if err := <-configureDone; !errors.Is(err, wantErr) {
		t.Fatalf("switch error = %v, want %v", err, wantErr)
	}
	<-storeDone

	healthPersistence.mu.Lock()
	gotPath := healthPersistence.path
	healthPersistence.mu.Unlock()
	if gotPath != oldPath {
		t.Fatalf("failed switch changed active path: got %q want %q", gotPath, oldPath)
	}
	if record, ok := restoredMemberHealth("node"); !ok || record.Failures != 4 {
		t.Fatalf("failed switch did not preserve usable old state: record=%#v ok=%v", record, ok)
	}

	if err := FlushHealthState(); err != nil {
		t.Fatal(err)
	}
	readHealthStateFile = originalReader
	writeHealthStateFile = originalWriter
	if got := readPersistedFailures(t, oldPath, "node"); got != 4 {
		t.Fatalf("concurrent store was not flushed to the old path: failures=%d", got)
	}
}

func TestConfigureHealthPersistenceOldFlushFailureDoesNotSwitch(t *testing.T) {
	ResetSharedStateStore()
	resetHealthPersistenceForTest()
	originalReader := readHealthStateFile
	originalWriter := writeHealthStateFile
	t.Cleanup(func() {
		readHealthStateFile = originalReader
		writeHealthStateFile = originalWriter
		ResetSharedStateStore()
		resetHealthPersistenceForTest()
	})

	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old-health.yaml")
	newPath := filepath.Join(dir, "new-health.yaml")
	if err := ConfigureHealthPersistence(oldPath); err != nil {
		t.Fatal(err)
	}
	healthPersistence.mu.Lock()
	healthPersistence.records["node"] = persistedMemberHealth{Failures: 7}
	healthPersistence.dirty = true
	healthPersistence.mu.Unlock()

	wantErr := errors.New("injected old-path write failure")
	var newPathRead atomic.Bool
	writeHealthStateFile = func(path string, _ []byte, _ os.FileMode) error {
		if path == oldPath {
			return wantErr
		}
		return nil
	}
	readHealthStateFile = func(path string) ([]byte, error) {
		if path == newPath {
			newPathRead.Store(true)
		}
		return originalReader(path)
	}

	if err := ConfigureHealthPersistence(newPath); !errors.Is(err, wantErr) {
		t.Fatalf("switch error = %v, want %v", err, wantErr)
	}
	if newPathRead.Load() {
		t.Fatal("new path was read after the required old-path flush failed")
	}
	healthPersistence.mu.Lock()
	gotPath := healthPersistence.path
	dirty := healthPersistence.dirty
	healthPersistence.mu.Unlock()
	if gotPath != oldPath || !dirty {
		t.Fatalf("old state was not retained for retry: path=%q dirty=%v", gotPath, dirty)
	}
	if record, ok := restoredMemberHealth("node"); !ok || record.Failures != 7 {
		t.Fatalf("old in-memory record was lost: record=%#v ok=%v", record, ok)
	}

	readHealthStateFile = originalReader
	writeHealthStateFile = originalWriter
	if err := FlushHealthState(); err != nil {
		t.Fatalf("retry old-path flush: %v", err)
	}
	if got := readPersistedFailures(t, oldPath, "node"); got != 7 {
		t.Fatalf("retained old state was not retryable: failures=%d", got)
	}
}

func readPersistedFailures(t *testing.T, path, tag string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state persistedHealthFile
	if err := yaml.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state.Nodes[tag].Failures
}
