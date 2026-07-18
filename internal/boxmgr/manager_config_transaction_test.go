package boxmgr

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
)

func TestManagerConfigSnapshotIsolatesCallerAndSnapshot(t *testing.T) {
	retryEnabled := true
	managementEnabled := false
	callerCfg := &config.Config{
		Nodes:         []config.NodeConfig{{Name: "owned", URI: "socks5://127.0.0.1:1080"}},
		Subscriptions: []string{"https://example.test/subscription"},
		Pool:          config.PoolConfig{RetryEnabled: &retryEnabled},
		Management:    config.ManagementConfig{Enabled: &managementEnabled},
	}
	manager := New(callerCfg, monitor.Config{})

	callerCfg.Nodes[0].Name = "caller-mutated"
	callerCfg.Subscriptions[0] = "https://caller.test/subscription"
	*callerCfg.Pool.RetryEnabled = false
	*callerCfg.Management.Enabled = true

	first, revision := manager.ConfigSnapshot()
	if revision != 1 {
		t.Fatalf("initial revision = %d, want 1", revision)
	}
	if first.Nodes[0].Name != "owned" || first.Subscriptions[0] != "https://example.test/subscription" {
		t.Fatalf("manager retained caller-owned storage: %+v", first)
	}
	if !*first.Pool.RetryEnabled || *first.Management.Enabled {
		t.Fatal("manager retained caller-owned optional boolean pointers")
	}

	first.Nodes[0].Name = "snapshot-mutated"
	first.Subscriptions[0] = "https://snapshot.test/subscription"
	*first.Pool.RetryEnabled = false
	second, secondRevision := manager.ConfigSnapshot()
	if secondRevision != revision {
		t.Fatalf("snapshot read changed revision: %d -> %d", revision, secondRevision)
	}
	if second.Nodes[0].Name != "owned" || second.Subscriptions[0] != "https://example.test/subscription" {
		t.Fatalf("snapshot mutation reached manager state: %+v", second)
	}
	if !*second.Pool.RetryEnabled {
		t.Fatal("snapshot pointer mutation reached manager state")
	}
}

func TestCommitConfigRejectsRevisionConflictBeforePersistence(t *testing.T) {
	manager := New(transactionTestConfig(t), monitor.Config{})
	candidate, revision := manager.ConfigSnapshot()
	persistCalled := false
	err := manager.CommitConfig(context.Background(), revision-1, candidate, func(*config.Config) (func() error, error) {
		persistCalled = true
		return nil, nil
	})
	if !errors.Is(err, ErrConfigRevisionConflict) {
		t.Fatalf("CommitConfig error = %v, want revision conflict", err)
	}
	if persistCalled {
		t.Fatal("stale candidate reached persistence callback")
	}
	_, currentRevision := manager.ConfigSnapshot()
	if currentRevision != revision {
		t.Fatalf("conflict changed revision: got %d, want %d", currentRevision, revision)
	}
}

func TestCommitConfigRollsBackPersistenceFailure(t *testing.T) {
	manager := New(transactionTestConfig(t), monitor.Config{})
	candidate, revision := manager.ConfigSnapshot()
	persistErr := errors.New("simulated persistence failure")
	rollbackCalled := false
	err := manager.CommitConfig(context.Background(), revision, candidate, func(*config.Config) (func() error, error) {
		return func() error {
			rollbackCalled = true
			return nil
		}, persistErr
	})
	if !errors.Is(err, persistErr) {
		t.Fatalf("CommitConfig error = %v, want %v", err, persistErr)
	}
	if !rollbackCalled {
		t.Fatal("persistence failure did not invoke rollback")
	}
	_, currentRevision := manager.ConfigSnapshot()
	if currentRevision != revision {
		t.Fatalf("persistence failure changed revision: got %d, want %d", currentRevision, revision)
	}
}

func TestCommitConfigRollsBackReloadFailure(t *testing.T) {
	manager := New(transactionTestConfig(t), monitor.Config{})
	candidate, revision := manager.ConfigSnapshot()
	candidate.ExternalIP = "203.0.113.10"
	rollbackCalled := false
	err := manager.CommitConfig(context.Background(), revision, candidate, func(*config.Config) (func() error, error) {
		return func() error {
			rollbackCalled = true
			return nil
		}, nil
	})
	if err == nil || err.Error() != "manager not started" {
		t.Fatalf("CommitConfig error = %v, want manager-not-started failure", err)
	}
	if !rollbackCalled {
		t.Fatal("reload failure did not invoke persistence rollback")
	}
	snapshot, currentRevision := manager.ConfigSnapshot()
	if currentRevision != revision {
		t.Fatalf("reload failure changed revision: got %d, want %d", currentRevision, revision)
	}
	if snapshot.ExternalIP == candidate.ExternalIP {
		t.Fatal("failed reload adopted candidate configuration")
	}
}

func TestSuccessfulReloadAndCommitAdoptOneOwnedRevision(t *testing.T) {
	cfg := transactionTestConfig(t)
	probeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer probeServer.Close()
	cfg.Nodes[0].URI = "socks5://" + startTestSOCKS5Proxy(t) + "#transaction-node"
	cfg.Management.ProbeTarget = probeServer.URL
	cfg.SubscriptionRefresh.HealthCheckTimeout = 2 * time.Second
	manager := New(cfg, monitor.Config{Enabled: false, ProbeTarget: probeServer.URL})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer manager.Close()

	candidate, initialRevision := manager.ConfigSnapshot()
	candidate.ExternalIP = "203.0.113.20"
	if err := manager.Reload(candidate); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	candidate.ExternalIP = "198.51.100.20"
	afterReload, reloadRevision := manager.ConfigSnapshot()
	if reloadRevision != initialRevision+1 {
		t.Fatalf("reload revision = %d, want %d", reloadRevision, initialRevision+1)
	}
	if afterReload.ExternalIP != "203.0.113.20" {
		t.Fatalf("caller mutation reached reloaded config: %q", afterReload.ExternalIP)
	}

	transactionCandidate := afterReload.Clone()
	transactionCandidate.ExternalIP = "203.0.113.30"
	var persisted *config.Config
	rollbackCalled := false
	if err := manager.CommitConfig(context.Background(), reloadRevision, transactionCandidate, func(received *config.Config) (func() error, error) {
		persisted = received
		return func() error {
			rollbackCalled = true
			return nil
		}, nil
	}); err != nil {
		t.Fatalf("CommitConfig: %v", err)
	}
	transactionCandidate.ExternalIP = "198.51.100.30"
	persisted.ExternalIP = "192.0.2.30"
	afterCommit, commitRevision := manager.ConfigSnapshot()
	if commitRevision != reloadRevision+1 {
		t.Fatalf("commit revision = %d, want %d", commitRevision, reloadRevision+1)
	}
	if afterCommit.ExternalIP != "203.0.113.30" {
		t.Fatalf("candidate or persistence callback retained manager storage: %q", afterCommit.ExternalIP)
	}
	if rollbackCalled {
		t.Fatal("successful commit invoked rollback")
	}
}

func TestCommitConfigCancellationBeforeRuntimePublishRollsBack(t *testing.T) {
	cfg := transactionTestConfig(t)
	probeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer probeServer.Close()
	cfg.Nodes[0].URI = "socks5://" + startTestSOCKS5Proxy(t) + "#transaction-cancel"
	cfg.Management.ProbeTarget = probeServer.URL
	cfg.SubscriptionRefresh.HealthCheckTimeout = 2 * time.Second
	manager := New(cfg, monitor.Config{Enabled: false, ProbeTarget: probeServer.URL})
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer manager.Close()

	candidate, revision := manager.ConfigSnapshot()
	previousExternalIP := candidate.ExternalIP
	candidate.ExternalIP = "203.0.113.91"
	reachedCommit := make(chan struct{})
	releaseCommit := make(chan struct{})
	manager.beforeRuntimeCommit = func() {
		close(reachedCommit)
		<-releaseCommit
	}
	defer func() { manager.beforeRuntimeCommit = nil }()

	ctx, cancel := context.WithCancel(context.Background())
	rollbackCalled := false
	result := make(chan error, 1)
	go func() {
		result <- manager.CommitConfig(ctx, revision, candidate, func(*config.Config) (func() error, error) {
			return func() error {
				rollbackCalled = true
				return nil
			}, nil
		})
	}()
	select {
	case <-reachedCommit:
	case <-time.After(5 * time.Second):
		t.Fatal("reload did not reach its final publish boundary")
	}
	cancel()
	close(releaseCommit)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("CommitConfig error=%v, want context cancellation", err)
	}
	if !rollbackCalled {
		t.Fatal("canceled runtime transaction did not roll back persistence")
	}
	committed, currentRevision := manager.ConfigSnapshot()
	if currentRevision != revision || committed.ExternalIP != previousExternalIP {
		t.Fatalf("canceled runtime transaction was published: revision=%d external_ip=%q", currentRevision, committed.ExternalIP)
	}
}

func TestNodeCRUDAndTriggerReloadShareLockWithoutDeadlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("mode: pool\nnodes: []\n"), 0o600); err != nil {
		t.Fatalf("write config fixture: %v", err)
	}
	cfg := &config.Config{Mode: "pool"}
	cfg.SetFilePath(path)
	manager := New(cfg, monitor.Config{})

	start := make(chan struct{})
	createDone := make(chan error, 1)
	reloadDone := make(chan error, 1)
	go func() {
		<-start
		_, err := manager.CreateNode(context.Background(), config.NodeConfig{
			Name: "created",
			URI:  "socks5://127.0.0.1:1080#created",
		})
		createDone <- err
	}()
	go func() {
		<-start
		reloadDone <- manager.TriggerReload(context.Background())
	}()
	close(start)

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	select {
	case err := <-createDone:
		if err != nil {
			t.Fatalf("concurrent node creation failed: %v", err)
		}
	case <-deadline.C:
		t.Fatal("node CRUD and reload deadlocked")
	}
	select {
	case err := <-reloadDone:
		if err == nil {
			t.Fatal("reload unexpectedly succeeded without a running manager")
		}
	case <-deadline.C:
		t.Fatal("node CRUD and reload deadlocked")
	}
	snapshot, revision := manager.ConfigSnapshot()
	if revision != 2 {
		t.Fatalf("saved node revision = %d, want 2", revision)
	}
	if len(snapshot.Nodes) != 1 || snapshot.Nodes[0].Name != "created" {
		t.Fatalf("saved node missing from manager snapshot: %+v", snapshot.Nodes)
	}
}

func transactionTestConfig(t *testing.T) *config.Config {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.Config{
		Mode: "pool",
		Listener: config.ListenerConfig{
			Address: "127.0.0.1",
			Port:    findManagerTestPort(t),
		},
		Pool: config.PoolConfig{
			HealthStateFile: filepath.Join(dir, "health-state.yaml"),
		},
		Nodes: []config.NodeConfig{{
			Name:   "transaction-node",
			URI:    "socks5://127.0.0.1:9#transaction-node",
			Source: config.NodeSourceInline,
		}},
	}
	cfg.SetFilePath(filepath.Join(dir, "config.yaml"))
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize transaction config: %v", err)
	}
	return cfg
}
