package boxmgr

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
)

func TestManagerStartsManagementOnlyWithoutNodes(t *testing.T) {
	managementPort := findManagerTestPort(t)
	cfg := newManagementOnlyTestConfig(t, managementPort, "www.apple.com:80")
	manager := New(cfg, monitor.Config{
		Enabled:     true,
		Listen:      cfg.Management.Listen,
		ProbeTarget: cfg.Management.ProbeTarget,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start management-only manager: %v", err)
	}
	defer manager.Close()

	assertManagementUIResponds(t, cfg.Management.Listen)
	manager.mu.RLock()
	managementOnly := manager.managementOnly
	currentBox := manager.currentBox
	manager.mu.RUnlock()
	if !managementOnly || currentBox != nil {
		t.Fatalf("unexpected runtime state: managementOnly=%v currentBox=%v", managementOnly, currentBox != nil)
	}
	snapshot, revision := manager.ConfigSnapshot()
	if snapshot == nil || len(snapshot.Nodes) != 0 || revision != 1 {
		t.Fatalf("unexpected config snapshot: nodes=%d revision=%d", len(snapshot.Nodes), revision)
	}
}

func TestManagementOnlyManagerPromotesToProxyRuntime(t *testing.T) {
	probeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer probeServer.Close()

	managementPort := findManagerTestPort(t)
	proxyPort := findManagerTestPort(t)
	for proxyPort == managementPort {
		proxyPort = findManagerTestPort(t)
	}
	cfg := newManagementOnlyTestConfig(t, managementPort, probeServer.URL)
	manager := New(cfg, monitor.Config{
		Enabled:     true,
		Listen:      cfg.Management.Listen,
		ProbeTarget: cfg.Management.ProbeTarget,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start management-only manager: %v", err)
	}
	defer manager.Close()
	assertManagementUIResponds(t, cfg.Management.Listen)

	candidate, revision := manager.ConfigSnapshot()
	candidate.Listener.Port = proxyPort
	candidate.Nodes = []config.NodeConfig{{
		Name: "first-node",
		URI:  "socks5://" + startTestSOCKS5Proxy(t) + "#first-node",
	}}
	if err := manager.CommitConfig(context.Background(), revision, candidate, nil); err != nil {
		t.Fatalf("promote management-only manager: %v", err)
	}

	manager.mu.RLock()
	managementOnly := manager.managementOnly
	currentBox := manager.currentBox
	manager.mu.RUnlock()
	if managementOnly || currentBox == nil {
		t.Fatalf("proxy runtime was not published: managementOnly=%v currentBox=%v", managementOnly, currentBox != nil)
	}
	assertListenerAccepts(t, proxyPort)
	assertManagementUIResponds(t, cfg.Management.Listen)
	_, newRevision := manager.ConfigSnapshot()
	if newRevision != revision+1 {
		t.Fatalf("config revision = %d, want %d", newRevision, revision+1)
	}
}

func TestFailedFirstRuntimeKeepsManagementOnlyState(t *testing.T) {
	managementPort := findManagerTestPort(t)
	cfg := newManagementOnlyTestConfig(t, managementPort, "www.apple.com:80")
	manager := New(cfg, monitor.Config{
		Enabled:     true,
		Listen:      cfg.Management.Listen,
		ProbeTarget: cfg.Management.ProbeTarget,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start management-only manager: %v", err)
	}
	defer manager.Close()

	candidate, revision := manager.ConfigSnapshot()
	// Deliberately collide with the live management listener after sing-box has
	// registered the candidate node with the monitor manager.
	candidate.Listener.Port = managementPort
	candidate.Nodes = []config.NodeConfig{{
		Name: "rejected-node",
		URI:  "socks5://" + startTestSOCKS5Proxy(t) + "#rejected-node",
	}}
	if err := manager.CommitConfig(context.Background(), revision, candidate, nil); err == nil {
		t.Fatal("port-conflicting first runtime unexpectedly started")
	}

	manager.mu.RLock()
	managementOnly := manager.managementOnly
	currentBox := manager.currentBox
	manager.mu.RUnlock()
	if !managementOnly || currentBox != nil {
		t.Fatalf("failed runtime changed state: managementOnly=%v currentBox=%v", managementOnly, currentBox != nil)
	}
	if snapshots := manager.MonitorManager().Snapshot(); len(snapshots) != 0 {
		t.Fatalf("failed runtime left %d candidate nodes in monitoring", len(snapshots))
	}
	_, newRevision := manager.ConfigSnapshot()
	if newRevision != revision {
		t.Fatalf("failed runtime changed revision from %d to %d", revision, newRevision)
	}
	assertManagementUIResponds(t, cfg.Management.Listen)
}

func newManagementOnlyTestConfig(t *testing.T, managementPort uint16, probeTarget string) *config.Config {
	t.Helper()
	enabled := true
	cfg := &config.Config{
		Mode: "pool",
		Listener: config.ListenerConfig{
			Address: "127.0.0.1",
			Port:    findManagerTestPort(t),
		},
		Management: config.ManagementConfig{
			Enabled:     &enabled,
			Listen:      fmt.Sprintf("127.0.0.1:%d", managementPort),
			ProbeTarget: probeTarget,
		},
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			Enabled:            true,
			MinAvailableNodes:  1,
			HealthCheckTimeout: 2 * time.Second,
		},
	}
	cfg.SetFilePath(filepath.Join(t.TempDir(), "config.yaml"))
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize management-only config: %v", err)
	}
	return cfg
}

func assertManagementUIResponds(t *testing.T, listen string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		response, err := client.Get("http://" + listen + "/")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
			lastErr = fmt.Errorf("status %s", response.Status)
		} else {
			lastErr = err
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("management UI %s did not respond: %v", listen, lastErr)
}
