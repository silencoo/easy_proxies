package boxmgr

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
)

func TestReloadBuildFailureLeavesExistingListenerRunning(t *testing.T) {
	listenPort := findManagerTestPort(t)
	cfg := &config.Config{
		Mode: "multi-port",
		MultiPort: config.MultiPortConfig{
			Address:        "127.0.0.1",
			BasePort:       listenPort,
			PortReuseDelay: time.Hour,
		},
		Nodes: []config.NodeConfig{{
			Name: "working-config",
			URI:  "socks5://127.0.0.1:9#working-config",
		}},
	}
	cfg.SetFilePath(filepath.Join(t.TempDir(), "config.yaml"))
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize config: %v", err)
	}
	// This test exercises listener handoff, not external probe availability.
	cfg.SubscriptionRefresh.MinAvailableNodes = 0

	manager := New(cfg, monitor.Config{Enabled: false})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer manager.Close()
	assertListenerAccepts(t, listenPort)

	replacement := *cfg
	replacement.Nodes = []config.NodeConfig{{
		Name: "invalid-config",
		URI:  "unsupported://example.invalid#invalid-config",
	}}
	replacement.SetFilePath(cfg.FilePath())
	if err := manager.ReloadWithPortMap(&replacement, manager.CurrentPortMap()); err == nil {
		t.Fatal("expected invalid replacement to fail")
	}

	assertListenerAccepts(t, listenPort)
}

func findManagerTestPort(t *testing.T) uint16 {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return uint16(port)
}

func assertListenerAccepts(t *testing.T, port uint16) {
	t.Helper()
	address := net.JoinHostPort("127.0.0.1", fmt.Sprint(port))
	deadline := time.Now().Add(3 * time.Second)
	for {
		connection, err := net.DialTimeout("tcp", address, 250*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener %s did not accept connections: %v", address, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
