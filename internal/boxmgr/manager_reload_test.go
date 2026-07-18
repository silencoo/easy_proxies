package boxmgr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/geoip"
	"easy_proxies/internal/monitor"
)

func TestNodeLevelReloadKeepsGeoCredentialsUntilCommit(t *testing.T) {
	listenPort := findManagerTestPort(t)
	geoPort := findManagerTestPort(t)
	for geoPort == listenPort {
		geoPort = findManagerTestPort(t)
	}
	cfg := &config.Config{
		Mode: "multi-port",
		MultiPort: config.MultiPortConfig{
			Address:  "127.0.0.1",
			BasePort: listenPort,
			Username: "old-user",
			Password: "old-password",
		},
		Nodes: []config.NodeConfig{{
			Name: "credential-node",
			URI:  "socks5://127.0.0.1:9#credential-node",
			Port: listenPort,
		}},
	}
	cfg.SetFilePath(filepath.Join(t.TempDir(), "config.yaml"))
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize config: %v", err)
	}
	cfg.SubscriptionRefresh.MinAvailableNodes = 0

	manager := New(cfg, monitor.Config{Enabled: false})
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer manager.Close()

	routerCfg := geoip.RouterConfig{
		Listen:   "127.0.0.1",
		Port:     geoPort,
		Username: cfg.MultiPort.Username,
		Password: cfg.MultiPort.Password,
	}
	router := geoip.NewRouter(routerCfg, nil)
	if err := router.Start(context.Background()); err != nil {
		t.Fatalf("start GeoIP router: %v", err)
	}
	manager.mu.Lock()
	manager.geoRouter = router
	manager.cfg.GeoIP = config.GeoIPConfig{Enabled: true, Listen: routerCfg.Listen, Port: routerCfg.Port}
	instance := manager.currentBox
	runtimeCtx := manager.runtimeCtx
	oldOptions := manager.runtimeOptions
	oldCfg := manager.cfg.Clone()
	manager.mu.Unlock()

	newCfg := oldCfg.Clone()
	newCfg.MultiPort.Username = "new-user"
	newCfg.MultiPort.Password = "new-password"
	if !canReloadNodesInPlace(oldCfg, newCfg) {
		t.Fatal("credential-only multi-port update unexpectedly requires full handoff")
	}

	reachedCommit := make(chan struct{})
	releaseCommit := make(chan struct{})
	manager.beforeRuntimeCommit = func() {
		close(reachedCommit)
		<-releaseCommit
	}
	defer func() { manager.beforeRuntimeCommit = nil }()
	operationCtx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		manager.reloadMu.Lock()
		result <- manager.reloadNodesInPlace(operationCtx, runtimeCtx, instance, oldCfg, oldOptions, newCfg)
		manager.reloadMu.Unlock()
	}()

	select {
	case <-reachedCommit:
	case <-time.After(5 * time.Second):
		cancel()
		close(releaseCommit)
		t.Fatal("node-level reload did not reach the final commit boundary")
	}
	if got := router.Config(); got.Username != routerCfg.Username || got.Password != routerCfg.Password {
		cancel()
		close(releaseCommit)
		t.Fatalf("uncommitted GeoIP credentials were published: %#v", got)
	}
	cancel()
	close(releaseCommit)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("reload error = %v, want context cancellation", err)
	}
	if got := router.Config(); got.Username != routerCfg.Username || got.Password != routerCfg.Password {
		t.Fatalf("failed reload changed GeoIP credentials: %#v", got)
	}
}

func TestNodeLevelReloadRejectsUnhealthyCandidateBeforeCutover(t *testing.T) {
	probeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer probeServer.Close()
	upstreamAddress := startTestSOCKS5Proxy(t)
	listenPort := findManagerTestPort(t)

	cfg := &config.Config{
		Mode: "multi-port",
		MultiPort: config.MultiPortConfig{
			Address:  "127.0.0.1",
			BasePort: listenPort,
		},
		Nodes: []config.NodeConfig{{
			Name: "healthy",
			URI:  "socks5://" + upstreamAddress + "#healthy",
			Port: listenPort,
		}},
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			MinAvailableNodes:  1,
			HealthCheckTimeout: 2 * time.Second,
		},
		Management: config.ManagementConfig{
			Listen:      "127.0.0.1:9091",
			ProbeTarget: probeServer.URL,
		},
	}
	cfg.SetFilePath(filepath.Join(t.TempDir(), "config.yaml"))
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize config: %v", err)
	}
	manager := New(cfg, monitor.Config{Enabled: false, ProbeTarget: probeServer.URL})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer manager.Close()

	existing, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprint(listenPort)), time.Second)
	if err != nil {
		t.Fatalf("open existing connection: %v", err)
	}
	defer existing.Close()

	closedProxyPort := findManagerTestPort(t)
	replacement := *cfg
	replacement.Nodes = []config.NodeConfig{{
		Name: "unhealthy",
		URI:  fmt.Sprintf("socks5://127.0.0.1:%d#unhealthy", closedProxyPort),
		Port: listenPort,
	}}
	replacement.SetFilePath(cfg.FilePath())
	if err := replacement.NormalizeWithPortMap(manager.CurrentPortMap()); err != nil {
		t.Fatalf("normalize replacement: %v", err)
	}
	err = manager.Reload(&replacement)
	if err == nil || !strings.Contains(err.Error(), "before cutover") {
		t.Fatalf("expected pre-cutover health rejection, got %v", err)
	}

	_ = existing.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := existing.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatalf("write greeting after rejected reload: %v", err)
	}
	response := make([]byte, 2)
	if _, err := io.ReadFull(existing, response); err != nil {
		t.Fatalf("rejected candidate interrupted old connection: %v", err)
	}
	if response[0] != 0x05 || response[1] != 0x00 {
		t.Fatalf("unexpected SOCKS greeting response: %v", response)
	}
	assertListenerAccepts(t, listenPort)
}

func TestNodeLevelReloadSynchronouslyRecordsCandidateHealth(t *testing.T) {
	probeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer probeServer.Close()
	healthyUpstream := startTestSOCKS5Proxy(t)
	firstPort := findManagerTestPort(t)
	secondPort := findManagerTestPort(t)
	for secondPort == firstPort {
		secondPort = findManagerTestPort(t)
	}

	cfg := &config.Config{
		Mode: "multi-port",
		MultiPort: config.MultiPortConfig{
			Address:  "127.0.0.1",
			BasePort: firstPort,
		},
		Nodes: []config.NodeConfig{{
			Name: "healthy",
			URI:  "socks5://" + healthyUpstream + "#healthy",
			Port: firstPort,
		}},
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			MinAvailableNodes:  1,
			HealthCheckTimeout: 2 * time.Second,
		},
		Management: config.ManagementConfig{
			Listen:      "127.0.0.1:9091",
			ProbeTarget: probeServer.URL,
		},
	}
	cfg.SetFilePath(filepath.Join(t.TempDir(), "config.yaml"))
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize config: %v", err)
	}

	manager := New(cfg, monitor.Config{Enabled: false, ProbeTarget: probeServer.URL})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer manager.Close()

	closedProxyPort := findManagerTestPort(t)
	replacement := *cfg
	replacement.Nodes = append(cloneNodes(cfg.Nodes), config.NodeConfig{
		Name: "unhealthy-added",
		URI:  fmt.Sprintf("socks5://127.0.0.1:%d#unhealthy-added", closedProxyPort),
		Port: secondPort,
	})
	replacement.SetFilePath(cfg.FilePath())
	if err := replacement.NormalizeWithPortMap(manager.CurrentPortMap()); err != nil {
		t.Fatalf("normalize replacement: %v", err)
	}
	if err := manager.Reload(&replacement); err != nil {
		t.Fatalf("node-level reload: %v", err)
	}

	var healthy, unhealthy *monitor.Snapshot
	for _, snapshot := range manager.MonitorManager().Snapshot() {
		snapshot := snapshot
		switch snapshot.Name {
		case "healthy":
			healthy = &snapshot
		case "unhealthy-added":
			unhealthy = &snapshot
		}
	}
	if healthy == nil || unhealthy == nil {
		t.Fatalf("missing monitor snapshots: healthy=%v unhealthy=%v", healthy != nil, unhealthy != nil)
	}
	if !healthy.InitialCheckDone || !healthy.Available {
		t.Fatalf("healthy node was not synchronously marked available: %+v", *healthy)
	}
	if !unhealthy.InitialCheckDone || unhealthy.Available {
		t.Fatalf("unhealthy node was not synchronously excluded: %+v", *unhealthy)
	}
}

func TestNodeLevelReloadKeepsUnchangedConnectionAndAddsListener(t *testing.T) {
	firstPort := findManagerTestPort(t)
	secondPort := findManagerTestPort(t)
	for secondPort == firstPort {
		secondPort = findManagerTestPort(t)
	}
	cfg := &config.Config{
		Mode: "multi-port",
		MultiPort: config.MultiPortConfig{
			Address:        "127.0.0.1",
			BasePort:       firstPort,
			PortReuseDelay: time.Hour,
		},
		Nodes: []config.NodeConfig{{
			Name: "unchanged",
			URI:  "socks5://127.0.0.1:9#unchanged",
			Port: firstPort,
		}},
	}
	cfg.SetFilePath(filepath.Join(t.TempDir(), "config.yaml"))
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize config: %v", err)
	}
	cfg.SubscriptionRefresh.MinAvailableNodes = 0
	cfg.SubscriptionRefresh.DrainTimeout = 50 * time.Millisecond

	manager := New(cfg, monitor.Config{Enabled: false})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start manager: %v", err)
	}
	defer manager.Close()

	address := net.JoinHostPort("127.0.0.1", fmt.Sprint(firstPort))
	existing, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatalf("open existing client connection: %v", err)
	}
	defer existing.Close()

	replacement := *cfg
	replacement.Nodes = append(cloneNodes(cfg.Nodes), config.NodeConfig{
		Name: "added",
		URI:  "socks5://127.0.0.1:10#added",
		Port: secondPort,
	})
	replacement.SetFilePath(cfg.FilePath())
	if err := replacement.NormalizeWithPortMap(manager.CurrentPortMap()); err != nil {
		t.Fatalf("normalize replacement: %v", err)
	}
	// This test exercises connection-preserving listener changes, not health
	// gating. Normalize applies the production default minimum of one.
	replacement.SubscriptionRefresh.MinAvailableNodes = 0
	if err := manager.Reload(&replacement); err != nil {
		t.Fatalf("node-level reload: %v", err)
	}

	// The connection was accepted by the pre-reload inbound. Completing a
	// SOCKS5 greeting after the reload proves the unchanged inbound and its
	// active connection were not closed by a whole-Box restart.
	_ = existing.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := existing.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatalf("write greeting on existing connection: %v", err)
	}
	response := make([]byte, 2)
	if _, err := io.ReadFull(existing, response); err != nil {
		t.Fatalf("existing connection was interrupted by reload: %v", err)
	}
	if response[0] != 0x05 || response[1] != 0x00 {
		t.Fatalf("unexpected SOCKS greeting response: %v", response)
	}

	assertListenerAccepts(t, firstPort)
	assertListenerAccepts(t, secondPort)
}

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

func startTestSOCKS5Proxy(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go handleTestSOCKS5Connection(connection)
		}
	}()
	return listener.Addr().String()
}

func handleTestSOCKS5Connection(client net.Conn) {
	defer client.Close()
	header := make([]byte, 2)
	if _, err := io.ReadFull(client, header); err != nil || header[0] != 0x05 {
		return
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(client, methods); err != nil {
		return
	}
	if _, err := client.Write([]byte{0x05, 0x00}); err != nil {
		return
	}
	request := make([]byte, 4)
	if _, err := io.ReadFull(client, request); err != nil || request[0] != 0x05 || request[1] != 0x01 {
		return
	}
	var host string
	switch request[3] {
	case 0x01:
		address := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(client, address); err != nil {
			return
		}
		host = net.IP(address).String()
	case 0x03:
		length := make([]byte, 1)
		if _, err := io.ReadFull(client, length); err != nil {
			return
		}
		address := make([]byte, int(length[0]))
		if _, err := io.ReadFull(client, address); err != nil {
			return
		}
		host = string(address)
	case 0x04:
		address := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(client, address); err != nil {
			return
		}
		host = net.IP(address).String()
	default:
		return
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(client, portBytes); err != nil {
		return
	}
	upstream, err := net.DialTimeout("tcp", net.JoinHostPort(host, fmt.Sprint(binary.BigEndian.Uint16(portBytes))), time.Second)
	if err != nil {
		_, _ = client.Write([]byte{0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer upstream.Close()
	if _, err := client.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}
	done := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(upstream, client)
		done <- struct{}{}
	}()
	_, _ = io.Copy(client, upstream)
	<-done
}
