package boxmgr

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"

	"easy_proxies/internal/config"
	"easy_proxies/internal/geoip"
	"easy_proxies/internal/monitor"
)

func listenerPort(t *testing.T, listener net.Listener) uint16 {
	t.Helper()
	_, text, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(text)
	if err != nil || port < 1 || port > 65535 {
		t.Fatalf("invalid listener port %q", text)
	}
	return uint16(port)
}

func TestEnsureGeoIPRouterPreservesOldListenerWhenCandidateBindFails(t *testing.T) {
	oldProbe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	oldPort := listenerPort(t, oldProbe)
	_ = oldProbe.Close()
	oldRouter := geoip.NewRouter(geoip.RouterConfig{Listen: "127.0.0.1", Port: oldPort}, nil)
	if err := oldRouter.Start(context.Background()); err != nil {
		t.Fatalf("start old router: %v", err)
	}
	defer oldRouter.Stop()

	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	candidatePort := listenerPort(t, occupied)
	cfg := &config.Config{GeoIP: config.GeoIPConfig{Enabled: true, Listen: "127.0.0.1", Port: candidatePort}}
	manager := New(cfg, monitor.Config{})
	manager.geoRouter = oldRouter

	if err := manager.ensureGeoIPRouter(context.Background(), cfg.Clone()); err == nil {
		t.Fatal("occupied candidate listener unexpectedly started")
	}
	if manager.geoRouter != oldRouter || !oldRouter.IsRunning() {
		t.Fatal("failed candidate replaced or stopped the live GeoIP router")
	}
}

func TestEnsureGeoIPRouterUpdatesSameEndpointInPlace(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listenerPort(t, probe)
	_ = probe.Close()
	router := geoip.NewRouter(geoip.RouterConfig{Listen: "127.0.0.1", Port: port, Username: "old", Password: "old"}, nil)
	if err := router.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer router.Stop()
	cfg := &config.Config{GeoIP: config.GeoIPConfig{Enabled: true, Listen: "127.0.0.1", Port: port}}
	cfg.Listener.Username = "new"
	cfg.Listener.Password = "secret"
	manager := New(cfg, monitor.Config{})
	manager.geoRouter = router

	if err := manager.ensureGeoIPRouter(context.Background(), cfg); err != nil {
		t.Fatalf("update router: %v", err)
	}
	if manager.geoRouter != router {
		t.Fatal("same endpoint was rebound instead of updated in place")
	}
	got := router.Config()
	if got.Username != "new" || got.Password != "secret" {
		t.Fatalf("credentials not updated: %#v", got)
	}
}

func TestSelectGeoIPRouterPortUsesConfiguredOrDefaultPort(t *testing.T) {
	tests := []struct {
		name string
		port uint16
		want uint16
	}{
		{name: "configured", port: 4321, want: 4321},
		{name: "default", port: 0, want: defaultGeoIPRouterPort},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := selectGeoIPRouterPort(&config.Config{GeoIP: config.GeoIPConfig{Port: test.port}})
			if err != nil {
				t.Fatalf("selectGeoIPRouterPort() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("selectGeoIPRouterPort() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestSelectGeoIPRouterPortAvoidsEveryConfiguredListener(t *testing.T) {
	enabled := true
	cfg := &config.Config{
		Mode:     "hybrid",
		Listener: config.ListenerConfig{Port: 1221},
		Nodes: []config.NodeConfig{
			{Name: "first", Port: 1222},
			{Name: "second", Port: 1223},
		},
		Management: config.ManagementConfig{
			Enabled: &enabled,
			Listen:  "[::1]:1224",
		},
	}

	got, err := selectGeoIPRouterPort(cfg)
	if err != nil {
		t.Fatalf("selectGeoIPRouterPort() error = %v", err)
	}
	if got != 1225 {
		t.Fatalf("selectGeoIPRouterPort() = %d, want 1225", got)
	}
}

func TestSelectGeoIPRouterPortIgnoresInactiveDedicatedAndManagementListeners(t *testing.T) {
	disabled := false
	cfg := &config.Config{
		Mode:  "pool",
		GeoIP: config.GeoIPConfig{Port: 24000},
		Nodes: []config.NodeConfig{{Name: "inactive dedicated port", Port: 24000}},
		Management: config.ManagementConfig{
			Enabled: &disabled,
			Listen:  "127.0.0.1:24000",
		},
	}

	got, err := selectGeoIPRouterPort(cfg)
	if err != nil {
		t.Fatalf("selectGeoIPRouterPort() error = %v", err)
	}
	if got != 24000 {
		t.Fatalf("selectGeoIPRouterPort() = %d, want 24000", got)
	}
}

func TestSelectGeoIPRouterPortRejectsInvalidManagementListen(t *testing.T) {
	enabled := true
	_, err := selectGeoIPRouterPort(&config.Config{
		Management: config.ManagementConfig{Enabled: &enabled, Listen: "127.0.0.1"},
	})
	if err == nil {
		t.Fatal("selectGeoIPRouterPort() error = nil, want invalid management listen error")
	}
}

func TestChooseGeoIPRouterPortHandles65535WithoutOverflow(t *testing.T) {
	reserved := map[uint16]struct{}{65535: {}}
	got, err := chooseGeoIPRouterPort(65535, reserved)
	if err != nil {
		t.Fatalf("chooseGeoIPRouterPort() error = %v", err)
	}
	if got != 1 {
		t.Fatalf("chooseGeoIPRouterPort() = %d, want 1", got)
	}
}

func TestChooseGeoIPRouterPortFailsAfterExhaustingValidRange(t *testing.T) {
	reserved := make(map[uint16]struct{}, maxTCPPort)
	for port := 1; port <= maxTCPPort; port++ {
		reserved[uint16(port)] = struct{}{}
	}

	got, err := chooseGeoIPRouterPort(65535, reserved)
	if got != 0 {
		t.Fatalf("chooseGeoIPRouterPort() = %d, want 0 on exhaustion", got)
	}
	if !errors.Is(err, errNoAvailableGeoIPRouterPort) {
		t.Fatalf("chooseGeoIPRouterPort() error = %v, want %v", err, errNoAvailableGeoIPRouterPort)
	}
}

func TestSelectGeoIPRouterPortExposesRuntimePortInsteadOfZero(t *testing.T) {
	cfg := &config.Config{GeoIP: config.GeoIPConfig{Enabled: true}}
	port, err := selectGeoIPRouterPort(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if port == 0 {
		t.Fatal("runtime GeoIP port must be exportable")
	}
}
