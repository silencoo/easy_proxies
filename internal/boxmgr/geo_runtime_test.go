package boxmgr

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/geoip"
	"easy_proxies/internal/monitor"

	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
)

type redirectExitDialer struct {
	address string
	err     error
}

func TestGeoDatabaseUpdateCallbackDefersWhileReloadLockIsHeld(t *testing.T) {
	lookup, err := geoip.New("")
	if err != nil {
		t.Fatal(err)
	}
	manager := New(&config.Config{}, monitor.Config{})
	manager.mu.Lock()
	manager.geoLookup = lookup
	manager.mu.Unlock()

	manager.reloadMu.Lock()
	returned := make(chan struct{})
	go func() {
		manager.handleGeoDatabaseUpdate(lookup)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		manager.reloadMu.Unlock()
		<-returned
		_ = lookup.Close()
		t.Fatal("GeoIP update callback waited for reloadMu and can deadlock Lookup.Close")
	}

	// A second notification for the same lookup is coalesced while the deferred
	// worker is waiting for the lifecycle transaction to finish.
	manager.handleGeoDatabaseUpdate(lookup)
	manager.geoUpdateMu.Lock()
	queued := len(manager.geoUpdateQueued)
	manager.geoUpdateMu.Unlock()
	if queued != 1 {
		manager.reloadMu.Unlock()
		_ = lookup.Close()
		t.Fatalf("queued GeoIP updates = %d, want one coalesced callback", queued)
	}
	manager.reloadMu.Unlock()

	deadline := time.Now().Add(time.Second)
	for {
		manager.geoUpdateMu.Lock()
		queued = len(manager.geoUpdateQueued)
		manager.geoUpdateMu.Unlock()
		if queued == 0 {
			break
		}
		if time.Now().After(deadline) {
			_ = lookup.Close()
			t.Fatal("deferred GeoIP update did not drain after reloadMu was released")
		}
		time.Sleep(time.Millisecond)
	}
	if err := lookup.Close(); err != nil {
		t.Fatal(err)
	}
}

func (d redirectExitDialer) DialContext(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
	if d.err != nil {
		return nil, d.err
	}
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

type fakeIPRegionLookup map[string]geoip.RegionInfo

func (l fakeIPRegionLookup) LookupIP(ip string) geoip.RegionInfo {
	if region, ok := l[ip]; ok {
		return region
	}
	return geoip.RegionInfo{Code: geoip.RegionOther, Country: "Unknown"}
}

func TestDiscoverExitRegionsUsesEachOutboundObservedIP(t *testing.T) {
	serverUS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("8.8.8.8"))
	}))
	defer serverUS.Close()
	serverJP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ip":"1.1.1.1"}`))
	}))
	defer serverJP.Close()

	results := discoverExitRegions(
		context.Background(),
		map[string]geoip.OutboundDialer{
			"node-us":  redirectExitDialer{address: serverUS.Listener.Addr().String()},
			"node-jp":  redirectExitDialer{address: serverJP.Listener.Addr().String()},
			"node-old": redirectExitDialer{err: errors.New("proxy unavailable")},
		},
		fakeIPRegionLookup{
			"8.8.8.8": {Code: geoip.RegionUS, Country: "United States"},
			"1.1.1.1": {Code: geoip.RegionJP, Country: "Japan"},
			"9.9.9.9": {Code: geoip.RegionUS, Country: "United States"},
		},
		serverUS.URL,
		time.Second,
		3,
		map[string]string{"node-old": "9.9.9.9"},
	)
	if got := results["node-us"]; got.ExitIP != "8.8.8.8" || got.Region.Code != geoip.RegionUS || got.Err != nil {
		t.Fatalf("US node used wrong exit classification: %#v", got)
	}
	if got := results["node-jp"]; got.ExitIP != "1.1.1.1" || got.Region.Code != geoip.RegionJP || got.Err != nil {
		t.Fatalf("JP node used wrong exit classification: %#v", got)
	}
	if got := results["node-old"]; got.ExitIP != "9.9.9.9" || got.Region.Code != geoip.RegionUS || got.Err == nil {
		t.Fatalf("failed node did not retain its last real exit classification: %#v", got)
	}
}

func TestClassifyKnownExitIPsReusesObservations(t *testing.T) {
	exitIPs := map[string]string{
		"node-a": "8.8.8.8",
		"node-b": "1.1.1.1",
		"empty":  "",
	}
	first := classifyKnownExitIPs(exitIPs, fakeIPRegionLookup{
		"8.8.8.8": {Code: geoip.RegionUS, Country: "United States"},
		"1.1.1.1": {Code: geoip.RegionJP, Country: "Japan"},
	})
	if got := first["node-a"]; got.ExitIP != "8.8.8.8" || got.Region.Code != geoip.RegionUS {
		t.Fatalf("initial classification mismatch: %#v", got)
	}
	if _, exists := first["empty"]; exists {
		t.Fatal("empty exit IP should not be classified")
	}

	second := classifyKnownExitIPs(exitIPs, fakeIPRegionLookup{
		"8.8.8.8": {Code: geoip.RegionSG, Country: "Singapore"},
		"1.1.1.1": {Code: geoip.RegionUS, Country: "United States"},
	})
	if got := second["node-a"]; got.ExitIP != "8.8.8.8" || got.Region.Code != geoip.RegionSG {
		t.Fatalf("updated database did not reclassify the saved observation: %#v", got)
	}
	if got := second["node-b"]; got.ExitIP != "1.1.1.1" || got.Region.Code != geoip.RegionUS {
		t.Fatalf("updated database did not reclassify second observation: %#v", got)
	}
}

func TestApplyGeoPoolChangesRestoresStalePoolWhenLaterRemovalFails(t *testing.T) {
	oldPools := map[string]option.Outbound{
		"proxy-pool":   {Tag: "proxy-pool", Type: "old-global"},
		"pool-a-stale": {Tag: "pool-a-stale", Type: "old-stale-a"},
		"pool-z-stale": {Tag: "pool-z-stale", Type: "old-stale-z"},
	}
	desiredPools := map[string]option.Outbound{
		"proxy-pool": {Tag: "proxy-pool", Type: "new-global"},
		"pool-us":    {Tag: "pool-us", Type: "new-region"},
	}
	runtimePools := make(map[string]option.Outbound, len(oldPools))
	for tag, outbound := range oldPools {
		runtimePools[tag] = outbound
	}

	err := applyGeoPoolChanges(
		desiredPools,
		oldPools,
		func(outbound option.Outbound) error {
			runtimePools[outbound.Tag] = outbound
			return nil
		},
		func(tag string) error {
			if tag == "pool-z-stale" {
				// sing-box can report a remove error after detaching the outbound.
				delete(runtimePools, tag)
				return errors.New("injected removal failure")
			}
			if _, exists := runtimePools[tag]; !exists {
				return errors.New("pool does not exist")
			}
			delete(runtimePools, tag)
			return nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "remove stale GeoIP pool pool-z-stale") {
		t.Fatalf("applyGeoPoolChanges error=%v, want injected stale-pool removal failure", err)
	}
	if !reflect.DeepEqual(runtimePools, oldPools) {
		t.Fatalf("rollback left runtime pools=%#v, want original=%#v", runtimePools, oldPools)
	}
}

func TestDiscoverExitRegionsProductionProbeReturnsAtTimeout(t *testing.T) {
	manager := New(&config.Config{}, monitor.Config{})
	manager.exitProbeSlots = make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	manager.probeExitIP = func(context.Context, geoip.OutboundDialer, string) (string, error) {
		calls.Add(1)
		<-release // Deliberately ignore the supplied context.
		return "8.8.8.8", nil
	}
	dialer := &redirectExitDialer{}

	started := time.Now()
	results := discoverExitRegionsWithProbe(
		context.Background(),
		map[string]geoip.OutboundDialer{"hung": dialer},
		fakeIPRegionLookup{},
		"https://exit.invalid/ip",
		35*time.Millisecond,
		1,
		nil,
		manager.discoverExitIPBounded,
	)
	elapsed := time.Since(started)
	if elapsed < 20*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("bounded production probe returned after %s, want near its 35ms timeout", elapsed)
	}
	if got := results["hung"]; !errors.Is(got.Err, context.DeadlineExceeded) {
		t.Fatalf("hung production probe error=%v, want deadline exceeded", got.Err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("underlying production probes=%d, want 1", got)
	}

	close(release)
	eventuallyBoxManager(t, time.Second, func() bool {
		manager.exitProbeMu.Lock()
		remaining := len(manager.exitProbeCalls)
		manager.exitProbeMu.Unlock()
		return remaining == 0
	}, "completed exit-IP probe did not clean up")
}

func TestDiscoverExitIPBoundedCoalescesSameFlightKey(t *testing.T) {
	manager := New(&config.Config{}, monitor.Config{})
	release := make(chan struct{})
	var calls atomic.Int32
	manager.probeExitIP = func(context.Context, geoip.OutboundDialer, string) (string, error) {
		calls.Add(1)
		<-release
		return "1.1.1.1", nil
	}
	dialer := &redirectExitDialer{}
	run := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := manager.discoverExitIPBounded(ctx, "same", dialer, "https://exit.invalid/ip")
		return err
	}
	if err := run(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first hung probe error=%v, want deadline exceeded", err)
	}
	if err := run(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("joined hung probe error=%v, want deadline exceeded", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("same flight key launched %d probes, want 1", got)
	}

	close(release)
	eventuallyBoxManager(t, time.Second, func() bool {
		manager.exitProbeMu.Lock()
		remaining := len(manager.exitProbeCalls)
		manager.exitProbeMu.Unlock()
		return remaining == 0
	}, "coalesced exit-IP probe did not clean up")
}

func TestDiscoverExitIPBoundedCapsDistinctFlightKeys(t *testing.T) {
	manager := New(&config.Config{}, monitor.Config{})
	manager.exitProbeSlots = make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	manager.probeExitIP = func(context.Context, geoip.OutboundDialer, string) (string, error) {
		calls.Add(1)
		<-release
		return "", nil
	}
	dialer := &redirectExitDialer{}
	for _, tag := range []string{"first", "second"} {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
		_, err := manager.discoverExitIPBounded(ctx, tag, dialer, "https://exit.invalid/ip")
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("hung probe %q error=%v, want deadline exceeded", tag, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	_, err := manager.discoverExitIPBounded(ctx, "over-cap", dialer, "https://exit.invalid/ip")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("over-cap probe error=%v, want deadline exceeded", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("distinct-key cap launched %d probes, want 2", got)
	}

	close(release)
	eventuallyBoxManager(t, time.Second, func() bool {
		manager.exitProbeMu.Lock()
		remaining := len(manager.exitProbeCalls)
		manager.exitProbeMu.Unlock()
		return remaining == 0 && len(manager.exitProbeSlots) == 0
	}, "bounded exit-IP probes did not release their slots")
}
