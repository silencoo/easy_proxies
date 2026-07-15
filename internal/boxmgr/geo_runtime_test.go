package boxmgr

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"easy_proxies/internal/geoip"

	M "github.com/sagernet/sing/common/metadata"
)

type redirectExitDialer struct {
	address string
	err     error
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
