package monitor

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"easy_proxies/internal/config"
)

func TestExportUsesIPv6AndPerNodeCredentials(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()

	first := manager.Register(NodeInfo{
		Tag: "first", Name: "first", Mode: "multi-port",
		ListenAddress: "::", Port: 24001,
		Username: "node user", Password: "p@ss:#%",
	})
	first.MarkInitialCheckDone(true)
	first.MarkAvailable(true)
	second := manager.Register(NodeInfo{
		Tag: "second", Name: "second", Mode: "multi-port",
		ListenAddress: "::", Port: 24002,
		Username: "global", Password: "",
	})
	second.MarkInitialCheckDone(true)
	second.MarkAvailable(true)

	server := &Server{
		mgr: manager,
		cfg: Config{ExternalIP: "2001:db8::10"},
		cfgSrc: &config.Config{
			Mode:       "multi-port",
			ExternalIP: "2001:db8::10",
		},
	}
	recorder := httptest.NewRecorder()
	server.handleExport(recorder, httptest.NewRequest(http.MethodGet, "/api/export?scheme=all", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store, max-age=0" {
		t.Fatalf("Cache-Control=%q", got)
	}

	found := map[string]map[string]bool{}
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parsed, err := url.Parse(line)
		if err != nil {
			t.Fatalf("parse exported URI %q: %v", line, err)
		}
		if parsed.Hostname() != "2001:db8::10" {
			t.Errorf("exported host=%q", parsed.Hostname())
		}
		username := ""
		password := ""
		if parsed.User != nil {
			username = parsed.User.Username()
			password, _ = parsed.User.Password()
		}
		key := parsed.Port() + ":" + username + ":" + password
		if found[key] == nil {
			found[key] = map[string]bool{}
		}
		found[key][parsed.Scheme] = true
	}
	for _, key := range []string{"24001:node user:p@ss:#%", "24002:global:"} {
		if !found[key]["http"] || !found[key]["socks5"] {
			t.Errorf("missing per-node exports for %q: %#v", key, found[key])
		}
	}
}

func TestExportAddressRejectsInjectedExternalHost(t *testing.T) {
	if got := exportAddress("0.0.0.0", "example.com\r\nInjected: yes"); got != "0.0.0.0" {
		t.Fatalf("invalid external host selected: %q", got)
	}
}

func TestExportGeoIPUsesProxyUsernameRegionSelectors(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	server := &Server{
		mgr: manager,
		cfgSrc: &config.Config{
			Mode:     "pool",
			Listener: config.ListenerConfig{Address: "127.0.0.1", Username: "crawler", Password: "secret"},
			GeoIP:    config.GeoIPConfig{Enabled: true, Listen: "127.0.0.1", Port: 1221},
		},
	}
	recorder := httptest.NewRecorder()
	server.handleExport(recorder, httptest.NewRequest(http.MethodGet, "/api/export", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	for _, region := range []string{"jp", "kr", "us", "hk", "tw", "sg", "other"} {
		if !strings.Contains(recorder.Body.String(), url.UserPassword("crawler@"+region, "secret").String()+"@127.0.0.1:1221") {
			t.Errorf("missing %s selector in export:\n%s", region, recorder.Body.String())
		}
	}
	if strings.Contains(recorder.Body.String(), "支持路径") {
		t.Fatal("export still documents the removed path-based selector")
	}
}
