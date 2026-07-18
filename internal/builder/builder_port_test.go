package builder

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/option"
)

func TestBuildNodeOutboundRejectsPortsOutsideUint16Range(t *testing.T) {
	vmessPayload := base64.StdEncoding.EncodeToString([]byte(`{"v":"2","ps":"bad","add":"example.com","port":"65536","id":"b831381d-6324-4d53-ad4f-8cda48b30811","aid":"0","net":"tcp"}`))
	ssrPassword := base64.RawURLEncoding.EncodeToString([]byte("secret"))
	ssrPayload := base64.RawURLEncoding.EncodeToString([]byte("example.com:65536:origin:aes-256-gcm:plain:" + ssrPassword))

	tests := []struct {
		name string
		uri  string
	}{
		{name: "vless zero", uri: "vless://b831381d-6324-4d53-ad4f-8cda48b30811@example.com:0"},
		{name: "trojan overflow", uri: "trojan://secret@example.com:65536"},
		{name: "socks overflow", uri: "socks5://example.com:70000"},
		{name: "http overflow", uri: "http://example.com:99999"},
		{name: "hysteria2 overflow", uri: "hysteria2://secret@example.com:65536"},
		{name: "shadowsocks overflow", uri: "ss://aes-256-gcm:secret@example.com:65536"},
		{name: "shadowsocksr overflow", uri: "ssr://" + ssrPayload},
		{name: "vmess overflow", uri: "vmess://" + vmessPayload},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := buildNodeOutbound("bad-port", test.uri, false); err == nil {
				t.Fatalf("buildNodeOutbound(%q) unexpectedly accepted an invalid port", test.uri)
			}
		})
	}
}

func TestBuildHTTPProxyOptionsUsesSchemeDefaultPort(t *testing.T) {
	tests := []struct {
		scheme string
		port   uint16
		tls    bool
	}{
		{scheme: "http", port: 8080},
		{scheme: "https", port: 443, tls: true},
	}

	for _, test := range tests {
		t.Run(test.scheme, func(t *testing.T) {
			outbound, err := buildNodeOutbound("proxy", test.scheme+"://proxy.example", false)
			if err != nil {
				t.Fatalf("buildNodeOutbound() error = %v", err)
			}
			opts := outbound.Options.(*option.HTTPOutboundOptions)
			if opts.ServerPort != test.port {
				t.Fatalf("ServerPort = %d, want %d", opts.ServerPort, test.port)
			}
			if (opts.TLS != nil) != test.tls {
				t.Fatalf("TLS configured = %v, want %v", opts.TLS != nil, test.tls)
			}
		})
	}
}

func TestBuildHysteria2RejectsInvalidPortSets(t *testing.T) {
	for _, portSet := range []string{
		"0",
		"65536",
		"10000-70000",
		"20000-10000",
		"10000,,20000",
		"not-a-port",
	} {
		t.Run(strings.NewReplacer(":", "_", "/", "_").Replace(portSet), func(t *testing.T) {
			raw := "hysteria2://secret@example.com:443?ports=" + url.QueryEscape(portSet)
			if _, err := buildNodeOutbound("bad-hy2", raw, false); err == nil {
				t.Fatalf("accepted invalid Hysteria2 port set %q", portSet)
			}
		})
	}
}

func TestBuildHysteria2DecodesEscapedPassword(t *testing.T) {
	const password = "p@ss:/?#%"
	raw := fmt.Sprintf("hysteria2://%s@[2001:db8::1]:443", url.User(password).String())
	outbound, err := buildNodeOutbound("hy2", raw, false)
	if err != nil {
		t.Fatalf("buildNodeOutbound() error = %v", err)
	}
	opts := outbound.Options.(*option.Hysteria2OutboundOptions)
	if opts.Password != password || opts.Server != "2001:db8::1" || opts.ServerPort != 443 {
		t.Fatalf("options = %+v", opts)
	}
}
