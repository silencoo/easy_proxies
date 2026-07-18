package config

import (
	"encoding/base64"
	"net/url"
	"strings"
	"testing"

	"easy_proxies/internal/ssruri"
)

func TestIsProxyURIRecognizesCompatibilitySchemes(t *testing.T) {
	for _, uri := range []string{
		" socks5h://example.com:1080",
		"SHADOWSOCKS://aes-256-gcm:password@example.com:8388",
		"shadowsocksr://payload",
		"hysteria://example.com:443?auth=x",
	} {
		if !IsProxyURI(uri) {
			t.Fatalf("IsProxyURI(%q) = false", uri)
		}
	}
}

func TestParseSubscriptionDoesNotMisdetectProxiesSubstring(t *testing.T) {
	content := "vless://b831381d-6324-4d53-ad4f-8cda48b30811@example.com:443#contains-proxies:"
	nodes, err := parseSubscriptionContent(content)
	if err != nil {
		t.Fatalf("parseSubscriptionContent() error = %v", err)
	}
	if len(nodes) != 1 || nodes[0].URI != content {
		t.Fatalf("nodes = %#v", nodes)
	}
}

func TestParseSubscriptionDetectsClashKeyBeyondOldPrefixLimit(t *testing.T) {
	content := strings.Repeat("# provider metadata padding\n", 900) + `proxies:
  - name: "[ikuuu]🇭🇰 香港Z05 | IEPL"
    type: vless
    server: example.com
    port: 443
    uuid: b831381d-6324-4d53-ad4f-8cda48b30811
`
	nodes, err := parseSubscriptionContent(content)
	if err != nil {
		t.Fatalf("parseSubscriptionContent() error = %v", err)
	}
	if len(nodes) != 1 || nodes[0].Name != "[ikuuu]🇭🇰 香港Z05 | IEPL" {
		t.Fatalf("nodes = %#v", nodes)
	}
}

func TestParseSubscriptionSupportsRawURLBase64AndRejectsBase64FalsePositive(t *testing.T) {
	uri := "socks5h://user:password@example.com:1080#remote-dns"
	encoded := base64.RawURLEncoding.EncodeToString([]byte(uri + "\n"))
	nodes, err := parseSubscriptionContent(encoded)
	if err != nil || len(nodes) != 1 || nodes[0].URI != uri {
		t.Fatalf("decoded nodes = %#v, error = %v", nodes, err)
	}

	notSubscription := base64.StdEncoding.EncodeToString([]byte("ordinary opaque data"))
	nodes, err = parseSubscriptionContent(notSubscription)
	if err != nil || len(nodes) != 0 {
		t.Fatalf("opaque base64 nodes = %#v, error = %v", nodes, err)
	}
}

func TestParseClashYAMLSkipsOnlyMalformedEntry(t *testing.T) {
	content := `proxies:
  - name: broken
    type: vless
    server: broken.example
    port: not-a-port
    uuid: broken
  - name: "可用节点"
    type: vless
    server: good.example
    port: "443"
    uuid: b831381d-6324-4d53-ad4f-8cda48b30811
`
	nodes, err := parseClashYAML(content)
	if err != nil {
		t.Fatalf("parseClashYAML() error = %v", err)
	}
	if len(nodes) != 1 || nodes[0].Name != "可用节点" || !strings.Contains(nodes[0].URI, "good.example:443") {
		t.Fatalf("nodes = %#v", nodes)
	}
}

func TestConvertClashSSRPreservesUnicodeNameAndParameters(t *testing.T) {
	content := `proxies:
  - name: "[ikuuu]🇭🇰 香港Z05 | IEPL"
    type: ssr
    server: 2001:db8::8
    port: 8443
    cipher: chacha20-ietf
    password: "p@ss:word"
    protocol: auth_aes128_md5
    protocol-param: "42:user"
    obfs: tls1.2_ticket_auth
    obfs-param: cdn.example
`
	nodes, err := parseClashYAML(content)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("nodes = %#v, error = %v", nodes, err)
	}
	parsed, err := ssruri.Parse(nodes[0].URI)
	if err != nil {
		t.Fatalf("ssruri.Parse() error = %v", err)
	}
	if parsed.Server != "2001:db8::8" || parsed.Password != "p@ss:word" || parsed.ProtocolParam != "42:user" || parsed.Remarks != nodes[0].Name {
		t.Fatalf("parsed = %+v", parsed)
	}
	if got := ExtractNodeName(nodes[0].URI); got != nodes[0].Name {
		t.Fatalf("ExtractNodeName() = %q", got)
	}
}

func TestConvertClashHysteriaV1(t *testing.T) {
	content := `proxies:
  - name: "香港 Hysteria"
    type: hysteria
    server: 2001:db8::9
    port: 20088
    auth-str: "auth:token"
    up: "500"
    down: 1000
    peer: sni.example
    alpn: [h3, hq-29]
    skip-cert-verify: true
    obfs: xplus
    obfs-password: obfs-secret
    recv-window: 1024
    recv-window-conn: 512
    disable-mtu-discovery: true
`
	nodes, err := parseClashYAML(content)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("nodes = %#v, error = %v", nodes, err)
	}
	parsed, err := url.Parse(nodes[0].URI)
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	if parsed.Host != "[2001:db8::9]:20088" || parsed.Query().Get("auth") != "auth:token" || parsed.Query().Get("upmbps") != "500" || parsed.Query().Get("obfsParam") != "obfs-secret" {
		t.Fatalf("URI = %q", nodes[0].URI)
	}
	if got := ExtractNodeName(nodes[0].URI); got != "香港 Hysteria" {
		t.Fatalf("ExtractNodeName() = %q", got)
	}
}
