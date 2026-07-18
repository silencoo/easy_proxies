package builder

import (
	"encoding/base64"
	"strings"
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
)

func TestBuildNodeOutboundShadowsocksCompatibilityFormats(t *testing.T) {
	legacy := base64.RawURLEncoding.EncodeToString([]byte("aes-256-gcm:p@ssword@example.com:8388"))
	for _, raw := range []string{
		"ss://" + legacy + "#legacy",
		"ss://YWVzLTI1Ni1nY206cGFzc3dvcmQ=@example.com:8388#sip002",
		"shadowsocks://aes-256-gcm:password@example.com:8388#plain",
	} {
		outbound, err := buildNodeOutbound("ss-node", raw, false)
		if err != nil {
			t.Fatalf("buildNodeOutbound(%q) error = %v", raw, err)
		}
		if outbound.Type != C.TypeShadowsocks {
			t.Fatalf("type = %q", outbound.Type)
		}
		opts, ok := outbound.Options.(*option.ShadowsocksOutboundOptions)
		if !ok || opts.Server != "example.com" || opts.ServerPort != 8388 {
			t.Fatalf("options = %#v", outbound.Options)
		}
	}
}

func TestBuildNodeOutboundRejectsShadowsocksPluginWithoutEchoingIt(t *testing.T) {
	const pluginSecret = "plugin-secret-must-not-leak"
	_, err := buildNodeOutbound("ss-plugin", "ss://aes-256-gcm:password@example.com:8388?plugin="+pluginSecret, false)
	if err == nil || !strings.Contains(err.Error(), "plugin is not supported") {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), pluginSecret) {
		t.Fatalf("error leaked plugin query: %q", err)
	}
}

func TestBuildNodeOutboundSupportsSocks5h(t *testing.T) {
	outbound, err := buildNodeOutbound("socks-node", "socks5h://user:p%40ss@example.com:1080#remote-dns", false)
	if err != nil {
		t.Fatalf("buildNodeOutbound() error = %v", err)
	}
	if outbound.Type != C.TypeSOCKS {
		t.Fatalf("type = %q", outbound.Type)
	}
	opts := outbound.Options.(*option.SOCKSOutboundOptions)
	if opts.Server != "example.com" || opts.ServerPort != 1080 || opts.Username != "user" || opts.Password != "p@ss" {
		t.Fatalf("options = %+v", opts)
	}
}

func TestBuildNodeOutboundSupportsSSR(t *testing.T) {
	password := base64.RawURLEncoding.EncodeToString([]byte("secret"))
	payload := "example.com:8443:auth_aes128_md5:chacha20-ietf:tls1.2_ticket_auth:" + password + "/?obfsparam=" + base64.RawURLEncoding.EncodeToString([]byte("cdn.example"))
	raw := "ssr://" + base64.RawURLEncoding.EncodeToString([]byte(payload))
	outbound, err := buildNodeOutbound("ssr-node", raw, false)
	if err != nil {
		t.Fatalf("buildNodeOutbound() error = %v", err)
	}
	if outbound.Type != C.TypeShadowsocksR {
		t.Fatalf("type = %q", outbound.Type)
	}
	opts := outbound.Options.(*option.ShadowsocksROutboundOptions)
	if opts.Server != "example.com" || opts.ServerPort != 8443 || opts.Password != "secret" || opts.ObfsParam != "cdn.example" {
		t.Fatalf("options = %+v", opts)
	}
}

func TestBuildNodeOutboundSupportsHysteriaV1(t *testing.T) {
	raw := "hysteria://example.com:20088?auth=token%3Avalue&upmbps=500&down=1000Mbps&peer=sni.example&insecure=1&alpn=h3%2Chq-29&obfs=xplus&obfsParam=obfs-secret&recv_window=1024&disable_mtu_discovery=true#%E9%A6%99%E6%B8%AF"
	outbound, err := buildNodeOutbound("hysteria-node", raw, false)
	if err != nil {
		t.Fatalf("buildNodeOutbound() error = %v", err)
	}
	if outbound.Type != C.TypeHysteria {
		t.Fatalf("type = %q", outbound.Type)
	}
	opts := outbound.Options.(*option.HysteriaOutboundOptions)
	if opts.Server != "example.com" || opts.ServerPort != 20088 || opts.AuthString != "token:value" {
		t.Fatalf("endpoint/auth = %+v", opts)
	}
	if opts.UpMbps != 500 || opts.DownMbps != 1000 || opts.Obfs != "obfs-secret" || opts.ReceiveWindow != 1024 || !opts.DisableMTUDiscovery {
		t.Fatalf("protocol options = %+v", opts)
	}
	if opts.TLS == nil || opts.TLS.ServerName != "sni.example" || !opts.TLS.Insecure || len(opts.TLS.ALPN) != 2 {
		t.Fatalf("TLS options = %+v", opts.TLS)
	}
}

func TestBuildHysteriaV1RejectsInvalidBandwidthWithoutCredentialLeak(t *testing.T) {
	const secret = "auth-secret-must-not-leak"
	_, err := buildNodeOutbound("bad-hysteria", "hysteria://example.com:443?auth="+secret+"&upmbps=fast", false)
	if err == nil || !strings.Contains(err.Error(), "upload bandwidth") {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked auth: %q", err)
	}
}
