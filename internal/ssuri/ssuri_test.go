package ssuri

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestParseSupportedFormats(t *testing.T) {
	legacy := base64.RawURLEncoding.EncodeToString([]byte("aes-256-gcm:p@ss:word@example.com:8388"))
	tests := []struct {
		name     string
		raw      string
		method   string
		password string
		server   string
		port     int
		fragment string
	}{
		{"legacy whole payload", "ss://" + legacy + "#%E9%A6%99%E6%B8%AF", "aes-256-gcm", "p@ss:word", "example.com", 8388, "香港"},
		{"SIP002 padded userinfo", "ss://YWVzLTI1Ni1nY206cGFzc3dvcmQ=@example.com:8389#Node", "aes-256-gcm", "password", "example.com", 8389, "Node"},
		{"SIP002 raw URL userinfo", "ss://YWVzLTI1Ni1nY206cGFzc3dvcmQ@example.com:8390", "aes-256-gcm", "password", "example.com", 8390, ""},
		{"plain escaped userinfo", "shadowsocks://aes-256-gcm:p%40ss%3Aword@example.com:443#node+plus", "aes-256-gcm", "p@ss:word", "example.com", 443, "node+plus"},
		{"IPv6 and default port", "ss://aes-128-gcm:password@[2001:db8::1]#IPv6", "aes-128-gcm", "password", "2001:db8::1", 8388, "IPv6"},
		{"Unicode host", "SS://chacha20-ietf-poly1305:password@例子.test:9000#%F0%9F%8C%8F", "chacha20-ietf-poly1305", "password", "例子.test", 9000, "🌏"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Parse(test.raw)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if got.Method != test.method || got.Password != test.password || got.Server != test.server || got.Port != test.port || got.Fragment != test.fragment {
				t.Fatalf("Parse() = %+v", got)
			}
		})
	}
}

func TestParsePreservesPluginQuery(t *testing.T) {
	got, err := Parse("ss://aes-256-gcm:password@example.com:8388?plugin=v2ray-plugin%3Btls%3Bhost%3Dcdn.example#Query")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got.Query.Get("plugin") != "v2ray-plugin;tls;host=cdn.example" {
		t.Fatalf("plugin = %q", got.Query.Get("plugin"))
	}
}

func TestParseRejectsMalformedOrNonUTF8WithoutLeakingPayload(t *testing.T) {
	nonUTF8 := base64.RawURLEncoding.EncodeToString([]byte{0xff, 0xfe, 0xfd})
	secretPayload := base64.RawURLEncoding.EncodeToString([]byte("aes-256-gcm:super-secret@example.com:not-a-port"))
	for _, raw := range []string{
		"ss://@@@",
		"ss://:password@example.com:8388",
		"ss://aes-256-gcm:@example.com:8388",
		"ss://aes-256-gcm:password@2001:db8::1:8388",
		"ss://aes-256-gcm:password@[2001:db8::1:8388",
		"ss://aes-256-gcm:password@example.com:0",
		"ss://" + nonUTF8,
		"ss://" + secretPayload,
	} {
		t.Run(raw[:min(len(raw), 24)], func(t *testing.T) {
			_, err := Parse(raw)
			if err == nil {
				t.Fatalf("Parse(%q) unexpectedly succeeded", raw)
			}
			if strings.Contains(err.Error(), "super-secret") || strings.Contains(err.Error(), secretPayload) {
				t.Fatalf("error leaked payload: %q", err)
			}
		})
	}
}

func TestParseIgnoresMalformedOptionalFragment(t *testing.T) {
	got, err := Parse("ss://aes-256-gcm:password@example.com:8388#bad%zz")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got.Server != "example.com" || got.Fragment != "" {
		t.Fatalf("Parse() = %+v", got)
	}
}
