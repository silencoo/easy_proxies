package geoip

import (
	"encoding/base64"
	"testing"
)

func TestExtractHostFromCompatibilityURIs(t *testing.T) {
	legacySS := base64.RawURLEncoding.EncodeToString([]byte("aes-256-gcm:password@ss.example:8388"))
	ssrPassword := base64.RawURLEncoding.EncodeToString([]byte("password"))
	ssrPayload := "[2001:db8::7]:443:origin:aes-256-cfb:plain:" + ssrPassword
	ssr := base64.RawURLEncoding.EncodeToString([]byte(ssrPayload))
	tests := map[string]string{
		"ss://" + legacySS + "#legacy":                                 "ss.example",
		"shadowsocks://aes-256-gcm:password@[2001:db8::6]:8388#sip002": "2001:db8::6",
		"ssr://" + ssr: "2001:db8::7",
		"socks5h://user:password@socks.example:1080": "socks.example",
		"hysteria://hy.example:443?auth=secret":      "hy.example",
	}
	for raw, want := range tests {
		if got := extractHostFromURI(raw); got != want {
			t.Fatalf("extractHostFromURI(%q) = %q, want %q", raw, got, want)
		}
	}
}
