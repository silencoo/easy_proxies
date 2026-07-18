package ssruri

import (
	"encoding/base64"
	"strings"
	"testing"
)

func encodeRaw(text string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(text))
}

func TestParseSSRVariantsAndUnicodeRemarks(t *testing.T) {
	password := encodeRaw("p@ss:word")
	remarks := encodeRaw("[ikuuu]🇭🇰 香港Z05 | IEPL")
	protocolParam := base64.StdEncoding.EncodeToString([]byte("42:user+name"))
	payload := "[2001:db8::8]:8443:auth_aes128_md5:chacha20-ietf:tls1.2_ticket_auth:" + password + "/?remarks=" + remarks + "&protoparam=" + protocolParam
	raw := "SSR://" + base64.RawURLEncoding.EncodeToString([]byte(payload))
	got, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got.Server != "2001:db8::8" || got.Port != 8443 || got.Password != "p@ss:word" {
		t.Fatalf("endpoint = %+v", got)
	}
	if got.Protocol != "auth_aes128_md5" || got.Method != "chacha20-ietf" || got.Obfs != "tls1.2_ticket_auth" {
		t.Fatalf("protocol fields = %+v", got)
	}
	if got.Remarks != "[ikuuu]🇭🇰 香港Z05 | IEPL" || got.ProtocolParam != "42:user+name" {
		t.Fatalf("decoded params = %+v", got)
	}
}

func TestParseSSRFallsBackToOuterFragment(t *testing.T) {
	payload := "example.com:443:origin:aes-256-cfb:plain:" + encodeRaw("password")
	got, err := Parse("ssr://" + encodeRaw(payload) + "#%E8%8A%82%E7%82%B9")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got.Remarks != "节点" {
		t.Fatalf("remarks = %q", got.Remarks)
	}
}

func TestParseSSRRejectsMalformedPayloadWithoutLeak(t *testing.T) {
	secret := "credential-must-not-leak"
	bad := base64.RawURLEncoding.EncodeToString([]byte("host:notaport:origin:aes-256-cfb:plain:" + encodeRaw(secret)))
	_, err := Parse("ssr://" + bad)
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), bad) {
		t.Fatalf("error leaked payload: %q", err)
	}
}
