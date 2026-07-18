package monitor

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestFormatProbeFailureRedactsCredentialsAndOpaqueURIData(t *testing.T) {
	err := errors.New("request https://api-user:api-pass@probe.example/private/token?key=secret: context deadline exceeded")
	formatted := FormatProbeFailure("node-a", "vless://uuid:password@node.example:443/private?token=secret", err)
	for _, secret := range []string{"uuid", "password", "api-user", "api-pass", "/private", "key=secret", "token=secret"} {
		if strings.Contains(formatted, secret) {
			t.Fatalf("diagnostic leaked %q: %s", secret, formatted)
		}
	}
	if !strings.Contains(formatted, "vless@node.example:443") || !strings.Contains(formatted, "https://probe.example") {
		t.Fatalf("diagnostic lost useful endpoint context: %s", formatted)
	}
}

func TestFormatProbeFailureRedactsBase64StyleURIHost(t *testing.T) {
	const payload = "eyJhZGQiOiJzZWNyZXQuZXhhbXBsZSIsInBzIjoic2VjcmV0LW5hbWUifQ=="
	formatted := FormatProbeFailure("node-a", "vmess://"+payload, errors.New("failed vmess://"+payload))
	if strings.Contains(formatted, payload) || strings.Contains(formatted, "secret") {
		t.Fatalf("diagnostic leaked opaque vmess payload: %s", formatted)
	}
}

func TestProbeErrorClassification(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{context.DeadlineExceeded, "read_timeout"},
		{context.Canceled, "cancelled"},
		{errors.New("x509: certificate signed by unknown authority"), "tls_failed"},
		{errors.New("dial tcp 203.0.113.1:443: connection refused"), "dial_refused"},
	}
	for _, test := range tests {
		got, _ := classifyProbeError(test.err)
		if got != test.want {
			t.Errorf("classifyProbeError(%q) = %q, want %q", test.err, got, test.want)
		}
	}
}
