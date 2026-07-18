package config

import (
	"strings"
	"testing"
)

func TestValidateInboundCredentialsRejectsOneSidedAuthentication(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "pool password only", cfg: Config{Mode: "pool", Listener: ListenerConfig{Password: "secret"}}},
		{name: "pool username only", cfg: Config{Mode: "pool", Listener: ListenerConfig{Username: "user"}}},
		{name: "multi-port password only", cfg: Config{Mode: "multi-port", MultiPort: MultiPortConfig{Password: "secret"}}},
		{name: "node password only", cfg: Config{Mode: "multi-port", Nodes: []NodeConfig{{Password: "secret"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.cfg.validateInboundCredentials(); err == nil {
				t.Fatal("one-sided credentials were accepted")
			}
		})
	}
}

func TestValidateInboundCredentialsAcceptsEmptyAndCompletePairs(t *testing.T) {
	cfg := Config{
		Mode:      "hybrid",
		Listener:  ListenerConfig{Username: "pool-user", Password: "pool-pass"},
		MultiPort: MultiPortConfig{},
		Nodes: []NodeConfig{
			{},
			{Username: "node-user", Password: "node-pass"},
		},
	}
	if err := cfg.validateInboundCredentials(); err != nil {
		t.Fatalf("complete credentials rejected: %v", err)
	}
}

func TestSanitizeNodeNameRemovesLogControlCharacters(t *testing.T) {
	got := sanitizeNodeName("  provider\r\n[ERROR]\t node\x00  ")
	if strings.ContainsAny(got, "\r\n\t\x00") {
		t.Fatalf("control characters survived: %q", got)
	}
	if got != "provider[ERROR] node" {
		t.Fatalf("sanitized name = %q", got)
	}
}
