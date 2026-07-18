package config

import (
	"strings"
	"testing"
)

func TestNormalizeRejectsInvalidProbeTarget(t *testing.T) {
	cfg := &Config{
		Mode: "pool",
		Management: ManagementConfig{
			Listen:      "127.0.0.1:9091",
			ProbeTarget: "example.com:not-a-port",
		},
		Nodes: []NodeConfig{{Name: "node", URI: "socks5://127.0.0.1:1080"}},
	}
	err := cfg.NormalizeWithPortMap(nil)
	if err == nil || !strings.Contains(err.Error(), "probe_target") {
		t.Fatalf("invalid target error=%v", err)
	}
}
