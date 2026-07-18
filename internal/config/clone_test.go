package config

import "testing"

func TestConfigCloneIsolatesMutableFields(t *testing.T) {
	retryEnabled := true
	managementEnabled := false
	original := &Config{
		Nodes: []NodeConfig{{Name: "first", URI: "socks5://127.0.0.1:1080"}},
		Subscriptions: []string{
			"https://example.test/subscription",
		},
		Pool:       PoolConfig{RetryEnabled: &retryEnabled},
		Management: ManagementConfig{Enabled: &managementEnabled},
		filePath:   "config.yaml",
	}

	clone := original.Clone()
	if clone == original {
		t.Fatal("Clone returned the original pointer")
	}
	if clone.FilePath() != original.FilePath() {
		t.Fatalf("Clone lost file path: got %q, want %q", clone.FilePath(), original.FilePath())
	}

	clone.Nodes[0].Name = "clone-node"
	clone.Subscriptions[0] = "https://clone.test/subscription"
	*clone.Pool.RetryEnabled = false
	*clone.Management.Enabled = true
	if original.Nodes[0].Name != "first" {
		t.Fatalf("clone node mutation reached original: %q", original.Nodes[0].Name)
	}
	if original.Subscriptions[0] != "https://example.test/subscription" {
		t.Fatalf("clone subscription mutation reached original: %q", original.Subscriptions[0])
	}
	if !*original.Pool.RetryEnabled {
		t.Fatal("clone retry pointer mutation reached original")
	}
	if *original.Management.Enabled {
		t.Fatal("clone management pointer mutation reached original")
	}

	original.Nodes[0].URI = "socks5://127.0.0.1:2080"
	original.Subscriptions = append(original.Subscriptions, "https://second.test/subscription")
	if clone.Nodes[0].URI != "socks5://127.0.0.1:1080" {
		t.Fatalf("original node mutation reached clone: %q", clone.Nodes[0].URI)
	}
	if len(clone.Subscriptions) != 1 {
		t.Fatalf("original subscription append changed clone length: %d", len(clone.Subscriptions))
	}
}

func TestConfigClonePreservesNilOptionalBooleans(t *testing.T) {
	clone := (&Config{}).Clone()
	if clone.Pool.RetryEnabled != nil {
		t.Fatal("nil pool retry option became non-nil")
	}
	if clone.Management.Enabled != nil {
		t.Fatal("nil management enabled option became non-nil")
	}
	if (*Config)(nil).Clone() != nil {
		t.Fatal("nil config clone should remain nil")
	}
}
