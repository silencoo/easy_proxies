package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDeduplicatesLegacySubscriptionCache(t *testing.T) {
	dir := t.TempDir()
	nodesPath := filepath.Join(dir, "nodes.txt")
	if err := os.WriteFile(nodesPath, []byte(strings.Join([]string{
		"socks5://127.0.0.1:1080#first-name",
		"socks5://127.0.0.1:1080#renamed-duplicate",
		"socks5://127.0.0.1:1081#unique",
	}, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	configPath := filepath.Join(dir, "config.yaml")
	configYAML := "mode: pool\n" +
		"nodes_file: " + filepath.ToSlash(nodesPath) + "\n" +
		"subscriptions:\n  - http://127.0.0.1:1/subscription\n"
	if err := os.WriteFile(configPath, []byte(configYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config with legacy duplicate cache: %v", err)
	}
	if len(cfg.Nodes) != 2 {
		t.Fatalf("loaded %d nodes, want 2 stable identities", len(cfg.Nodes))
	}
	if cfg.Nodes[0].Name != "first-name" {
		t.Fatalf("first-seen cached node was not retained: %#v", cfg.Nodes[0])
	}
	for _, node := range cfg.Nodes {
		if node.Source != NodeSourceSubscription {
			t.Fatalf("cached node source = %q, want subscription", node.Source)
		}
	}
}

func TestNormalizeRejectsDuplicateInlineIdentity(t *testing.T) {
	cfg := &Config{Mode: "pool", Nodes: []NodeConfig{
		{Name: "first", URI: "socks5://127.0.0.1:1080#first"},
		{Name: "second", URI: "socks5://127.0.0.1:1080#second"},
	}}
	if err := cfg.NormalizeWithPortMap(nil); err == nil || !strings.Contains(err.Error(), "same proxy identity") {
		t.Fatalf("duplicate inline identity error = %v", err)
	}
}

func TestDedupeExternalIdentityPrefersInlineNode(t *testing.T) {
	nodes := []NodeConfig{
		{Name: "cached", URI: "socks5://127.0.0.1:1080#cached", Source: NodeSourceSubscription},
		{Name: "inline", URI: "socks5://127.0.0.1:1080#inline", Source: NodeSourceInline},
		{Name: "duplicate cache", URI: "socks5://127.0.0.1:1080#again", Source: NodeSourceFile},
	}
	unique, deduped, err := dedupeExternalNodeKeys(nodes)
	if err != nil {
		t.Fatal(err)
	}
	if deduped != 2 || len(unique) != 1 || unique[0].Name != "inline" {
		t.Fatalf("unique=%#v deduped=%d", unique, deduped)
	}
}
