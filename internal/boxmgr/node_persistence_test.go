package boxmgr

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
)

func TestCreateNodePersistsWebUINodeInlineWithSubscriptions(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")
	nodesPath := filepath.Join(tempDir, "nodes.txt")
	if err := os.WriteFile(configPath, []byte(`mode: pool
subscriptions:
  - https://example.invalid/subscription
nodes_file: nodes.txt
nodes: []
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		Mode:          "pool",
		NodesFile:     nodesPath,
		Subscriptions: []string{"https://example.invalid/subscription"},
		Nodes: []config.NodeConfig{{
			Name:   "subscription",
			URI:    "trojan://pw@subscription.example:443#subscription",
			Source: config.NodeSourceSubscription,
		}},
	}
	cfg.SetFilePath(configPath)
	manager := New(cfg, monitor.Config{})
	manualURI := "socks5://user:pass@127.0.0.1:1080#manual"
	created, err := manager.CreateNode(context.Background(), config.NodeConfig{Name: "manual", URI: manualURI})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	if created.Source != config.NodeSourceInline {
		t.Fatalf("WebUI node source=%q, want inline", created.Source)
	}

	configData, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(configData), manualURI) {
		t.Fatalf("manual node was not persisted in config.yaml: %s", configData)
	}
	nodesData, err := os.ReadFile(nodesPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(nodesData), manualURI) || !strings.Contains(string(nodesData), "subscription.example") {
		t.Fatalf("nodes.txt did not remain subscription-only: %s", nodesData)
	}
}
