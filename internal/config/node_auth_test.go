package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExternalNodeAuthOverridePersistsAcrossReload(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")
	nodesPath := filepath.Join(tempDir, "nodes.txt")
	uri := "socks5://upstream-secret@example.com:1080?a=1&b=2#provider-name"
	configData := `mode: multi-port
multi_port:
  address: 127.0.0.1
  base_port: 31000
  username: default-user
  password: default-password
nodes_file: nodes.txt
`
	if err := os.WriteFile(configPath, []byte(configData), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nodesPath, []byte(uri+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load initial config: %v", err)
	}
	if cfg.Nodes[0].Username != "default-user" {
		t.Fatalf("expected default credentials, got %#v", cfg.Nodes[0])
	}
	cfg.Nodes[0].Username = "custom-user"
	cfg.Nodes[0].Password = "custom-password"
	if err := cfg.SaveNodes(); err != nil {
		t.Fatalf("save external node override: %v", err)
	}

	reloaded, err := Load(configPath)
	if err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if reloaded.Nodes[0].Username != "custom-user" || reloaded.Nodes[0].Password != "custom-password" {
		t.Fatalf("custom credentials were lost: %#v", reloaded.Nodes[0])
	}

	authData, err := os.ReadFile(filepath.Join(tempDir, defaultNodeAuthFile))
	if err != nil {
		t.Fatalf("read auth sidecar: %v", err)
	}
	if strings.Contains(string(authData), "upstream-secret") || strings.Contains(string(authData), "example.com") {
		t.Fatalf("auth sidecar leaked upstream URI material: %s", authData)
	}
	if !strings.Contains(string(authData), "custom-user") || !strings.Contains(string(authData), "custom-password") {
		t.Fatalf("auth sidecar did not contain the listener override: %s", authData)
	}
}

func TestSubscriptionCandidateReappliesStableAuthOverride(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("mode: multi-port\nnodes:\n  - uri: socks5://127.0.0.1:1080#bootstrap\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalURI := "vless://id@example.com:443?b=2&a=1#old-name"
	cfg := &Config{
		Mode: "multi-port",
		MultiPort: MultiPortConfig{
			Address:  "127.0.0.1",
			BasePort: 32000,
			Username: "default-user",
			Password: "default-password",
		},
		Nodes: []NodeConfig{{
			URI:      originalURI,
			Username: "custom-user",
			Password: "custom-password",
			Source:   NodeSourceSubscription,
		}},
	}
	cfg.SetFilePath(configPath)
	if err := cfg.persistNodeAuthOverrides(); err != nil {
		t.Fatalf("persist override: %v", err)
	}

	candidate := *cfg
	candidate.Nodes = []NodeConfig{{
		URI:    "vless://id@example.com:443?a=1&b=2#renamed-by-provider",
		Source: NodeSourceSubscription,
	}}
	if err := candidate.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize refreshed candidate: %v", err)
	}
	if candidate.Nodes[0].Username != "custom-user" || candidate.Nodes[0].Password != "custom-password" {
		t.Fatalf("subscription refresh lost stable auth override: %#v", candidate.Nodes[0])
	}
}

func TestDefaultExternalCredentialsDoNotBecomeOverrides(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &Config{
		Mode: "multi-port",
		MultiPort: MultiPortConfig{
			Username: "default-user",
			Password: "default-password",
		},
		Nodes: []NodeConfig{{
			URI:      "socks5://127.0.0.1:1080#node",
			Username: "default-user",
			Password: "default-password",
			Source:   NodeSourceFile,
		}},
	}
	cfg.SetFilePath(filepath.Join(tempDir, "config.yaml"))
	if err := cfg.persistNodeAuthOverrides(); err != nil {
		t.Fatalf("persist defaults: %v", err)
	}
	state, err := cfg.loadNodeAuthState()
	if err != nil {
		t.Fatalf("load auth state: %v", err)
	}
	if len(state.Overrides) != 0 {
		t.Fatalf("global defaults were incorrectly pinned as overrides: %#v", state.Overrides)
	}
}
