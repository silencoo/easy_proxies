package config

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestNodeKeyIgnoresDisplayNameAndQueryOrder(t *testing.T) {
	first := NodeConfig{URI: "vless://user@Example.COM:443?b=2&a=1#Los%20Angeles"}
	second := NodeConfig{URI: "vless://user@example.com:443?a=1&b=2#Tokyo"}
	if first.NodeKey() != second.NodeKey() {
		t.Fatal("display name or query ordering changed the stable node key")
	}
}

func TestNodeKeyIgnoresVMessDisplayName(t *testing.T) {
	buildURI := func(name string) string {
		document := map[string]any{
			"v": "2", "ps": name, "add": "example.com", "port": "443",
			"id": "00000000-0000-0000-0000-000000000001", "net": "ws",
		}
		data, err := json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		return "vmess://" + base64.StdEncoding.EncodeToString(data)
	}

	first := NodeConfig{URI: buildURI("old name")}
	second := NodeConfig{URI: buildURI("new name")}
	if first.NodeKey() != second.NodeKey() {
		t.Fatal("vmess ps field changed the stable node key")
	}
}

func TestDedicatedPortsPersistAcrossRestartAndReordering(t *testing.T) {
	tempDir := t.TempDir()
	basePort := findAvailablePortRange(t, 4)
	configPath := filepath.Join(tempDir, "config.yaml")
	uriA := "vless://secret-a@example.com:443?encryption=none#node-a"
	uriB := "vless://secret-b@example.net:443?encryption=none#node-b"
	uriC := "vless://secret-c@example.org:443?encryption=none#node-c"

	newConfig := func(uris ...string) *Config {
		nodes := make([]NodeConfig, 0, len(uris))
		for _, uri := range uris {
			nodes = append(nodes, NodeConfig{URI: uri})
		}
		cfg := &Config{
			Mode: "multi-port",
			MultiPort: MultiPortConfig{
				Address:        "127.0.0.1",
				BasePort:       basePort,
				PortReuseDelay: 24 * time.Hour,
			},
			Nodes: nodes,
		}
		cfg.SetFilePath(configPath)
		return cfg
	}

	initial := newConfig(uriA, uriB)
	if err := initial.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize initial config: %v", err)
	}
	if err := initial.PersistPortMap(); err != nil {
		t.Fatalf("persist initial port map: %v", err)
	}
	portA := initial.Nodes[0].Port
	portB := initial.Nodes[1].Port

	reordered := newConfig(uriB, uriA)
	if err := reordered.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize reordered config: %v", err)
	}
	if reordered.Nodes[0].Port != portB || reordered.Nodes[1].Port != portA {
		t.Fatalf("ports drifted after restart/reorder: got %d,%d want %d,%d", reordered.Nodes[0].Port, reordered.Nodes[1].Port, portB, portA)
	}
	if err := reordered.PersistPortMap(); err != nil {
		t.Fatalf("persist reordered port map: %v", err)
	}

	removed := newConfig(uriB)
	if err := removed.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize removed config: %v", err)
	}
	if err := removed.PersistPortMap(); err != nil {
		t.Fatalf("persist removed config: %v", err)
	}

	withReplacement := newConfig(uriB, uriC)
	if err := withReplacement.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize replacement config: %v", err)
	}
	if withReplacement.Nodes[1].Port == portA {
		t.Fatalf("new node immediately reused tombstoned port %d", portA)
	}
	if err := withReplacement.PersistPortMap(); err != nil {
		t.Fatalf("persist replacement config: %v", err)
	}

	returned := newConfig(uriC, uriA, uriB)
	if err := returned.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize returned config: %v", err)
	}
	if returned.Nodes[1].Port != portA {
		t.Fatalf("returning node did not reclaim port: got %d want %d", returned.Nodes[1].Port, portA)
	}

	portMapData, err := os.ReadFile(filepath.Join(tempDir, defaultPortMapFile))
	if err != nil {
		t.Fatalf("read port map: %v", err)
	}
	for _, secret := range []string{"secret-a", "secret-b", "secret-c"} {
		if containsString(string(portMapData), secret) {
			t.Fatalf("port map leaked subscription credential %q", secret)
		}
	}
}

func TestWriteFileAtomicConcurrentWriters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.yaml")
	payloads := make(map[string]struct{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		payload := fmt.Sprintf("writer: %d\nvalue: %q\n", i, fmt.Sprintf("payload-%04d", i))
		payloads[payload] = struct{}{}
		wg.Add(1)
		go func(content string) {
			defer wg.Done()
			if err := WriteFileAtomic(path, []byte(content), 0o600); err != nil {
				t.Errorf("atomic write: %v", err)
			}
		}(payload)
	}
	wg.Wait()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read final file: %v", err)
	}
	if _, ok := payloads[string(data)]; !ok {
		t.Fatalf("final file was partial or corrupted: %q", data)
	}
}

func TestReplaceFileAtomicReplacesExistingFile(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.mmdb")
	destination := filepath.Join(dir, "active.mmdb")
	if err := os.WriteFile(source, []byte("new database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("old database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ReplaceFileAtomic(source, destination); err != nil {
		t.Fatalf("replace existing file: %v", err)
	}
	data, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("read replacement: %v", err)
	}
	if string(data) != "new database" {
		t.Fatalf("destination contains %q", data)
	}
	if _, err := os.Stat(source); !os.IsNotExist(err) {
		t.Fatalf("source still exists after replacement: %v", err)
	}
}

func TestNormalizeGeoIPAutoUpdateInterval(t *testing.T) {
	cfg := &Config{GeoIP: GeoIPConfig{AutoUpdateEnabled: true}}
	cfg.normalizeGeoIPConfig()
	if cfg.GeoIP.AutoUpdateInterval != 24*time.Hour {
		t.Fatalf("auto-update interval = %v, want 24h", cfg.GeoIP.AutoUpdateInterval)
	}
}

func findAvailablePortRange(t *testing.T, count int) uint16 {
	t.Helper()
	for base := 30000; base+count < 60000; base++ {
		available := true
		for offset := 0; offset < count; offset++ {
			if !IsPortAvailable("127.0.0.1", uint16(base+offset)) {
				available = false
				break
			}
		}
		if available {
			return uint16(base)
		}
	}
	t.Fatal("could not find an available port range")
	return 0
}

func containsString(value, substring string) bool {
	for i := 0; i+len(substring) <= len(value); i++ {
		if value[i:i+len(substring)] == substring {
			return true
		}
	}
	return false
}
