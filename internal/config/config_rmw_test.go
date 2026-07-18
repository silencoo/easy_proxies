package config

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestConcurrentSaveSettingsAndSaveNodesPreserveBothUpdates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	seed := `mode: pool
external_ip: 192.0.2.1
listener:
  address: 127.0.0.1
  port: 1080
nodes:
  - name: old-node
    uri: socks5://127.0.0.1:1081
`
	if err := os.WriteFile(path, []byte(seed), 0o600); err != nil {
		t.Fatal(err)
	}

	settings := &Config{
		filePath:   path,
		Mode:       "pool",
		ExternalIP: "203.0.113.42",
		Listener:   ListenerConfig{Address: "127.0.0.1", Port: 1080},
	}
	nodes := &Config{
		filePath: path,
		Nodes: []NodeConfig{{
			Name:   "new-node",
			URI:    "socks5://127.0.0.1:1082",
			Source: NodeSourceInline,
		}},
	}

	release := holdPathLock(t, path)
	errs := runConcurrentUpdates(
		settings.SaveSettings,
		nodes.SaveNodes,
	)
	release()
	assertUpdateResults(t, errs)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var saved Config
	if err := yaml.Unmarshal(data, &saved); err != nil {
		t.Fatalf("decode saved config: %v", err)
	}
	if saved.ExternalIP != settings.ExternalIP {
		t.Fatalf("settings update was lost: external_ip=%q", saved.ExternalIP)
	}
	if len(saved.Nodes) != 1 || saved.Nodes[0].Name != "new-node" {
		t.Fatalf("node update was lost: %#v", saved.Nodes)
	}
}

func TestConcurrentNodeAuthUpdatesPreserveUnrelatedOverrides(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	newConfig := func(uri, username string) *Config {
		return &Config{
			filePath: configPath,
			Mode:     "multi-port",
			Nodes: []NodeConfig{{
				URI:      uri,
				Source:   NodeSourceFile,
				Username: username,
				Password: "listener-password",
			}},
		}
	}
	first := newConfig("socks5://127.0.0.1:1081#first", "first-user")
	second := newConfig("socks5://127.0.0.1:1082#second", "second-user")

	release := holdPathLock(t, first.nodeAuthPath())
	errs := runConcurrentUpdates(
		first.persistNodeAuthOverrides,
		second.persistNodeAuthOverrides,
	)
	release()
	assertUpdateResults(t, errs)

	state, err := first.loadNodeAuthState()
	if err != nil {
		t.Fatalf("load auth state: %v", err)
	}
	for _, cfg := range []*Config{first, second} {
		key := cfg.Nodes[0].NodeKey()
		if override, ok := state.Overrides[key]; !ok || override.Username != cfg.Nodes[0].Username {
			t.Fatalf("override for %q was lost: %#v", key, state.Overrides)
		}
	}
}

func TestConcurrentPortLeaseUpdatesPreserveUnrelatedLeases(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	newConfig := func(uri string, port uint16) *Config {
		return &Config{
			filePath: configPath,
			Mode:     "multi-port",
			MultiPort: MultiPortConfig{
				PortReuseDelay: time.Hour,
			},
			Nodes: []NodeConfig{{URI: uri, Port: port}},
		}
	}
	first := newConfig("socks5://127.0.0.1:1081#first", 32001)
	second := newConfig("socks5://127.0.0.1:1082#second", 32002)

	release := holdPathLock(t, first.portMapPath())
	errs := runConcurrentUpdates(first.PersistPortMap, second.PersistPortMap)
	release()
	assertUpdateResults(t, errs)

	state, err := first.loadPortMappingState()
	if err != nil {
		t.Fatalf("load port map: %v", err)
	}
	for _, cfg := range []*Config{first, second} {
		node := cfg.Nodes[0]
		if lease, ok := state.Leases[node.NodeKey()]; !ok || lease.Port != node.Port {
			t.Fatalf("lease for port %d was lost: %#v", node.Port, state.Leases)
		}
	}
}

// holdPathLock establishes a known contention point so all tested updates are
// concurrent at the sidecar boundary. The returned function releases it.
func holdPathLock(t *testing.T, path string) func() {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- withFileLock(path, func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out acquiring test file lock")
	}
	return func() {
		close(release)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("release test file lock: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out releasing test file lock")
		}
	}
}

func runConcurrentUpdates(updates ...func() error) <-chan error {
	start := make(chan struct{})
	started := sync.WaitGroup{}
	started.Add(len(updates))
	errs := make(chan error, len(updates))
	for _, update := range updates {
		go func(update func() error) {
			<-start
			started.Done()
			errs <- update()
		}(update)
	}
	close(start)
	started.Wait()
	return errs
}

func assertUpdateResults(t *testing.T, errs <-chan error) {
	t.Helper()
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("concurrent update failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for concurrent update")
		}
	}
}
