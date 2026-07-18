package config

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSaveNodesTransactionRollsBackEveryFileOnLateFailure(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	nodesPath := filepath.Join(dir, "nodes.txt")
	originalConfig := []byte("mode: pool\nnodes_file: nodes.txt\nnodes:\n  - name: inline-old\n    uri: socks5://127.0.0.1:1080\n")
	originalNodes := []byte("socks5://127.0.0.1:1081#external-old\n")
	if err := os.WriteFile(configPath, originalConfig, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nodesPath, originalNodes, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Nodes = []NodeConfig{
		{Name: "inline-new", URI: "socks5://127.0.0.1:1082", Source: NodeSourceInline},
		{Name: "external-new", URI: "socks5://127.0.0.1:1083", Source: NodeSourceSubscription},
	}

	previousHook := removeNodeAuthOverridesForTransaction
	removeNodeAuthOverridesForTransaction = func(*Config, []NodeConfig) (FileSnapshot, bool, error) {
		return FileSnapshot{}, false, errors.New("injected late sidecar failure")
	}
	t.Cleanup(func() { removeNodeAuthOverridesForTransaction = previousHook })
	if _, err := cfg.SaveNodesTransaction([]NodeConfig{{URI: "socks5://127.0.0.1:1081"}}); err == nil {
		t.Fatal("transaction unexpectedly succeeded")
	}

	if got, err := os.ReadFile(configPath); err != nil || string(got) != string(originalConfig) {
		t.Fatalf("config rollback failed: err=%v data=%q", err, got)
	}
	if got, err := os.ReadFile(nodesPath); err != nil || string(got) != string(originalNodes) {
		t.Fatalf("nodes rollback failed: err=%v data=%q", err, got)
	}
	if _, err := os.Stat(cfg.nodeAuthPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new auth sidecar survived rollback: %v", err)
	}
}

func TestSaveNodesTransactionSuccessfulRollbackIsCompleteAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	nodesPath := filepath.Join(dir, "nodes.txt")
	originalConfig := []byte("mode: pool\nnodes_file: nodes.txt\nnodes:\n  - name: inline-old\n    uri: socks5://127.0.0.1:1080\n")
	originalNodes := []byte("socks5://127.0.0.1:1081#external-old\n")
	if err := os.WriteFile(configPath, originalConfig, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nodesPath, originalNodes, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Nodes = []NodeConfig{
		{Name: "inline-new", URI: "socks5://127.0.0.1:1082", Source: NodeSourceInline},
		{Name: "external-new", URI: "socks5://127.0.0.1:1083", Source: NodeSourceSubscription},
	}

	rollback, err := cfg.SaveNodesTransaction(nil)
	if err != nil {
		t.Fatalf("save nodes transaction: %v", err)
	}
	if err := rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if err := rollback(); err != nil {
		t.Fatalf("idempotent rollback: %v", err)
	}
	if got, err := os.ReadFile(configPath); err != nil || string(got) != string(originalConfig) {
		t.Fatalf("config rollback failed: err=%v data=%q", err, got)
	}
	if got, err := os.ReadFile(nodesPath); err != nil || string(got) != string(originalNodes) {
		t.Fatalf("nodes rollback failed: err=%v data=%q", err, got)
	}
	if _, err := os.Stat(cfg.nodeAuthPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new auth sidecar survived rollback: %v", err)
	}
}

func TestSaveNodesTransactionConflictDoesNotPartiallyRollbackOtherFiles(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	nodesPath := filepath.Join(dir, "nodes.txt")
	originalConfig := []byte("mode: pool\nnodes_file: nodes.txt\nnodes:\n  - name: inline-old\n    uri: socks5://127.0.0.1:1080\n")
	originalNodes := []byte("socks5://127.0.0.1:1081#external-old\n")
	if err := os.WriteFile(configPath, originalConfig, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nodesPath, originalNodes, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Nodes = []NodeConfig{
		{Name: "inline-new", URI: "socks5://127.0.0.1:1082", Source: NodeSourceInline},
		{Name: "external-new", URI: "socks5://127.0.0.1:1083", Source: NodeSourceSubscription},
	}

	rollback, err := cfg.SaveNodesTransaction(nil)
	if err != nil {
		t.Fatalf("save nodes transaction: %v", err)
	}
	newerConfig := []byte("mode: pool\nexternal_ip: 203.0.113.9\nnodes_file: nodes.txt\n")
	if err := WriteFileAtomic(configPath, newerConfig, 0o600); err != nil {
		t.Fatalf("concurrent config write: %v", err)
	}

	err = rollback()
	if !errors.Is(err, ErrRollbackConflict) {
		t.Fatalf("rollback error = %v, want ErrRollbackConflict", err)
	}
	if got, readErr := os.ReadFile(configPath); readErr != nil || string(got) != string(newerConfig) {
		t.Fatalf("rollback overwrote newer config: err=%v data=%q", readErr, got)
	}
	if got, readErr := os.ReadFile(nodesPath); readErr != nil || string(got) == string(originalNodes) || !bytes.Contains(got, []byte("127.0.0.1:1083")) {
		t.Fatalf("conflicting rollback partially restored nodes: err=%v data=%q", readErr, got)
	}
}

func TestPersistPortMapTransactionRollbackPreservesNewerConcurrentWrite(t *testing.T) {
	dir := t.TempDir()
	portMapPath := filepath.Join(dir, "port-map.yaml")
	cfg := &Config{
		Mode: "multi-port",
		MultiPort: MultiPortConfig{
			PortMapFile: portMapPath,
		},
		Nodes: []NodeConfig{{
			Name: "node",
			URI:  "socks5://127.0.0.1:1080#node",
			Port: 12001,
		}},
	}
	cfg.SetFilePath(filepath.Join(dir, "config.yaml"))
	if err := cfg.PersistPortMap(); err != nil {
		t.Fatalf("persist initial port map: %v", err)
	}
	cfg.Nodes[0].Port = 12002
	rollback, err := cfg.PersistPortMapTransaction()
	if err != nil {
		t.Fatalf("persist port map transaction: %v", err)
	}

	newerPortMap := []byte("version: 1\nleases:\n  concurrent-process:\n    port: 13001\n")
	if err := WriteFileAtomic(portMapPath, newerPortMap, 0o600); err != nil {
		t.Fatalf("concurrent port-map write: %v", err)
	}
	err = rollback()
	if !errors.Is(err, ErrRollbackConflict) {
		t.Fatalf("rollback error = %v, want ErrRollbackConflict", err)
	}
	if got, readErr := os.ReadFile(portMapPath); readErr != nil || string(got) != string(newerPortMap) {
		t.Fatalf("rollback overwrote newer port map: err=%v data=%q", readErr, got)
	}
}

func TestSaveSubscriptionStateTransactionClearsAuthAndRollsBackAllFiles(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	nodesPath := filepath.Join(dir, "nodes.txt")
	seedConfig := []byte("mode: pool\nnodes_file: nodes.txt\nmanagement:\n  listen: 127.0.0.1:9091\n  probe_target: example.com:80\nnodes:\n  - name: inline\n    uri: socks5://127.0.0.1:1080\n")
	seedNodes := []byte("socks5://127.0.0.1:1081#external\n")
	if err := os.WriteFile(configPath, seedConfig, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nodesPath, seedNodes, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.updateNodeAuthState(func(state *nodeAuthState) {
		state.Overrides["external-key"] = nodeAuthOverride{Username: "secret-user", Password: "secret-password"}
	}); err != nil {
		t.Fatal(err)
	}
	authBefore, err := os.ReadFile(cfg.nodeAuthPath())
	if err != nil {
		t.Fatal(err)
	}

	rollback, err := cfg.SaveSubscriptionStateTransaction(nil, true)
	if err != nil {
		t.Fatalf("save subscription state: %v", err)
	}
	if data, err := os.ReadFile(nodesPath); err != nil || len(data) != 0 {
		t.Fatalf("nodes cache was not cleared: err=%v data=%q", err, data)
	}
	state, err := cfg.loadNodeAuthState()
	if err != nil || len(state.Overrides) != 0 {
		t.Fatalf("auth sidecar was not cleared: err=%v state=%#v", err, state)
	}

	if err := rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if got, err := os.ReadFile(configPath); err != nil || !bytes.Equal(got, seedConfig) {
		t.Fatalf("config rollback failed: err=%v data=%q", err, got)
	}
	if got, err := os.ReadFile(nodesPath); err != nil || !bytes.Equal(got, seedNodes) {
		t.Fatalf("nodes rollback failed: err=%v data=%q", err, got)
	}
	if got, err := os.ReadFile(cfg.nodeAuthPath()); err != nil || !bytes.Equal(got, authBefore) {
		t.Fatalf("auth rollback failed: err=%v data=%q", err, got)
	}
}
