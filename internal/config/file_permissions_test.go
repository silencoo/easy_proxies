package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCredentialBearingFilesAreWrittenOwnerOnly(t *testing.T) {
	dir := t.TempDir()

	nodesPath := filepath.Join(dir, "nodes.txt")
	if err := writeNodesToFile(nodesPath, []NodeConfig{{
		Name: "test-node",
		URI:  "socks5://test-user:test-only-password@127.0.0.1:1080",
	}}); err != nil {
		t.Fatalf("write nodes: %v", err)
	}
	assertOwnerOnlyFileMode(t, nodesPath)

	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("mode: pool\nnodes:\n  - uri: socks5://127.0.0.1:1080\n"), 0o644); err != nil {
		t.Fatalf("seed config: %v", err)
	}
	cfg := &Config{
		filePath: configPath,
		Mode:     "pool",
		Listener: ListenerConfig{Password: "test-only-password"},
	}
	if err := cfg.SaveSettings(); err != nil {
		t.Fatalf("save settings: %v", err)
	}
	assertOwnerOnlyFileMode(t, configPath)
}

func TestAtomicWriteDoesNotWidenExistingStricterMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix permission bits consistently")
	}

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("old"), 0o400); err != nil {
		t.Fatalf("seed restrictive file: %v", err)
	}
	if err := writeFileWithLock(path, []byte("new"), 0o600); err != nil {
		t.Fatalf("replace restrictive file: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat restrictive file: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o400 {
		t.Fatalf("file mode widened: got %04o, want 0400", got)
	}
}

func assertOwnerOnlyFileMode(t *testing.T, path string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		return
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", filepath.Base(path), err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("%s mode = %04o, want 0600", filepath.Base(path), got)
	}
}
