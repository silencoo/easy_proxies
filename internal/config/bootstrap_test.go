package config

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestEnsureDefaultFileCreatesLoadableManagementOnlyConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "config.yaml")

	created, err := EnsureDefaultFile(path)
	if err != nil {
		t.Fatalf("create default config: %v", err)
	}
	if !created {
		t.Fatal("first call reported that it did not create the config")
	}

	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated config: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load generated config: %v", err)
	}
	if !cfg.ManagementEnabled() {
		t.Fatal("generated config must enable management")
	}
	if cfg.Management.Listen != "127.0.0.1:9091" {
		t.Fatalf("management listen = %q, want loopback default", cfg.Management.Listen)
	}
	if cfg.Listener.Address != "127.0.0.1" {
		t.Fatalf("listener address = %q, want loopback", cfg.Listener.Address)
	}
	if !cfg.SubscriptionRefresh.Enabled {
		t.Fatal("generated config must enable subscription refresh")
	}
	if len(cfg.Nodes) != 0 || len(cfg.Subscriptions) != 0 {
		t.Fatalf("generated config should start empty: nodes=%d subscriptions=%d", len(cfg.Nodes), len(cfg.Subscriptions))
	}

	created, err = EnsureDefaultFile(path)
	if err != nil {
		t.Fatalf("preserve existing config: %v", err)
	}
	if created {
		t.Fatal("second call unexpectedly replaced the existing config")
	}
	second, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read preserved config: %v", err)
	}
	if string(first) != string(second) {
		t.Fatal("existing config changed on the second call")
	}
}

func TestEnsureDefaultFileConcurrentCreation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	const callers = 8
	var createdCount atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, callers)

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			created, err := EnsureDefaultFile(path)
			if err != nil {
				errs <- err
				return
			}
			if created {
				createdCount.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent creation failed: %v", err)
	}
	if got := createdCount.Load(); got != 1 {
		t.Fatalf("created count = %d, want 1", got)
	}
	if _, err := Load(path); err != nil {
		t.Fatalf("load concurrently generated config: %v", err)
	}
}

func TestEmptyNodesRequireManagement(t *testing.T) {
	enabled := true
	cfg := &Config{Management: ManagementConfig{Enabled: &enabled, Listen: "127.0.0.1:9091", ProbeTarget: "www.apple.com:80"}}
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("management-only config should be valid: %v", err)
	}

	disabled := false
	cfg.Management.Enabled = &disabled
	if err := cfg.NormalizeWithPortMap(nil); err == nil {
		t.Fatal("empty nodes unexpectedly accepted with management disabled")
	}
}
