package subscription

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"easy_proxies/internal/config"
)

type fakeBoxManager struct {
	reloadErr error
	config    *config.Config
}

func (f *fakeBoxManager) CurrentPortMap() map[string]uint16 {
	return nil
}

func (f *fakeBoxManager) ReloadWithPortMap(newCfg *config.Config, _ map[string]uint16) error {
	f.config = newCfg
	return f.reloadErr
}

func TestRefreshRestoresNodeFileWhenRuntimeReloadFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("socks5://new-user:new-password@127.0.0.1:1080#new-node\n"))
	}))
	defer server.Close()

	tempDir := t.TempDir()
	nodesPath := filepath.Join(tempDir, "nodes.txt")
	oldContent := []byte("socks5://old-user:old-password@127.0.0.1:1081#old-node\n")
	if err := os.WriteFile(nodesPath, oldContent, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		Mode:          "pool",
		NodesFile:     nodesPath,
		Subscriptions: []string{server.URL},
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			Timeout:            2 * time.Second,
			HealthCheckTimeout: 2 * time.Second,
		},
	}
	cfg.SetFilePath(filepath.Join(tempDir, "config.yaml"))
	fake := &fakeBoxManager{reloadErr: errors.New("replacement failed")}
	manager := New(cfg, fake)
	defer manager.Stop()

	manager.doRefresh()

	got, err := os.ReadFile(nodesPath)
	if err != nil {
		t.Fatalf("read restored nodes file: %v", err)
	}
	if string(got) != string(oldContent) {
		t.Fatalf("failed refresh replaced bootable nodes file:\n got %q\nwant %q", got, oldContent)
	}
	if status := manager.Status(); status.LastError == "" {
		t.Fatal("failed refresh did not report an error")
	}
}

func TestRefreshCommitsNodeFileAfterSuccessfulRuntimeReload(t *testing.T) {
	newURI := "socks5://new-user:new-password@127.0.0.1:1080#new-node"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(newURI + "\n"))
	}))
	defer server.Close()

	tempDir := t.TempDir()
	nodesPath := filepath.Join(tempDir, "nodes.txt")
	if err := os.WriteFile(nodesPath, []byte("socks5://old@127.0.0.1:1081#old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		Mode:          "pool",
		NodesFile:     nodesPath,
		Subscriptions: []string{server.URL},
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			Timeout:            2 * time.Second,
			HealthCheckTimeout: 2 * time.Second,
		},
	}
	cfg.SetFilePath(filepath.Join(tempDir, "config.yaml"))
	fake := &fakeBoxManager{}
	manager := New(cfg, fake)
	defer manager.Stop()

	manager.doRefresh()

	got, err := os.ReadFile(nodesPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != newURI+"\n" {
		t.Fatalf("unexpected committed nodes: %q", got)
	}
	if fake.config == nil || len(fake.config.Nodes) != 1 || fake.config.Nodes[0].Source != config.NodeSourceSubscription {
		t.Fatalf("runtime did not receive subscription node: %#v", fake.config)
	}
	if status := manager.Status(); status.LastError != "" || status.NodeCount != 1 {
		t.Fatalf("unexpected refresh status: %#v", status)
	}
}
