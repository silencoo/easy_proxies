package subscription

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"easy_proxies/internal/config"
)

type fakeBoxManager struct {
	mu        sync.Mutex
	reloadErr error
	config    *config.Config
}

func (f *fakeBoxManager) CurrentPortMap() map[string]uint16 {
	return nil
}

func (f *fakeBoxManager) ReloadWithPortMap(newCfg *config.Config, _ map[string]uint16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.config = newCfg
	return f.reloadErr
}

func (f *fakeBoxManager) currentConfig() *config.Config {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.config
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
	runtimeCfg := fake.currentConfig()
	if runtimeCfg == nil || len(runtimeCfg.Nodes) != 1 || runtimeCfg.Nodes[0].Source != config.NodeSourceSubscription {
		t.Fatalf("runtime did not receive subscription node: %#v", runtimeCfg)
	}
	if status := manager.Status(); status.LastError != "" || status.NodeCount != 1 {
		t.Fatalf("unexpected refresh status: %#v", status)
	}
}

func TestRefreshUsesPerURLCacheForOnlyFailedSource(t *testing.T) {
	var secondFails atomic.Bool
	var firstVersion atomic.Int32
	firstVersion.Store(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/first":
			_, _ = w.Write([]byte("trojan://pw@first-v" + fmt.Sprint(firstVersion.Load()) + ".example:443#first\n"))
		case "/second":
			if secondFails.Load() {
				http.Error(w, "temporary failure", http.StatusBadGateway)
				return
			}
			_, _ = w.Write([]byte("trojan://pw@second.example:443#second\n"))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(tempDir, []string{server.URL + "/first", server.URL + "/second"})
	fake := &fakeBoxManager{}
	manager := New(cfg, fake)
	defer manager.Stop()
	if err := manager.doRefresh(); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}

	firstVersion.Store(2)
	secondFails.Store(true)
	if err := manager.doRefresh(); err != nil {
		t.Fatalf("partial refresh with source cache: %v", err)
	}
	runtimeCfg := fake.currentConfig()
	joined := nodeURIs(runtimeCfg.Nodes)
	if !strings.Contains(joined, "first-v2.example") || !strings.Contains(joined, "second.example") || strings.Contains(joined, "first-v1.example") {
		t.Fatalf("per-URL fallback did not merge fresh and cached sources: %s", joined)
	}
}

func TestRefreshUsesAggregateCacheBeforePerURLCacheExists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/failed" {
			http.Error(w, "temporary failure", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte("trojan://pw@fresh.example:443#fresh\n"))
	}))
	defer server.Close()

	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(tempDir, []string{server.URL + "/fresh", server.URL + "/failed"})
	oldURI := "trojan://pw@last-known-good.example:443#old"
	if err := os.WriteFile(cfg.NodesFile, []byte(oldURI+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := &fakeBoxManager{}
	manager := New(cfg, fake)
	defer manager.Stop()
	if err := manager.doRefresh(); err != nil {
		t.Fatalf("aggregate fallback refresh: %v", err)
	}
	runtimeCfg := fake.currentConfig()
	if got := nodeURIs(runtimeCfg.Nodes); got != oldURI {
		t.Fatalf("first partial refresh should keep aggregate cache, got %q", got)
	}
}

func TestRefreshPreservesInlineNodesAndKeepsThemOutOfNodesFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("trojan://pw@subscription.example:443#subscription\n"))
	}))
	defer server.Close()

	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(tempDir, []string{server.URL})
	inlineURI := "socks5://user:pass@127.0.0.1:1080#manual"
	cfg.Nodes = []config.NodeConfig{{Name: "manual", URI: inlineURI, Source: config.NodeSourceInline}}
	fake := &fakeBoxManager{}
	manager := New(cfg, fake)
	defer manager.Stop()
	if err := manager.doRefresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	runtimeCfg := fake.currentConfig()
	if len(runtimeCfg.Nodes) != 2 || runtimeCfg.Nodes[0].Source != config.NodeSourceInline {
		t.Fatalf("inline node was not preserved: %#v", runtimeCfg.Nodes)
	}
	fileContent, err := os.ReadFile(cfg.NodesFile)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(fileContent), inlineURI) || !strings.Contains(string(fileContent), "subscription.example") {
		t.Fatalf("nodes file mixed inline and subscription nodes: %q", fileContent)
	}
}

func TestConcurrentRefreshWaitersAreNotDropped(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() { close(entered) })
		<-release
		_, _ = w.Write([]byte("trojan://pw@node.example:443#node\n"))
	}))
	defer server.Close()

	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(tempDir, []string{server.URL})
	cfg.SubscriptionRefresh.Timeout = 3 * time.Second
	fake := &fakeBoxManager{}
	manager := New(cfg, fake)
	defer manager.Stop()
	manager.Start()

	const callers = 8
	start := make(chan struct{})
	errors := make(chan error, callers)
	for index := 0; index < callers; index++ {
		go func() {
			<-start
			errors <- manager.RefreshNow()
		}()
	}
	close(start)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	time.Sleep(25 * time.Millisecond)
	close(release)
	for index := 0; index < callers; index++ {
		if err := <-errors; err != nil {
			t.Fatalf("refresh waiter %d failed: %v", index, err)
		}
	}
	status := manager.Status()
	if status.IsRefreshing || status.RefreshCount == 0 || status.LastError != "" {
		t.Fatalf("unexpected final refresh status: %+v", status)
	}
}

func newSubscriptionTestConfig(tempDir string, subscriptions []string) *config.Config {
	cfg := &config.Config{
		Mode:          "pool",
		NodesFile:     filepath.Join(tempDir, "nodes.txt"),
		Subscriptions: append([]string(nil), subscriptions...),
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			Timeout:            2 * time.Second,
			HealthCheckTimeout: 100 * time.Millisecond,
			FetchConcurrency:   4,
		},
	}
	cfg.SetFilePath(filepath.Join(tempDir, "config.yaml"))
	return cfg
}

func nodeURIs(nodes []config.NodeConfig) string {
	values := make([]string, 0, len(nodes))
	for _, node := range nodes {
		values = append(values, node.URI)
	}
	return strings.Join(values, "\n")
}
