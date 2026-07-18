package subscription

import (
	"context"
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

	"easy_proxies/internal/commitguard"
	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
)

type fakeBoxManager struct {
	mu            sync.Mutex
	reloadErr     error
	config        *config.Config
	revision      uint64
	beforePublish func()
	afterPublish  func()
}

func newFakeBoxManager(cfg *config.Config) *fakeBoxManager {
	return &fakeBoxManager{config: cfg.Clone(), revision: 1}
}

func (f *fakeBoxManager) ConfigSnapshot() (*config.Config, uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.config.Clone(), f.revision
}

func (f *fakeBoxManager) CommitConfig(ctx context.Context, expectedRevision uint64, candidate *config.Config, persist func(*config.Config) (func() error, error)) error {
	f.mu.Lock()
	if err := ctx.Err(); err != nil {
		f.mu.Unlock()
		return err
	}
	if expectedRevision != f.revision {
		current := f.revision
		f.mu.Unlock()
		return fmt.Errorf("revision conflict: expected %d, current %d", expectedRevision, current)
	}
	owned := candidate.Clone()
	if err := owned.NormalizeWithPortMap(nil); err != nil {
		f.mu.Unlock()
		return err
	}
	var rollback func() error
	if persist != nil {
		var err error
		rollback, err = persist(owned.Clone())
		if err != nil {
			f.mu.Unlock()
			if rollback != nil {
				_ = rollback()
			}
			return err
		}
	}
	if f.reloadErr != nil {
		reloadErr := f.reloadErr
		f.mu.Unlock()
		if rollback != nil {
			_ = rollback()
		}
		return reloadErr
	}
	beforePublish := f.beforePublish
	afterPublish := f.afterPublish
	f.mu.Unlock()
	if beforePublish != nil {
		beforePublish()
	}
	markCommitted, releaseCommitBarrier, err := commitguard.Acquire(ctx)
	if err != nil {
		if rollback != nil {
			_ = rollback()
		}
		return err
	}
	f.mu.Lock()
	if expectedRevision != f.revision {
		current := f.revision
		f.mu.Unlock()
		releaseCommitBarrier()
		if rollback != nil {
			_ = rollback()
		}
		return fmt.Errorf("revision conflict: expected %d, current %d", expectedRevision, current)
	}
	f.config = owned
	f.revision++
	f.mu.Unlock()
	markCommitted()
	releaseCommitBarrier()
	if afterPublish != nil {
		afterPublish()
	}
	return nil
}

func (f *fakeBoxManager) currentConfig() *config.Config {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.config.Clone()
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
			Timeout:              2 * time.Second,
			HealthCheckTimeout:   2 * time.Second,
			AllowPrivateNetworks: true,
		},
	}
	cfg.SetFilePath(filepath.Join(tempDir, "config.yaml"))
	writeSubscriptionTestConfig(t, cfg)
	fake := newFakeBoxManager(cfg)
	fake.reloadErr = errors.New("replacement failed")
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
			Timeout:              2 * time.Second,
			HealthCheckTimeout:   2 * time.Second,
			AllowPrivateNetworks: true,
		},
	}
	cfg.SetFilePath(filepath.Join(tempDir, "config.yaml"))
	writeSubscriptionTestConfig(t, cfg)
	fake := newFakeBoxManager(cfg)
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
	cfg := newSubscriptionTestConfig(t, tempDir, []string{server.URL + "/first", server.URL + "/second"})
	fake := newFakeBoxManager(cfg)
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
	cfg := newSubscriptionTestConfig(t, tempDir, []string{server.URL + "/fresh", server.URL + "/failed"})
	oldURI := "trojan://pw@last-known-good.example:443#old"
	if err := os.WriteFile(cfg.NodesFile, []byte(oldURI+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := newFakeBoxManager(cfg)
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
	cfg := newSubscriptionTestConfig(t, tempDir, []string{server.URL})
	inlineURI := "socks5://user:pass@127.0.0.1:1080#manual"
	cfg.Nodes = []config.NodeConfig{{Name: "manual", URI: inlineURI, Source: config.NodeSourceInline}}
	fake := newFakeBoxManager(cfg)
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
	cfg := newSubscriptionTestConfig(t, tempDir, []string{server.URL})
	cfg.SubscriptionRefresh.Timeout = 3 * time.Second
	fake := newFakeBoxManager(cfg)
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

func newSubscriptionTestConfig(t *testing.T, tempDir string, subscriptions []string) *config.Config {
	t.Helper()
	cfg := &config.Config{
		Mode:          "pool",
		NodesFile:     filepath.Join(tempDir, "nodes.txt"),
		Subscriptions: append([]string(nil), subscriptions...),
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			Timeout:              2 * time.Second,
			HealthCheckTimeout:   100 * time.Millisecond,
			FetchConcurrency:     4,
			AllowPrivateNetworks: true,
		},
	}
	cfg.SetFilePath(filepath.Join(tempDir, "config.yaml"))
	writeSubscriptionTestConfig(t, cfg)
	return cfg
}

func writeSubscriptionTestConfig(t *testing.T, cfg *config.Config) {
	t.Helper()
	if err := os.WriteFile(cfg.FilePath(), []byte("mode: pool\n"), 0o600); err != nil {
		t.Fatalf("write config fixture: %v", err)
	}
}

func TestNodesModifiedRemainsLatchedUntilSuccessfulRefresh(t *testing.T) {
	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(t, tempDir, nil)
	manager := New(cfg, newFakeBoxManager(cfg))
	defer manager.Stop()

	original := []config.NodeConfig{{URI: "socks5://127.0.0.1:1080#old"}}
	changed := []config.NodeConfig{{URI: "socks5://127.0.0.1:1081#new"}}
	if err := config.WriteNodesToFile(cfg.NodesFile, changed); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	manager.lastSubHash = manager.computeNodesHash(original)
	manager.lastNodesModTime = time.Now().Add(-time.Hour)
	manager.mu.Unlock()

	if !manager.Status().NodesModified {
		t.Fatal("first status did not detect the modified node file")
	}
	if !manager.Status().NodesModified {
		t.Fatal("modified state was not latched across status reads")
	}
}

func TestRefreshRebasesOntoConcurrentConfigAndInlineNodeChanges(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		_, _ = w.Write([]byte("trojan://pw@subscription.example:443#subscription\n"))
	}))
	defer server.Close()

	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(t, tempDir, []string{server.URL})
	cfg.Nodes = []config.NodeConfig{{Name: "first-inline", URI: "socks5://127.0.0.1:1080#first", Source: config.NodeSourceInline}}
	fake := newFakeBoxManager(cfg)
	manager := New(cfg, fake)
	defer manager.Stop()

	refreshDone := make(chan error, 1)
	go func() { refreshDone <- manager.doRefresh() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("subscription fetch did not start")
	}

	concurrent, revision := fake.ConfigSnapshot()
	concurrent.ExternalIP = "203.0.113.9"
	concurrent.Nodes = append(concurrent.Nodes, config.NodeConfig{
		Name:   "second-inline",
		URI:    "socks5://127.0.0.1:1081#second",
		Source: config.NodeSourceInline,
	})
	if err := fake.CommitConfig(context.Background(), revision, concurrent, nil); err != nil {
		t.Fatalf("commit concurrent config: %v", err)
	}
	close(release)
	if err := <-refreshDone; err != nil {
		t.Fatalf("refresh: %v", err)
	}

	committed := fake.currentConfig()
	if committed.ExternalIP != "203.0.113.9" {
		t.Fatalf("concurrent settings update was lost: external_ip=%q", committed.ExternalIP)
	}
	if got := nodeURIs(committed.Nodes); !strings.Contains(got, "#first") || !strings.Contains(got, "#second") || !strings.Contains(got, "subscription.example") {
		t.Fatalf("concurrent inline node update was lost: %s", got)
	}
}

func TestRefreshPersistenceFailureRollsBackFilesAndRuntime(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()

	for _, testCase := range []struct {
		name string
		fail func(*Manager)
	}{
		{
			name: "save settings",
			fail: func(manager *Manager) {
				manager.saveSettingsFn = func(*config.Config) error { return errors.New("save failed") }
			},
		},
		{
			name: "write nodes after settings",
			fail: func(manager *Manager) {
				manager.writeNodesFn = func(string, []config.NodeConfig) error { return errors.New("write failed") }
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tempDir := t.TempDir()
			cfg := newSubscriptionTestConfig(t, tempDir, []string{server.URL})
			cfg.Nodes = []config.NodeConfig{{Name: "old", URI: "socks5://127.0.0.1:1080#old", Source: config.NodeSourceInline}}
			oldNodes := []byte("trojan://pw@old.example:443#old-subscription\n")
			if err := os.WriteFile(cfg.NodesFile, oldNodes, 0o600); err != nil {
				t.Fatal(err)
			}
			oldConfig, err := os.ReadFile(cfg.FilePath())
			if err != nil {
				t.Fatal(err)
			}
			fake := newFakeBoxManager(cfg)
			manager := New(cfg, fake)
			defer manager.Stop()
			testCase.fail(manager)

			if err := manager.doRefresh(); err == nil {
				t.Fatal("refresh unexpectedly succeeded")
			}
			gotConfig, err := os.ReadFile(cfg.FilePath())
			if err != nil {
				t.Fatal(err)
			}
			gotNodes, err := os.ReadFile(cfg.NodesFile)
			if err != nil {
				t.Fatal(err)
			}
			if string(gotConfig) != string(oldConfig) || string(gotNodes) != string(oldNodes) {
				t.Fatalf("failed transaction changed disk:\nconfig=%q\nnodes=%q", gotConfig, gotNodes)
			}
			if got := nodeURIs(fake.currentConfig().Nodes); strings.Contains(got, "new.example") {
				t.Fatalf("failed transaction changed runtime: %s", got)
			}
		})
	}
}

func TestSubscriptionPersistenceRollbackPreservesNewerConcurrentWrites(t *testing.T) {
	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(t, tempDir, []string{"https://old.example/subscription"})
	oldNodes := []byte("trojan://old@old.example:443#old\n")
	if err := os.WriteFile(cfg.NodesFile, oldNodes, 0o600); err != nil {
		t.Fatal(err)
	}
	manager := New(cfg, newFakeBoxManager(cfg))
	defer manager.Stop()

	candidate := cfg.Clone()
	candidate.Subscriptions = []string{"https://new.example/subscription"}
	rollback, err := manager.persistSubscriptionState(candidate, []config.NodeConfig{{
		Name:   "new",
		URI:    "trojan://new@new.example:443#new",
		Source: config.NodeSourceSubscription,
	}})
	if err != nil {
		t.Fatalf("persist subscription transaction: %v", err)
	}

	newerConfig := []byte("mode: pool\nexternal_ip: 203.0.113.8\n")
	newerNodes := []byte("socks5://127.0.0.1:13000#concurrent\n")
	if err := config.WriteFileAtomic(cfg.FilePath(), newerConfig, 0o600); err != nil {
		t.Fatalf("concurrent config write: %v", err)
	}
	if err := config.WriteFileAtomic(cfg.NodesFile, newerNodes, 0o600); err != nil {
		t.Fatalf("concurrent nodes write: %v", err)
	}

	err = rollback()
	if !errors.Is(err, config.ErrRollbackConflict) {
		t.Fatalf("rollback error = %v, want ErrRollbackConflict", err)
	}
	if got, readErr := os.ReadFile(cfg.FilePath()); readErr != nil || string(got) != string(newerConfig) {
		t.Fatalf("rollback overwrote newer config: err=%v data=%q", readErr, got)
	}
	if got, readErr := os.ReadFile(cfg.NodesFile); readErr != nil || string(got) != string(newerNodes) {
		t.Fatalf("rollback overwrote newer nodes: err=%v data=%q", readErr, got)
	}
}

func TestClearingSubscriptionsRemovesSubscriptionNodesAndKeepsInlineNodes(t *testing.T) {
	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(t, tempDir, []string{"https://subscriptions.example/list"})
	cfg.Nodes = []config.NodeConfig{
		{Name: "inline", URI: "socks5://127.0.0.1:1080#inline", Source: config.NodeSourceInline},
		{Name: "subscription", URI: "trojan://pw@subscription.example:443#subscription", Source: config.NodeSourceSubscription},
	}
	if err := os.WriteFile(cfg.NodesFile, []byte(cfg.Nodes[1].URI+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := newFakeBoxManager(cfg)
	manager := New(cfg, fake)
	defer manager.Stop()

	if err := manager.UpdateConfigAndRefresh(nil, false, time.Hour, 4, false); err != nil {
		t.Fatalf("clear subscriptions: %v", err)
	}
	committed := fake.currentConfig()
	if len(committed.Subscriptions) != 0 || len(committed.Nodes) != 1 || committed.Nodes[0].Source != config.NodeSourceInline {
		t.Fatalf("unexpected cleared config: subscriptions=%v nodes=%#v", committed.Subscriptions, committed.Nodes)
	}
	data, err := os.ReadFile(cfg.NodesFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("subscription cache was not cleared: %q", data)
	}
}

func TestClearingSubscriptionsRejectsAResultWithNoNodes(t *testing.T) {
	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(t, tempDir, []string{"https://subscriptions.example/list"})
	cfg.Nodes = []config.NodeConfig{{Name: "subscription", URI: "trojan://pw@subscription.example:443#subscription", Source: config.NodeSourceSubscription}}
	oldNodes := []byte(cfg.Nodes[0].URI + "\n")
	if err := os.WriteFile(cfg.NodesFile, oldNodes, 0o600); err != nil {
		t.Fatal(err)
	}
	oldConfig, err := os.ReadFile(cfg.FilePath())
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeBoxManager(cfg)
	manager := New(cfg, fake)
	defer manager.Stop()

	if err := manager.UpdateConfigAndRefresh(nil, false, time.Hour, 4, false); err == nil {
		t.Fatal("clear unexpectedly accepted an empty runtime config")
	}
	committed := fake.currentConfig()
	if len(committed.Subscriptions) != 1 || len(committed.Nodes) != 1 {
		t.Fatalf("rejected clear changed runtime: subscriptions=%v nodes=%#v", committed.Subscriptions, committed.Nodes)
	}
	gotConfig, _ := os.ReadFile(cfg.FilePath())
	gotNodes, _ := os.ReadFile(cfg.NodesFile)
	if string(gotConfig) != string(oldConfig) || string(gotNodes) != string(oldNodes) {
		t.Fatalf("rejected clear changed disk: config=%q nodes=%q", gotConfig, gotNodes)
	}
}

func TestUpdateWaiterIsNotCompletedByAnEarlierRefresh(t *testing.T) {
	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	secondEntered := make(chan struct{})
	secondRelease := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/first":
			close(firstEntered)
			<-firstRelease
			_, _ = w.Write([]byte("trojan://pw@first.example:443#first\n"))
		case "/second":
			close(secondEntered)
			<-secondRelease
			_, _ = w.Write([]byte("trojan://pw@second.example:443#second\n"))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(t, tempDir, []string{server.URL + "/first"})
	fake := newFakeBoxManager(cfg)
	manager := New(cfg, fake)
	defer manager.Stop()
	manager.Start()

	firstDone := make(chan error, 1)
	go func() { firstDone <- manager.RefreshNow() }()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first refresh did not start")
	}
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- manager.UpdateConfigAndRefresh([]string{server.URL + "/second"}, true, time.Hour, 4, true)
	}()
	close(firstRelease)
	if err := <-firstDone; err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	select {
	case <-secondEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("second refresh did not start")
	}
	select {
	case err := <-secondDone:
		t.Fatalf("second waiter completed from the earlier refresh: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(secondRelease)
	if err := <-secondDone; err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	if got := nodeURIs(fake.currentConfig().Nodes); !strings.Contains(got, "second.example") || strings.Contains(got, "first.example") {
		t.Fatalf("second refresh did not publish its own result: %s", got)
	}
}

func TestStopIsIdempotentWaitsForLoopAndCancelsWaiters(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		close(entered)
		<-request.Context().Done()
	}))
	defer server.Close()

	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(t, tempDir, []string{server.URL})
	cfg.SubscriptionRefresh.Timeout = 10 * time.Second
	manager := New(cfg, newFakeBoxManager(cfg))
	manager.Start()
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- manager.RefreshNow() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}

	stopDone := make(chan struct{}, 2)
	go func() { manager.Stop(); stopDone <- struct{}{} }()
	go func() { manager.Stop(); stopDone <- struct{}{} }()
	for index := 0; index < 2; index++ {
		select {
		case <-stopDone:
		case <-time.After(2 * time.Second):
			t.Fatal("Stop did not wait for and finish the refresh loop")
		}
	}
	if err := <-refreshDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("refresh waiter error = %v, want context.Canceled", err)
	}
	manager.mu.RLock()
	waiterCount := len(manager.waiters)
	manager.mu.RUnlock()
	if waiterCount != 0 {
		t.Fatalf("Stop left %d refresh waiters", waiterCount)
	}
	manager.Stop()
}

func TestUpdateTimeoutCancelsBatchBeforeItCanCommit(t *testing.T) {
	secondEntered := make(chan struct{})
	releaseSecond := make(chan struct{})
	var releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/slow-first":
			time.Sleep(250 * time.Millisecond)
			_, _ = w.Write([]byte("trojan://pw@first.example:443#first\n"))
		case "/blocked-second":
			close(secondEntered)
			// Deliberately ignore request.Context here. The client-side batch
			// cancellation must still make the fetch return and reject commit.
			<-releaseSecond
			_, _ = w.Write([]byte("trojan://pw@second.example:443#second\n"))
		default:
			http.NotFound(w, request)
		}
	}))
	defer func() {
		releaseOnce.Do(func() { close(releaseSecond) })
		server.Close()
	}()

	tempDir := t.TempDir()
	oldURL := "https://old-subscription.example/list"
	cfg := newSubscriptionTestConfig(t, tempDir, []string{oldURL})
	cfg.SubscriptionRefresh.Timeout = 400 * time.Millisecond
	cfg.SubscriptionRefresh.HealthCheckTimeout = 40 * time.Millisecond
	cfg.SubscriptionRefresh.FetchConcurrency = 1
	cfg.Nodes = []config.NodeConfig{{Name: "inline", URI: "socks5://127.0.0.1:1080#inline", Source: config.NodeSourceInline}}
	fake := newFakeBoxManager(cfg)
	manager := New(cfg, fake)
	manager.waitBudgetFn = func(*config.Config, int) time.Duration { return 440 * time.Millisecond }
	defer manager.Stop()

	started := time.Now()
	err := manager.UpdateConfigAndRefresh(
		[]string{server.URL + "/slow-first", server.URL + "/blocked-second"},
		true,
		time.Hour,
		1,
		true,
	)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("update error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("canceled batch took too long to stop: %s", elapsed)
	}
	select {
	case <-secondEntered:
	default:
		t.Fatal("test did not reach the blocked second fetch")
	}

	// Releasing a handler after the API has returned must not resurrect the
	// canceled transaction and publish its nodes later.
	releaseOnce.Do(func() { close(releaseSecond) })
	time.Sleep(100 * time.Millisecond)
	committed, revision := fake.ConfigSnapshot()
	if revision != 1 {
		t.Fatalf("canceled refresh changed config revision to %d", revision)
	}
	if len(committed.Subscriptions) != 1 || committed.Subscriptions[0] != oldURL {
		t.Fatalf("canceled refresh changed subscriptions: %v", committed.Subscriptions)
	}
	if got := nodeURIs(committed.Nodes); strings.Contains(got, "first.example") || strings.Contains(got, "second.example") {
		t.Fatalf("canceled refresh committed fetched nodes: %s", got)
	}
	manager.mu.RLock()
	pending := manager.pendingUpdate
	manager.mu.RUnlock()
	if pending != nil {
		t.Fatalf("canceled update remained pending: %#v", pending)
	}
}

func TestRefreshNowTimeoutCancelsBatchBeforeItCanCommit(t *testing.T) {
	releaseSecond := make(chan struct{})
	var releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/slow-first":
			time.Sleep(200 * time.Millisecond)
			_, _ = w.Write([]byte("trojan://pw@first.example:443#first\n"))
		case "/blocked-second":
			<-releaseSecond
			_, _ = w.Write([]byte("trojan://pw@second.example:443#second\n"))
		default:
			http.NotFound(w, request)
		}
	}))
	defer func() {
		releaseOnce.Do(func() { close(releaseSecond) })
		server.Close()
	}()

	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(t, tempDir, []string{server.URL + "/slow-first", server.URL + "/blocked-second"})
	cfg.SubscriptionRefresh.Timeout = 350 * time.Millisecond
	cfg.SubscriptionRefresh.HealthCheckTimeout = 30 * time.Millisecond
	cfg.SubscriptionRefresh.FetchConcurrency = 1
	cfg.Nodes = []config.NodeConfig{{Name: "inline", URI: "socks5://127.0.0.1:1080#inline", Source: config.NodeSourceInline}}
	fake := newFakeBoxManager(cfg)
	manager := New(cfg, fake)
	manager.waitBudgetFn = func(*config.Config, int) time.Duration { return 380 * time.Millisecond }
	defer manager.Stop()

	err := manager.RefreshNow()
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("refresh error = %v, want context deadline exceeded", err)
	}
	releaseOnce.Do(func() { close(releaseSecond) })
	time.Sleep(100 * time.Millisecond)
	committed, revision := fake.ConfigSnapshot()
	if revision != 1 {
		t.Fatalf("canceled RefreshNow changed config revision to %d", revision)
	}
	if got := nodeURIs(committed.Nodes); strings.Contains(got, "first.example") || strings.Contains(got, "second.example") {
		t.Fatalf("canceled RefreshNow committed fetched nodes: %s", got)
	}
}

func TestFailedUpdateIsDiscardedAndLaterRefreshUsesCommittedURL(t *testing.T) {
	var oldRequests atomic.Int32
	var failedRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/old":
			oldRequests.Add(1)
			_, _ = w.Write([]byte("trojan://pw@old.example:443#old\n"))
		case "/failed":
			failedRequests.Add(1)
			http.Error(w, "provider failed", http.StatusBadGateway)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	tempDir := t.TempDir()
	oldURL := server.URL + "/old"
	failedURL := server.URL + "/failed"
	cfg := newSubscriptionTestConfig(t, tempDir, []string{oldURL})
	cfg.Nodes = []config.NodeConfig{{Name: "inline", URI: "socks5://127.0.0.1:1080#inline", Source: config.NodeSourceInline}}
	// A config update must not reinterpret the previous provider's aggregate
	// restart cache as a successful first fetch for a different URL.
	if err := os.WriteFile(cfg.NodesFile, []byte("trojan://pw@cached-old.example:443#cached\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := newFakeBoxManager(cfg)
	manager := New(cfg, fake)
	defer manager.Stop()

	if err := manager.UpdateConfigAndRefresh([]string{failedURL}, true, time.Hour, 1, true); err == nil {
		t.Fatal("failed subscription update unexpectedly succeeded")
	}
	manager.mu.RLock()
	baseAfterFailure := manager.baseCfg.Clone()
	pendingAfterFailure := manager.pendingUpdate
	manager.mu.RUnlock()
	if pendingAfterFailure != nil {
		t.Fatalf("failed update remained pending: %#v", pendingAfterFailure)
	}
	if len(baseAfterFailure.Subscriptions) != 1 || baseAfterFailure.Subscriptions[0] != oldURL {
		t.Fatalf("failed update changed manager base config: %v", baseAfterFailure.Subscriptions)
	}

	if err := manager.RefreshNow(); err != nil {
		t.Fatalf("refresh committed URL after failed update: %v", err)
	}
	if got := failedRequests.Load(); got != 1 {
		t.Fatalf("failed URL was retried %d times, want exactly once", got)
	}
	if got := oldRequests.Load(); got != 1 {
		t.Fatalf("committed URL requests = %d, want 1", got)
	}
	committed := fake.currentConfig()
	if len(committed.Subscriptions) != 1 || committed.Subscriptions[0] != oldURL {
		t.Fatalf("later refresh committed failed URL: %v", committed.Subscriptions)
	}
	if got := nodeURIs(committed.Nodes); !strings.Contains(got, "old.example") || strings.Contains(got, "failed") {
		t.Fatalf("later refresh used the wrong provider: %s", got)
	}
}

func TestQueuedUpdateCancellationDoesNotWaitForAnOlderRefresh(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/old" {
			close(entered)
			<-release
			_, _ = w.Write([]byte("trojan://pw@old.example:443#old\n"))
			return
		}
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()

	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(t, tempDir, []string{server.URL + "/old"})
	cfg.Nodes = []config.NodeConfig{{Name: "inline", URI: "socks5://127.0.0.1:1080#inline", Source: config.NodeSourceInline}}
	manager := New(cfg, newFakeBoxManager(cfg))
	manager.waitBudgetFn = func(*config.Config, int) time.Duration { return 75 * time.Millisecond }
	defer manager.Stop()
	oldDone := make(chan error, 1)
	go func() { oldDone <- manager.doRefresh() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("older refresh did not start")
	}

	started := time.Now()
	err := manager.UpdateConfigAndRefresh([]string{server.URL + "/new"}, true, time.Hour, 1, true)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued update error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("queued update waited for the older refresh: %s", elapsed)
	}
	close(release)
	if err := <-oldDone; err != nil {
		t.Fatalf("older refresh: %v", err)
	}
}

func TestRevisionBoundUpdateRejectsConcurrentChangeAfterFetchStarts(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	tempDir := t.TempDir()
	originalURL := "https://original.example/subscription"
	cfg := newSubscriptionTestConfig(t, tempDir, []string{originalURL})
	cfg.Nodes = []config.NodeConfig{{Name: "inline", URI: "socks5://127.0.0.1:1080#inline", Source: config.NodeSourceInline}}
	fake := newFakeBoxManager(cfg)
	manager := New(cfg, fake)
	defer manager.Stop()

	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- manager.UpdateConfigAndRefreshAtRevision(
			[]string{server.URL},
			true,
			time.Hour,
			1,
			true,
			1,
		)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("revision-bound refresh did not start fetching")
	}

	concurrent, revision := fake.ConfigSnapshot()
	concurrent.Management.ProbeConcurrency = 23
	if err := fake.CommitConfig(context.Background(), revision, concurrent, nil); err != nil {
		t.Fatalf("commit concurrent settings update: %v", err)
	}
	close(release)

	if err := <-refreshDone; !errors.Is(err, monitor.ErrSubscriptionConfigRevisionConflict) {
		t.Fatalf("refresh error=%v, want revision conflict", err)
	}
	committed, committedRevision := fake.ConfigSnapshot()
	if committedRevision != 2 {
		t.Fatalf("config revision=%d, want only the concurrent commit", committedRevision)
	}
	if committed.Management.ProbeConcurrency != 23 {
		t.Fatalf("concurrent settings update was lost: %#v", committed.Management)
	}
	if len(committed.Subscriptions) != 1 || committed.Subscriptions[0] != originalURL {
		t.Fatalf("stale subscription settings were committed: %v", committed.Subscriptions)
	}
	if got := nodeURIs(committed.Nodes); strings.Contains(got, "new.example") {
		t.Fatalf("stale subscription nodes were committed: %s", got)
	}
}

func TestRefreshWaitBudgetIncludesEveryURLWave(t *testing.T) {
	cfg := &config.Config{
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			Timeout:            2 * time.Second,
			HealthCheckTimeout: time.Millisecond,
			DrainTimeout:       time.Millisecond,
			FetchConcurrency:   16,
		},
		Management: config.ManagementConfig{ProbeConcurrency: 1024},
	}
	oneWave := refreshWaitBudget(cfg, 16)
	eightWaves := refreshWaitBudget(cfg, 128)
	if difference := eightWaves - oneWave; difference < 7*cfg.SubscriptionRefresh.Timeout {
		t.Fatalf("128-URL budget only grew by %s; want at least seven additional fetch waves", difference)
	}
}

func TestDeletedNodesFileIsStickyModified(t *testing.T) {
	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(t, tempDir, nil)
	manager := New(cfg, newFakeBoxManager(cfg))
	defer manager.Stop()
	nodes := []config.NodeConfig{{URI: "socks5://127.0.0.1:1080#old"}}
	if err := config.WriteNodesToFile(cfg.NodesFile, nodes); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(cfg.NodesFile)
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	manager.lastSubHash = manager.computeNodesHash(nodes)
	manager.lastNodesModTime = info.ModTime()
	manager.mu.Unlock()
	if err := os.Remove(cfg.NodesFile); err != nil {
		t.Fatal(err)
	}
	if !manager.CheckNodesModified() {
		t.Fatal("deleted nodes file was reported unchanged")
	}
	if err := config.WriteNodesToFile(cfg.NodesFile, nodes); err != nil {
		t.Fatal(err)
	}
	if !manager.CheckNodesModified() {
		t.Fatal("modified state was not sticky after recreating the nodes file")
	}
}

func TestNewDesiredCancelsActivePendingAndFailedReplacementKeepsCommittedConfig(t *testing.T) {
	aEntered := make(chan struct{})
	aRelease := make(chan struct{})
	bEntered := make(chan struct{})
	var releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/a":
			close(aEntered)
			<-aRelease
			_, _ = w.Write([]byte("trojan://pw@a.example:443#a\n"))
		case "/b":
			close(bEntered)
			http.Error(w, "replacement failed", http.StatusBadGateway)
		default:
			http.NotFound(w, request)
		}
	}))
	defer func() {
		releaseOnce.Do(func() { close(aRelease) })
		server.Close()
	}()

	tempDir := t.TempDir()
	originalURL := "https://original.example/subscription"
	cfg := newSubscriptionTestConfig(t, tempDir, []string{originalURL})
	cfg.Nodes = []config.NodeConfig{{Name: "inline", URI: "socks5://127.0.0.1:1080#inline", Source: config.NodeSourceInline}}
	fake := newFakeBoxManager(cfg)
	manager := New(cfg, fake)
	defer manager.Stop()

	aDone := make(chan error, 1)
	go func() {
		aDone <- manager.UpdateConfigAndRefresh([]string{server.URL + "/a"}, true, time.Hour, 1, true)
	}()
	select {
	case <-aEntered:
	case <-time.After(time.Second):
		t.Fatal("desired A did not enter its blocked fetch")
	}

	bDone := make(chan error, 1)
	go func() {
		bDone <- manager.UpdateConfigAndRefresh([]string{server.URL + "/b"}, true, time.Hour, 1, true)
	}()
	select {
	case <-bEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("desired B did not cancel A and begin its own fetch")
	}
	releaseOnce.Do(func() { close(aRelease) })

	if err := <-aDone; !errors.Is(err, errSubscriptionUpdateSuperseded) {
		t.Fatalf("desired A error = %v, want superseded", err)
	}
	if err := <-bDone; err == nil {
		t.Fatal("failing desired B unexpectedly succeeded")
	}
	committed, revision := fake.ConfigSnapshot()
	if revision != 1 {
		t.Fatalf("stale desired changed config revision to %d", revision)
	}
	if len(committed.Subscriptions) != 1 || committed.Subscriptions[0] != originalURL {
		t.Fatalf("stale desired replaced committed subscriptions: %v", committed.Subscriptions)
	}
	if got := nodeURIs(committed.Nodes); strings.Contains(got, "a.example") || strings.Contains(got, "b.example") {
		t.Fatalf("stale desired committed nodes: %s", got)
	}
}

func TestNewDesiredSupersedesOldGenerationAtFinalPublishBarrier(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/a":
			_, _ = w.Write([]byte("trojan://pw@a.example:443#a\n"))
		case "/b":
			_, _ = w.Write([]byte("trojan://pw@b.example:443#b\n"))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(t, tempDir, []string{server.URL + "/original"})
	cfg.Nodes = []config.NodeConfig{{Name: "inline", URI: "socks5://127.0.0.1:1080#inline", Source: config.NodeSourceInline}}
	fake := newFakeBoxManager(cfg)
	publishEntered := make(chan struct{})
	releasePublish := make(chan struct{})
	var firstPublish sync.Once
	fake.beforePublish = func() {
		firstPublish.Do(func() {
			close(publishEntered)
			<-releasePublish
		})
	}
	manager := New(cfg, fake)
	defer manager.Stop()

	aDone := make(chan error, 1)
	go func() {
		aDone <- manager.UpdateConfigAndRefresh([]string{server.URL + "/a"}, true, time.Hour, 1, true)
	}()
	select {
	case <-publishEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("desired A did not reach the final publish barrier")
	}

	bDone := make(chan error, 1)
	go func() {
		bDone <- manager.UpdateConfigAndRefresh([]string{server.URL + "/b"}, true, time.Hour, 1, true)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		manager.mu.RLock()
		pending := manager.pendingUpdate
		isB := pending != nil && len(pending.config.Subscriptions) == 1 && pending.config.Subscriptions[0] == server.URL+"/b"
		manager.mu.RUnlock()
		if isB {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("desired B was not registered while A was waiting to publish")
		}
		time.Sleep(time.Millisecond)
	}
	close(releasePublish)

	if err := <-aDone; !errors.Is(err, errSubscriptionUpdateSuperseded) {
		t.Fatalf("desired A error=%v, want superseded", err)
	}
	if err := <-bDone; err != nil {
		t.Fatalf("desired B failed: %v", err)
	}
	committed, revision := fake.ConfigSnapshot()
	if revision != 2 {
		t.Fatalf("config revision=%d, want one committed generation", revision)
	}
	if len(committed.Subscriptions) != 1 || committed.Subscriptions[0] != server.URL+"/b" {
		t.Fatalf("committed subscriptions=%v", committed.Subscriptions)
	}
	if got := nodeURIs(committed.Nodes); strings.Contains(got, "a.example") || !strings.Contains(got, "b.example") {
		t.Fatalf("wrong generation published: %s", got)
	}
}

func TestCommittedRefreshIgnoresLateTicketCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("trojan://pw@committed.example:443#committed\n"))
	}))
	defer server.Close()

	tempDir := t.TempDir()
	cfg := newSubscriptionTestConfig(t, tempDir, []string{server.URL})
	fake := newFakeBoxManager(cfg)
	published := make(chan struct{})
	release := make(chan struct{})
	var publishOnce sync.Once
	fake.afterPublish = func() {
		publishOnce.Do(func() { close(published) })
		<-release
	}
	manager := New(cfg, fake)
	defer manager.Stop()

	ticket, err := manager.updateConfig([]string{server.URL}, true, time.Hour, 1, true, true, nil)
	if err != nil {
		t.Fatalf("schedule refresh: %v", err)
	}
	select {
	case <-published:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not cross the final publish barrier")
	}
	if canceled := manager.cancelRefreshTicket(ticket.sequence, context.DeadlineExceeded); canceled {
		t.Fatal("committed refresh was still reported cancelable")
	}
	manager.mu.RLock()
	_, hasCanceledResult := manager.canceled[ticket.sequence]
	manager.mu.RUnlock()
	if hasCanceledResult {
		t.Fatal("late timeout was recorded for an already committed refresh")
	}
	close(release)

	select {
	case err := <-ticket.result:
		if err != nil {
			t.Fatalf("committed refresh result: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("committed refresh did not complete its ticket")
	}
	committed, revision := fake.ConfigSnapshot()
	if revision != 2 || len(committed.Nodes) != 1 || !strings.Contains(committed.Nodes[0].URI, "committed.example") {
		t.Fatalf("unexpected committed state: revision=%d nodes=%#v", revision, committed.Nodes)
	}
}

func nodeURIs(nodes []config.NodeConfig) string {
	values := make([]string, 0, len(nodes))
	for _, node := range nodes {
		values = append(values, node.URI)
	}
	return strings.Join(values, "\n")
}
