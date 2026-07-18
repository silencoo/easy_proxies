package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
)

// Logger defines logging interface.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type boxManager interface {
	CurrentPortMap() map[string]uint16
	ReloadWithPortMap(newCfg *config.Config, portMap map[string]uint16) error
}

type configNodeLister interface {
	ListConfigNodes(ctx context.Context) ([]config.NodeConfig, error)
}

// Option configures the Manager.
type Option func(*Manager)

// WithLogger sets a custom logger.
func WithLogger(l Logger) Option {
	return func(m *Manager) { m.logger = l }
}

// Manager handles periodic subscription refresh.
type Manager struct {
	mu sync.RWMutex

	baseCfg    *config.Config
	boxMgr     boxManager
	logger     Logger
	httpClient *http.Client // Custom HTTP client with connection pooling

	status        monitor.SubscriptionStatus
	ctx           context.Context
	cancel        context.CancelFunc
	refreshMu     sync.Mutex // serializes refreshes with subscription config changes
	loopOnce      sync.Once
	manualRefresh chan struct{}
	configChanged chan struct{}
	requestSeq    uint64
	waiters       map[uint64]chan error
	sourceCache   map[string][]config.NodeConfig

	// Track nodes.txt content hash to detect modifications
	lastSubHash      string    // Hash of nodes.txt content after last subscription refresh
	lastNodesModTime time.Time // Last known modification time of nodes.txt
}

// New creates a SubscriptionManager.
func New(cfg *config.Config, boxMgr boxManager, opts ...Option) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	// Create optimized HTTP client with connection pooling
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second, // Overall timeout
	}

	m := &Manager{
		baseCfg:       cfg,
		boxMgr:        boxMgr,
		ctx:           ctx,
		cancel:        cancel,
		manualRefresh: make(chan struct{}, 1),
		configChanged: make(chan struct{}, 1),
		waiters:       make(map[uint64]chan error),
		sourceCache:   make(map[string][]config.NodeConfig),
		httpClient:    httpClient,
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.logger == nil {
		m.logger = defaultLogger{}
	}
	return m
}

// Start begins the periodic refresh loop.
func (m *Manager) Start() {
	m.mu.RLock()
	enabled := m.baseCfg.SubscriptionRefresh.Enabled
	hasSubscriptions := len(m.baseCfg.Subscriptions) > 0
	interval := m.baseCfg.SubscriptionRefresh.Interval
	m.mu.RUnlock()
	m.startLoop()
	if !enabled {
		m.logger.Infof("subscription refresh disabled")
		return
	}
	if !hasSubscriptions {
		m.logger.Infof("no subscriptions configured, refresh disabled")
		return
	}

	m.logger.Infof("starting subscription refresh, interval: %s", interval)
}

// Stop stops the periodic refresh.
func (m *Manager) Stop() {
	m.mu.RLock()
	cancel := m.cancel
	m.mu.RUnlock()
	if cancel != nil {
		cancel()
	}

	// Close idle connections
	if m.httpClient != nil {
		m.httpClient.CloseIdleConnections()
	}
}

// UpdateConfig hot-reloads subscription URLs and refresh settings without restart.
func (m *Manager) UpdateConfig(urls []string, enabled bool, interval time.Duration) {
	m.updateConfig(urls, enabled, interval, false)
}

// UpdateConfigAndRefresh updates subscription config and synchronously waits for
// the first refresh to complete before returning. This ensures the caller (WebUI API)
// can confirm the update took effect.
func (m *Manager) UpdateConfigAndRefresh(urls []string, enabled bool, interval time.Duration) error {
	waiter := m.updateConfig(urls, enabled, interval, true)
	if waiter == nil {
		return nil
	}
	m.mu.RLock()
	timeout := m.baseCfg.SubscriptionRefresh.Timeout
	healthTimeout := m.baseCfg.SubscriptionRefresh.HealthCheckTimeout
	waitCtx := m.ctx
	m.mu.RUnlock()
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := timeout + healthTimeout

	ctx, cancel := context.WithTimeout(waitCtx, deadline)
	defer cancel()

	select {
	case <-ctx.Done():
		return fmt.Errorf("refresh timeout: %w", ctx.Err())
	case err := <-waiter:
		return err
	}
}

// RefreshNow triggers an immediate refresh.
func (m *Manager) RefreshNow() error {
	m.mu.RLock()
	hasSubscriptions := len(m.baseCfg.Subscriptions) > 0
	timeout := m.baseCfg.SubscriptionRefresh.Timeout
	healthTimeout := m.baseCfg.SubscriptionRefresh.HealthCheckTimeout
	waitCtx := m.ctx
	m.mu.RUnlock()
	if !hasSubscriptions {
		return fmt.Errorf("no subscription URLs configured")
	}
	m.startLoop()
	waiter := m.requestRefresh(true)

	// Wait for refresh to complete or timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	ctx, cancel := context.WithTimeout(waitCtx, timeout+healthTimeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return fmt.Errorf("refresh timeout: %w", ctx.Err())
	case err := <-waiter:
		return err
	}
}

func (m *Manager) startLoop() {
	m.loopOnce.Do(func() {
		go m.refreshLoop()
	})
}

func (m *Manager) updateConfig(urls []string, enabled bool, interval time.Duration, wait bool) <-chan error {
	m.startLoop()

	// Do not allow an in-flight refresh based on the old URLs to publish after
	// the new settings have been persisted.
	m.refreshMu.Lock()
	m.mu.Lock()
	m.baseCfg.Subscriptions = append([]string(nil), urls...)
	m.baseCfg.SubscriptionRefresh.Enabled = enabled
	if interval > 0 {
		m.baseCfg.SubscriptionRefresh.Interval = interval
	}
	effectiveInterval := m.baseCfg.SubscriptionRefresh.Interval
	saveErr := m.baseCfg.SaveSettings()
	m.mu.Unlock()
	m.refreshMu.Unlock()
	if saveErr != nil {
		m.logger.Errorf("failed to save subscription config: %v", saveErr)
	}

	select {
	case m.configChanged <- struct{}{}:
	default:
	}
	if len(urls) == 0 {
		m.logger.Infof("no subscription URLs configured, skipping refresh")
		return nil
	}

	m.logger.Infof("subscription config updated: %d URLs, enabled=%v, interval=%s", len(urls), enabled, effectiveInterval)
	return m.requestRefresh(wait)
}

func (m *Manager) requestRefresh(wait bool) <-chan error {
	m.mu.Lock()
	m.requestSeq++
	sequence := m.requestSeq
	var waiter chan error
	if wait {
		waiter = make(chan error, 1)
		m.waiters[sequence] = waiter
	}
	m.mu.Unlock()

	select {
	case m.manualRefresh <- struct{}{}:
	default:
	}
	return waiter
}

func (m *Manager) requestedSequence() uint64 {
	m.mu.RLock()
	sequence := m.requestSeq
	m.mu.RUnlock()
	return sequence
}

func (m *Manager) completeRequests(upTo uint64, err error) {
	m.mu.Lock()
	for sequence, waiter := range m.waiters {
		if sequence > upTo {
			continue
		}
		waiter <- err
		close(waiter)
		delete(m.waiters, sequence)
	}
	m.mu.Unlock()
}

// Status returns the current refresh status.
func (m *Manager) Status() monitor.SubscriptionStatus {
	m.mu.RLock()
	status := m.status
	m.mu.RUnlock()

	// Check if nodes have been modified since last refresh
	status.NodesModified = m.CheckNodesModified()
	return status
}

// refreshLoop is the manager's only scheduler. A stable loop/channel pair
// avoids dropped manual requests while settings are being changed.
func (m *Manager) refreshLoop() {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		m.mu.RLock()
		autoEnabled := m.baseCfg.SubscriptionRefresh.Enabled && len(m.baseCfg.Subscriptions) > 0
		interval := m.baseCfg.SubscriptionRefresh.Interval
		loopCtx := m.ctx
		m.mu.RUnlock()
		if interval <= 0 {
			interval = time.Hour
		}

		var timerChannel <-chan time.Time
		if autoEnabled {
			timer.Reset(interval)
			timerChannel = timer.C
			m.mu.Lock()
			m.status.NextRefresh = time.Now().Add(interval)
			m.mu.Unlock()
		} else {
			m.mu.Lock()
			m.status.NextRefresh = time.Time{}
			m.mu.Unlock()
		}

		select {
		case <-loopCtx.Done():
			return
		case <-m.configChanged:
			stopTimer(timer)
			continue
		case <-timerChannel:
			target := m.requestedSequence()
			err := m.doRefresh()
			m.completeRequests(target, err)
		case <-m.manualRefresh:
			stopTimer(timer)
			target := m.requestedSequence()
			err := m.doRefresh()
			m.completeRequests(target, err)
		}
	}
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

// doRefresh performs one atomic fetch, file update, and runtime reload.
func (m *Manager) doRefresh() (refreshErr error) {
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()

	m.mu.Lock()
	m.status.IsRefreshing = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.status.IsRefreshing = false
		m.status.RefreshCount++
		m.status.LastRefresh = time.Now()
		if refreshErr != nil {
			m.status.LastError = refreshErr.Error()
		} else {
			m.status.LastError = ""
		}
		m.mu.Unlock()
	}()

	m.mu.RLock()
	baseCfg := *m.baseCfg
	baseCfg.Nodes = cloneNodes(m.baseCfg.Nodes)
	baseCfg.Subscriptions = append([]string(nil), m.baseCfg.Subscriptions...)
	fetchCtx := m.ctx
	m.mu.RUnlock()
	nodesFilePath := nodesFilePathForConfig(&baseCfg)

	m.logger.Infof("starting subscription refresh")
	plan, err := m.fetchAllSubscriptions(fetchCtx, &baseCfg, nodesFilePath)
	if err != nil {
		m.logger.Errorf("fetch subscriptions failed: %v", err)
		return err
	}
	m.logger.Infof("prepared %d subscription nodes", len(plan.nodes))
	if lister, ok := m.boxMgr.(configNodeLister); ok {
		currentNodes, listErr := lister.ListConfigNodes(fetchCtx)
		if listErr != nil {
			return fmt.Errorf("snapshot current explicit nodes: %w", listErr)
		}
		baseCfg.Nodes = cloneNodes(currentNodes)
	}

	// Persist the candidate before switching runtime, retaining the previous
	// bytes so a rejected candidate cannot replace the last bootable cache.
	previousNodes, readErr := os.ReadFile(nodesFilePath)
	previousFileExisted := readErr == nil
	if readErr != nil && !os.IsNotExist(readErr) {
		return fmt.Errorf("snapshot nodes.txt: %w", readErr)
	}
	if err := m.writeNodesToFile(nodesFilePath, plan.nodes); err != nil {
		return fmt.Errorf("write nodes.txt: %w", err)
	}

	portMap := m.boxMgr.CurrentPortMap()
	newCfg := m.createNewConfig(&baseCfg, cloneNodes(plan.nodes))
	if err := m.boxMgr.ReloadWithPortMap(newCfg, portMap); err != nil {
		var restoreErr error
		if previousFileExisted {
			restoreErr = config.WriteFileAtomic(nodesFilePath, previousNodes, 0o644)
		} else {
			restoreErr = os.Remove(nodesFilePath)
			if os.IsNotExist(restoreErr) {
				restoreErr = nil
			}
		}
		if restoreErr != nil {
			return fmt.Errorf("reload: %w; restore nodes.txt: %v", err, restoreErr)
		}
		return fmt.Errorf("reload: %w", err)
	}

	newHash := m.computeNodesHash(plan.nodes)
	m.mu.Lock()
	m.baseCfg = newCfg
	for key := range m.sourceCache {
		if _, active := plan.activeKeys[key]; !active {
			delete(m.sourceCache, key)
		}
	}
	for key, nodes := range plan.cacheUpdates {
		m.sourceCache[key] = cloneNodes(nodes)
	}
	m.lastSubHash = newHash
	if info, statErr := os.Stat(nodesFilePath); statErr == nil {
		m.lastNodesModTime = info.ModTime()
	} else {
		m.lastNodesModTime = time.Now()
	}
	m.status.NodesModified = false
	m.status.NodeCount = len(newCfg.Nodes)
	m.mu.Unlock()

	if plan.usedFallback {
		m.logger.Warnf("subscription refresh completed with the aggregate restart cache")
	} else {
		m.logger.Infof("subscription refresh completed, %d nodes active", len(newCfg.Nodes))
	}
	return nil
}

func nodesFilePathForConfig(cfg *config.Config) string {
	if cfg.NodesFile != "" {
		return cfg.NodesFile
	}
	return filepath.Join(filepath.Dir(cfg.FilePath()), "nodes.txt")
}

// getNodesFilePath returns the path to nodes.txt.
func (m *Manager) getNodesFilePath() string {
	m.mu.RLock()
	nodesFile := m.baseCfg.NodesFile
	configPath := m.baseCfg.FilePath()
	m.mu.RUnlock()
	if nodesFile != "" {
		return nodesFile
	}
	return filepath.Join(filepath.Dir(configPath), "nodes.txt")
}

// writeNodesToFile writes nodes to a file (one URI per line).
func (m *Manager) writeNodesToFile(path string, nodes []config.NodeConfig) error {
	return config.WriteNodesToFile(path, nodes)
}

// computeNodesHash computes a hash of node URIs for change detection.
func (m *Manager) computeNodesHash(nodes []config.NodeConfig) string {
	var uris []string
	for _, node := range nodes {
		uris = append(uris, node.URI)
	}
	content := strings.Join(uris, "\n")
	hash := sha256.Sum256([]byte(content))
	return hex.EncodeToString(hash[:])
}

// CheckNodesModified checks if nodes.txt has been modified since last refresh.
// Uses file modification time as a fast path to avoid unnecessary file reads.
func (m *Manager) CheckNodesModified() bool {
	m.mu.RLock()
	lastHash := m.lastSubHash
	lastMod := m.lastNodesModTime
	m.mu.RUnlock()

	if lastHash == "" {
		return false // No previous refresh, can't determine modification
	}

	nodesFilePath := m.getNodesFilePath()

	// Fast path: check modification time first
	info, err := os.Stat(nodesFilePath)
	if err != nil {
		return false // File doesn't exist or can't stat
	}
	modTime := info.ModTime()
	if !modTime.After(lastMod) {
		return false // File hasn't been modified
	}

	// Slow path: file was modified, compute hash
	data, err := os.ReadFile(nodesFilePath)
	if err != nil {
		return false // File doesn't exist or can't read
	}

	// Parse nodes from file content
	var nodes []config.NodeConfig
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if config.IsProxyURI(line) {
			nodes = append(nodes, config.NodeConfig{URI: line})
		}
	}

	currentHash := m.computeNodesHash(nodes)
	changed := currentHash != lastHash

	// Update cached mod time
	m.mu.Lock()
	m.lastNodesModTime = modTime
	m.mu.Unlock()

	return changed
}

// MarkNodesModified updates the modification status.
func (m *Manager) MarkNodesModified() {
	m.mu.Lock()
	m.status.NodesModified = true
	m.mu.Unlock()
}

type subscriptionFetchPlan struct {
	nodes        []config.NodeConfig
	cacheUpdates map[string][]config.NodeConfig
	activeKeys   map[string]struct{}
	usedFallback bool
}

// fetchAllSubscriptions fetches all unique URLs concurrently. Once a complete
// refresh has populated sourceCache, a failed source can be replaced by only
// that source's last known-good nodes. On the first incomplete refresh after a
// restart, nodes.txt remains the conservative aggregate fallback.
func (m *Manager) fetchAllSubscriptions(ctx context.Context, baseCfg *config.Config, nodesFilePath string) (subscriptionFetchPlan, error) {
	results, stats := config.FetchSubscriptionSources(ctx, baseCfg.Subscriptions, config.SubscriptionFetchOptions{
		Timeout:     baseCfg.SubscriptionRefresh.Timeout,
		Concurrency: baseCfg.SubscriptionRefresh.FetchConcurrency,
		Client:      m.httpClient,
		Loggerf: func(format string, args ...any) {
			m.logger.Infof(format, args...)
		},
	})
	if stats.DedupedURLs > 0 {
		m.logger.Infof("subscription URL dedupe removed %d duplicate entries", stats.DedupedURLs)
	}

	plan := subscriptionFetchPlan{
		cacheUpdates: make(map[string][]config.NodeConfig),
		activeKeys:   make(map[string]struct{}, len(results)),
	}
	unresolved := 0
	var lastErr error
	for _, result := range results {
		plan.activeKeys[result.Key] = struct{}{}
		if result.Err == nil && len(result.Nodes) > 0 {
			nodes := cloneNodes(result.Nodes)
			plan.nodes = append(plan.nodes, nodes...)
			plan.cacheUpdates[result.Key] = nodes
			continue
		}

		m.mu.RLock()
		cached := cloneNodes(m.sourceCache[result.Key])
		m.mu.RUnlock()
		if len(cached) > 0 {
			m.logger.Warnf("using %d cached nodes for one unavailable subscription", len(cached))
			plan.nodes = append(plan.nodes, cached...)
			continue
		}
		unresolved++
		if result.Err != nil {
			lastErr = result.Err
		} else {
			lastErr = fmt.Errorf("subscription returned no usable nodes")
		}
	}

	if unresolved > 0 {
		cachedNodes, cacheErr := config.LoadNodesFromFile(nodesFilePath)
		if cacheErr == nil && len(cachedNodes) > 0 {
			cachedNodes, _ = config.DedupeNodesByStableIdentity(cachedNodes)
			m.logger.Warnf("keeping %d aggregate cached nodes because %d subscription sources have no runtime cache", len(cachedNodes), unresolved)
			plan.nodes = cachedNodes
			plan.cacheUpdates = nil
			plan.usedFallback = true
			return plan, nil
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("%d subscription sources could not be refreshed", unresolved)
		}
		if cacheErr != nil && !os.IsNotExist(cacheErr) {
			return subscriptionFetchPlan{}, fmt.Errorf("%w; read aggregate cache: %v", lastErr, cacheErr)
		}
		return subscriptionFetchPlan{}, lastErr
	}

	plan.nodes, stats.DedupedNodes = config.DedupeNodesByStableIdentity(plan.nodes)
	if stats.DedupedNodes > 0 {
		m.logger.Infof("subscription node dedupe removed %d duplicate entries", stats.DedupedNodes)
	}
	if len(plan.nodes) == 0 {
		return subscriptionFetchPlan{}, fmt.Errorf("no nodes fetched from subscriptions")
	}
	return plan, nil
}

// createNewConfig merges explicit inline nodes with refreshed subscription
// nodes. Inline nodes win stable-identity collisions so user configuration is
// never replaced by subscription metadata.
func (m *Manager) createNewConfig(baseCfg *config.Config, nodes []config.NodeConfig) *config.Config {
	newCfg := *baseCfg
	merged := make([]config.NodeConfig, 0, len(baseCfg.Nodes)+len(nodes))
	for _, node := range baseCfg.Nodes {
		if node.Source == config.NodeSourceInline {
			merged = append(merged, node)
		}
	}
	for _, node := range nodes {
		node.Source = config.NodeSourceSubscription
		merged = append(merged, node)
	}
	merged, _ = config.DedupeNodesByStableIdentity(merged)
	for index := range merged {
		merged[index].Name = strings.TrimSpace(merged[index].Name)
		merged[index].URI = strings.TrimSpace(merged[index].URI)
		if merged[index].Name == "" {
			merged[index].Name = config.ExtractNodeName(merged[index].URI)
		}
		if merged[index].Name == "" {
			merged[index].Name = fmt.Sprintf("node-%d", index)
		}
	}
	newCfg.Nodes = merged
	newCfg.Subscriptions = append([]string(nil), baseCfg.Subscriptions...)
	return &newCfg
}

func cloneNodes(nodes []config.NodeConfig) []config.NodeConfig {
	return append([]config.NodeConfig(nil), nodes...)
}

type defaultLogger struct{}

func (defaultLogger) Infof(format string, args ...any) {
	log.Printf("[subscription] "+format, args...)
}

func (defaultLogger) Warnf(format string, args ...any) {
	log.Printf("[subscription] WARN: "+format, args...)
}

func (defaultLogger) Errorf(format string, args ...any) {
	log.Printf("[subscription] ERROR: "+format, args...)
}
