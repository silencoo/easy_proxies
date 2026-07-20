package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"easy_proxies/internal/commitguard"
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
	ConfigSnapshot() (*config.Config, uint64)
	CommitConfig(
		ctx context.Context,
		expectedRevision uint64,
		candidate *config.Config,
		persist func(*config.Config) (rollback func() error, err error),
	) error
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

	baseCfg *config.Config
	boxMgr  boxManager
	logger  Logger

	status        monitor.SubscriptionStatus
	ctx           context.Context
	cancel        context.CancelFunc
	refreshSlot   chan struct{} // serializes refreshes with context-aware cancellation
	loopOnce      sync.Once
	loopWG        sync.WaitGroup
	stopOnce      sync.Once
	stopped       bool
	manualRefresh chan struct{}
	configChanged chan struct{}
	requestSeq    uint64
	completedSeq  uint64
	waiters       map[uint64]chan error
	canceled      map[uint64]error
	activeBatch   *refreshBatch
	pendingUpdate *pendingConfigUpdate
	sourceCache   map[string][]config.NodeConfig

	// Track nodes.txt content hash to detect modifications
	lastSubHash      string    // Hash of nodes.txt content after last subscription refresh
	lastNodesModTime time.Time // Last known modification time of nodes.txt

	saveSettingsFn func(*config.Config) error
	writeNodesFn   func(string, []config.NodeConfig) error
	waitBudgetFn   func(*config.Config, int) time.Duration
}

// New creates a SubscriptionManager.
func New(cfg *config.Config, boxMgr boxManager, opts ...Option) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		baseCfg:       cfg.Clone(),
		boxMgr:        boxMgr,
		ctx:           ctx,
		cancel:        cancel,
		manualRefresh: make(chan struct{}, 1),
		refreshSlot:   make(chan struct{}, 1),
		configChanged: make(chan struct{}, 1),
		waiters:       make(map[uint64]chan error),
		canceled:      make(map[uint64]error),
		sourceCache:   make(map[string][]config.NodeConfig),
		waitBudgetFn:  refreshWaitBudget,
	}
	m.refreshSlot <- struct{}{}
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
	m.stopOnce.Do(func() {
		m.mu.Lock()
		m.stopped = true
		cancel := m.cancel
		if m.activeBatch != nil {
			m.activeBatch.cancel()
		}
		m.pendingUpdate = nil
		m.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		m.loopWG.Wait()

		// Active batches complete their own tickets before the loop exits. Any
		// requests still present here were queued but never started, so they can
		// now be canceled without leaving work capable of committing later.
		m.mu.Lock()
		for sequence, waiter := range m.waiters {
			waiter <- context.Canceled
			close(waiter)
			delete(m.waiters, sequence)
		}
		m.mu.Unlock()
	})
}

// UpdateConfig hot-reloads subscription URLs and refresh settings without restart.
func (m *Manager) UpdateConfig(urls []string, enabled bool, interval time.Duration) {
	cleanURLs, err := config.ValidateSubscriptionURLs(urls)
	if err != nil {
		m.logger.Errorf("refusing invalid subscription config: %v", err)
		return
	}
	m.mu.RLock()
	allowPrivateNetworks := m.baseCfg.SubscriptionRefresh.AllowPrivateNetworks
	m.mu.RUnlock()
	if _, err := m.updateConfig(cleanURLs, enabled, interval, 0, allowPrivateNetworks, false, nil); err != nil {
		m.logger.Errorf("failed to update subscription config: %v", err)
	}
}

// UpdateConfigAndRefresh updates subscription config and synchronously waits for
// the first refresh to complete before returning. This ensures the caller (WebUI API)
// can confirm the update took effect.
func (m *Manager) UpdateConfigAndRefresh(urls []string, enabled bool, interval time.Duration, fetchConcurrency int, allowPrivateNetworks bool) error {
	return m.updateConfigAndRefresh(urls, enabled, interval, fetchConcurrency, allowPrivateNetworks, nil)
}

// UpdateConfigAndRefreshAtRevision applies subscription settings only if the
// same configuration revision is still active at the final commit. Network
// fetching happens before that commit, so the revision must travel with the
// queued update instead of being checked only by the HTTP handler.
func (m *Manager) UpdateConfigAndRefreshAtRevision(urls []string, enabled bool, interval time.Duration, fetchConcurrency int, allowPrivateNetworks bool, expectedRevision uint64) error {
	return m.updateConfigAndRefresh(urls, enabled, interval, fetchConcurrency, allowPrivateNetworks, &expectedRevision)
}

func (m *Manager) updateConfigAndRefresh(urls []string, enabled bool, interval time.Duration, fetchConcurrency int, allowPrivateNetworks bool, expectedRevision *uint64) error {
	cleanURLs, err := config.ValidateSubscriptionURLs(urls)
	if err != nil {
		return err
	}
	ticket, err := m.updateConfig(cleanURLs, enabled, interval, fetchConcurrency, allowPrivateNetworks, true, expectedRevision)
	if err != nil || ticket == nil {
		return err
	}
	m.mu.RLock()
	waitCtx := m.ctx
	m.mu.RUnlock()
	ctx, cancel := context.WithTimeout(waitCtx, ticket.waitFor)
	defer cancel()

	select {
	case <-ctx.Done():
		if m.cancelRefreshTicket(ticket.sequence, ctx.Err()) {
			resultErr := <-ticket.result
			if resultErr == nil {
				return nil
			}
			return fmt.Errorf("refresh timeout: %w", ctx.Err())
		}
		return <-ticket.result
	case err := <-ticket.result:
		return err
	}
}

// RefreshNow triggers an immediate refresh.
func (m *Manager) RefreshNow() error {
	m.mu.RLock()
	hasSubscriptions := len(m.baseCfg.Subscriptions) > 0
	waitCtx := m.ctx
	m.mu.RUnlock()
	if !hasSubscriptions {
		return fmt.Errorf("no subscription URLs configured")
	}
	if !m.startLoop() {
		return context.Canceled
	}
	ticket := m.requestRefresh(true, nil, nil)

	ctx, cancel := context.WithTimeout(waitCtx, ticket.waitFor)
	defer cancel()

	select {
	case <-ctx.Done():
		if m.cancelRefreshTicket(ticket.sequence, ctx.Err()) {
			resultErr := <-ticket.result
			if resultErr == nil {
				return nil
			}
			return fmt.Errorf("refresh timeout: %w", ctx.Err())
		}
		return <-ticket.result
	case err := <-ticket.result:
		return err
	}
}

func (m *Manager) startLoop() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return false
	}
	m.loopOnce.Do(func() {
		m.loopWG.Add(1)
		go func() {
			defer m.loopWG.Done()
			m.refreshLoop()
		}()
	})
	return true
}

func (m *Manager) updateConfig(urls []string, enabled bool, interval time.Duration, fetchConcurrency int, allowPrivateNetworks bool, wait bool, expectedRevision *uint64) (*refreshTicket, error) {
	if !m.startLoop() {
		return nil, context.Canceled
	}

	liveCfg, liveRevision := m.boxMgr.ConfigSnapshot()
	if liveCfg == nil {
		return nil, errors.New("active config is unavailable")
	}
	if expectedRevision != nil && liveRevision != *expectedRevision {
		return nil, configRevisionConflict(*expectedRevision, liveRevision)
	}
	desired := liveCfg.Clone()
	desired.Subscriptions = append([]string(nil), urls...)
	desired.SubscriptionRefresh.Enabled = enabled
	if interval > 0 {
		desired.SubscriptionRefresh.Interval = interval
	}
	if fetchConcurrency > 0 {
		desired.SubscriptionRefresh.FetchConcurrency = config.NormalizeSubscriptionFetchConcurrency(fetchConcurrency)
	}
	desired.SubscriptionRefresh.AllowPrivateNetworks = allowPrivateNetworks

	m.notifyConfigChanged()
	m.logger.Infof("subscription config prepared: %d URLs, enabled=%v, interval=%s", len(urls), enabled, desired.SubscriptionRefresh.Interval)
	return m.requestRefresh(wait, desired, expectedRevision), nil
}

type refreshTicket struct {
	sequence uint64
	result   chan error
	waitFor  time.Duration
}

var errSubscriptionUpdateSuperseded = errors.New("subscription update superseded by a newer configuration")

func cloneRevision(revision *uint64) *uint64 {
	if revision == nil {
		return nil
	}
	cloned := *revision
	return &cloned
}

func configRevisionConflict(expected, current uint64) error {
	return fmt.Errorf("%w: expected %d, current %d", monitor.ErrSubscriptionConfigRevisionConflict, expected, current)
}

type pendingConfigUpdate struct {
	sequence         uint64
	config           *config.Config
	expectedRevision *uint64
}

type refreshBatch struct {
	upTo            uint64
	selectedPending uint64
	cancel          context.CancelFunc
	committed       bool
}

func (m *Manager) requestRefresh(wait bool, desired *config.Config, expectedRevision *uint64) *refreshTicket {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		if !wait {
			return nil
		}
		result := make(chan error, 1)
		result <- context.Canceled
		close(result)
		return &refreshTicket{result: result}
	}
	m.requestSeq++
	sequence := m.requestSeq
	if desired != nil {
		m.supersedePendingLocked()
		m.pendingUpdate = &pendingConfigUpdate{
			sequence:         sequence,
			config:           desired.Clone(),
			expectedRevision: cloneRevision(expectedRevision),
		}
	}
	var ticket *refreshTicket
	if wait {
		result := make(chan error, 1)
		m.waiters[sequence] = result
		budgetCfg := m.baseCfg
		urlCount := len(m.baseCfg.Subscriptions)
		if desired != nil {
			budgetCfg = desired
			urlCount = len(desired.Subscriptions)
		}
		ticket = &refreshTicket{
			sequence: sequence,
			result:   result,
			waitFor:  m.waitBudgetFn(budgetCfg, urlCount),
		}
	}
	m.mu.Unlock()

	select {
	case m.manualRefresh <- struct{}{}:
	default:
	}
	return ticket
}

func (m *Manager) supersedePendingLocked() {
	if m.pendingUpdate == nil {
		return
	}
	sequence := m.pendingUpdate.sequence
	if m.activeBatch != nil && m.activeBatch.selectedPending == sequence {
		if m.activeBatch.committed {
			m.pendingUpdate = nil
			return
		}
		m.canceled[sequence] = errSubscriptionUpdateSuperseded
		m.activeBatch.cancel()
		m.pendingUpdate = nil
		return
	}
	if waiter, ok := m.waiters[sequence]; ok {
		waiter <- errSubscriptionUpdateSuperseded
		close(waiter)
		delete(m.waiters, sequence)
	}
	delete(m.canceled, sequence)
	m.pendingUpdate = nil
}

// cancelRefreshTicket cancels the scheduled/active batch containing sequence.
// The caller then waits for ticket.result, which guarantees it cannot return a
// timeout while the same batch is still able to commit in the background.
func (m *Manager) cancelRefreshTicket(sequence uint64, err error) bool {
	m.mu.Lock()
	_, pending := m.waiters[sequence]
	if !pending {
		m.mu.Unlock()
		return false
	}
	if m.activeBatch != nil && sequence <= m.activeBatch.upTo {
		if m.activeBatch.committed {
			// The runtime and persistence transaction has crossed its final
			// publish barrier. A caller-side timeout can no longer cancel it and
			// must not replace its eventual success result with a stale timeout.
			m.mu.Unlock()
			return false
		}
		m.canceled[sequence] = err
		m.activeBatch.cancel()
		m.mu.Unlock()
		return true
	}
	// This request has not started. Remove its pending configuration and
	// complete it immediately; no earlier batch needs to finish before the
	// caller can safely observe cancellation.
	if m.pendingUpdate != nil && m.pendingUpdate.sequence == sequence {
		m.pendingUpdate = nil
	}
	delete(m.canceled, sequence)
	waiter := m.waiters[sequence]
	delete(m.waiters, sequence)
	waiter <- err
	close(waiter)
	m.mu.Unlock()
	return false
}

func (m *Manager) notifyConfigChanged() {
	select {
	case m.configChanged <- struct{}{}:
	default:
	}
}

func (m *Manager) requestedSequence() uint64 {
	m.mu.RLock()
	sequence := m.requestSeq
	m.mu.RUnlock()
	return sequence
}

func (m *Manager) completeRequests(upTo uint64, err error) {
	m.mu.Lock()
	if upTo > m.completedSeq {
		m.completedSeq = upTo
	}
	if m.activeBatch != nil && m.activeBatch.upTo == upTo {
		m.activeBatch.cancel()
		m.activeBatch = nil
	}
	for sequence, waiter := range m.waiters {
		if sequence > upTo {
			continue
		}
		requestErr := err
		if canceledErr, ok := m.canceled[sequence]; ok {
			requestErr = canceledErr
		}
		waiter <- requestErr
		close(waiter)
		delete(m.waiters, sequence)
	}
	for sequence := range m.canceled {
		if sequence <= upTo {
			delete(m.canceled, sequence)
		}
	}
	m.mu.Unlock()
}

func (m *Manager) pendingManualTarget() (uint64, bool) {
	m.mu.RLock()
	target := m.requestSeq
	pending := target > m.completedSeq
	m.mu.RUnlock()
	return target, pending
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
			err := m.runScheduledRefresh(target)
			m.completeRequests(target, err)
		case <-m.manualRefresh:
			stopTimer(timer)
			target, pending := m.pendingManualTarget()
			if !pending {
				continue
			}
			err := m.runScheduledRefresh(target)
			m.completeRequests(target, err)
		}
	}
}

func (m *Manager) runScheduledRefresh(target uint64) error {
	ctx, cancel := context.WithCancel(m.ctx)
	m.mu.Lock()
	m.activeBatch = &refreshBatch{upTo: target, cancel: cancel}
	for sequence := range m.canceled {
		if sequence <= target {
			cancel()
			break
		}
	}
	m.mu.Unlock()
	return m.doRefreshContext(ctx, target)
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

// refreshWaitBudget is a conservative upper bound for one legal refresh. A
// source timeout applies per URL, not per batch, so the fetch portion must
// include every concurrency wave. Candidate preflight is likewise bounded per
// node wave. The small fixed allowance covers config generation and atomic I/O.
func refreshWaitBudget(cfg *config.Config, urlCount int) time.Duration {
	if cfg == nil {
		return 30 * time.Second
	}
	fetchTimeout := cfg.SubscriptionRefresh.Timeout
	if fetchTimeout <= 0 {
		fetchTimeout = 30 * time.Second
	}
	fetchConcurrency := config.NormalizeSubscriptionFetchConcurrency(cfg.SubscriptionRefresh.FetchConcurrency)
	fetchWaves := ceilingDivision(urlCount, fetchConcurrency)
	budget := multiplyDuration(fetchTimeout, fetchWaves)

	inlineNodes := 0
	for _, node := range cfg.Nodes {
		if node.Source == config.NodeSourceInline {
			inlineNodes++
		}
	}
	maximumSubscriptionNodes := urlCount * config.MaxSubscriptionNodesPerSource
	if maximumSubscriptionNodes > config.MaxSubscriptionNodesTotal {
		maximumSubscriptionNodes = config.MaxSubscriptionNodesTotal
	}
	probeConcurrency := cfg.ProbeConcurrencyOrDefault()
	if probeConcurrency < 1 {
		probeConcurrency = 1
	}
	healthTimeout := cfg.SubscriptionRefresh.HealthCheckTimeout
	if healthTimeout <= 0 {
		healthTimeout = 60 * time.Second
	}
	healthWaves := ceilingDivision(inlineNodes+maximumSubscriptionNodes, probeConcurrency)
	budget = addDuration(budget, multiplyDuration(healthTimeout, healthWaves))

	drainTimeout := cfg.SubscriptionRefresh.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 30 * time.Second
	}
	budget = addDuration(budget, drainTimeout)
	budget = addDuration(budget, 5*time.Second)
	if budget <= 0 {
		return 30 * time.Second
	}
	return budget
}

func ceilingDivision(value, divisor int) int {
	if value <= 0 {
		return 0
	}
	if divisor <= 0 {
		return value
	}
	return 1 + (value-1)/divisor
}

func multiplyDuration(value time.Duration, factor int) time.Duration {
	if value <= 0 || factor <= 0 {
		return 0
	}
	const maximum = time.Duration(1<<63 - 1)
	if int64(factor) > int64(maximum/value) {
		return maximum
	}
	return value * time.Duration(factor)
}

func addDuration(left, right time.Duration) time.Duration {
	const maximum = time.Duration(1<<63 - 1)
	if right > 0 && left > maximum-right {
		return maximum
	}
	return left + right
}

// doRefresh performs one atomic fetch, file update, and runtime reload.
func (m *Manager) doRefresh() (refreshErr error) {
	return m.doRefreshContext(m.ctx, ^uint64(0))
}

func (m *Manager) doRefreshContext(refreshCtx context.Context, targetSequence uint64) (refreshErr error) {
	select {
	case <-refreshCtx.Done():
		return refreshCtx.Err()
	case <-m.refreshSlot:
	}
	defer func() { m.refreshSlot <- struct{}{} }()

	var selectedPending uint64
	var selectedExpectedRevision *uint64
	defer func() {
		if refreshErr != nil && selectedPending != 0 {
			m.mu.Lock()
			if m.pendingUpdate != nil && m.pendingUpdate.sequence == selectedPending {
				m.pendingUpdate = nil
			}
			m.mu.Unlock()
		}
	}()

	m.mu.Lock()
	m.status.IsRefreshing = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.status.IsRefreshing = false
		m.status.RefreshCount++
		m.status.LastRefresh = time.Now()
		if refreshErr != nil {
			m.status.LastError = monitor.SanitizeProbeError(refreshErr)
		} else {
			m.status.LastError = ""
		}
		m.mu.Unlock()
	}()

	m.mu.Lock()
	baseCfg := m.baseCfg.Clone()
	if m.pendingUpdate != nil && m.pendingUpdate.sequence <= targetSequence {
		selectedPending = m.pendingUpdate.sequence
		selectedExpectedRevision = cloneRevision(m.pendingUpdate.expectedRevision)
		baseCfg = m.pendingUpdate.config.Clone()
		if m.activeBatch != nil && m.activeBatch.upTo == targetSequence {
			m.activeBatch.selectedPending = selectedPending
		}
	}
	m.mu.Unlock()
	if baseCfg == nil {
		return errors.New("subscription config is unavailable")
	}
	if err := refreshCtx.Err(); err != nil {
		return err
	}
	if selectedPending != 0 && len(baseCfg.Subscriptions) == 0 {
		if err := m.validatePendingGeneration(refreshCtx, selectedPending); err != nil {
			return err
		}
		committed, err := m.commitClearedSubscriptions(refreshCtx, baseCfg, targetSequence, selectedPending, selectedExpectedRevision)
		if err != nil {
			return err
		}
		m.mu.Lock()
		m.baseCfg = committed.Clone()
		if m.pendingUpdate != nil && m.pendingUpdate.sequence == selectedPending {
			m.pendingUpdate = nil
		}
		m.sourceCache = make(map[string][]config.NodeConfig)
		m.lastSubHash = ""
		m.lastNodesModTime = time.Time{}
		m.status.NodesModified = false
		m.status.NodeCount = len(committed.Nodes)
		m.mu.Unlock()
		m.logger.Infof("subscription config cleared; %d inline nodes remain", len(committed.Nodes))
		return nil
	}
	// A queued manual signal can outlive a synchronous clear operation. Treat
	// it as already satisfied instead of turning a successful clear into a
	// spurious "no nodes fetched" status error.
	if len(baseCfg.Subscriptions) == 0 {
		return nil
	}
	nodesFilePath := nodesFilePathForConfig(baseCfg)

	m.logger.Infof("starting subscription refresh")
	plan, err := m.fetchAllSubscriptions(refreshCtx, baseCfg, nodesFilePath, selectedPending == 0)
	if err != nil {
		m.logger.Errorf("fetch subscriptions failed: %v", err)
		return err
	}
	m.logger.Infof("prepared %d subscription nodes", len(plan.nodes))
	if err := m.validatePendingGeneration(refreshCtx, selectedPending); err != nil {
		return err
	}
	newCfg, committedNodesPath, err := m.commitRefreshPlan(refreshCtx, baseCfg, plan.nodes, targetSequence, selectedPending, selectedExpectedRevision)
	if err != nil {
		return err
	}

	newHash := m.computeNodesHash(plan.nodes)
	m.mu.Lock()
	m.baseCfg = newCfg.Clone()
	if selectedPending != 0 && m.pendingUpdate != nil && m.pendingUpdate.sequence == selectedPending {
		m.pendingUpdate = nil
	}
	for key := range m.sourceCache {
		if _, active := plan.activeKeys[key]; !active {
			delete(m.sourceCache, key)
		}
	}
	for key, nodes := range plan.cacheUpdates {
		m.sourceCache[key] = cloneNodes(nodes)
	}
	m.lastSubHash = newHash
	if info, statErr := os.Stat(committedNodesPath); statErr == nil {
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

const maxConfigCommitAttempts = 4

// commitRefreshPlan rebases the fetched nodes onto the latest committed
// configuration. This is deliberately done after the network fetch so a
// settings update or inline-node CRUD operation that completes while a slow
// provider is responding is not overwritten by a stale snapshot.
func (m *Manager) commitRefreshPlan(ctx context.Context, desired *config.Config, subscriptionNodes []config.NodeConfig, targetSequence, pendingGeneration uint64, expectedRevision *uint64) (*config.Config, string, error) {
	guardedCtx := commitguard.With(ctx, m.acquireCommitBarrier(ctx, targetSequence, pendingGeneration))
	for attempt := 0; attempt < maxConfigCommitAttempts; attempt++ {
		if err := m.validatePendingGeneration(ctx, pendingGeneration); err != nil {
			return nil, "", err
		}
		latest, revision := m.boxMgr.ConfigSnapshot()
		if latest == nil {
			return nil, "", errors.New("active config is unavailable")
		}
		if expectedRevision != nil && revision != *expectedRevision {
			return nil, "", configRevisionConflict(*expectedRevision, revision)
		}
		applySubscriptionSettings(latest, desired)
		candidate := m.createNewConfig(latest, cloneNodes(subscriptionNodes))
		nodesPath := nodesFilePathForConfig(candidate)
		err := m.boxMgr.CommitConfig(guardedCtx, revision, candidate, func(committed *config.Config) (func() error, error) {
			if err := m.validatePendingGeneration(ctx, pendingGeneration); err != nil {
				return nil, err
			}
			return m.persistSubscriptionState(committed, subscriptionNodes)
		})
		if err == nil {
			committed, _ := m.boxMgr.ConfigSnapshot()
			if committed == nil {
				return nil, "", errors.New("committed config is unavailable")
			}
			return committed, nodesPath, nil
		}

		_, currentRevision := m.boxMgr.ConfigSnapshot()
		if expectedRevision != nil && currentRevision != *expectedRevision {
			return nil, "", configRevisionConflict(*expectedRevision, currentRevision)
		}
		if currentRevision == revision {
			return nil, "", fmt.Errorf("commit subscription refresh: %w", err)
		}
		if expectedRevision != nil {
			return nil, "", configRevisionConflict(*expectedRevision, currentRevision)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, "", ctxErr
		}
	}
	return nil, "", errors.New("commit subscription refresh: configuration changed too frequently")
}

func (m *Manager) commitClearedSubscriptions(ctx context.Context, desired *config.Config, targetSequence, pendingGeneration uint64, expectedRevision *uint64) (*config.Config, error) {
	guardedCtx := commitguard.With(ctx, m.acquireCommitBarrier(ctx, targetSequence, pendingGeneration))
	for attempt := 0; attempt < maxConfigCommitAttempts; attempt++ {
		if err := m.validatePendingGeneration(ctx, pendingGeneration); err != nil {
			return nil, err
		}
		latest, revision := m.boxMgr.ConfigSnapshot()
		if latest == nil {
			return nil, errors.New("active config is unavailable")
		}
		if expectedRevision != nil && revision != *expectedRevision {
			return nil, configRevisionConflict(*expectedRevision, revision)
		}
		applySubscriptionSettings(latest, desired)
		candidate := m.createNewConfig(latest, nil)
		// Zero nodes are valid only for the first-run management-only state.
		// Once a pool has active nodes, clearing subscriptions must not
		// accidentally tear the runtime down to an unusable empty pool.
		if len(latest.Nodes) > 0 && len(candidate.Nodes) == 0 {
			return nil, errors.New("cannot clear subscriptions because no inline nodes would remain")
		}
		err := m.boxMgr.CommitConfig(guardedCtx, revision, candidate, func(committed *config.Config) (func() error, error) {
			if err := m.validatePendingGeneration(ctx, pendingGeneration); err != nil {
				return nil, err
			}
			return m.persistSubscriptionState(committed, nil)
		})
		if err == nil {
			committed, _ := m.boxMgr.ConfigSnapshot()
			if committed == nil {
				return nil, errors.New("committed config is unavailable")
			}
			return committed, nil
		}
		_, currentRevision := m.boxMgr.ConfigSnapshot()
		if expectedRevision != nil && currentRevision != *expectedRevision {
			return nil, configRevisionConflict(*expectedRevision, currentRevision)
		}
		if currentRevision == revision {
			return nil, fmt.Errorf("clear subscriptions: %w", err)
		}
		if expectedRevision != nil {
			return nil, configRevisionConflict(*expectedRevision, currentRevision)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
	}
	return nil, errors.New("clear subscriptions: configuration changed too frequently")
}

func (m *Manager) acquireCommitBarrier(ctx context.Context, targetSequence, pendingGeneration uint64) commitguard.AcquireFunc {
	return func() (func(), func(), error) {
		m.mu.Lock()
		if err := ctx.Err(); err != nil {
			m.mu.Unlock()
			return nil, nil, err
		}
		if pendingGeneration != 0 {
			current := m.pendingUpdate != nil && m.pendingUpdate.sequence == pendingGeneration
			if canceledErr, canceled := m.canceled[pendingGeneration]; canceled {
				m.mu.Unlock()
				return nil, nil, canceledErr
			}
			if !current {
				m.mu.Unlock()
				return nil, nil, errSubscriptionUpdateSuperseded
			}
		}
		markCommitted := func() {
			if m.activeBatch != nil && m.activeBatch.upTo == targetSequence {
				m.activeBatch.committed = true
			}
		}
		return markCommitted, m.mu.Unlock, nil
	}
}

func (m *Manager) validatePendingGeneration(ctx context.Context, generation uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if generation == 0 {
		return nil
	}
	m.mu.RLock()
	current := m.pendingUpdate != nil && m.pendingUpdate.sequence == generation
	m.mu.RUnlock()
	if !current {
		return errSubscriptionUpdateSuperseded
	}
	return nil
}

func applySubscriptionSettings(target, desired *config.Config) {
	target.Subscriptions = append([]string(nil), desired.Subscriptions...)
	target.SubscriptionRefresh = desired.SubscriptionRefresh
}

// persistSubscriptionState updates config.yaml and nodes.txt as one logical
// transaction. CommitConfig invokes the returned rollback after either a
// persistence or runtime reload failure.
func (m *Manager) persistSubscriptionState(candidate *config.Config, subscriptionNodes []config.NodeConfig) (func() error, error) {
	if m.saveSettingsFn == nil && m.writeNodesFn == nil {
		return candidate.SaveSubscriptionStateTransaction(subscriptionNodes, len(candidate.Subscriptions) == 0)
	}
	configPath := candidate.FilePath()
	nodesPath := nodesFilePathForConfig(candidate)
	if strings.TrimSpace(configPath) == "" {
		return nil, errors.New("config file path is unknown")
	}
	if filepath.Clean(configPath) == filepath.Clean(nodesPath) {
		return nil, errors.New("config file and nodes file must be different")
	}

	configBefore, err := config.CaptureFileSnapshot(configPath)
	if err != nil {
		return nil, fmt.Errorf("snapshot config: %w", err)
	}
	nodesBefore, err := config.CaptureFileSnapshot(nodesPath)
	if err != nil {
		return nil, fmt.Errorf("snapshot nodes: %w", err)
	}
	configExpected := configBefore
	nodesExpected := nodesBefore
	checkpointConfig := func() error {
		var err error
		configExpected, err = config.CaptureFileSnapshot(configPath)
		if err != nil {
			return fmt.Errorf("snapshot persisted config: %w", err)
		}
		return nil
	}
	checkpointNodes := func() error {
		var err error
		nodesExpected, err = config.CaptureFileSnapshot(nodesPath)
		if err != nil {
			return fmt.Errorf("snapshot persisted nodes: %w", err)
		}
		return nil
	}
	rollback := func() error {
		return errors.Join(
			config.RestoreFileSnapshotCAS(nodesBefore, nodesExpected),
			config.RestoreFileSnapshotCAS(configBefore, configExpected),
		)
	}

	saveSettings := m.saveSettingsFn
	if saveSettings == nil {
		saveSettings = func(cfg *config.Config) error { return cfg.SaveSettings() }
	}
	writeNodes := m.writeNodesFn
	if writeNodes == nil {
		writeNodes = config.WriteNodesToFile
	}
	if err := saveSettings(candidate); err != nil {
		if checkpointErr := checkpointConfig(); checkpointErr != nil {
			return nil, fmt.Errorf("save subscription settings: %w; capture rollback checkpoint: %v", err, checkpointErr)
		}
		return rollback, fmt.Errorf("save subscription settings: %w", err)
	}
	if err := checkpointConfig(); err != nil {
		return nil, fmt.Errorf("capture subscription config rollback checkpoint: %w", err)
	}
	if err := writeNodes(nodesPath, cloneNodes(subscriptionNodes)); err != nil {
		if checkpointErr := checkpointNodes(); checkpointErr != nil {
			return nil, fmt.Errorf("write subscription nodes: %w; capture rollback checkpoint: %v", err, checkpointErr)
		}
		return rollback, fmt.Errorf("write subscription nodes: %w", err)
	}
	if err := checkpointNodes(); err != nil {
		return nil, fmt.Errorf("capture subscription nodes rollback checkpoint: %w", err)
	}
	return rollback, nil
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
	if m.status.NodesModified {
		m.mu.RUnlock()
		return true
	}
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
		// Once a successful refresh established a hash, deletion, permission
		// loss, and other read failures are all observable modifications. Latch
		// the state so a later recreation cannot hide the diagnostic event.
		m.MarkNodesModified()
		return true
	}
	modTime := info.ModTime()
	if !modTime.After(lastMod) {
		return false // File hasn't been modified
	}

	// Slow path: file was modified, compute hash
	data, err := os.ReadFile(nodesFilePath)
	if err != nil {
		m.MarkNodesModified()
		return true
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
	if changed {
		m.status.NodesModified = true
	}
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
func (m *Manager) fetchAllSubscriptions(ctx context.Context, baseCfg *config.Config, nodesFilePath string, allowAggregateFallback bool) (subscriptionFetchPlan, error) {
	results, stats := config.FetchSubscriptionSources(ctx, baseCfg.Subscriptions, config.SubscriptionFetchOptions{
		Timeout:              baseCfg.SubscriptionRefresh.Timeout,
		Concurrency:          baseCfg.SubscriptionRefresh.FetchConcurrency,
		AllowPrivateNetworks: baseCfg.SubscriptionRefresh.AllowPrivateNetworks,
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

	if unresolved > 0 && allowAggregateFallback {
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
	if unresolved > 0 {
		if lastErr == nil {
			lastErr = fmt.Errorf("%d subscription sources could not be refreshed", unresolved)
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
