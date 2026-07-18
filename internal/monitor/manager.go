package monitor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"easy_proxies/internal/probetarget"

	M "github.com/sagernet/sing/common/metadata"
)

// Config mirrors user settings needed by the monitoring server.
type Config struct {
	Enabled          bool
	Listen           string
	ProbeTarget      string
	Password         string
	TLSCertFile      string
	TLSKeyFile       string
	ProxyUsername    string // 代理池的用户名（用于导出）
	ProxyPassword    string // 代理池的密码（用于导出）
	ExternalIP       string // 外部 IP 地址，用于导出时替换 0.0.0.0
	SkipCertVerify   bool   // 全局跳过 SSL 证书验证
	ProbeConcurrency int    // 全局批量探测并发数
}

// NodeInfo is static metadata about a proxy entry.
type NodeInfo struct {
	Tag           string `json:"tag"`
	Name          string `json:"name"`
	URI           string `json:"-"`
	Mode          string `json:"mode"`
	ListenAddress string `json:"listen_address,omitempty"`
	Port          uint16 `json:"port,omitempty"`
	Username      string `json:"-"`
	Password      string `json:"-"`
	Region        string `json:"region,omitempty"`  // GeoIP region code: "jp", "kr", "us", "hk", "tw", "other"
	Country       string `json:"country,omitempty"` // Full country name from GeoIP
	ExitIP        string `json:"exit_ip,omitempty"` // Public egress IP observed through this node
}

// TimelineEvent represents a single usage event for debug tracking.
type TimelineEvent struct {
	Time      time.Time `json:"time"`
	Success   bool      `json:"success"`
	LatencyMs int64     `json:"latency_ms"`
	Error     string    `json:"error,omitempty"`
}

const maxTimelineSize = 20

// Snapshot is a runtime view of a proxy node.
type Snapshot struct {
	NodeInfo
	FailureCount      int             `json:"failure_count"`
	SuccessCount      int64           `json:"success_count"`
	Blacklisted       bool            `json:"blacklisted"`
	BlacklistedUntil  time.Time       `json:"blacklisted_until"`
	CoolingDown       bool            `json:"cooling_down"`
	CooldownUntil     time.Time       `json:"cooldown_until"`
	ActiveConnections int32           `json:"active_connections"`
	LastError         string          `json:"last_error,omitempty"`
	LastFailure       time.Time       `json:"last_failure,omitempty"`
	LastSuccess       time.Time       `json:"last_success,omitempty"`
	LastProbeLatency  time.Duration   `json:"last_probe_latency,omitempty"`
	LastLatencyMs     int64           `json:"last_latency_ms"`
	Available         bool            `json:"available"`
	InitialCheckDone  bool            `json:"initial_check_done"`
	Timeline          []TimelineEvent `json:"timeline,omitempty"`
}

// PersistedHealthState is the restart-safe subset of a node's monitor state.
// Active connections, callbacks and the short debug timeline are deliberately
// process-local and are not restored.
type PersistedHealthState struct {
	FailureCount     int           `yaml:"failure_count,omitempty"`
	SuccessCount     int64         `yaml:"success_count,omitempty"`
	BlacklistedUntil time.Time     `yaml:"blacklisted_until,omitempty"`
	CooldownUntil    time.Time     `yaml:"cooldown_until,omitempty"`
	LastError        string        `yaml:"last_error,omitempty"`
	LastFailure      time.Time     `yaml:"last_failure,omitempty"`
	LastSuccess      time.Time     `yaml:"last_success,omitempty"`
	LastProbeLatency time.Duration `yaml:"last_probe_latency,omitempty"`
	Available        bool          `yaml:"available,omitempty"`
	InitialCheckDone bool          `yaml:"initial_check_done,omitempty"`
}

type probeFunc func(ctx context.Context) (time.Duration, error)
type releaseFunc func()

type EntryHandle struct {
	ref *entry
}

type entry struct {
	info             NodeInfo
	failure          int
	success          int64
	timeline         []TimelineEvent
	blacklist        bool
	until            time.Time
	coolingDown      bool
	cooldownUntil    time.Time
	lastError        string
	lastFail         time.Time
	lastOK           time.Time
	lastProbe        time.Duration
	active           atomic.Int32
	probe            probeFunc
	release          releaseFunc
	blacklistFn      func(time.Duration)
	initialCheckDone bool
	available        bool
	mu               sync.RWMutex
	probeMu          sync.Mutex
	probeGeneration  uint64
	probeCall        *inFlightProbe
	probeSlots       chan struct{}
	probeLifecycleMu *sync.RWMutex
	probeStopped     *atomic.Bool
	probeWG          *sync.WaitGroup
}

type probeOutcome struct {
	latency time.Duration
	err     error
}

type inFlightProbe struct {
	generation uint64
	done       chan struct{}
	result     probeOutcome
}

type probeSweepRequest struct {
	generation uint64
	ctx        context.Context
	timeout    time.Duration
	done       chan struct{}
	err        error
}

const (
	defaultProbeConcurrency = 32
	maxProbeConcurrency     = 1024
	defaultProbeTimeout     = 10 * time.Second
	maxHungProbeCallbacks   = 64
)

var (
	errProbeSuperseded     = errors.New("probe callback was replaced")
	errProbeManagerStopped = errors.New("probe manager stopped")
)

// ProbeTarget is an immutable snapshot of the live health-check destination.
// TLS remains enabled for HTTPS even when verification is explicitly skipped.
type ProbeTarget struct {
	Destination    M.Socksaddr
	Host           string
	TLS            bool
	SkipCertVerify bool
}

// Manager aggregates all node states for the UI/API.
type Manager struct {
	cfg              Config
	probeTarget      ProbeTarget
	probeReady       bool
	probeConcurrency int
	mu               sync.RWMutex
	nodes            map[string]*entry
	ctx              context.Context
	cancel           context.CancelFunc
	logger           Logger
	loggerMu         sync.RWMutex

	probeSweepActive atomic.Int32
	probeSweepTotal  atomic.Int32
	probeSweepDone   atomic.Int32
	probeSweepOK     atomic.Int32
	probeSweepFail   atomic.Int32

	probeGate           sync.Mutex
	sweepRunning        bool
	requestedGeneration uint64
	completedGeneration uint64
	probeRequests       []*probeSweepRequest
	probeCond           *sync.Cond
	probeSlots          chan struct{}
	probeLifecycleMu    sync.RWMutex
	probeStopped        atomic.Bool
	probeWG             sync.WaitGroup
}

// Logger interface for logging
type Logger interface {
	Info(args ...any)
	Warn(args ...any)
}

func clampProbeConcurrency(n int) int {
	if n <= 0 {
		return defaultProbeConcurrency
	}
	if n > maxProbeConcurrency {
		return maxProbeConcurrency
	}
	return n
}

func resolveProbeTarget(value string, skipCertVerify bool) (ProbeTarget, bool, error) {
	parsed, ready, err := probetarget.Parse(value)
	if err != nil || !ready {
		return ProbeTarget{}, ready, err
	}
	return ProbeTarget{
		Destination:    M.ParseSocksaddrHostPort(parsed.Host, parsed.Port),
		Host:           parsed.Host,
		TLS:            parsed.TLS,
		SkipCertVerify: skipCertVerify,
	}, true, nil
}

// ResolveProbeTarget returns an immutable target suitable for validating a
// candidate configuration without mutating the live monitor manager.
func ResolveProbeTarget(value string, skipCertVerify bool) (ProbeTarget, bool, error) {
	return resolveProbeTarget(value, skipCertVerify)
}

// NewManager constructs a manager and pre-validates the probe target.
func NewManager(cfg Config) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	target, ready, err := resolveProbeTarget(cfg.ProbeTarget, cfg.SkipCertVerify)
	if err != nil {
		cancel()
		return nil, err
	}
	m := &Manager{
		cfg:              cfg,
		probeTarget:      target,
		probeReady:       ready,
		probeConcurrency: clampProbeConcurrency(cfg.ProbeConcurrency),
		nodes:            make(map[string]*entry),
		ctx:              ctx,
		cancel:           cancel,
		probeSlots:       make(chan struct{}, maxHungProbeCallbacks),
	}
	m.probeCond = sync.NewCond(&m.probeGate)
	return m, nil
}

// ProbeSweepProgress returns a consistent snapshot for the API and WebUI.
func (m *Manager) ProbeSweepProgress() (active bool, done, total, ok, failed int) {
	return m.probeSweepActive.Load() == 1,
		int(m.probeSweepDone.Load()),
		int(m.probeSweepTotal.Load()),
		int(m.probeSweepOK.Load()),
		int(m.probeSweepFail.Load())
}

// SetProbeConcurrency applies a live, process-wide worker limit.
func (m *Manager) SetProbeConcurrency(n int) {
	m.mu.Lock()
	m.probeConcurrency = clampProbeConcurrency(n)
	m.mu.Unlock()
}

func (m *Manager) ProbeConcurrency() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.probeConcurrency
}

// SetProbeTarget updates both the destination and TLS verification policy for
// existing probe callbacks. Pool callbacks resolve this snapshot per request.
func (m *Manager) SetProbeTarget(value string, skipCertVerify bool) error {
	target, ready, err := resolveProbeTarget(value, skipCertVerify)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.cfg.ProbeTarget = value
	m.cfg.SkipCertVerify = skipCertVerify
	m.probeTarget = target
	m.probeReady = ready
	m.mu.Unlock()
	return nil
}

// SetLogger sets the logger for the manager.
func (m *Manager) SetLogger(logger Logger) {
	m.loggerMu.Lock()
	m.logger = logger
	m.loggerMu.Unlock()
}

func (m *Manager) loggerSnapshot() Logger {
	m.loggerMu.RLock()
	defer m.loggerMu.RUnlock()
	return m.logger
}

// StartPeriodicHealthCheck starts a background goroutine that periodically checks all nodes.
// interval: how often to check (e.g., 30 * time.Second)
// timeout: timeout for each probe (e.g., 10 * time.Second)
func (m *Manager) StartPeriodicHealthCheck(interval, timeout time.Duration) {
	go func() {
		// The initial pass uses the same process-wide coordinator as periodic,
		// post-reload and WebUI-triggered sweeps.
		_ = m.probeAllNodesContext(m.ctx, timeout)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-m.ctx.Done():
				return
			case <-ticker.C:
				_ = m.probeAllNodesContext(m.ctx, timeout)
			}
		}
	}()

	if logger := m.loggerSnapshot(); logger != nil {
		logger.Info("periodic health check started, interval: ", interval)
	}
}

// ProbeAllNow triggers a one-time health check on all nodes (e.g. after reload).
func (m *Manager) ProbeAllNow(timeout time.Duration) {
	_ = m.ProbeAllNowContext(context.Background(), timeout)
}

// ProbeAllNowContext triggers a synchronous health check and lets callers
// cancel both their wait and the underlying sweep work. Overlapping requests
// are still coalesced into the fewest possible passes.
func (m *Manager) ProbeAllNowContext(ctx context.Context, timeout time.Duration) error {
	return m.probeAllNodesContext(ctx, timeout)
}

func (m *Manager) probeAllNodesContext(ctx context.Context, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.probeStopped.Load() {
		return errProbeManagerStopped
	}

	timeout = probeTimeout(timeout)
	request := &probeSweepRequest{
		ctx:     ctx,
		timeout: timeout,
		done:    make(chan struct{}),
	}
	m.probeGate.Lock()
	if m.probeStopped.Load() {
		m.probeGate.Unlock()
		return errProbeManagerStopped
	}
	m.requestedGeneration++
	request.generation = m.requestedGeneration
	m.probeRequests = append(m.probeRequests, request)
	startCoordinator := !m.sweepRunning
	if startCoordinator {
		m.sweepRunning = true
		m.probeSweepActive.Store(1)
	}
	m.probeGate.Unlock()
	if startCoordinator {
		go m.runProbeCoordinator()
	}

	select {
	case <-request.done:
		return request.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runProbeCoordinator serializes batch probes by generation. Requests that
// arrive during a pass are folded into one follow-up pass. A pass is canceled
// only after every operation context represented by that pass is canceled.
func (m *Manager) runProbeCoordinator() {
	for {
		m.probeGate.Lock()
		if len(m.probeRequests) == 0 {
			m.sweepRunning = false
			m.probeSweepActive.Store(0)
			m.probeCond.Broadcast()
			m.probeGate.Unlock()
			return
		}
		batch := m.probeRequests
		m.probeRequests = nil
		sweepGeneration := batch[len(batch)-1].generation
		m.probeGate.Unlock()

		active := make([]*probeSweepRequest, 0, len(batch))
		sweepTimeout := time.Duration(0)
		for _, request := range batch {
			if request.ctx.Err() != nil {
				continue
			}
			active = append(active, request)
			if request.timeout > sweepTimeout {
				sweepTimeout = request.timeout
			}
		}

		var sweepErr error
		if len(active) > 0 {
			sweepCtx, cancelSweep := context.WithCancel(context.Background())
			var remaining atomic.Int32
			remaining.Store(int32(len(active)))
			stops := make([]func() bool, 0, len(active)+1)
			for _, request := range active {
				stops = append(stops, context.AfterFunc(request.ctx, func() {
					if remaining.Add(-1) == 0 {
						cancelSweep()
					}
				}))
			}
			stops = append(stops, context.AfterFunc(m.ctx, cancelSweep))
			sweepErr = m.runProbeSweep(sweepCtx, sweepTimeout)
			cancelSweep()
			for _, stop := range stops {
				stop()
			}
			if m.ctx.Err() != nil {
				sweepErr = errProbeManagerStopped
			}
		}

		m.probeGate.Lock()
		if sweepGeneration > m.completedGeneration {
			m.completedGeneration = sweepGeneration
		}
		caughtUp := len(m.probeRequests) == 0
		if caughtUp {
			m.sweepRunning = false
			m.probeSweepActive.Store(0)
		}
		for _, request := range batch {
			if err := request.ctx.Err(); err != nil {
				request.err = err
			} else {
				request.err = sweepErr
			}
			close(request.done)
		}
		m.probeCond.Broadcast()
		m.probeGate.Unlock()
		if caughtUp {
			return
		}
	}
}

func probeTimeout(value time.Duration) time.Duration {
	if value <= 0 {
		return defaultProbeTimeout
	}
	return value
}

func sweepTimeout(total, workers int, perProbe time.Duration) time.Duration {
	if total <= 0 || workers <= 0 {
		return perProbe
	}
	batches := (total + workers - 1) / workers
	// One extra batch is bounded scheduling slack. Every individual probe still
	// has its own tighter deadline.
	return time.Duration(batches+1) * perProbe
}

// runProbeSweep executes one bounded worker-pool pass. The outer sweep context
// and the per-entry contexts both have deadlines, so even an outbound that
// ignores cancellation cannot keep a worker or WaitGroup stuck indefinitely.
func (m *Manager) runProbeSweep(operationCtx context.Context, timeout time.Duration) error {
	if operationCtx == nil {
		operationCtx = context.Background()
	}
	if err := operationCtx.Err(); err != nil {
		return err
	}
	m.mu.RLock()
	entries := make([]*entry, 0, len(m.nodes))
	for _, e := range m.nodes {
		entries = append(entries, e)
	}
	m.mu.RUnlock()

	m.probeSweepTotal.Store(int32(len(entries)))
	m.probeSweepDone.Store(0)
	m.probeSweepOK.Store(0)
	m.probeSweepFail.Store(0)
	if len(entries) == 0 {
		return nil
	}

	logger := m.loggerSnapshot()
	if logger != nil {
		logger.Info("starting health check for ", len(entries), " nodes")
	}
	if _, ready := m.DestinationForProbe(); !ready {
		m.probeSweepDone.Store(int32(len(entries)))
		if logger != nil {
			logger.Warn("probe target not configured; existing node state left unchanged")
		}
		return nil
	}

	perProbe := probeTimeout(timeout)
	workerLimit := m.ProbeConcurrency()
	if workerLimit > len(entries) {
		workerLimit = len(entries)
	}
	sweepCtx, sweepCancel := context.WithTimeout(operationCtx, sweepTimeout(len(entries), workerLimit, perProbe))
	defer sweepCancel()

	jobs := make(chan *entry, len(entries))
	var wg sync.WaitGroup
	var availableCount atomic.Int32
	var failedCount atomic.Int32

	for worker := 0; worker < workerLimit; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for entry := range jobs {
				if operationCtx.Err() != nil {
					m.probeSweepDone.Add(1)
					continue
				}
				entry.probeMu.Lock()
				entry.mu.RLock()
				probeFn := entry.probe
				tag := entry.info.Tag
				uri := entry.info.URI
				entry.mu.RUnlock()
				probeGeneration := entry.probeGeneration
				entry.probeMu.Unlock()

				if probeFn == nil {
					if entry.markProbeResultGeneration(probeGeneration, 0, nil) {
						availableCount.Add(1)
						m.probeSweepOK.Add(1)
					}
					m.probeSweepDone.Add(1)
					continue
				}

				probeCtx, cancel := context.WithTimeout(sweepCtx, perProbe)
				latency, err, probeGeneration := entry.executeProbeGeneration(probeCtx, sweepCtx, perProbe)
				cancel()
				if operationCtx.Err() != nil {
					m.probeSweepDone.Add(1)
					continue
				}
				if !entry.markProbeResultGeneration(probeGeneration, latency, err) {
					m.probeSweepDone.Add(1)
					continue
				}
				if err != nil {
					failedCount.Add(1)
					m.probeSweepFail.Add(1)
					if logger != nil {
						logger.Warn("probe failed: ", FormatProbeFailure(tag, uri, err))
					}
				} else {
					availableCount.Add(1)
					m.probeSweepOK.Add(1)
				}
				m.probeSweepDone.Add(1)
			}
		}()
	}
	queueing := true
	for _, entry := range entries {
		select {
		case jobs <- entry:
		case <-sweepCtx.Done():
			queueing = false
		}
		if !queueing {
			break
		}
	}
	close(jobs)
	wg.Wait()

	if logger != nil {
		logger.Info("health check completed: ", availableCount.Load(), " available, ", failedCount.Load(), " failed")
	}
	if m.ctx.Err() != nil {
		return errProbeManagerStopped
	}
	return operationCtx.Err()
}

// Stop stops the periodic health check.
func (m *Manager) Stop() {
	m.probeLifecycleMu.Lock()
	m.probeStopped.Store(true)
	if m.cancel != nil {
		m.cancel()
	}
	m.probeLifecycleMu.Unlock()
}

// StopAndWait prevents new callbacks and waits for cooperative in-flight
// probes. The caller controls the upper bound for legacy callbacks that ignore
// cancellation.
func (m *Manager) StopAndWait(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.Stop()
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan struct{})
	go func() {
		m.probeWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Register ensures a node is tracked and returns its entry.
func (m *Manager) Register(info NodeInfo) *EntryHandle {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.nodes[info.Tag]
	if !ok {
		e = &entry{
			info:             info,
			timeline:         make([]TimelineEvent, 0, maxTimelineSize),
			probeSlots:       m.probeSlots,
			probeLifecycleMu: &m.probeLifecycleMu,
			probeStopped:     &m.probeStopped,
			probeWG:          &m.probeWG,
		}
		m.nodes[info.Tag] = e
	} else {
		e.mu.Lock()
		e.info = info
		e.mu.Unlock()
	}
	return &EntryHandle{ref: e}
}

// ClearNodes removes all registered nodes. Call before re-registering
// during a config reload so stale entries don't persist in the dashboard.
func (m *Manager) ClearNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = make(map[string]*entry)
}

// RetainNodeURIs removes monitor entries that are no longer present after a
// successful reload while preserving health history for unchanged nodes.
func (m *Manager) RetainNodeURIs(activeURIs map[string]struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for tag, entry := range m.nodes {
		entry.mu.RLock()
		uri := strings.TrimSpace(entry.info.URI)
		entry.mu.RUnlock()
		if _, ok := activeURIs[uri]; !ok {
			delete(m.nodes, tag)
		}
	}
}

// DestinationForProbe exposes an immutable snapshot of the live probe target.
func (m *Manager) DestinationForProbe() (ProbeTarget, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.probeReady {
		return ProbeTarget{}, false
	}
	return m.probeTarget, true
}

// Snapshot returns a sorted copy of current node states.
// If onlyAvailable is true, only returns nodes that passed initial health check.
func (m *Manager) Snapshot() []Snapshot {
	return m.SnapshotFiltered(false)
}

// SnapshotFiltered returns a sorted copy of current node states.
// If onlyAvailable is true, only returns nodes that passed initial health check.
// Nodes that haven't been checked yet are also included (they will be checked on first use).
func (m *Manager) SnapshotFiltered(onlyAvailable bool) []Snapshot {
	m.mu.RLock()
	list := make([]*entry, 0, len(m.nodes))
	for _, e := range m.nodes {
		list = append(list, e)
	}
	m.mu.RUnlock()
	snapshots := make([]Snapshot, 0, len(list))
	for _, e := range list {
		snap := e.snapshot()
		// 如果只要可用节点：
		// - 跳过已完成检查但不可用的节点
		// - 保留未完成检查的节点（它们会在首次使用时被检查）
		if onlyAvailable && ((snap.InitialCheckDone && !snap.Available) || snap.Blacklisted || snap.CoolingDown) {
			continue
		}
		snapshots = append(snapshots, snap)
	}
	// 按延迟排序（延迟小的在前面，未测试的排在最后）
	sort.Slice(snapshots, func(i, j int) bool {
		latencyI := snapshots[i].LastLatencyMs
		latencyJ := snapshots[j].LastLatencyMs
		// -1 表示未测试，排在最后
		if latencyI < 0 && latencyJ < 0 {
			return snapshots[i].Name < snapshots[j].Name // 都未测试时按名称排序
		}
		if latencyI < 0 {
			return false // i 未测试，排在后面
		}
		if latencyJ < 0 {
			return true // j 未测试，i 排在前面
		}
		if latencyI == latencyJ {
			return snapshots[i].Name < snapshots[j].Name // 延迟相同时按名称排序
		}
		return latencyI < latencyJ
	})
	return snapshots
}

// Probe triggers a manual health check.
func (m *Manager) Probe(ctx context.Context, tag string) (time.Duration, error) {
	e, err := m.entry(tag)
	if err != nil {
		return 0, err
	}
	e.mu.RLock()
	probeAvailable := e.probe != nil
	e.mu.RUnlock()
	if !probeAvailable {
		return 0, errors.New("probe not available for this node")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultProbeTimeout)
		defer cancel()
	}
	latency, err, probeGeneration := e.executeProbeGeneration(ctx, m.ctx, defaultProbeTimeout)
	if !e.markProbeResultGeneration(probeGeneration, latency, err) {
		return 0, errProbeSuperseded
	}
	if err != nil {
		return 0, err
	}
	return latency, nil
}

// Release clears blacklist state for the given node.
func (m *Manager) Release(tag string) error {
	e, err := m.entry(tag)
	if err != nil {
		return err
	}
	e.mu.RLock()
	release := e.release
	e.mu.RUnlock()
	if release == nil {
		return errors.New("release not available for this node")
	}
	release()
	return nil
}

// ManualBlacklist manually blacklists a node for the given duration.
func (m *Manager) ManualBlacklist(tag string, duration time.Duration) error {
	e, err := m.entry(tag)
	if err != nil {
		return err
	}
	e.mu.RLock()
	fn := e.blacklistFn
	e.mu.RUnlock()

	if fn != nil {
		// Blacklist in pool shared state (affects routing)
		fn(duration)
	}
	// Also mark in monitor state (affects UI display)
	e.blacklistUntil(time.Now().Add(duration))
	return nil
}

func (m *Manager) entry(tag string) (*entry, error) {
	m.mu.RLock()
	e, ok := m.nodes[tag]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("node %s not found", tag)
	}
	return e, nil
}

func (e *entry) snapshot() Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	latencyMs := int64(-1)
	if e.lastProbe > 0 {
		latencyMs = e.lastProbe.Milliseconds()
		if latencyMs == 0 {
			latencyMs = 1
		}
	}

	var timelineCopy []TimelineEvent
	if len(e.timeline) > 0 {
		timelineCopy = make([]TimelineEvent, len(e.timeline))
		copy(timelineCopy, e.timeline)
	}

	return Snapshot{
		NodeInfo:          e.info,
		FailureCount:      e.failure,
		SuccessCount:      e.success,
		Blacklisted:       e.blacklist,
		BlacklistedUntil:  e.until,
		CoolingDown:       e.coolingDown,
		CooldownUntil:     e.cooldownUntil,
		ActiveConnections: e.active.Load(),
		LastError:         e.lastError,
		LastFailure:       e.lastFail,
		LastSuccess:       e.lastOK,
		LastProbeLatency:  e.lastProbe,
		LastLatencyMs:     latencyMs,
		Available:         e.available,
		InitialCheckDone:  e.initialCheckDone,
		Timeline:          timelineCopy,
	}
}

func (e *entry) recordFailure(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	errStr := SanitizeProbeError(err)
	e.failure++
	e.lastError = errStr
	e.lastFail = time.Now()
	e.appendTimelineLocked(false, 0, errStr)
}

func (e *entry) recordSuccess() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.success++
	e.lastOK = time.Now()
	e.appendTimelineLocked(true, 0, "")
}

func (e *entry) recordSuccessWithLatency(latency time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.success++
	e.lastOK = time.Now()
	e.lastProbe = latency
	latencyMs := latency.Milliseconds()
	if latencyMs == 0 && latency > 0 {
		latencyMs = 1
	}
	e.appendTimelineLocked(true, latencyMs, "")
}

func (e *entry) appendTimelineLocked(success bool, latencyMs int64, errStr string) {
	evt := TimelineEvent{
		Time:      time.Now(),
		Success:   success,
		LatencyMs: latencyMs,
		Error:     errStr,
	}
	if len(e.timeline) >= maxTimelineSize {
		copy(e.timeline, e.timeline[1:])
		e.timeline[len(e.timeline)-1] = evt
	} else {
		e.timeline = append(e.timeline, evt)
	}
}

func (e *entry) blacklistUntil(until time.Time) {
	e.mu.Lock()
	e.blacklist = true
	e.until = until
	e.mu.Unlock()
}

func (e *entry) clearBlacklist() {
	e.mu.Lock()
	e.blacklist = false
	e.until = time.Time{}
	e.mu.Unlock()
}

func (e *entry) cooldown(until time.Time) {
	e.mu.Lock()
	e.coolingDown = true
	e.cooldownUntil = until
	e.available = false
	e.mu.Unlock()
}

func (e *entry) clearCooldown() {
	e.mu.Lock()
	e.coolingDown = false
	e.cooldownUntil = time.Time{}
	if e.initialCheckDone && !e.blacklist {
		e.available = true
	}
	e.mu.Unlock()
}

func (e *entry) incActive() {
	e.active.Add(1)
}

func (e *entry) decActive() {
	e.active.Add(-1)
}

func (e *entry) setProbe(fn probeFunc) {
	e.probeMu.Lock()
	defer e.probeMu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.probe = fn
	e.probeGeneration++
	// A callback replacement represents a new runtime generation. Persisted or
	// previously observed availability is not evidence that the replacement is
	// healthy, and an older in-flight callback may not carry that state across.
	e.initialCheckDone = false
	e.available = false
}

func (e *entry) setRelease(fn releaseFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.release = fn
}

func (e *entry) recordProbeLatency(d time.Duration) {
	e.mu.Lock()
	e.lastProbe = d
	e.mu.Unlock()
}

func (e *entry) markProbeResult(latency time.Duration, err error) {
	now := time.Now()
	e.mu.Lock()
	e.initialCheckDone = true
	if err != nil {
		e.lastError = SanitizeProbeError(err)
		e.lastFail = now
		e.available = false
	} else {
		e.lastError = ""
		e.lastOK = now
		e.lastProbe = latency
		// A successful transport probe confirms the underlying outbound, but it
		// must not bypass an administrative blacklist or transient cooldown.
		e.available = !e.blacklist && !e.coolingDown
	}
	e.mu.Unlock()
}

func (e *entry) markProbeResultGeneration(generation uint64, latency time.Duration, err error) bool {
	e.probeMu.Lock()
	defer e.probeMu.Unlock()
	if generation != e.probeGeneration {
		return false
	}
	e.markProbeResult(latency, err)
	return true
}

// executeProbe deduplicates concurrent probes for one entry and races the
// underlying callback against the caller's deadline. If a broken outbound
// ignores context while dialing, later sweeps join the same in-flight call
// instead of leaking another goroutine for that node on every pass.
func (e *entry) executeProbe(waitCtx, runBase context.Context, runTimeout time.Duration) (time.Duration, error) {
	latency, err, _ := e.executeProbeGeneration(waitCtx, runBase, runTimeout)
	return latency, err
}

func (e *entry) executeProbeGeneration(
	waitCtx context.Context,
	runBase context.Context,
	runTimeout time.Duration,
) (time.Duration, error, uint64) {
	if waitCtx == nil {
		waitCtx = context.Background()
	}
	if runBase == nil {
		runBase = context.Background()
	}
	runTimeout = probeTimeout(runTimeout)
	acquiredSlot := false
	for {
		e.probeMu.Lock()
		e.mu.RLock()
		probe := e.probe
		e.mu.RUnlock()
		if probe == nil {
			generation := e.probeGeneration
			e.probeMu.Unlock()
			if acquiredSlot {
				<-e.probeSlots
			}
			return 0, errors.New("probe not available for this node"), generation
		}
		generation := e.probeGeneration
		call := e.probeCall
		// Join any older in-flight callback even if reload installed a new probe
		// generation. Once it returns, the next request starts the latest one.
		if call != nil {
			e.probeMu.Unlock()
			if acquiredSlot {
				<-e.probeSlots
			}
			return e.waitForProbeCall(waitCtx, call)
		}
		if e.probeSlots != nil && !acquiredSlot {
			e.probeMu.Unlock()
			select {
			case e.probeSlots <- struct{}{}:
				acquiredSlot = true
				continue
			case <-waitCtx.Done():
				return 0, waitCtx.Err(), generation
			}
		}

		// WaitGroup.Add must not race with StopAndWait.Wait while the counter is
		// zero. The manager lifecycle lock forms the barrier: Stop takes the
		// write lock before publishing the stopped state, while every new
		// callback checks that state and registers itself under a read lock.
		trackedProbe := false
		if e.probeLifecycleMu != nil {
			e.probeLifecycleMu.RLock()
			if e.probeStopped != nil && e.probeStopped.Load() {
				e.probeLifecycleMu.RUnlock()
				e.probeMu.Unlock()
				if acquiredSlot {
					<-e.probeSlots
				}
				return 0, errProbeManagerStopped, generation
			}
			if e.probeWG != nil {
				e.probeWG.Add(1)
				trackedProbe = true
			}
			e.probeLifecycleMu.RUnlock()
		}

		call = &inFlightProbe{generation: generation, done: make(chan struct{})}
		e.probeCall = call
		e.probeMu.Unlock()
		runCtx, cancel := context.WithTimeout(runBase, runTimeout)
		go func(probe probeFunc, call *inFlightProbe, releaseSlot, tracked bool) {
			defer cancel()
			if tracked {
				defer e.probeWG.Done()
			}
			if releaseSlot {
				defer func() { <-e.probeSlots }()
			}
			outcome := probeOutcome{}
			func() {
				defer func() {
					if recover() != nil {
						outcome.err = errors.New("probe callback panicked")
					}
				}()
				outcome.latency, outcome.err = probe(runCtx)
			}()
			e.probeMu.Lock()
			call.result = outcome
			close(call.done)
			if e.probeCall == call {
				e.probeCall = nil
			}
			e.probeMu.Unlock()
		}(probe, call, acquiredSlot, trackedProbe)
		return e.waitForProbeCall(waitCtx, call)
	}
}

func (e *entry) waitForProbeCall(ctx context.Context, call *inFlightProbe) (time.Duration, error, uint64) {
	select {
	case <-call.done:
		e.probeMu.Lock()
		current := call.generation == e.probeGeneration
		e.probeMu.Unlock()
		if !current {
			return 0, errProbeSuperseded, call.generation
		}
		return call.result.latency, call.result.err, call.generation
	case <-ctx.Done():
		return 0, ctx.Err(), call.generation
	}
}

// RecordFailure updates failure counters.
func (h *EntryHandle) RecordFailure(err error) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.recordFailure(err)
}

// RecordSuccess updates the last success timestamp.
func (h *EntryHandle) RecordSuccess() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.recordSuccess()
}

// RecordSuccessWithLatency updates the last success timestamp and latency.
func (h *EntryHandle) RecordSuccessWithLatency(latency time.Duration) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.recordSuccessWithLatency(latency)
}

// LastLatency returns the most recent successful probe latency.
func (h *EntryHandle) LastLatency() time.Duration {
	if h == nil || h.ref == nil {
		return 0
	}
	h.ref.mu.RLock()
	defer h.ref.mu.RUnlock()
	return h.ref.lastProbe
}

// Blacklist marks the node unavailable until the given deadline.
func (h *EntryHandle) Blacklist(until time.Time) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.blacklistUntil(until)
}

// ClearBlacklist removes the blacklist flag.
func (h *EntryHandle) ClearBlacklist() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.clearBlacklist()
}

// Cooldown marks a node temporarily unavailable without treating the fault as
// a durable blacklist strike.
func (h *EntryHandle) Cooldown(until time.Time) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.cooldown(until)
}

// ClearCooldown removes the transient cooldown flag.
func (h *EntryHandle) ClearCooldown() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.clearCooldown()
}

// ExportHealthState returns the restart-safe monitor fields.
func (h *EntryHandle) ExportHealthState() PersistedHealthState {
	if h == nil || h.ref == nil {
		return PersistedHealthState{}
	}
	e := h.ref
	e.mu.RLock()
	defer e.mu.RUnlock()
	return PersistedHealthState{
		FailureCount:     e.failure,
		SuccessCount:     e.success,
		BlacklistedUntil: e.until,
		CooldownUntil:    e.cooldownUntil,
		LastError:        SanitizeProbeError(errors.New(e.lastError)),
		LastFailure:      e.lastFail,
		LastSuccess:      e.lastOK,
		LastProbeLatency: e.lastProbe,
		Available:        e.available,
		InitialCheckDone: e.initialCheckDone,
	}
}

// RestoreHealthState hydrates monitor fields before probes resume.
func (h *EntryHandle) RestoreHealthState(state PersistedHealthState) {
	if h == nil || h.ref == nil {
		return
	}
	e := h.ref
	e.mu.Lock()
	e.failure = state.FailureCount
	e.success = state.SuccessCount
	e.lastError = SanitizeProbeError(errors.New(state.LastError))
	e.lastFail = state.LastFailure
	e.lastOK = state.LastSuccess
	e.lastProbe = state.LastProbeLatency
	e.available = state.Available
	e.initialCheckDone = state.InitialCheckDone
	if state.BlacklistedUntil.After(time.Now()) {
		e.blacklist = true
		e.until = state.BlacklistedUntil
		e.available = false
	} else {
		e.blacklist = false
		e.until = time.Time{}
	}
	if state.CooldownUntil.After(time.Now()) {
		e.coolingDown = true
		e.cooldownUntil = state.CooldownUntil
		e.available = false
	} else {
		e.coolingDown = false
		e.cooldownUntil = time.Time{}
	}
	e.mu.Unlock()
}

// IncActive increments the active connection counter.
func (h *EntryHandle) IncActive() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.incActive()
}

// DecActive decrements the active connection counter.
func (h *EntryHandle) DecActive() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.decActive()
}

// SetProbe assigns a probe function.
func (h *EntryHandle) SetProbe(fn func(ctx context.Context) (time.Duration, error)) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.setProbe(fn)
}

// SetRelease assigns a release function.
func (h *EntryHandle) SetRelease(fn func()) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.setRelease(fn)
}

// SetBlacklistFn assigns a manual blacklist function.
func (h *EntryHandle) SetBlacklistFn(fn func(time.Duration)) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.mu.Lock()
	h.ref.blacklistFn = fn
	h.ref.mu.Unlock()
}

// MarkInitialCheckDone marks the initial health check as completed.
func (h *EntryHandle) MarkInitialCheckDone(available bool) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.mu.Lock()
	h.ref.initialCheckDone = true
	h.ref.available = available
	if available {
		h.ref.lastError = ""
	}
	h.ref.mu.Unlock()
}

// MarkAvailable updates the availability status.
func (h *EntryHandle) MarkAvailable(available bool) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.mu.Lock()
	h.ref.available = available
	h.ref.mu.Unlock()
}
