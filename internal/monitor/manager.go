package monitor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

// Config mirrors user settings needed by the monitoring server.
type Config struct {
	Enabled          bool
	Listen           string
	ProbeTarget      string
	Password         string
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
	URI           string `json:"uri"`
	Mode          string `json:"mode"`
	ListenAddress string `json:"listen_address,omitempty"`
	Port          uint16 `json:"port,omitempty"`
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

const (
	defaultProbeConcurrency = 32
	maxProbeConcurrency     = 1024
	defaultProbeTimeout     = 10 * time.Second
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

	probeSweepActive atomic.Int32
	probeSweepTotal  atomic.Int32
	probeSweepDone   atomic.Int32
	probeSweepOK     atomic.Int32
	probeSweepFail   atomic.Int32

	probeGate      sync.Mutex
	sweepRunning   bool
	rerunRequested bool
	rerunTimeout   time.Duration
	sweepIdle      chan struct{}
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

func resolveProbeTarget(value string, skipCertVerify bool) (ProbeTarget, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return ProbeTarget{}, false
	}
	lower := strings.ToLower(value)
	if strings.Contains(lower, "://") && !strings.HasPrefix(lower, "http://") && !strings.HasPrefix(lower, "https://") {
		return ProbeTarget{}, false
	}
	useTLS := strings.HasPrefix(lower, "https://")
	target := value
	if strings.HasPrefix(lower, "https://") {
		target = value[len("https://"):]
	} else if strings.HasPrefix(lower, "http://") {
		target = value[len("http://"):]
	}
	if idx := strings.IndexAny(target, "/?#"); idx >= 0 {
		target = target[:idx]
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return ProbeTarget{}, false
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		host = strings.Trim(target, "[]")
		if useTLS {
			port = "443"
		} else {
			port = "80"
		}
	}
	if host == "" {
		return ProbeTarget{}, false
	}
	return ProbeTarget{
		Destination:    M.ParseSocksaddrHostPort(host, parsePort(port)),
		Host:           host,
		TLS:            useTLS,
		SkipCertVerify: skipCertVerify,
	}, true
}

// NewManager constructs a manager and pre-validates the probe target.
func NewManager(cfg Config) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	target, ready := resolveProbeTarget(cfg.ProbeTarget, cfg.SkipCertVerify)
	m := &Manager{
		cfg:              cfg,
		probeTarget:      target,
		probeReady:       ready,
		probeConcurrency: clampProbeConcurrency(cfg.ProbeConcurrency),
		nodes:            make(map[string]*entry),
		ctx:              ctx,
		cancel:           cancel,
	}
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
func (m *Manager) SetProbeTarget(value string, skipCertVerify bool) {
	target, ready := resolveProbeTarget(value, skipCertVerify)
	m.mu.Lock()
	m.cfg.ProbeTarget = value
	m.cfg.SkipCertVerify = skipCertVerify
	m.probeTarget = target
	m.probeReady = ready
	m.mu.Unlock()
}

// SetLogger sets the logger for the manager.
func (m *Manager) SetLogger(logger Logger) {
	m.logger = logger
}

// StartPeriodicHealthCheck starts a background goroutine that periodically checks all nodes.
// interval: how often to check (e.g., 30 * time.Second)
// timeout: timeout for each probe (e.g., 10 * time.Second)
func (m *Manager) StartPeriodicHealthCheck(interval, timeout time.Duration) {
	go func() {
		// The initial pass uses the same process-wide coordinator as periodic,
		// post-reload and WebUI-triggered sweeps.
		m.probeAllNodes(timeout)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-m.ctx.Done():
				return
			case <-ticker.C:
				m.probeAllNodes(timeout)
			}
		}
	}()

	if m.logger != nil {
		m.logger.Info("periodic health check started, interval: ", interval)
	}
}

// ProbeAllNow triggers a one-time health check on all nodes (e.g. after reload).
func (m *Manager) ProbeAllNow(timeout time.Duration) {
	m.probeAllNodes(timeout)
}

// probeAllNodes serializes all batch probe sources. A trigger that overlaps an
// active sweep requests one coalesced follow-up pass and waits for the flight to
// become idle; callers that validate a reload therefore never observe stale
// pre-sweep availability.
func (m *Manager) probeAllNodes(timeout time.Duration) {
	timeout = probeTimeout(timeout)
	m.probeGate.Lock()
	if m.sweepRunning {
		m.rerunRequested = true
		if timeout > m.rerunTimeout {
			m.rerunTimeout = timeout
		}
		idle := m.sweepIdle
		m.probeGate.Unlock()
		if idle != nil {
			<-idle
		}
		return
	}
	m.sweepRunning = true
	m.sweepIdle = make(chan struct{})
	idle := m.sweepIdle
	m.probeSweepActive.Store(1)
	m.probeGate.Unlock()

	followupRan := false
	for {
		m.runProbeSweep(timeout)

		m.probeGate.Lock()
		if m.rerunRequested && !followupRan {
			m.rerunRequested = false
			timeout = m.rerunTimeout
			m.rerunTimeout = 0
			followupRan = true
			m.probeGate.Unlock()
			continue
		}
		m.rerunRequested = false
		m.rerunTimeout = 0
		m.sweepRunning = false
		m.sweepIdle = nil
		m.probeSweepActive.Store(0)
		close(idle)
		m.probeGate.Unlock()
		return
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
func (m *Manager) runProbeSweep(timeout time.Duration) {
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
		return
	}

	if m.logger != nil {
		m.logger.Info("starting health check for ", len(entries), " nodes")
	}
	if _, ready := m.DestinationForProbe(); !ready {
		for _, entry := range entries {
			entry.markProbeResult(0, nil)
			m.probeSweepOK.Add(1)
			m.probeSweepDone.Add(1)
		}
		if m.logger != nil {
			m.logger.Warn("probe target not configured; nodes left available without verification")
		}
		return
	}

	perProbe := probeTimeout(timeout)
	workerLimit := m.ProbeConcurrency()
	if workerLimit > len(entries) {
		workerLimit = len(entries)
	}
	sweepCtx, sweepCancel := context.WithTimeout(m.ctx, sweepTimeout(len(entries), workerLimit, perProbe))
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
				entry.mu.RLock()
				probeFn := entry.probe
				tag := entry.info.Tag
				uri := entry.info.URI
				entry.mu.RUnlock()

				if probeFn == nil {
					entry.markProbeResult(0, nil)
					availableCount.Add(1)
					m.probeSweepOK.Add(1)
					m.probeSweepDone.Add(1)
					continue
				}

				probeCtx, cancel := context.WithTimeout(sweepCtx, perProbe)
				latency, err := entry.executeProbe(probeCtx)
				cancel()
				entry.markProbeResult(latency, err)
				if err != nil {
					failedCount.Add(1)
					m.probeSweepFail.Add(1)
					if m.logger != nil {
						m.logger.Warn("probe failed: ", FormatProbeFailure(tag, uri, err))
					}
				} else {
					availableCount.Add(1)
					m.probeSweepOK.Add(1)
				}
				m.probeSweepDone.Add(1)
			}
		}()
	}
	for _, entry := range entries {
		jobs <- entry
	}
	close(jobs)
	wg.Wait()

	if m.logger != nil {
		m.logger.Info("health check completed: ", availableCount.Load(), " available, ", failedCount.Load(), " failed")
	}
}

// Stop stops the periodic health check.
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
}

func parsePort(value string) uint16 {
	p, err := strconv.Atoi(value)
	if err != nil || p <= 0 || p > 65535 {
		return 80
	}
	return uint16(p)
}

// Register ensures a node is tracked and returns its entry.
func (m *Manager) Register(info NodeInfo) *EntryHandle {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.nodes[info.Tag]
	if !ok {
		e = &entry{
			info:     info,
			timeline: make([]TimelineEvent, 0, maxTimelineSize),
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
		if onlyAvailable && ((snap.InitialCheckDone && !snap.Available) || snap.Blacklisted) {
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
	latency, err := e.executeProbe(ctx)
	e.markProbeResult(latency, err)
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
	if e.release == nil {
		return errors.New("release not available for this node")
	}
	e.release()
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
		e.available = true
	}
	e.mu.Unlock()
}

// executeProbe deduplicates concurrent probes for one entry and races the
// underlying callback against the caller's deadline. If a broken outbound
// ignores context while dialing, later sweeps join the same in-flight call
// instead of leaking another goroutine for that node on every pass.
func (e *entry) executeProbe(ctx context.Context) (time.Duration, error) {
	e.probeMu.Lock()
	e.mu.RLock()
	probe := e.probe
	e.mu.RUnlock()
	if probe == nil {
		e.probeMu.Unlock()
		return 0, errors.New("probe not available for this node")
	}
	generation := e.probeGeneration
	call := e.probeCall
	if call == nil || call.generation != generation {
		call = &inFlightProbe{generation: generation, done: make(chan struct{})}
		e.probeCall = call
		go func() {
			outcome := probeOutcome{}
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						outcome.err = fmt.Errorf("probe panic: %v", recovered)
					}
				}()
				outcome.latency, outcome.err = probe(ctx)
			}()
			e.probeMu.Lock()
			call.result = outcome
			close(call.done)
			if e.probeCall == call {
				e.probeCall = nil
			}
			e.probeMu.Unlock()
		}()
	}
	done := call.done
	e.probeMu.Unlock()

	select {
	case <-done:
		return call.result.latency, call.result.err
	case <-ctx.Done():
		return 0, ctx.Err()
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
		LastError:        e.lastError,
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
	e.lastError = state.LastError
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
