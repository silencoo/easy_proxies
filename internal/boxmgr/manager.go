package boxmgr

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"easy_proxies/internal/builder"
	"easy_proxies/internal/commitguard"
	"easy_proxies/internal/config"
	"easy_proxies/internal/geoip"
	"easy_proxies/internal/monitor"
	"easy_proxies/internal/outbound/pool"

	"github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/include"
	singlog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

// Ensure Manager implements monitor.NodeManager.
var _ monitor.NodeManager = (*Manager)(nil)

const (
	defaultDrainTimeout       = 10 * time.Second
	defaultDrainOperationWait = 5 * time.Second
	defaultHealthCheckTimeout = 30 * time.Second
	defaultShutdownTimeout    = 10 * time.Second
	healthCheckPollInterval   = 500 * time.Millisecond
	periodicHealthInterval    = 5 * time.Minute
	periodicHealthTimeout     = 10 * time.Second
	maxHungPreflightProbes    = 64
	maxHungExitIPProbes       = 32
)

var outboundInitializationIndexPattern = regexp.MustCompile(`initialize outbound\[(\d+)\]`)

var (
	activeManagerMu sync.Mutex
	activeManager   *Manager
)

// Logger defines logging interface for the manager.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// Option configures the Manager.
type Option func(*Manager)

// WithLogger sets a custom logger.
func WithLogger(l Logger) Option {
	return func(m *Manager) { m.logger = l }
}

// Manager owns the lifecycle of the active sing-box instance.
type Manager struct {
	mu       sync.RWMutex
	reloadMu sync.Mutex
	startMu  sync.Mutex

	currentBox      *box.Box
	managementOnly  bool
	runtimeCtx      context.Context
	runtimeOptions  option.Options
	monitorMgr      *monitor.Manager
	monitorServer   *monitor.Server
	geoRouter       *geoip.Router
	geoLookup       *geoip.Lookup
	geoLookupPath   string
	geoAutoInterval time.Duration
	exitIPs         map[string]string
	geoUpdateMu     sync.Mutex
	geoUpdateQueued map[*geoip.Lookup]struct{}
	cfg             *config.Config
	revision        uint64
	monitorCfg      monitor.Config

	drainTimeout        time.Duration
	drainOperationWait  time.Duration
	minAvailableNodes   int
	logger              Logger
	beforeStartPublish  func()
	beforeRuntimeCommit func()

	baseCtx            context.Context
	healthCheckStarted bool

	preflightMu    sync.Mutex
	preflightCalls map[string]*preflightProbeCall
	preflightSlots chan struct{}
	exitProbeMu    sync.Mutex
	exitProbeCalls map[string]*exitIPProbeCall
	exitProbeSlots chan struct{}
	probeExitIP    func(context.Context, geoip.OutboundDialer, string) (string, error)
	auxProbeWG     sync.WaitGroup

	drainMu               sync.Mutex
	drainPending          map[string]runtimeDrainTarget
	drainInstance         *box.Box
	drainWake             chan struct{}
	drainStop             chan struct{}
	drainWG               sync.WaitGroup
	drainStarted          bool
	drainClosed           bool
	drainDiscarding       bool
	removeDrainedOutbound func(*box.Box, string) error

	closeOnce   sync.Once
	closeDone   chan struct{}
	closeErr    error
	closed      bool
	ownsRuntime bool
}

type builtInstance struct {
	box     *box.Box
	ctx     context.Context
	options option.Options
}

type preflightProbeCall struct {
	done chan struct{}
	err  error
}

type exitIPProbeCall struct {
	done chan struct{}
	ip   string
	err  error
}

type runtimeDrainTarget struct {
	tag      string
	expected adapter.Outbound
	option   option.Outbound
	due      time.Time
	timeout  time.Duration
	attempts int
	inFlight bool
	opDone   chan struct{}
}

type managerCloseState struct {
	currentBox    *box.Box
	monitorServer *monitor.Server
	monitorMgr    *monitor.Manager
	geoRouter     *geoip.Router
	geoLookup     *geoip.Lookup
	ownsRuntime   bool
}

// New creates a BoxManager with the given config.
func New(cfg *config.Config, monitorCfg monitor.Config, opts ...Option) *Manager {
	ownedCfg := cfg.Clone()
	m := &Manager{
		cfg:            ownedCfg,
		monitorCfg:     monitorCfg,
		closeDone:      make(chan struct{}),
		preflightSlots: make(chan struct{}, maxHungPreflightProbes),
		exitProbeCalls: make(map[string]*exitIPProbeCall),
		exitProbeSlots: make(chan struct{}, maxHungExitIPProbes),
		drainWake:      make(chan struct{}, 1),
		drainStop:      make(chan struct{}),
	}
	if ownedCfg != nil {
		m.revision = 1
	}
	m.applyConfigSettings(ownedCfg)
	for _, opt := range opts {
		opt(m)
	}
	if m.logger == nil {
		m.logger = defaultLogger{}
	}
	if m.drainTimeout <= 0 {
		m.drainTimeout = defaultDrainTimeout
	}
	if m.drainOperationWait <= 0 {
		m.drainOperationWait = defaultDrainOperationWait
	}
	return m
}

// Start creates and starts the initial sing-box instance.
func (m *Manager) Start(ctx context.Context) error {
	m.startMu.Lock()
	defer m.startMu.Unlock()
	// Start, Reload/CommitConfig, and Close all publish or detach the same owned
	// runtime. Hold the lifecycle transaction lock until startup is either fully
	// committed or completely cleaned up.
	m.reloadMu.Lock()
	started := false
	cleanupOnFailure := false
	var startupPortRollback func() error
	defer func() {
		m.reloadMu.Unlock()
		if cleanupOnFailure && !started {
			if startupPortRollback != nil {
				_ = startupPortRollback()
			}
			_ = m.Close()
		}
	}()
	m.mu.RLock()
	alreadyRunning := m.currentBox != nil || m.managementOnly
	closed := m.closed
	m.mu.RUnlock()
	if closed {
		return errors.New("box manager is closed")
	}
	if alreadyRunning {
		return errors.New("sing-box already running")
	}
	activeManagerMu.Lock()
	if activeManager != nil && activeManager != m {
		activeManagerMu.Unlock()
		return errors.New("another box manager already owns the process-wide pool runtime")
	}
	activeManager = m
	m.mu.Lock()
	m.ownsRuntime = true
	m.mu.Unlock()
	activeManagerMu.Unlock()
	cleanupOnFailure = true
	if ctx == nil {
		ctx = context.Background()
	}
	if err := m.ensureMonitor(); err != nil {
		return err
	}

	m.mu.Lock()
	if m.cfg == nil {
		m.mu.Unlock()
		return errors.New("box manager requires config")
	}
	m.applyConfigSettings(m.cfg)
	m.baseCtx = ctx
	cfg := m.cfg
	m.mu.Unlock()
	if err := pool.ConfigureHealthPersistence(cfg.HealthStatePath()); err != nil {
		return fmt.Errorf("load pool health state: %w", err)
	}
	if len(cfg.Nodes) == 0 {
		if !cfg.ManagementEnabled() {
			return errors.New("cannot start without proxy nodes when management is disabled")
		}
		m.mu.Lock()
		m.managementOnly = true
		m.mu.Unlock()
		if err := m.startMonitorServer(ctx); err != nil {
			return err
		}
		m.logger.Infof("management-only mode started with no proxy nodes; add a subscription or node in the WebUI at %s", cfg.Management.Listen)
		started = true
		return nil
	}

	// Try to start, with automatic port conflict resolution
	var built *builtInstance
	var lastStartErr error
	boxStarted := false
	maxRetries := 10
	for retry := 0; retry < maxRetries; retry++ {
		var err error
		built, err = m.createBox(ctx, cfg)
		if err != nil {
			return err
		}
		if err = built.box.Start(); err != nil {
			lastStartErr = err
			_ = built.box.Close()
			// Check if it's a port conflict error
			if conflictPort := extractPortFromBindError(err); conflictPort > 0 {
				m.logger.Warnf("port %d is in use, reassigning and retrying...", conflictPort)
				if reassigned := reassignConflictingPort(cfg, conflictPort); reassigned {
					pool.ResetSharedStateStore() // Reset shared state for rebuild
					continue
				}
			}
			return fmt.Errorf("start sing-box: %w", err)
		}
		boxStarted = true
		break // Success
	}
	if !boxStarted {
		return fmt.Errorf("start sing-box after %d port-reassignment attempts: %w", maxRetries, lastStartErr)
	}
	if m.beforeStartPublish != nil {
		m.beforeStartPublish()
	}
	var err error
	startupPortRollback, err = cfg.PersistPortMapTransaction()
	if err != nil {
		_ = built.box.Close()
		return fmt.Errorf("persist startup port map: %w", err)
	}

	m.mu.Lock()
	m.currentBox = built.box
	m.runtimeCtx = built.ctx
	m.runtimeOptions = built.options
	m.mu.Unlock()
	m.updateHealthPersistence(cfg)

	// Start periodic health check after nodes are registered.
	m.startPeriodicHealthChecks()

	// Wait for initial health check if min nodes configured. A startup-fetched
	// subscription is not allowed to replace the restart cache unless this
	// generation reaches the configured availability threshold.
	startupHealthAccepted := true
	if cfg.SubscriptionRefresh.MinAvailableNodes > 0 {
		timeout := cfg.SubscriptionRefresh.HealthCheckTimeout
		if timeout <= 0 {
			timeout = defaultHealthCheckTimeout
		}
		if err := m.waitForHealthCheck(ctx, timeout); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			m.logger.Warnf("initial health check warning: %v", err)
			startupHealthAccepted = false
			// Don't fail startup, just warn
		}
	}

	m.logger.Infof("sing-box instance started with %d nodes", len(cfg.Nodes))

	// Start GeoIP router if enabled
	if cfg.GeoIP.Enabled {
		if err := m.refreshExitGeoIP(ctx, cfg); err != nil {
			return fmt.Errorf("initialize GeoIP pools: %w", err)
		}
		if err := m.ensureGeoIPRouter(ctx, cfg); err != nil {
			return fmt.Errorf("start GeoIP router: %w", err)
		}
	}
	if len(cfg.Subscriptions) > 0 {
		if startupHealthAccepted {
			if err := cfg.SaveSubscriptionCache(); err != nil {
				return fmt.Errorf("commit startup subscription cache: %w", err)
			}
		} else {
			m.logger.Warnf("startup subscription cache was not replaced because the candidate did not meet min_available_nodes")
		}
	}
	if err := m.startMonitorServer(ctx); err != nil {
		return err
	}

	started = true
	return nil
}

// Reload applies node-only changes through sing-box's runtime managers. Global
// topology changes still use the validated full-instance handoff below.
func (m *Manager) Reload(newCfg *config.Config) error {
	if newCfg == nil {
		return errors.New("new config is nil")
	}
	ownedCfg := newCfg.Clone()
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	return m.reloadLocked(context.Background(), ownedCfg)
}

// reloadLocked applies an already-owned configuration while reloadMu is held.
// Successful callers adopt exactly one new configuration revision.
func (m *Manager) reloadLocked(ctx context.Context, newCfg *config.Config) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.ensureNotClosed(); err != nil {
		return err
	}
	rollbackPortMap, err := newCfg.PersistPortMapTransaction()
	if err != nil {
		return fmt.Errorf("persist candidate port map: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return rollbackConfigPersistence(rollbackPortMap, err)
	}
	if err := m.reloadRuntimeLocked(ctx, newCfg); err != nil {
		if rollbackErr := rollbackPortMap(); rollbackErr != nil {
			return fmt.Errorf("%w; port-map rollback failed: %v", err, rollbackErr)
		}
		return err
	}
	return nil
}

func (m *Manager) reloadRuntimeLocked(operationCtx context.Context, newCfg *config.Config) error {
	m.mu.RLock()
	managementOnly := m.managementOnly
	runtimeBaseCtx := m.baseCtx
	oldBox := m.currentBox
	oldCfg := m.cfg
	runtimeCtx := m.runtimeCtx
	oldOptions := m.runtimeOptions
	m.mu.RUnlock()
	if oldBox == nil {
		if !managementOnly {
			return errors.New("manager not started")
		}
		return m.reloadManagementOnlyRuntimeLocked(operationCtx, runtimeBaseCtx, oldCfg, newCfg)
	}

	if operationCtx == nil {
		operationCtx = context.Background()
	}
	if runtimeBaseCtx == nil {
		runtimeBaseCtx = context.Background()
	}

	m.logger.Infof("reloading with %d nodes", len(newCfg.Nodes))
	if canReloadNodesInPlace(oldCfg, newCfg) && runtimeCtx != nil {
		if err := m.reloadNodesInPlace(operationCtx, runtimeCtx, oldBox, oldCfg, oldOptions, newCfg); !errors.Is(err, errRuntimeReloadUnsupported) {
			return err
		}
		m.logger.Warnf("node-level reload is not available for this change; using full validated handoff")
	}

	// box.New performs outbound/inbound validation but does not bind listeners.
	// Do this first so malformed subscriptions cannot interrupt live traffic.
	built, err := m.createBox(runtimeBaseCtx, newCfg)
	if err != nil {
		return fmt.Errorf("create replacement box: %w", err)
	}
	if err := operationCtx.Err(); err != nil {
		_ = built.box.Close()
		return err
	}

	// Release the old sing-box ports immediately before starting the already
	// validated replacement. Keep the GeoIP listener published: its dialers are
	// swapped only after the candidate runtime and region pools are ready.
	if err := m.discardRuntimeDrains(oldBox); err != nil {
		_ = built.box.Close()
		return fmt.Errorf("prepare full runtime handoff: %w", err)
	}
	if err := oldBox.Close(); err != nil {
		m.logger.Warnf("error closing old instance: %v", err)
	}
	m.mu.Lock()
	m.currentBox = nil
	m.mu.Unlock()

	if err := built.box.Start(); err != nil {
		_ = built.box.Close()
		rollbackErr := m.rollbackToOldConfig(runtimeBaseCtx, oldCfg)
		if rollbackErr != nil {
			return fmt.Errorf("start replacement box: %w; rollback failed: %v", err, rollbackErr)
		}
		return fmt.Errorf("start replacement box: %w (old configuration restored)", err)
	}
	if err := operationCtx.Err(); err != nil {
		_ = built.box.Close()
		rollbackErr := m.rollbackToOldConfig(runtimeBaseCtx, oldCfg)
		if rollbackErr != nil {
			return fmt.Errorf("%w; rollback failed: %v", err, rollbackErr)
		}
		return fmt.Errorf("%w (old configuration restored)", err)
	}

	m.applyLiveProbeSettings(newCfg)

	m.mu.Lock()
	m.currentBox = built.box
	m.runtimeCtx = built.ctx
	m.runtimeOptions = built.options
	m.mu.Unlock()
	m.updateHealthPersistence(newCfg)
	rollbackReplacement := func(cause error) error {
		_ = built.box.Close()
		m.mu.Lock()
		if m.currentBox == built.box {
			m.currentBox = nil
			m.runtimeCtx = nil
			m.runtimeOptions = option.Options{}
		}
		m.mu.Unlock()
		rollbackErr := m.rollbackToOldConfig(runtimeBaseCtx, oldCfg)
		if rollbackErr != nil {
			return fmt.Errorf("%w; rollback failed: %v", cause, rollbackErr)
		}
		return fmt.Errorf("%w (old configuration restored)", cause)
	}

	if m.monitorMgr != nil {
		m.monitorMgr.RetainNodeURIs(nodeURISet(newCfg.Nodes))
	}

	// Validate the configured minimum before committing the reload. Active probe
	// failures also update the shared routing blacklist.
	minAvailableNodes := newCfg.SubscriptionRefresh.MinAvailableNodes
	if m.monitorMgr != nil {
		healthTimeout := newCfg.SubscriptionRefresh.HealthCheckTimeout
		if healthTimeout <= 0 {
			healthTimeout = defaultHealthCheckTimeout
		}
		if _, probeConfigured := m.monitorMgr.DestinationForProbe(); probeConfigured && minAvailableNodes > 0 {
			if err := m.monitorMgr.ProbeAllNowContext(operationCtx, healthTimeout); err != nil {
				return rollbackReplacement(fmt.Errorf("replacement health check: %w", err))
			}
			available, total := m.availableNodeCount()
			if available < minAvailableNodes {
				healthErr := fmt.Errorf("health check rejected replacement: %d/%d nodes available (need >= %d)", available, total, minAvailableNodes)
				return rollbackReplacement(healthErr)
			}
		} else {
			go m.monitorMgr.ProbeAllNow(periodicHealthTimeout)
		}
	}
	if newCfg.GeoIP.Enabled {
		if err := m.refreshExitGeoIP(operationCtx, newCfg); err != nil {
			return rollbackReplacement(fmt.Errorf("prepare replacement GeoIP pools: %w", err))
		}
		if err := m.ensureGeoIPRouter(runtimeBaseCtx, newCfg); err != nil {
			return rollbackReplacement(fmt.Errorf("prepare replacement GeoIP router: %w", err))
		}
	}
	if err := operationCtx.Err(); err != nil {
		return rollbackReplacement(err)
	}
	if m.beforeRuntimeCommit != nil {
		m.beforeRuntimeCommit()
	}
	if err := operationCtx.Err(); err != nil {
		return rollbackReplacement(err)
	}
	markCommitted, releaseCommitBarrier, err := commitguard.Acquire(operationCtx)
	if err != nil {
		return rollbackReplacement(err)
	}

	committedCfg, monitorServer := m.adoptConfig(newCfg, true)
	markCommitted()
	releaseCommitBarrier()
	if monitorServer != nil {
		monitorServer.SetConfig(committedCfg)
	}

	m.logger.Infof("reload completed successfully with %d nodes", len(newCfg.Nodes))

	if !newCfg.GeoIP.Enabled {
		m.mu.Lock()
		oldGeoRouter := m.geoRouter
		m.geoRouter = nil
		m.mu.Unlock()
		if oldGeoRouter != nil {
			oldGeoRouter.Stop()
		}
		m.stopGeoLookup()
	}

	return nil
}

// reloadManagementOnlyRuntimeLocked updates first-run configuration or starts
// the first proxy runtime after the WebUI has obtained usable nodes.
// reloadMu is held by the caller throughout this transition.
func (m *Manager) reloadManagementOnlyRuntimeLocked(
	operationCtx context.Context,
	runtimeBaseCtx context.Context,
	oldCfg *config.Config,
	newCfg *config.Config,
) error {
	if operationCtx == nil {
		operationCtx = context.Background()
	}
	if runtimeBaseCtx == nil {
		runtimeBaseCtx = context.Background()
	}
	if len(newCfg.Nodes) == 0 {
		m.applyLiveProbeSettings(newCfg)
		if err := m.commitManagementOnlyConfig(operationCtx, newCfg); err != nil {
			return err
		}
		m.logger.Infof("management-only configuration updated; waiting for proxy nodes")
		return nil
	}
	restoreManagementOnlyState := func() {
		m.applyLiveProbeSettings(oldCfg)
		m.updateHealthPersistence(oldCfg)
		if m.monitorMgr != nil {
			var oldNodes []config.NodeConfig
			if oldCfg != nil {
				oldNodes = oldCfg.Nodes
			}
			m.monitorMgr.RetainNodeURIs(nodeURISet(oldNodes))
		}
	}

	m.logger.Infof("starting first proxy runtime with %d nodes", len(newCfg.Nodes))
	built, err := m.createBox(runtimeBaseCtx, newCfg)
	if err != nil {
		restoreManagementOnlyState()
		return fmt.Errorf("create first proxy runtime: %w", err)
	}
	if err := operationCtx.Err(); err != nil {
		_ = built.box.Close()
		restoreManagementOnlyState()
		return err
	}
	if err := built.box.Start(); err != nil {
		_ = built.box.Close()
		restoreManagementOnlyState()
		return fmt.Errorf("start first proxy runtime: %w", err)
	}

	m.applyLiveProbeSettings(newCfg)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = built.box.Close()
		restoreManagementOnlyState()
		return errManagerClosed
	}
	m.currentBox = built.box
	m.runtimeCtx = built.ctx
	m.runtimeOptions = built.options
	m.managementOnly = false
	m.mu.Unlock()
	m.updateHealthPersistence(newCfg)

	rollbackCandidate := func(cause error) error {
		_ = built.box.Close()
		m.mu.Lock()
		if m.currentBox == built.box {
			m.currentBox = nil
			m.runtimeCtx = nil
			m.runtimeOptions = option.Options{}
			m.managementOnly = true
		}
		oldGeoRouter := m.geoRouter
		m.geoRouter = nil
		m.mu.Unlock()
		if oldGeoRouter != nil {
			_ = oldGeoRouter.Stop()
		}
		m.stopGeoLookup()
		restoreManagementOnlyState()
		return cause
	}

	if m.monitorMgr != nil {
		healthTimeout := newCfg.SubscriptionRefresh.HealthCheckTimeout
		if healthTimeout <= 0 {
			healthTimeout = defaultHealthCheckTimeout
		}
		minAvailableNodes := newCfg.SubscriptionRefresh.MinAvailableNodes
		if _, probeConfigured := m.monitorMgr.DestinationForProbe(); probeConfigured && minAvailableNodes > 0 {
			if err := m.monitorMgr.ProbeAllNowContext(operationCtx, healthTimeout); err != nil {
				return rollbackCandidate(fmt.Errorf("first runtime health check: %w", err))
			}
			available, total := m.availableNodeCount()
			if available < minAvailableNodes {
				return rollbackCandidate(fmt.Errorf("health check rejected first runtime: %d/%d nodes available (need >= %d)", available, total, minAvailableNodes))
			}
		} else {
			go m.monitorMgr.ProbeAllNow(periodicHealthTimeout)
		}
	}
	if newCfg.GeoIP.Enabled {
		if err := m.refreshExitGeoIP(operationCtx, newCfg); err != nil {
			return rollbackCandidate(fmt.Errorf("prepare first runtime GeoIP pools: %w", err))
		}
		if err := m.ensureGeoIPRouter(runtimeBaseCtx, newCfg); err != nil {
			return rollbackCandidate(fmt.Errorf("prepare first runtime GeoIP router: %w", err))
		}
	}
	if err := operationCtx.Err(); err != nil {
		return rollbackCandidate(err)
	}
	if m.beforeRuntimeCommit != nil {
		m.beforeRuntimeCommit()
	}
	if err := operationCtx.Err(); err != nil {
		return rollbackCandidate(err)
	}

	markCommitted, releaseCommitBarrier, err := commitguard.Acquire(operationCtx)
	if err != nil {
		return rollbackCandidate(err)
	}
	committedCfg, monitorServer := m.adoptConfig(newCfg, true)
	markCommitted()
	releaseCommitBarrier()
	if monitorServer != nil {
		monitorServer.SetConfig(committedCfg)
	}
	m.startPeriodicHealthChecks()
	m.logger.Infof("first proxy runtime started successfully with %d nodes", len(newCfg.Nodes))
	return nil
}

func (m *Manager) commitManagementOnlyConfig(ctx context.Context, cfg *config.Config) error {
	if m.beforeRuntimeCommit != nil {
		m.beforeRuntimeCommit()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	markCommitted, releaseCommitBarrier, err := commitguard.Acquire(ctx)
	if err != nil {
		return err
	}
	committedCfg, monitorServer := m.adoptConfig(cfg, true)
	markCommitted()
	releaseCommitBarrier()
	if monitorServer != nil {
		monitorServer.SetConfig(committedCfg)
	}
	return nil
}

var errRuntimeReloadUnsupported = errors.New("runtime reload unsupported")

func canReloadNodesInPlace(oldCfg, newCfg *config.Config) bool {
	if oldCfg == nil || newCfg == nil || oldCfg.Mode != newCfg.Mode {
		return false
	}
	// These settings are owned by immutable Box services or by listeners that
	// are not node-scoped. Node credentials/ports and pool policy are handled by
	// the runtime diff below.
	if !reflect.DeepEqual(oldCfg.Listener, newCfg.Listener) ||
		oldCfg.MultiPort.Address != newCfg.MultiPort.Address ||
		oldCfg.LogLevel != newCfg.LogLevel ||
		!reflect.DeepEqual(oldCfg.Log, newCfg.Log) ||
		!reflect.DeepEqual(oldCfg.GeoIP, newCfg.GeoIP) ||
		oldCfg.SkipCertVerify != newCfg.SkipCertVerify {
		return false
	}
	// Exit-IP classification and region-pool replacement are themselves a
	// multi-outbound transaction. Keep GeoIP node-set changes on the validated
	// full handoff path so a failed classification cannot leave a partially
	// updated in-place runtime.
	if newCfg.GeoIP.Enabled && !reflect.DeepEqual(oldCfg.Nodes, newCfg.Nodes) {
		return false
	}
	return true
}

func (m *Manager) reloadNodesInPlace(
	ctx context.Context,
	runtimeCtx context.Context,
	instance *box.Box,
	oldCfg *config.Config,
	oldOptions option.Options,
	newCfg *config.Config,
) error {
	if newCfg.GeoIP.Enabled {
		// Keep the committed authentication policy in force while the runtime
		// diff is still fallible. GeoIP configuration and node membership are
		// identical on the in-place path, so the existing endpoint can be
		// repaired with the old configuration now and switched to the candidate
		// credentials at the final commit barrier below.
		if err := m.ensureGeoIPRouter(runtimeCtx, oldCfg); err != nil {
			return fmt.Errorf("ensure GeoIP router before runtime diff: %w", err)
		}
	}
	desiredOptions, err := builder.Build(newCfg)
	if err != nil {
		return fmt.Errorf("build runtime reload: %w", err)
	}

	oldBase, oldPools := splitRuntimeOutbounds(oldOptions)
	desiredBase, desiredPools := splitRuntimeOutbounds(desiredOptions)
	oldInbounds := runtimeInboundMap(oldOptions.Inbounds)
	desiredInbounds := runtimeInboundMap(desiredOptions.Inbounds)
	if _, ok := desiredPools[pool.Tag]; !ok {
		return fmt.Errorf("%w: desired global pool is missing", errRuntimeReloadUnsupported)
	}

	// Replacing an outbound under the same tag would close its transports before
	// the pool cutover. Stable node tags normally make this impossible; defer an
	// unexpected global outbound mutation to the full validated handoff.
	for tag, desired := range desiredBase {
		if previous, ok := oldBase[tag]; ok && !reflect.DeepEqual(previous, desired) {
			return fmt.Errorf("%w: outbound %s changed in place", errRuntimeReloadUnsupported, tag)
		}
	}

	addedBaseTags := mapDifferenceKeys(desiredBase, oldBase)
	removedBaseTags := mapDifferenceKeys(oldBase, desiredBase)
	createdBaseTags := make([]string, 0, len(addedBaseTags))
	reclaimedDrains := make(map[string]runtimeDrainTarget)
	rollbackAddedBase := func() {
		removeRuntimeOutbounds(instance, createdBaseTags)
		m.restoreRuntimeDrainTargets(instance, reclaimedDrains)
	}
	for _, tag := range addedBaseTags {
		target, reclaimed, err := m.reclaimRuntimeDrain(ctx, instance, tag, desiredBase[tag])
		if err != nil {
			rollbackAddedBase()
			return fmt.Errorf("reclaim draining outbound %s: %w", tag, err)
		}
		if reclaimed {
			reclaimedDrains[tag] = target
			continue
		}
		if err := createRuntimeOutbound(runtimeCtx, instance, desiredBase[tag]); err != nil {
			rollbackAddedBase()
			return fmt.Errorf("create candidate outbound %s: %w", tag, err)
		}
		createdBaseTags = append(createdBaseTags, tag)
	}

	if err := m.preflightCandidateSet(ctx, instance, sortedMapKeys(desiredBase), newCfg); err != nil {
		rollbackAddedBase()
		return err
	}

	type outboundChange struct {
		tag      string
		previous option.Outbound
		existed  bool
	}
	poolChanges := make([]outboundChange, 0)
	rollbackPools := func() {
		for idx := len(poolChanges) - 1; idx >= 0; idx-- {
			change := poolChanges[idx]
			if change.existed {
				if err := createRuntimeOutbound(runtimeCtx, instance, change.previous); err != nil {
					m.logger.Errorf("failed to roll back pool %s: %v", change.tag, err)
				}
			} else if err := instance.Outbound().Remove(change.tag); err != nil {
				m.logger.Errorf("failed to remove added pool %s during rollback: %v", change.tag, err)
			}
		}
	}

	poolTags := sortedMapKeys(desiredPools)
	sort.SliceStable(poolTags, func(i, j int) bool {
		return poolTags[i] == pool.Tag && poolTags[j] != pool.Tag
	})
	for _, tag := range poolTags {
		desired := desiredPools[tag]
		previous, existed := oldPools[tag]
		if existed && reflect.DeepEqual(previous, desired) {
			continue
		}
		if err := createRuntimeOutbound(runtimeCtx, instance, desired); err != nil {
			rollbackPools()
			rollbackAddedBase()
			return fmt.Errorf("switch pool %s: %w", tag, err)
		}
		poolChanges = append(poolChanges, outboundChange{tag: tag, previous: previous, existed: existed})
	}

	type inboundChange struct {
		tag      string
		previous option.Inbound
		existed  bool
	}
	inboundChanges := make([]inboundChange, 0)
	rollbackInbounds := func() {
		for idx := len(inboundChanges) - 1; idx >= 0; idx-- {
			change := inboundChanges[idx]
			if _, exists := instance.Inbound().Get(change.tag); exists {
				_ = instance.Inbound().Remove(change.tag)
			}
			if change.existed {
				if err := createRuntimeInbound(runtimeCtx, instance, change.previous); err != nil {
					m.logger.Errorf("failed to roll back inbound %s: %v", change.tag, err)
				}
			}
		}
	}
	failRuntimeSwitch := func(cause error) error {
		rollbackInbounds()
		rollbackPools()
		rollbackAddedBase()
		return cause
	}

	for _, tag := range sortedMapKeys(desiredInbounds) {
		desired := desiredInbounds[tag]
		previous, existed := oldInbounds[tag]
		if existed && reflect.DeepEqual(previous, desired) {
			continue
		}
		if err := createRuntimeInbound(runtimeCtx, instance, desired); err != nil {
			// Same-address credential changes cannot bind the new listener before
			// closing the changed old one. Limit that short handoff to this node.
			if !existed {
				return failRuntimeSwitch(fmt.Errorf("create inbound %s: %w", tag, err))
			}
			// Record the inverse before Remove. sing-box may detach the inbound
			// and only then surface a Close/dependency error.
			inboundChanges = append(inboundChanges, inboundChange{tag: tag, previous: previous, existed: true})
			if removeErr := instance.Inbound().Remove(tag); removeErr != nil {
				return failRuntimeSwitch(fmt.Errorf("replace inbound %s: %v (remove old: %w)", tag, err, removeErr))
			}
			if retryErr := createRuntimeInbound(runtimeCtx, instance, desired); retryErr != nil {
				return failRuntimeSwitch(fmt.Errorf("replace inbound %s after releasing listener: %w", tag, retryErr))
			}
			continue
		}
		inboundChanges = append(inboundChanges, inboundChange{tag: tag, previous: previous, existed: existed})
	}
	for _, tag := range mapDifferenceKeys(oldInbounds, desiredInbounds) {
		previous := oldInbounds[tag]
		inboundChanges = append(inboundChanges, inboundChange{tag: tag, previous: previous, existed: true})
		if err := instance.Inbound().Remove(tag); err != nil {
			return failRuntimeSwitch(fmt.Errorf("remove inbound %s: %w", tag, err))
		}
	}

	for _, tag := range mapDifferenceKeys(oldPools, desiredPools) {
		previous := oldPools[tag]
		poolChanges = append(poolChanges, outboundChange{tag: tag, previous: previous, existed: true})
		if err := instance.Outbound().Remove(tag); err != nil {
			return failRuntimeSwitch(fmt.Errorf("remove pool %s: %w", tag, err))
		}
	}

	draining := make(map[string]adapter.Outbound, len(removedBaseTags))
	for _, tag := range removedBaseTags {
		if outbound, ok := instance.Outbound().Outbound(tag); ok {
			draining[tag] = outbound
		}
	}
	if err := ctx.Err(); err != nil {
		return failRuntimeSwitch(err)
	}

	m.applyLiveProbeSettings(newCfg)
	if m.beforeRuntimeCommit != nil {
		m.beforeRuntimeCommit()
	}
	if err := ctx.Err(); err != nil {
		m.applyLiveProbeSettings(oldCfg)
		return failRuntimeSwitch(err)
	}
	markCommitted, releaseCommitBarrier, err := commitguard.Acquire(ctx)
	if err != nil {
		m.applyLiveProbeSettings(oldCfg)
		return failRuntimeSwitch(err)
	}
	if newCfg.GeoIP.Enabled {
		if err := m.ensureGeoIPRouter(runtimeCtx, newCfg); err != nil {
			releaseCommitBarrier()
			m.applyLiveProbeSettings(oldCfg)
			return failRuntimeSwitch(fmt.Errorf("commit GeoIP router configuration: %w", err))
		}
	}
	m.mu.Lock()
	m.runtimeOptions = desiredOptions
	committedCfg := m.adoptConfigLocked(newCfg, true)
	drainTimeout := m.drainTimeout
	monitorServer := m.monitorServer
	m.mu.Unlock()
	markCommitted()
	releaseCommitBarrier()
	m.updateHealthPersistence(newCfg)
	if monitorServer != nil {
		monitorServer.SetConfig(committedCfg)
	}
	if m.monitorMgr != nil {
		m.monitorMgr.RetainNodeURIs(nodeURISet(newCfg.Nodes))
		m.syncCommittedHealth(newCfg)
	}

	m.scheduleRuntimeDrains(instance, draining, oldBase, drainTimeout)
	if newCfg.GeoIP.Enabled {
		m.syncGeoRouterDialers()
	} else {
		m.mu.Lock()
		oldGeoRouter := m.geoRouter
		m.geoRouter = nil
		m.mu.Unlock()
		if oldGeoRouter != nil {
			oldGeoRouter.Stop()
		}
		m.stopGeoLookup()
	}

	m.logger.Infof(
		"node-level reload committed: %d added, %d removed, %d unchanged; draining removed outbounds for %s",
		len(addedBaseTags), len(removedBaseTags), len(desiredBase)-len(addedBaseTags), drainTimeout,
	)
	return nil
}

func splitRuntimeOutbounds(options option.Options) (map[string]option.Outbound, map[string]option.Outbound) {
	base := make(map[string]option.Outbound)
	pools := make(map[string]option.Outbound)
	for _, outbound := range options.Outbounds {
		if outbound.Type == pool.Type {
			pools[outbound.Tag] = outbound
		} else {
			base[outbound.Tag] = outbound
		}
	}
	return base, pools
}

func runtimeInboundMap(inbounds []option.Inbound) map[string]option.Inbound {
	result := make(map[string]option.Inbound, len(inbounds))
	for _, inbound := range inbounds {
		result[inbound.Tag] = inbound
	}
	return result
}

func sortedMapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func mapDifferenceKeys[T, U any](left map[string]T, right map[string]U) []string {
	keys := make([]string, 0)
	for key := range left {
		if _, ok := right[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func createRuntimeOutbound(runtimeCtx context.Context, instance *box.Box, outbound option.Outbound) error {
	logger := singlog.NewNOPFactory().NewLogger("runtime/outbound/" + outbound.Tag)
	outboundCtx := adapter.WithContext(runtimeCtx, &adapter.InboundContext{Outbound: outbound.Tag})
	return instance.Outbound().Create(outboundCtx, instance.Router(), logger, outbound.Tag, outbound.Type, outbound.Options)
}

func createRuntimeInbound(runtimeCtx context.Context, instance *box.Box, inbound option.Inbound) error {
	logger := singlog.NewNOPFactory().NewLogger("runtime/inbound/" + inbound.Tag)
	return instance.Inbound().Create(runtimeCtx, instance.Router(), logger, inbound.Tag, inbound.Type, inbound.Options)
}

func removeRuntimeOutbounds(instance *box.Box, tags []string) {
	for idx := len(tags) - 1; idx >= 0; idx-- {
		_ = instance.Outbound().Remove(tags[idx])
	}
}

func (m *Manager) preflightCandidateSet(ctx context.Context, instance *box.Box, tags []string, cfg *config.Config) error {
	minimum := cfg.SubscriptionRefresh.MinAvailableNodes
	if minimum <= 0 {
		return nil
	}
	if len(tags) < minimum {
		return fmt.Errorf("health check rejected candidate: %d nodes built (need >= %d)", len(tags), minimum)
	}
	target, configured, err := monitor.ResolveProbeTarget(cfg.Management.ProbeTarget, cfg.SkipCertVerify)
	if err != nil {
		return fmt.Errorf("health check rejected candidate probe target: %w", err)
	}
	if !configured {
		return nil
	}
	timeout := cfg.SubscriptionRefresh.HealthCheckTimeout
	if timeout <= 0 {
		timeout = defaultHealthCheckTimeout
	}

	workerCount := m.monitorMgr.ProbeConcurrency()
	if workerCount > len(tags) {
		workerCount = len(tags)
	}
	jobs := make(chan string)
	results := make(chan bool, len(tags))
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tag := range jobs {
				outbound, ok := instance.Outbound().Outbound(tag)
				if !ok {
					results <- false
					continue
				}
				probeCtx, cancel := context.WithTimeout(ctx, timeout)
				flightKey := preflightProbeFlightKey(instance, tag, target, outbound)
				err := m.runPreflightProbe(probeCtx, flightKey, func() error {
					return probeOutboundConnection(probeCtx, outbound, target)
				})
				cancel()
				results <- err == nil
			}
		}()
	}
	go func() {
		for _, tag := range tags {
			jobs <- tag
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()
	available := 0
	for success := range results {
		if success {
			available++
		}
	}
	if available < minimum {
		return fmt.Errorf("health check rejected candidate before cutover: %d/%d nodes available (need >= %d)", available, len(tags), minimum)
	}
	m.logger.Infof("candidate health check passed before cutover: %d/%d nodes available", available, len(tags))
	return nil
}

func preflightProbeFlightKey(instance *box.Box, tag string, target monitor.ProbeTarget, outbound adapter.Outbound) string {
	return fmt.Sprintf(
		"%p\x00%s\x00%s\x00%s\x00%t\x00%t\x00%T:%p",
		instance,
		tag,
		target.Destination.String(),
		target.Host,
		target.TLS,
		target.SkipCertVerify,
		outbound,
		outbound,
	)
}

// runPreflightProbe limits an uncooperative outbound to one legacy probe
// goroutine per stable node tag. Later reloads join that flight instead of
// starting another Dial that may ignore cancellation forever.
func (m *Manager) runPreflightProbe(ctx context.Context, tag string, probe func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if probe == nil {
		return errors.New("preflight probe is not configured")
	}

	m.preflightMu.Lock()
	if m.preflightCalls == nil {
		m.preflightCalls = make(map[string]*preflightProbeCall)
	}
	call := m.preflightCalls[tag]
	m.preflightMu.Unlock()
	if call != nil {
		return waitForPreflightProbe(ctx, call)
	}

	// Some legacy outbounds ignore cancellation. Bound those calls globally so
	// changing node tags cannot grow permanent goroutines and map entries
	// without limit.
	select {
	case m.preflightSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	m.preflightMu.Lock()
	if call = m.preflightCalls[tag]; call != nil {
		m.preflightMu.Unlock()
		<-m.preflightSlots
		return waitForPreflightProbe(ctx, call)
	}
	call = &preflightProbeCall{done: make(chan struct{})}
	m.preflightCalls[tag] = call
	m.preflightMu.Unlock()
	m.auxProbeWG.Add(1)
	go func() {
		defer m.auxProbeWG.Done()
		defer func() { <-m.preflightSlots }()
		var probeErr error
		func() {
			defer func() {
				if recover() != nil {
					probeErr = errors.New("preflight probe panicked")
				}
			}()
			probeErr = probe()
		}()

		m.preflightMu.Lock()
		call.err = probeErr
		close(call.done)
		if m.preflightCalls[tag] == call {
			delete(m.preflightCalls, tag)
		}
		m.preflightMu.Unlock()
	}()

	return waitForPreflightProbe(ctx, call)
}

func waitForPreflightProbe(ctx context.Context, call *preflightProbeCall) error {
	select {
	case <-call.done:
		return call.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) syncCommittedHealth(cfg *config.Config) {
	if m.monitorMgr == nil || cfg == nil {
		return
	}
	if _, configured := m.monitorMgr.DestinationForProbe(); !configured {
		return
	}
	timeout := cfg.SubscriptionRefresh.HealthCheckTimeout
	if timeout <= 0 {
		timeout = defaultHealthCheckTimeout
	}
	m.monitorMgr.ProbeAllNow(timeout)
}

func probeOutboundConnection(ctx context.Context, outbound adapter.Outbound, target monitor.ProbeTarget) error {
	connection, err := outbound.DialContext(ctx, N.NetworkTCP, target.Destination)
	if err != nil {
		return err
	}
	watchDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = connection.Close()
		case <-watchDone:
		}
	}()
	defer close(watchDone)
	defer connection.Close()
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		_ = connection.SetDeadline(deadline)
	}
	if target.TLS {
		tlsConn := tls.Client(connection, &tls.Config{
			ServerName:         target.Host,
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: target.SkipCertVerify, // Explicit global setting.
		})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return fmt.Errorf("TLS handshake: %w", err)
		}
		connection = tlsConn
	}
	host := target.Host
	if target.Destination.Port != 80 && target.Destination.Port != 443 {
		host = target.Destination.AddrString()
	}
	request := fmt.Sprintf("GET /generate_204 HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", host)
	if _, err := connection.Write([]byte(request)); err != nil {
		return err
	}
	_, err = bufio.NewReader(connection).ReadByte()
	return err
}

func (m *Manager) scheduleRuntimeDrains(
	instance *box.Box,
	draining map[string]adapter.Outbound,
	options map[string]option.Outbound,
	timeout time.Duration,
) {
	if instance == nil || len(draining) == 0 {
		return
	}
	if timeout < 0 {
		timeout = 0
	}
	now := time.Now()
	m.drainMu.Lock()
	if m.drainClosed {
		m.drainMu.Unlock()
		return
	}
	if m.drainInstance != instance {
		m.drainInstance = instance
		m.drainPending = make(map[string]runtimeDrainTarget, len(draining))
		m.drainDiscarding = false
	}
	for tag, expected := range draining {
		target := runtimeDrainTarget{
			tag:      tag,
			expected: expected,
			option:   options[tag],
			due:      now.Add(timeout),
			timeout:  timeout,
		}
		if existing, ok := m.drainPending[tag]; ok && existing.expected == expected && existing.due.Before(target.due) {
			target = existing
		}
		m.drainPending[tag] = target
	}
	if !m.drainStarted {
		m.drainStarted = true
		m.drainWG.Add(1)
		go m.runRuntimeDrainWorker()
	}
	m.drainMu.Unlock()
	m.wakeRuntimeDrainWorker()
}

// reclaimRuntimeDrain cancels a pending retirement when the exact same node is
// re-added during its drain window. The runtime outbound can then be reused
// without a duplicate-tag create or a later worker deleting the live node.
func (m *Manager) reclaimRuntimeDrain(
	ctx context.Context,
	instance *box.Box,
	tag string,
	desired option.Outbound,
) (runtimeDrainTarget, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		m.drainMu.Lock()
		if m.drainClosed || m.drainInstance != instance {
			m.drainMu.Unlock()
			return runtimeDrainTarget{}, false, nil
		}
		target, exists := m.drainPending[tag]
		if !exists || !reflect.DeepEqual(target.option, desired) {
			m.drainMu.Unlock()
			return runtimeDrainTarget{}, false, nil
		}
		if target.inFlight {
			done := target.opDone
			m.drainMu.Unlock()
			timer := time.NewTimer(2 * time.Second)
			select {
			case <-done:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				continue
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return runtimeDrainTarget{}, false, ctx.Err()
			case <-timer.C:
				return runtimeDrainTarget{}, false, fmt.Errorf("retirement is still in progress")
			}
		}
		current, ok := instance.Outbound().Outbound(tag)
		if !ok || current != target.expected {
			delete(m.drainPending, tag)
			m.drainMu.Unlock()
			return runtimeDrainTarget{}, false, nil
		}
		delete(m.drainPending, tag)
		m.drainMu.Unlock()
		m.wakeRuntimeDrainWorker()
		return target, true, nil
	}
}

func (m *Manager) restoreRuntimeDrainTargets(instance *box.Box, targets map[string]runtimeDrainTarget) {
	if instance == nil || len(targets) == 0 {
		return
	}
	m.drainMu.Lock()
	if m.drainClosed || m.drainInstance != instance {
		m.drainMu.Unlock()
		return
	}
	if m.drainPending == nil {
		m.drainPending = make(map[string]runtimeDrainTarget, len(targets))
	}
	for tag, target := range targets {
		target.inFlight = false
		target.opDone = nil
		m.drainPending[tag] = target
	}
	m.drainMu.Unlock()
	m.wakeRuntimeDrainWorker()
}

func (m *Manager) wakeRuntimeDrainWorker() {
	select {
	case m.drainWake <- struct{}{}:
	default:
	}
}

func (m *Manager) runRuntimeDrainWorker() {
	defer m.drainWG.Done()
	for {
		m.drainMu.Lock()
		if m.drainClosed {
			m.drainMu.Unlock()
			return
		}
		var next time.Time
		for _, target := range m.drainPending {
			if m.drainDiscarding {
				continue
			}
			if target.inFlight {
				continue
			}
			if next.IsZero() || target.due.Before(next) {
				next = target.due
			}
		}
		m.drainMu.Unlock()

		if next.IsZero() {
			select {
			case <-m.drainWake:
				continue
			case <-m.drainStop:
				return
			}
		}

		wait := time.Until(next)
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-m.drainWake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue
		case <-m.drainStop:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}

		now := time.Now()
		m.drainMu.Lock()
		instance := m.drainInstance
		var ready runtimeDrainTarget
		for tag, target := range m.drainPending {
			if m.drainDiscarding || target.inFlight || target.due.After(now) {
				continue
			}
			if ready.tag == "" || tag < ready.tag {
				ready = target
			}
		}
		if ready.tag != "" {
			ready.inFlight = true
			ready.opDone = make(chan struct{})
			m.drainPending[ready.tag] = ready
		}
		m.drainMu.Unlock()
		if ready.tag != "" {
			m.retireRuntimeOutbound(instance, ready)
		}
	}
}

func (m *Manager) retireRuntimeOutbound(instance *box.Box, target runtimeDrainTarget) {
	if instance == nil {
		return
	}
	m.drainMu.Lock()
	currentTarget, pending := m.drainPending[target.tag]
	if m.drainClosed || m.drainInstance != instance || !pending || currentTarget.opDone != target.opDone {
		m.drainMu.Unlock()
		return
	}
	m.drainMu.Unlock()

	current, ok := instance.Outbound().Outbound(target.tag)
	if !ok || current != target.expected {
		m.finishRuntimeDrain(instance, target, nil, false)
		return
	}
	// Keep removal on the single drain worker. It runs outside drainMu, while
	// drainWG guarantees that a full Close cannot race this operation against
	// box.Close on the same sing-box instance.
	remove := m.removeDrainedOutbound
	if remove == nil {
		remove = func(instance *box.Box, tag string) error {
			return instance.Outbound().Remove(tag)
		}
	}
	err := remove(instance, target.tag)
	m.finishRuntimeDrain(instance, target, err, true)
}

func (m *Manager) finishRuntimeDrain(instance *box.Box, target runtimeDrainTarget, removeErr error, attempted bool) {
	m.drainMu.Lock()
	current, pending := m.drainPending[target.tag]
	if m.drainClosed || m.drainInstance != instance || !pending || current.opDone != target.opDone {
		m.drainMu.Unlock()
		return
	}
	if removeErr != nil {
		current.attempts++
		delay := time.Second << min(current.attempts-1, 5)
		current.due = time.Now().Add(delay)
		current.inFlight = false
		close(current.opDone)
		current.opDone = nil
		m.drainPending[target.tag] = current
		m.drainMu.Unlock()
		m.logger.Warnf("failed to retire drained outbound %s; retrying in %s: %v", target.tag, delay, removeErr)
		m.wakeRuntimeDrainWorker()
		return
	}
	delete(m.drainPending, target.tag)
	close(current.opDone)
	m.drainMu.Unlock()
	if attempted {
		m.logger.Infof("retired outbound %s after %s drain", target.tag, target.timeout)
	}
}

func (m *Manager) discardRuntimeDrains(instance *box.Box) error {
	if instance == nil {
		return nil
	}
	m.drainMu.Lock()
	if m.drainInstance != instance {
		m.drainMu.Unlock()
		return nil
	}
	m.drainDiscarding = true
	inFlight := make([]chan struct{}, 0, 1)
	for _, target := range m.drainPending {
		if target.inFlight && target.opDone != nil {
			inFlight = append(inFlight, target.opDone)
		}
	}
	m.drainMu.Unlock()
	for _, done := range inFlight {
		timer := time.NewTimer(m.drainOperationWait)
		select {
		case <-done:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-m.drainStop:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return errors.New("runtime drain stopped during handoff")
		case <-timer.C:
			m.drainMu.Lock()
			if m.drainInstance == instance {
				m.drainDiscarding = false
			}
			m.drainMu.Unlock()
			return fmt.Errorf("timed out waiting %s for outbound retirement", m.drainOperationWait)
		}
	}
	m.drainMu.Lock()
	if m.drainInstance == instance {
		m.drainPending = nil
		m.drainInstance = nil
	}
	m.drainDiscarding = false
	m.drainMu.Unlock()
	m.wakeRuntimeDrainWorker()
	return nil
}

func (m *Manager) stopRuntimeDrains() error {
	m.drainMu.Lock()
	if !m.drainClosed {
		m.drainClosed = true
		m.drainPending = nil
		close(m.drainStop)
	}
	started := m.drainStarted
	m.drainMu.Unlock()
	if !started {
		return nil
	}
	done := make(chan struct{})
	go func() {
		m.drainWG.Wait()
		close(done)
	}()
	timer := time.NewTimer(m.drainOperationWait)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("timed out waiting %s for runtime drain worker", m.drainOperationWait)
	}
}

// rollbackToOldConfig attempts to restart with the previous configuration.
func (m *Manager) rollbackToOldConfig(ctx context.Context, oldCfg *config.Config) error {
	if oldCfg == nil {
		return errors.New("old config is nil")
	}
	m.logger.Warnf("attempting rollback to previous config...")
	built, err := m.createBox(ctx, oldCfg)
	if err != nil {
		m.logger.Errorf("rollback failed to create box: %v", err)
		return err
	}
	if err := built.box.Start(); err != nil {
		_ = built.box.Close()
		m.logger.Errorf("rollback failed to start box: %v", err)
		return err
	}
	m.applyLiveProbeSettings(oldCfg)
	m.mu.Lock()
	m.currentBox = built.box
	m.runtimeCtx = built.ctx
	m.runtimeOptions = built.options
	restoredCfg := m.adoptConfigLocked(oldCfg, false)
	monitorServer := m.monitorServer
	m.mu.Unlock()
	m.updateHealthPersistence(oldCfg)
	if monitorServer != nil {
		monitorServer.SetConfig(restoredCfg)
	}
	if m.monitorMgr != nil {
		m.monitorMgr.RetainNodeURIs(nodeURISet(oldCfg.Nodes))
	}
	if oldCfg.GeoIP.Enabled {
		if err := m.refreshExitGeoIP(ctx, oldCfg); err != nil {
			return fmt.Errorf("restore proxy exit IP classification: %w", err)
		}
		if err := m.ensureGeoIPRouter(ctx, oldCfg); err != nil {
			return fmt.Errorf("restore GeoIP router: %w", err)
		}
	} else {
		m.mu.Lock()
		oldGeoRouter := m.geoRouter
		m.geoRouter = nil
		m.mu.Unlock()
		if oldGeoRouter != nil {
			_ = oldGeoRouter.Stop()
		}
		m.stopGeoLookup()
	}
	m.logger.Infof("rollback successful")
	return nil
}

func nodeURISet(nodes []config.NodeConfig) map[string]struct{} {
	result := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		result[strings.TrimSpace(node.URI)] = struct{}{}
	}
	return result
}

// Close terminates the active instance and auxiliary components.
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.closeErr = m.closeDetachedComponents()
		close(m.closeDone)
	})
	<-m.closeDone
	return m.closeErr
}

func (m *Manager) closeDetachedComponents() error {
	state := m.detachForClose()
	// Freeze the producer side before waiting for any runtime-owned work. This
	// guarantees that StopAndWait observes a closed probe lifecycle and that no
	// new callback can begin while shutdown is deciding whether it is safe to
	// close sing-box.
	if state.monitorMgr != nil {
		state.monitorMgr.Stop()
	}
	drainErr := m.stopRuntimeDrains()

	// Stop accepting management requests before tearing down the runtime. The
	// deadline prevents a slow client from holding shutdown indefinitely. Most
	// importantly, no manager lock is held while Shutdown waits for handlers;
	// those handlers are therefore free to finish calls back into Manager.
	if state.monitorServer != nil {
		shutdownWithTimeout(defaultShutdownTimeout, state.monitorServer.Shutdown)
	}

	var probeErr error
	if state.monitorMgr != nil {
		probeCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		probeErr = state.monitorMgr.StopAndWait(probeCtx)
		cancel()
	}
	auxProbeCtx, auxProbeCancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	auxProbeErr := waitForWaitGroup(auxProbeCtx, &m.auxProbeWG)
	auxProbeCancel()
	if state.geoRouter != nil {
		state.geoRouter.Stop()
	}

	var boxErr error
	if drainErr != nil {
		m.logger.Warnf("forcing sing-box close after drain timeout: %v", drainErr)
	}
	if probeErr != nil {
		m.logger.Warnf("forcing sing-box close after probe timeout: %v", probeErr)
	}
	if auxProbeErr != nil {
		m.logger.Warnf("forcing sing-box close after auxiliary probe timeout: %v", auxProbeErr)
	}
	if state.currentBox != nil {
		boxErr = state.currentBox.Close()
	}
	if persistErr := pool.PersistHealthStateNow(); persistErr != nil {
		m.logger.Warnf("failed to persist pool health state during shutdown: %v", persistErr)
	}
	// Closing shared state makes any late, context-ignoring callback a no-op.
	// Reset only after Box.Close succeeds so no live listener is detached from
	// the registry merely because a cooperative wait exceeded its soft bound.
	if state.ownsRuntime && boxErr == nil {
		pool.ResetSharedStateStore()
	}
	var lookupErr error
	if state.geoLookup != nil {
		lookupErr = state.geoLookup.Close()
	}
	if state.ownsRuntime {
		activeManagerMu.Lock()
		if activeManager == m {
			activeManager = nil
		}
		activeManagerMu.Unlock()
	}
	return errors.Join(drainErr, probeErr, auxProbeErr, boxErr, lookupErr)
}

func waitForWaitGroup(ctx context.Context, group *sync.WaitGroup) error {
	if group == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// detachForClose atomically makes the manager unavailable to new reloads and
// returns the owned components. External lifecycle methods must only be called
// after this function has released both manager locks.
func (m *Manager) detachForClose() managerCloseState {
	m.reloadMu.Lock()
	m.mu.Lock()
	state := managerCloseState{
		currentBox:    m.currentBox,
		monitorServer: m.monitorServer,
		monitorMgr:    m.monitorMgr,
		geoRouter:     m.geoRouter,
		geoLookup:     m.geoLookup,
		ownsRuntime:   m.ownsRuntime,
	}
	m.currentBox = nil
	m.managementOnly = false
	m.runtimeCtx = nil
	m.runtimeOptions = option.Options{}
	m.monitorServer = nil
	m.monitorMgr = nil
	m.healthCheckStarted = false
	m.geoRouter = nil
	m.geoLookup = nil
	m.geoLookupPath = ""
	m.geoAutoInterval = 0
	m.exitIPs = nil
	m.baseCtx = nil
	m.closed = true
	m.ownsRuntime = false
	m.mu.Unlock()
	m.reloadMu.Unlock()
	return state
}

func shutdownWithTimeout(timeout time.Duration, shutdown func(context.Context)) {
	if shutdown == nil {
		return
	}
	if timeout <= 0 {
		timeout = defaultShutdownTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdown(ctx)
}

func (m *Manager) updateHealthPersistence(cfg *config.Config) {
	if cfg == nil {
		return
	}
	if err := pool.ConfigureHealthPersistence(cfg.HealthStatePath()); err != nil {
		m.logger.Warnf("failed to configure pool health persistence: %v", err)
		return
	}
	if err := pool.PersistHealthStateNow(); err != nil {
		m.logger.Warnf("failed to flush pool health state: %v", err)
	}
}

// MonitorManager returns the shared monitor manager.
func (m *Manager) MonitorManager() *monitor.Manager {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.monitorMgr
}

// MonitorServer returns the monitor HTTP server.
func (m *Manager) MonitorServer() *monitor.Server {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.monitorServer
}

// ConfigSnapshot returns an independently mutable view of the currently
// committed configuration and its optimistic-concurrency revision.
func (m *Manager) ConfigSnapshot() (*config.Config, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg.Clone(), m.revision
}

// ensureGeoIPRouter publishes a usable region-routing listener without first
// tearing down the current one. An unchanged endpoint is updated in place;
// an endpoint change binds the candidate before the old listener is stopped.
func (m *Manager) ensureGeoIPRouter(ctx context.Context, cfg *config.Config) error {
	geoipPort, err := selectGeoIPRouterPort(cfg)
	if err != nil {
		return fmt.Errorf("select GeoIP router port: %w", err)
	}
	if configuredPort := cfg.GeoIP.Port; configuredPort != 0 && geoipPort != configuredPort {
		m.logger.Warnf("GeoIP port %d conflicts with another configured listener; using %d instead", configuredPort, geoipPort)
	} else if configuredPort == 0 && geoipPort != defaultGeoIPRouterPort {
		m.logger.Warnf("default GeoIP port %d conflicts with another configured listener; using %d instead", defaultGeoIPRouterPort, geoipPort)
	}
	geoipListen := cfg.GeoIP.Listen
	geoipUsername := cfg.Listener.Username
	geoipPassword := cfg.Listener.Password
	defaultGeoIPListen := cfg.Listener.Address
	if cfg.Mode == "multi-port" {
		defaultGeoIPListen = cfg.MultiPort.Address
		geoipUsername = cfg.MultiPort.Username
		geoipPassword = cfg.MultiPort.Password
	}
	if geoipListen == "" {
		geoipListen = defaultGeoIPListen
	}

	routerCfg := geoip.RouterConfig{
		Listen:   geoipListen,
		Port:     geoipPort,
		Username: geoipUsername,
		Password: geoipPassword,
	}
	m.mu.RLock()
	current := m.geoRouter
	m.mu.RUnlock()
	if current != nil && current.IsRunning() && sameGeoIPRouterEndpoint(current.Config(), routerCfg) {
		current.UpdateCredentials(routerCfg.Username, routerCfg.Password)
		configureGeoIPRouterDialers(current)
		m.publishGeoIPRouterPort(cfg, geoipPort)
		return nil
	}

	router := geoip.NewRouter(routerCfg, nil)
	configureGeoIPRouterDialers(router)

	if err := router.Start(ctx); err != nil {
		return err
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = router.Stop()
		return errManagerClosed
	}
	previous := m.geoRouter
	m.geoRouter = router
	m.mu.Unlock()
	m.publishGeoIPRouterPort(cfg, geoipPort)
	if previous != nil {
		if err := previous.Stop(); err != nil {
			m.logger.Warnf("failed to stop replaced GeoIP router: %v", err)
		}
	}
	return nil
}

func sameGeoIPRouterEndpoint(left, right geoip.RouterConfig) bool {
	return strings.EqualFold(strings.Trim(strings.TrimSpace(left.Listen), "[]"), strings.Trim(strings.TrimSpace(right.Listen), "[]")) &&
		left.Port == right.Port
}

func configureGeoIPRouterDialers(router *geoip.Router) {
	if router == nil {
		return
	}
	for _, region := range geoip.AllRegions() {
		poolTag := fmt.Sprintf("pool-%s", region)
		if dialer, ok := pool.GetDialer(poolTag); ok {
			router.SetPool(region, dialer)
			log.Printf("   GeoIP: registered pool %s for selector %s", poolTag, region)
		} else {
			router.RemovePool(region)
		}
	}
	if dialer, ok := pool.GetDialer(pool.Tag); ok {
		router.SetGlobalPool(dialer)
	}
}

func (m *Manager) publishGeoIPRouterPort(cfg *config.Config, port uint16) {
	m.mu.Lock()
	managerOwnsArgument := m.cfg == cfg
	if managerOwnsArgument && m.cfg != nil {
		m.cfg.GeoIP.Port = port
	} else if cfg != nil {
		cfg.GeoIP.Port = port
	}
	runtimeCfg := m.cfg.Clone()
	monitorServer := m.monitorServer
	m.mu.Unlock()
	if managerOwnsArgument && monitorServer != nil {
		monitorServer.SetConfig(runtimeCfg)
	}
}

// newBoxRecover wraps box.New and converts library panics into credential-safe
// errors. If sing-box included the outbound index before panicking, preserve
// only that index so createBox can remove the bad node and retry. The original
// panic payload is never returned because it may contain a subscription URI.
func newBoxRecover(opts box.Options) (*box.Box, error) {
	instance, err := recoverBoxInitialization(func() (*box.Box, error) {
		return box.New(opts)
	})
	if err != nil {
		return nil, sanitizeBoxInitializationError(err)
	}
	return instance, nil
}

func sanitizeBoxInitializationError(err error) error {
	if err == nil {
		return nil
	}
	if match := outboundInitializationIndexPattern.FindStringSubmatch(err.Error()); match != nil {
		return fmt.Errorf("sing-box failed to initialize outbound[%s]", match[1])
	}
	return errors.New("sing-box initialization failed")
}

func recoverBoxInitialization(create func() (*box.Box, error)) (instance *box.Box, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			instance = nil
			panicText := fmt.Sprint(recovered)
			if match := outboundInitializationIndexPattern.FindStringSubmatch(panicText); match != nil {
				err = fmt.Errorf("sing-box panic during initialize outbound[%s]", match[1])
				return
			}
			err = errors.New("sing-box panic during initialization")
		}
	}()
	return create()
}

// createBox builds a sing-box instance from config.
// It retries automatically when individual outbounds fail sing-box validation,
// removing the offending outbound each time.
func (m *Manager) createBox(ctx context.Context, cfg *config.Config) (*builtInstance, error) {
	if cfg == nil {
		return nil, errors.New("config is nil")
	}
	if m.monitorMgr == nil {
		return nil, errors.New("monitor manager not initialized")
	}

	opts, err := builder.Build(cfg)
	if err != nil {
		return nil, fmt.Errorf("build sing-box options: %w", err)
	}

	maxRetries := len(cfg.Nodes)*3 + 50 // Dynamically scale retries to configuration size
	for attempt := 0; attempt <= maxRetries; attempt++ {
		inboundRegistry := include.InboundRegistry()
		outboundRegistry := include.OutboundRegistry()
		pool.Register(outboundRegistry)
		endpointRegistry := include.EndpointRegistry()
		dnsRegistry := include.DNSTransportRegistry()
		serviceRegistry := include.ServiceRegistry()

		boxCtx := box.Context(ctx, inboundRegistry, outboundRegistry, endpointRegistry, dnsRegistry, serviceRegistry)
		boxCtx = monitor.ContextWith(boxCtx, m.monitorMgr)
		// Pre-install the service registry so the context retained here observes
		// the managers that box.New registers on its derived context. This enables
		// safe runtime InboundManager/OutboundManager Create calls during reload.
		boxCtx = service.ContextWithDefaultRegistry(boxCtx)

		instance, err := newBoxRecover(box.Options{Context: boxCtx, Options: opts})
		if err == nil {
			if attempt > 0 {
				log.Printf("✅ sing-box instance created after removing %d invalid outbound(s)", attempt)
			}
			return &builtInstance{box: instance, ctx: boxCtx, options: opts}, nil
		}

		// Check if this is an outbound initialization error we can recover from
		matches := outboundInitializationIndexPattern.FindStringSubmatch(err.Error())
		if matches == nil {
			return nil, fmt.Errorf("create sing-box instance: %w", err)
		}

		idx, convErr := strconv.Atoi(matches[1])
		if convErr != nil || idx < 0 || idx >= len(opts.Outbounds) {
			return nil, fmt.Errorf("create sing-box instance: %w", err)
		}

		badTag := opts.Outbounds[idx].Tag
		log.Printf("⚠️  Outbound '%s' failed sing-box validation (removing and retrying)", badTag)

		// Remove the offending outbound
		opts.Outbounds = append(opts.Outbounds[:idx], opts.Outbounds[idx+1:]...)

		// Clean up pool outbounds that contained this tag
		var newOutbounds []option.Outbound
		var removedPoolTags []string
		for _, ob := range opts.Outbounds {
			if ob.Type == pool.Type {
				if poolOpts, ok := ob.Options.(*pool.Options); ok {
					poolOpts.Members = removeFromSlice(poolOpts.Members, badTag)
					delete(poolOpts.Metadata, badTag)
					for inboundTag, memberTag := range poolOpts.DedicatedMembers {
						if memberTag == badTag {
							delete(poolOpts.DedicatedMembers, inboundTag)
						}
					}

					// If the pool is now empty, remove it to avoid another validation error
					if len(poolOpts.Members) == 0 {
						log.Printf("⚠️  Removing empty pool '%s'", ob.Tag)
						removedPoolTags = append(removedPoolTags, ob.Tag)
						continue // skip adding this empty pool
					}
				}
			}
			newOutbounds = append(newOutbounds, ob)
		}
		opts.Outbounds = newOutbounds
		if len(opts.Inbounds) > 0 {
			validDedicated := make(map[string]struct{})
			for _, outboundOptions := range opts.Outbounds {
				if outboundOptions.Tag != pool.Tag {
					continue
				}
				if poolOptions, ok := outboundOptions.Options.(*pool.Options); ok {
					for inboundTag := range poolOptions.DedicatedMembers {
						validDedicated[inboundTag] = struct{}{}
					}
				}
			}
			filtered := opts.Inbounds[:0]
			for _, inboundOptions := range opts.Inbounds {
				if strings.HasPrefix(inboundOptions.Tag, "in-node-") {
					if _, ok := validDedicated[inboundOptions.Tag]; !ok {
						continue
					}
				}
				filtered = append(filtered, inboundOptions)
			}
			opts.Inbounds = filtered
		}

		// Also remove any routes that pointed to the removed pools or the badTag
		if (len(removedPoolTags) > 0 || badTag != "") && opts.Route != nil {
			removedSet := make(map[string]bool)
			for _, t := range removedPoolTags {
				removedSet[t] = true
			}
			removedSet[badTag] = true

			var newRules []option.Rule
			for _, r := range opts.Route.Rules {
				// We expect DefaultRules in our builder
				if r.Type == C.RuleTypeDefault {
					outboundTarget := r.DefaultOptions.RuleAction.RouteOptions.Outbound
					if !removedSet[outboundTarget] {
						newRules = append(newRules, r)
					} else {
						// Remove this rule since it points to a deleted outbound
					}
				} else {
					newRules = append(newRules, r)
				}
			}
			opts.Route.Rules = newRules
		}
	}

	return nil, fmt.Errorf("create sing-box instance: too many invalid outbounds (exceeded %d retries)", maxRetries)
}

// gracefulSwitch swaps the current box with a new one.
func (m *Manager) gracefulSwitch(newBox *box.Box) error {
	if newBox == nil {
		return errors.New("new box is nil")
	}

	m.mu.Lock()
	old := m.currentBox
	m.currentBox = newBox
	drainTimeout := m.drainTimeout
	m.mu.Unlock()

	if old != nil {
		go m.drainOldBox(old, drainTimeout)
	}

	m.logger.Infof("switched to new instance, draining old for %s", drainTimeout)
	return nil
}

// drainOldBox waits for drain timeout then closes the old box.
func (m *Manager) drainOldBox(oldBox *box.Box, timeout time.Duration) {
	if oldBox == nil {
		return
	}
	if timeout > 0 {
		time.Sleep(timeout)
	}
	if err := oldBox.Close(); err != nil {
		m.logger.Errorf("failed to close old instance: %v", err)
		return
	}
	m.logger.Infof("old instance closed after %s drain", timeout)
}

// removeFromSlice removes an element from a string slice.
func removeFromSlice(slice []string, element string) []string {
	result := make([]string, 0, len(slice))
	for _, s := range slice {
		if s != element {
			result = append(result, s)
		}
	}
	return result
}

// waitForHealthCheck polls until enough nodes are available or timeout.
func (m *Manager) waitForHealthCheck(ctx context.Context, timeout time.Duration) error {
	if m.monitorMgr == nil || m.minAvailableNodes <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		timeout = defaultHealthCheckTimeout
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(healthCheckPollInterval)
	defer ticker.Stop()

	for {
		available, total := m.availableNodeCount()
		if available >= m.minAvailableNodes {
			m.logger.Infof("health check passed: %d/%d nodes available", available, total)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout: %d/%d nodes available (need >= %d)", available, total, m.minAvailableNodes)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// availableNodeCount returns (available, total) node counts.
func (m *Manager) availableNodeCount() (int, int) {
	if m.monitorMgr == nil {
		return 0, 0
	}
	snapshots := m.monitorMgr.Snapshot()
	total := len(snapshots)
	available := 0
	for _, snap := range snapshots {
		if snap.InitialCheckDone && snap.Available {
			available++
		}
	}
	return available, total
}

// ensureMonitor initializes monitor manager and server if needed.
func (m *Manager) ensureMonitor() error {
	m.mu.Lock()
	if m.monitorMgr != nil {
		m.mu.Unlock()
		return nil
	}

	monitorMgr, err := monitor.NewManager(m.monitorCfg)
	if err != nil {
		m.mu.Unlock()
		return fmt.Errorf("init monitor manager: %w", err)
	}
	monitorMgr.SetLogger(monitorLoggerAdapter{logger: m.logger})
	m.monitorMgr = monitorMgr

	if m.monitorCfg.Enabled {
		if m.monitorServer == nil {
			m.monitorServer = monitor.NewServer(m.monitorCfg, monitorMgr, log.Default())
		}
		// Set config early so WebUI has data before Start() completes
		if m.monitorServer != nil && m.cfg != nil {
			m.monitorServer.SetConfig(m.cfg.Clone())
		}
		// Set NodeManager for config CRUD endpoints
		if m.monitorServer != nil {
			m.monitorServer.SetNodeManager(m)
		}
		// Note: StartPeriodicHealthCheck is called after nodes are registered in Start()
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) startMonitorServer(ctx context.Context) error {
	m.mu.RLock()
	server := m.monitorServer
	m.mu.RUnlock()
	if server == nil {
		return nil
	}
	if err := server.Start(ctx); err != nil {
		return fmt.Errorf("start monitor server: %w", err)
	}
	return nil
}

// applyConfigSettings extracts runtime settings from config.
func (m *Manager) applyConfigSettings(cfg *config.Config) {
	if cfg == nil {
		return
	}
	if cfg.SubscriptionRefresh.DrainTimeout > 0 {
		m.drainTimeout = cfg.SubscriptionRefresh.DrainTimeout
	} else if m.drainTimeout == 0 {
		m.drainTimeout = defaultDrainTimeout
	}
	m.minAvailableNodes = cfg.SubscriptionRefresh.MinAvailableNodes
}

func (m *Manager) startPeriodicHealthChecks() {
	m.mu.Lock()
	var monitorToStart *monitor.Manager
	if m.monitorMgr != nil && !m.healthCheckStarted {
		monitorToStart = m.monitorMgr
		m.healthCheckStarted = true
	}
	m.mu.Unlock()
	if monitorToStart != nil {
		monitorToStart.StartPeriodicHealthCheck(periodicHealthInterval, periodicHealthTimeout)
	}
}

// adoptConfigLocked installs an owned clone. The caller must hold m.mu.
func (m *Manager) adoptConfigLocked(cfg *config.Config, incrementRevision bool) *config.Config {
	ownedCfg := cfg.Clone()
	m.cfg = ownedCfg
	m.applyConfigSettings(ownedCfg)
	if incrementRevision {
		m.revision++
	}
	return ownedCfg.Clone()
}

func (m *Manager) adoptConfig(cfg *config.Config, incrementRevision bool) (*config.Config, *monitor.Server) {
	m.mu.Lock()
	snapshot := m.adoptConfigLocked(cfg, incrementRevision)
	server := m.monitorServer
	m.mu.Unlock()
	return snapshot, server
}

func (m *Manager) applyLiveProbeSettings(cfg *config.Config) {
	if cfg == nil || m.monitorMgr == nil {
		return
	}
	m.monitorMgr.SetProbeTarget(cfg.Management.ProbeTarget, cfg.SkipCertVerify)
	m.monitorMgr.SetProbeConcurrency(cfg.ProbeConcurrencyOrDefault())
}

// defaultLogger is the fallback logger using standard log.
type defaultLogger struct{}

func (defaultLogger) Infof(format string, args ...any) {
	log.Printf("[boxmgr] "+format, args...)
}

func (defaultLogger) Warnf(format string, args ...any) {
	log.Printf("[boxmgr] WARN: "+format, args...)
}

func (defaultLogger) Errorf(format string, args ...any) {
	log.Printf("[boxmgr] ERROR: "+format, args...)
}

// monitorLoggerAdapter adapts Logger to monitor.Logger interface.
type monitorLoggerAdapter struct {
	logger Logger
}

func (a monitorLoggerAdapter) Info(args ...any) {
	if a.logger != nil {
		a.logger.Infof("%s", fmt.Sprint(args...))
	}
}

func (a monitorLoggerAdapter) Warn(args ...any) {
	if a.logger != nil {
		a.logger.Warnf("%s", fmt.Sprint(args...))
	}
}

// --- NodeManager interface implementation ---

var errConfigUnavailable = errors.New("config is not initialized")

// ErrConfigRevisionConflict indicates that a transaction was prepared from a
// stale configuration snapshot.
var ErrConfigRevisionConflict = errors.New("config revision conflict")

var errManagerClosed = errors.New("box manager is closed")

func (m *Manager) ensureNotClosed() error {
	m.mu.RLock()
	closed := m.closed
	m.mu.RUnlock()
	if closed {
		return errManagerClosed
	}
	return nil
}

// ListConfigNodes returns a copy of all configured nodes.
func (m *Manager) ListConfigNodes(ctx context.Context) ([]config.NodeConfig, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.cfg == nil {
		return nil, errConfigUnavailable
	}
	return cloneNodes(m.cfg.Nodes), nil
}

// CreateNode adds a new node to the config and saves it.
func (m *Manager) CreateNode(ctx context.Context, node config.NodeConfig) (config.NodeConfig, error) {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	if err := m.ensureNotClosed(); err != nil {
		return config.NodeConfig{}, err
	}

	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return config.NodeConfig{}, err
		}
	}

	m.mu.RLock()
	candidate := m.cfg.Clone()
	m.mu.RUnlock()
	if candidate == nil {
		return config.NodeConfig{}, errConfigUnavailable
	}

	normalized, err := prepareNode(candidate, node, -1)
	if err != nil {
		return config.NodeConfig{}, err
	}

	// A node explicitly added through the WebUI belongs to user configuration.
	// Persist it inline so rewriting the subscription cache cannot remove it.
	normalized.Source = config.NodeSourceInline

	candidate.Nodes = append(candidate.Nodes, normalized)
	if _, err := candidate.SaveNodesTransaction(nil); err != nil {
		return config.NodeConfig{}, fmt.Errorf("save config: %w", err)
	}
	committedCfg, monitorServer := m.adoptConfig(candidate, true)
	if monitorServer != nil {
		monitorServer.SetConfig(committedCfg)
	}
	return normalized, nil
}

// UpdateNode updates an existing node by name and saves the config.
func (m *Manager) UpdateNode(ctx context.Context, name string, node config.NodeConfig) (config.NodeConfig, error) {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	if err := m.ensureNotClosed(); err != nil {
		return config.NodeConfig{}, err
	}

	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return config.NodeConfig{}, err
		}
	}

	name = strings.TrimSpace(name)
	m.mu.RLock()
	candidate := m.cfg.Clone()
	m.mu.RUnlock()
	if candidate == nil {
		return config.NodeConfig{}, errConfigUnavailable
	}

	idx := nodeIndexByIdentifier(candidate, name)
	if idx == -1 {
		return config.NodeConfig{}, monitor.ErrNodeNotFound
	}

	normalized, err := prepareNode(candidate, node, idx)
	if err != nil {
		return config.NodeConfig{}, err
	}

	// Preserve the original source
	normalized.Source = candidate.Nodes[idx].Source

	prev := candidate.Nodes[idx]
	candidate.Nodes[idx] = normalized
	var removedAuth []config.NodeConfig
	if prev.NodeKey() != normalized.NodeKey() {
		removedAuth = []config.NodeConfig{prev}
	}
	if _, err := candidate.SaveNodesTransaction(removedAuth); err != nil {
		return config.NodeConfig{}, fmt.Errorf("save config: %w", err)
	}
	committedCfg, monitorServer := m.adoptConfig(candidate, true)
	if monitorServer != nil {
		monitorServer.SetConfig(committedCfg)
	}
	return normalized, nil
}

// DeleteNode removes a node by name and saves the config.
func (m *Manager) DeleteNode(ctx context.Context, name string) error {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	if err := m.ensureNotClosed(); err != nil {
		return err
	}

	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	name = strings.TrimSpace(name)
	m.mu.RLock()
	candidate := m.cfg.Clone()
	m.mu.RUnlock()
	if candidate == nil {
		return errConfigUnavailable
	}

	idx := nodeIndexByIdentifier(candidate, name)
	if idx == -1 {
		return monitor.ErrNodeNotFound
	}

	deleted := candidate.Nodes[idx]
	candidate.Nodes = append(candidate.Nodes[:idx], candidate.Nodes[idx+1:]...)
	if _, err := candidate.SaveNodesTransaction([]config.NodeConfig{deleted}); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	committedCfg, monitorServer := m.adoptConfig(candidate, true)
	if monitorServer != nil {
		monitorServer.SetConfig(committedCfg)
	}
	return nil
}

// TriggerReload reloads the sing-box instance with current config.
func (m *Manager) TriggerReload(ctx context.Context) error {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	if err := m.ensureNotClosed(); err != nil {
		return err
	}

	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	m.mu.RLock()
	cfgCopy := m.cfg.Clone()
	var portMap map[string]uint16
	if m.cfg != nil {
		portMap = m.cfg.BuildPortMap()
	}
	m.mu.RUnlock()

	if cfgCopy == nil {
		return errConfigUnavailable
	}
	if err := cfgCopy.NormalizeWithPortMap(portMap); err != nil {
		return fmt.Errorf("normalize config with port map: %w", err)
	}
	return m.reloadLocked(ctx, cfgCopy)
}

// ReloadWithPortMap gracefully switches to a new configuration, preserving port assignments.
func (m *Manager) ReloadWithPortMap(newCfg *config.Config, portMap map[string]uint16) error {
	if newCfg == nil {
		return errors.New("new config is nil")
	}
	ownedCfg := newCfg.Clone()
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()

	// Apply persisted/runtime mappings and validate even when the previous mode
	// did not expose dedicated ports.
	if err := ownedCfg.NormalizeWithPortMap(portMap); err != nil {
		return fmt.Errorf("normalize config with port map: %w", err)
	}

	return m.reloadLocked(context.Background(), ownedCfg)
}

// CommitConfig persists and reloads a candidate configuration as one
// optimistic transaction. The persistence callback receives its own clone and
// may return a rollback action for any partial or completed disk mutation.
func (m *Manager) CommitConfig(
	ctx context.Context,
	expectedRevision uint64,
	candidate *config.Config,
	persist func(*config.Config) (rollback func() error, err error),
) error {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	if err := m.ensureNotClosed(); err != nil {
		return err
	}

	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if candidate == nil {
		return errors.New("candidate config is nil")
	}

	m.mu.RLock()
	currentRevision := m.revision
	var portMap map[string]uint16
	if m.cfg != nil {
		portMap = m.cfg.BuildPortMap()
	}
	m.mu.RUnlock()
	if expectedRevision != currentRevision {
		return fmt.Errorf("%w: expected %d, current %d", ErrConfigRevisionConflict, expectedRevision, currentRevision)
	}

	ownedCfg := candidate.Clone()
	if err := ownedCfg.NormalizeWithPortMap(portMap); err != nil {
		return fmt.Errorf("normalize candidate config: %w", err)
	}

	var rollback func() error
	if persist != nil {
		var err error
		rollback, err = persist(ownedCfg.Clone())
		if err != nil {
			return rollbackConfigPersistence(rollback, fmt.Errorf("persist candidate config: %w", err))
		}
	}
	if err := ctx.Err(); err != nil {
		return rollbackConfigPersistence(rollback, err)
	}
	if err := m.reloadLocked(ctx, ownedCfg); err != nil {
		return rollbackConfigPersistence(rollback, err)
	}
	return nil
}

func rollbackConfigPersistence(rollback func() error, cause error) error {
	if rollback == nil {
		return cause
	}
	if err := rollback(); err != nil {
		return fmt.Errorf("%w; persistence rollback failed: %v", cause, err)
	}
	return cause
}

// CurrentPortMap returns the current port mapping from the active configuration.
func (m *Manager) CurrentPortMap() map[string]uint16 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cfg == nil {
		return nil
	}
	return m.cfg.BuildPortMap()
}

// --- Helper functions ---

// portBindErrorRegex matches "listen tcp4 0.0.0.0:24282: bind: address already in use"
var portBindErrorRegex = regexp.MustCompile(`listen tcp[46]? [^:]+:(\d+): bind: address already in use`)

// extractPortFromBindError extracts the port number from a bind error message.
func extractPortFromBindError(err error) uint16 {
	if err == nil {
		return 0
	}
	matches := portBindErrorRegex.FindStringSubmatch(err.Error())
	if len(matches) < 2 {
		return 0
	}
	var port int
	fmt.Sscanf(matches[1], "%d", &port)
	if port > 0 && port <= 65535 {
		return uint16(port)
	}
	return 0
}

// reassignConflictingPort finds the node using the conflicting port and assigns a new port.
func reassignConflictingPort(cfg *config.Config, conflictPort uint16) bool {
	// Build set of used ports
	usedPorts := make(map[uint16]bool)
	if cfg.Mode == "hybrid" {
		usedPorts[cfg.Listener.Port] = true
	}
	for _, node := range cfg.Nodes {
		usedPorts[node.Port] = true
	}

	// Find and reassign the conflicting node
	for idx := range cfg.Nodes {
		if cfg.Nodes[idx].Port == conflictPort {
			// Use an int cursor so 65535 + 1 cannot wrap to port zero.
			newPort := int(conflictPort) + 1
			address := cfg.MultiPort.Address
			if address == "" {
				address = "0.0.0.0"
			}
			for newPort <= 65535 && (usedPorts[uint16(newPort)] || !config.IsPortAvailable(address, uint16(newPort))) {
				newPort++
			}
			if newPort > 65535 {
				log.Printf("❌ No available port found for node %q", cfg.Nodes[idx].Name)
				return false
			}
			log.Printf("⚠️  Port %d in use, reassigning node %q to port %d", conflictPort, cfg.Nodes[idx].Name, newPort)
			cfg.Nodes[idx].Port = uint16(newPort)
			return true
		}
	}
	return false
}

func cloneNodes(nodes []config.NodeConfig) []config.NodeConfig {
	if len(nodes) == 0 {
		return []config.NodeConfig{} // Return empty slice, not nil, for proper JSON serialization
	}
	out := make([]config.NodeConfig, len(nodes))
	copy(out, nodes)
	return out
}

// copyConfigLocked snapshots manager-owned config while the caller holds m.mu.
func (m *Manager) copyConfigLocked() *config.Config {
	return m.cfg.Clone()
}

func nodeIndexByName(cfg *config.Config, name string, skipIndex int) int {
	for idx, node := range cfg.Nodes {
		if idx != skipIndex && node.Name == name {
			return idx
		}
	}
	return -1
}

func nodeIndexByIdentifier(cfg *config.Config, identifier string) int {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return -1
	}
	for idx := range cfg.Nodes {
		if cfg.Nodes[idx].NodeKey() == identifier {
			return idx
		}
	}
	return nodeIndexByName(cfg, identifier, -1)
}

func portInUse(cfg *config.Config, port uint16, skipIndex int) bool {
	if port == 0 {
		return false
	}
	for idx, node := range cfg.Nodes {
		if idx == skipIndex {
			continue
		}
		if node.Port == port {
			return true
		}
	}
	return false
}

func prepareNode(cfg *config.Config, node config.NodeConfig, currentIndex int) (config.NodeConfig, error) {
	node.Name = strings.TrimSpace(node.Name)
	node.URI = strings.TrimSpace(node.URI)

	if node.URI == "" {
		return config.NodeConfig{}, fmt.Errorf("%w: URI 不能为空", monitor.ErrInvalidNode)
	}
	if len(node.URI) > config.MaxSubscriptionNodeURIBytes || !config.IsProxyURI(node.URI) {
		return config.NodeConfig{}, fmt.Errorf("%w: URI 格式无效或过长", monitor.ErrInvalidNode)
	}

	// Extract name from URI if not provided
	if node.Name == "" {
		if currentIndex >= 0 {
			node.Name = cfg.Nodes[currentIndex].Name
		} else {
			node.Name = config.ExtractNodeName(node.URI)
		}
		// Fallback to auto-generated name
		if node.Name == "" {
			node.Name = fmt.Sprintf("node-%d", len(cfg.Nodes)+1)
		}
	}
	if len(node.Name) > config.MaxSubscriptionNodeNameBytes || containsNodeNameControl(node.Name) {
		return config.NodeConfig{}, fmt.Errorf("%w: 节点名称无效或过长", monitor.ErrInvalidNode)
	}
	if (node.Username == "") != (node.Password == "") {
		return config.NodeConfig{}, fmt.Errorf("%w: 用户名和密码必须同时设置或同时留空", monitor.ErrInvalidNode)
	}

	// Check for name conflict (excluding current node when updating)
	if nodeIndexByName(cfg, node.Name, currentIndex) != -1 {
		return config.NodeConfig{}, fmt.Errorf("%w: 节点 %s 已存在", monitor.ErrNodeConflict, node.Name)
	}
	for index := range cfg.Nodes {
		if index != currentIndex && cfg.Nodes[index].NodeKey() == node.NodeKey() {
			return config.NodeConfig{}, fmt.Errorf("%w: 相同的代理节点已存在", monitor.ErrNodeConflict)
		}
	}

	// Handle dedicated-port specifics in both multi-port and hybrid modes.
	if cfg.Mode == "multi-port" || cfg.Mode == "hybrid" {
		if node.Port == 0 && currentIndex >= 0 {
			node.Port = cfg.Nodes[currentIndex].Port
		}
		if node.Port == 0 {
			candidateCfg := cfg.Clone()
			candidateCfg.Nodes = append(candidateCfg.Nodes, node)
			if err := candidateCfg.NormalizeWithPortMap(cfg.BuildPortMap()); err != nil {
				return config.NodeConfig{}, fmt.Errorf("%w: 分配稳定端口失败: %v", monitor.ErrInvalidNode, err)
			}
			node.Port = candidateCfg.Nodes[len(candidateCfg.Nodes)-1].Port
		} else if portInUse(cfg, node.Port, currentIndex) || (cfg.Mode == "hybrid" && node.Port == cfg.Listener.Port) {
			return config.NodeConfig{}, fmt.Errorf("%w: 端口 %d 已被占用", monitor.ErrNodeConflict, node.Port)
		}
		if node.Username == "" {
			node.Username = cfg.MultiPort.Username
			node.Password = cfg.MultiPort.Password
		}
	}

	return node, nil
}

func containsNodeNameControl(name string) bool {
	for _, character := range name {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}
