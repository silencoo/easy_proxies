package boxmgr

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"easy_proxies/internal/builder"
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
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

// Ensure Manager implements monitor.NodeManager.
var _ monitor.NodeManager = (*Manager)(nil)

const (
	defaultDrainTimeout       = 10 * time.Second
	defaultHealthCheckTimeout = 30 * time.Second
	healthCheckPollInterval   = 500 * time.Millisecond
	periodicHealthInterval    = 5 * time.Minute
	periodicHealthTimeout     = 10 * time.Second
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

	currentBox     *box.Box
	runtimeCtx     context.Context
	runtimeOptions option.Options
	monitorMgr     *monitor.Manager
	monitorServer  *monitor.Server
	geoRouter      *geoip.Router
	cfg            *config.Config
	monitorCfg     monitor.Config

	drainTimeout      time.Duration
	minAvailableNodes int
	logger            Logger

	baseCtx            context.Context
	healthCheckStarted bool
}

type builtInstance struct {
	box     *box.Box
	ctx     context.Context
	options option.Options
}

// New creates a BoxManager with the given config.
func New(cfg *config.Config, monitorCfg monitor.Config, opts ...Option) *Manager {
	m := &Manager{
		cfg:        cfg,
		monitorCfg: monitorCfg,
	}
	m.applyConfigSettings(cfg)
	for _, opt := range opts {
		opt(m)
	}
	if m.logger == nil {
		m.logger = defaultLogger{}
	}
	if m.drainTimeout <= 0 {
		m.drainTimeout = defaultDrainTimeout
	}
	return m
}

// Start creates and starts the initial sing-box instance.
func (m *Manager) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := m.ensureMonitor(ctx); err != nil {
		return err
	}

	m.mu.Lock()
	if m.cfg == nil {
		m.mu.Unlock()
		return errors.New("box manager requires config")
	}
	if m.currentBox != nil {
		m.mu.Unlock()
		return errors.New("sing-box already running")
	}
	m.applyConfigSettings(m.cfg)
	m.baseCtx = ctx
	cfg := m.cfg
	m.mu.Unlock()

	// Try to start, with automatic port conflict resolution
	var built *builtInstance
	maxRetries := 10
	for retry := 0; retry < maxRetries; retry++ {
		var err error
		built, err = m.createBox(ctx, cfg)
		if err != nil {
			return err
		}
		if err = built.box.Start(); err != nil {
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
		break // Success
	}

	m.mu.Lock()
	m.currentBox = built.box
	m.runtimeCtx = built.ctx
	m.runtimeOptions = built.options
	m.mu.Unlock()
	if err := cfg.PersistPortMap(); err != nil {
		m.logger.Warnf("failed to persist dedicated port map after start: %v", err)
	}

	// Start periodic health check after nodes are registered
	m.mu.Lock()
	if m.monitorMgr != nil && !m.healthCheckStarted {
		m.monitorMgr.StartPeriodicHealthCheck(periodicHealthInterval, periodicHealthTimeout)
		m.healthCheckStarted = true
	}
	m.mu.Unlock()

	// Wait for initial health check if min nodes configured
	if cfg.SubscriptionRefresh.MinAvailableNodes > 0 {
		timeout := cfg.SubscriptionRefresh.HealthCheckTimeout
		if timeout <= 0 {
			timeout = defaultHealthCheckTimeout
		}
		if err := m.waitForHealthCheck(timeout); err != nil {
			m.logger.Warnf("initial health check warning: %v", err)
			// Don't fail startup, just warn
		}
	}

	m.logger.Infof("sing-box instance started with %d nodes", len(cfg.Nodes))

	// Start GeoIP router if enabled
	if cfg.GeoIP.Enabled {
		m.startGeoIPRouter(ctx, cfg)
	}

	return nil
}

// Reload applies node-only changes through sing-box's runtime managers. Global
// topology changes still use the validated full-instance handoff below.
func (m *Manager) Reload(newCfg *config.Config) error {
	if newCfg == nil {
		return errors.New("new config is nil")
	}
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()

	m.mu.RLock()
	if m.currentBox == nil {
		m.mu.RUnlock()
		return errors.New("manager not started")
	}
	ctx := m.baseCtx
	oldBox := m.currentBox
	oldCfg := m.cfg
	runtimeCtx := m.runtimeCtx
	oldOptions := m.runtimeOptions
	m.mu.RUnlock()

	if ctx == nil {
		ctx = context.Background()
	}

	m.logger.Infof("reloading with %d nodes", len(newCfg.Nodes))
	if canReloadNodesInPlace(oldCfg, newCfg) && runtimeCtx != nil {
		if err := m.reloadNodesInPlace(ctx, runtimeCtx, oldBox, oldCfg, oldOptions, newCfg); !errors.Is(err, errRuntimeReloadUnsupported) {
			return err
		}
		m.logger.Warnf("node-level reload is not available for this change; using full validated handoff")
	}

	// box.New performs outbound/inbound validation but does not bind listeners.
	// Do this first so malformed subscriptions cannot interrupt live traffic.
	built, err := m.createBox(ctx, newCfg)
	if err != nil {
		return fmt.Errorf("create replacement box: %w", err)
	}

	// Stop auxiliary listeners, then release the old sing-box ports immediately
	// before starting the already validated replacement.
	m.mu.Lock()
	if m.geoRouter != nil {
		m.geoRouter.Stop()
		m.geoRouter = nil
	}
	m.mu.Unlock()
	if err := oldBox.Close(); err != nil {
		m.logger.Warnf("error closing old instance: %v", err)
	}
	m.mu.Lock()
	m.currentBox = nil
	m.mu.Unlock()

	if err := built.box.Start(); err != nil {
		_ = built.box.Close()
		rollbackErr := m.rollbackToOldConfig(ctx, oldCfg)
		if rollbackErr != nil {
			return fmt.Errorf("start replacement box: %w; rollback failed: %v", err, rollbackErr)
		}
		return fmt.Errorf("start replacement box: %w (old configuration restored)", err)
	}

	m.applyConfigSettings(newCfg)

	m.mu.Lock()
	m.currentBox = built.box
	m.runtimeCtx = built.ctx
	m.runtimeOptions = built.options
	m.cfg = newCfg
	m.mu.Unlock()

	// Sync config to monitor server so future WebUI settings changes target the current config pointer
	if m.monitorServer != nil {
		m.monitorServer.SetConfig(m.cfg)
	}
	if m.monitorMgr != nil {
		m.monitorMgr.RetainNodeURIs(nodeURISet(newCfg.Nodes))
	}

	// Validate the configured minimum before committing the reload. Active probe
	// failures also update the shared routing blacklist.
	if m.monitorMgr != nil {
		healthTimeout := newCfg.SubscriptionRefresh.HealthCheckTimeout
		if healthTimeout <= 0 {
			healthTimeout = defaultHealthCheckTimeout
		}
		if _, probeConfigured := m.monitorMgr.DestinationForProbe(); probeConfigured && m.minAvailableNodes > 0 {
			m.monitorMgr.ProbeAllNow(healthTimeout)
			available, total := m.availableNodeCount()
			if available < m.minAvailableNodes {
				_ = built.box.Close()
				m.mu.Lock()
				m.currentBox = nil
				m.mu.Unlock()
				healthErr := fmt.Errorf("health check rejected replacement: %d/%d nodes available (need >= %d)", available, total, m.minAvailableNodes)
				rollbackErr := m.rollbackToOldConfig(ctx, oldCfg)
				if rollbackErr != nil {
					return fmt.Errorf("%w; rollback failed: %v", healthErr, rollbackErr)
				}
				return fmt.Errorf("%w (old configuration restored)", healthErr)
			}
		} else {
			go m.monitorMgr.ProbeAllNow(periodicHealthTimeout)
		}
	}
	if err := newCfg.PersistPortMap(); err != nil {
		m.logger.Warnf("failed to persist dedicated port map after reload: %v", err)
	}

	m.logger.Infof("reload completed successfully with %d nodes", len(newCfg.Nodes))

	// Restart GeoIP router with new pools
	if newCfg.GeoIP.Enabled {
		m.startGeoIPRouter(ctx, newCfg)
	} else {
		m.mu.Lock()
		if m.geoRouter != nil {
			m.geoRouter.Stop()
			m.geoRouter = nil
		}
		m.mu.Unlock()
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
		oldCfg.SkipCertVerify != newCfg.SkipCertVerify {
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
	for _, tag := range addedBaseTags {
		if err := createRuntimeOutbound(runtimeCtx, instance, desiredBase[tag]); err != nil {
			removeRuntimeOutbounds(instance, createdBaseTags)
			return fmt.Errorf("create candidate outbound %s: %w", tag, err)
		}
		createdBaseTags = append(createdBaseTags, tag)
	}

	if err := m.preflightCandidateSet(ctx, instance, sortedMapKeys(desiredBase), newCfg); err != nil {
		removeRuntimeOutbounds(instance, createdBaseTags)
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
			removeRuntimeOutbounds(instance, createdBaseTags)
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
		removeRuntimeOutbounds(instance, createdBaseTags)
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
			if removeErr := instance.Inbound().Remove(tag); removeErr != nil {
				return failRuntimeSwitch(fmt.Errorf("replace inbound %s: %v (remove old: %w)", tag, err, removeErr))
			}
			if retryErr := createRuntimeInbound(runtimeCtx, instance, desired); retryErr != nil {
				_ = createRuntimeInbound(runtimeCtx, instance, previous)
				return failRuntimeSwitch(fmt.Errorf("replace inbound %s after releasing listener: %w", tag, retryErr))
			}
		}
		inboundChanges = append(inboundChanges, inboundChange{tag: tag, previous: previous, existed: existed})
	}
	for _, tag := range mapDifferenceKeys(oldInbounds, desiredInbounds) {
		previous := oldInbounds[tag]
		if err := instance.Inbound().Remove(tag); err != nil {
			return failRuntimeSwitch(fmt.Errorf("remove inbound %s: %w", tag, err))
		}
		inboundChanges = append(inboundChanges, inboundChange{tag: tag, previous: previous, existed: true})
	}

	for _, tag := range mapDifferenceKeys(oldPools, desiredPools) {
		previous := oldPools[tag]
		if err := instance.Outbound().Remove(tag); err != nil {
			return failRuntimeSwitch(fmt.Errorf("remove pool %s: %w", tag, err))
		}
		poolChanges = append(poolChanges, outboundChange{tag: tag, previous: previous, existed: true})
	}

	draining := make(map[string]adapter.Outbound, len(removedBaseTags))
	for _, tag := range removedBaseTags {
		if outbound, ok := instance.Outbound().Outbound(tag); ok {
			draining[tag] = outbound
		}
	}

	m.applyConfigSettings(newCfg)
	m.mu.Lock()
	m.cfg = newCfg
	m.runtimeOptions = desiredOptions
	drainTimeout := m.drainTimeout
	m.mu.Unlock()
	if m.monitorServer != nil {
		m.monitorServer.SetConfig(newCfg)
	}
	if m.monitorMgr != nil {
		m.monitorMgr.RetainNodeURIs(nodeURISet(newCfg.Nodes))
	}
	if err := newCfg.PersistPortMap(); err != nil {
		m.logger.Warnf("failed to persist dedicated port map after reload: %v", err)
	}

	for tag, outbound := range draining {
		go m.drainRuntimeOutbound(instance, tag, outbound, drainTimeout)
	}
	if newCfg.GeoIP.Enabled {
		m.startGeoIPRouter(ctx, newCfg)
	} else {
		m.mu.Lock()
		if m.geoRouter != nil {
			m.geoRouter.Stop()
			m.geoRouter = nil
		}
		m.mu.Unlock()
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
	destination, configured := m.monitorMgr.DestinationForProbe()
	if !configured {
		return nil
	}
	timeout := cfg.SubscriptionRefresh.HealthCheckTimeout
	if timeout <= 0 {
		timeout = defaultHealthCheckTimeout
	}

	workerCount := runtime.NumCPU() * 2
	if workerCount < 8 {
		workerCount = 8
	}
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
				err := probeOutbound(probeCtx, outbound, destination)
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

func probeOutbound(ctx context.Context, outbound adapter.Outbound, destination M.Socksaddr) error {
	connection, err := outbound.DialContext(ctx, N.NetworkTCP, destination)
	if err != nil {
		return err
	}
	defer connection.Close()
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		_ = connection.SetDeadline(deadline)
	}
	request := fmt.Sprintf("GET /generate_204 HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", destination.AddrString())
	if _, err := connection.Write([]byte(request)); err != nil {
		return err
	}
	_, err = bufio.NewReader(connection).ReadByte()
	return err
}

func (m *Manager) drainRuntimeOutbound(instance *box.Box, tag string, expected adapter.Outbound, timeout time.Duration) {
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		<-timer.C
	}
	current, ok := instance.Outbound().Outbound(tag)
	if !ok || current != expected {
		return
	}
	if err := instance.Outbound().Remove(tag); err != nil {
		m.logger.Warnf("failed to retire drained outbound %s: %v", tag, err)
		return
	}
	m.logger.Infof("retired outbound %s after %s drain", tag, timeout)
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
	m.applyConfigSettings(oldCfg)
	m.mu.Lock()
	m.currentBox = built.box
	m.runtimeCtx = built.ctx
	m.runtimeOptions = built.options
	m.cfg = oldCfg
	m.mu.Unlock()
	// Sync config pointer to monitor server after rollback
	if m.monitorServer != nil {
		m.monitorServer.SetConfig(m.cfg)
	}
	if m.monitorMgr != nil {
		m.monitorMgr.RetainNodeURIs(nodeURISet(oldCfg.Nodes))
	}
	if oldCfg.GeoIP.Enabled {
		m.startGeoIPRouter(ctx, oldCfg)
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
	m.mu.Lock()
	defer m.mu.Unlock()

	var err error
	if m.currentBox != nil {
		err = m.currentBox.Close()
		m.currentBox = nil
	}
	m.runtimeCtx = nil
	m.runtimeOptions = option.Options{}
	if m.monitorServer != nil {
		m.monitorServer.Shutdown(context.Background())
		m.monitorServer = nil
	}
	if m.monitorMgr != nil {
		m.monitorMgr.Stop()
		m.monitorMgr = nil
		m.healthCheckStarted = false
	}
	if m.geoRouter != nil {
		m.geoRouter.Stop()
		m.geoRouter = nil
	}
	m.baseCtx = nil
	return err
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

// startGeoIPRouter starts the GeoIP region-routing HTTP proxy server.
func (m *Manager) startGeoIPRouter(ctx context.Context, cfg *config.Config) {
	// Stop existing router if any
	m.mu.Lock()
	if m.geoRouter != nil {
		m.geoRouter.Stop()
		m.geoRouter = nil
	}
	m.mu.Unlock()

	geoipPort := cfg.GeoIP.Port
	if geoipPort == 0 {
		geoipPort = 1221 // Default GeoIP router port
	}
	// Avoid conflict with the pool listener port
	if geoipPort == cfg.Listener.Port {
		geoipPort = 1221
		if geoipPort == cfg.Listener.Port {
			geoipPort = cfg.Listener.Port + 1
		}
		log.Printf("⚠️  GeoIP port conflicts with listener port %d, using %d instead", cfg.Listener.Port, geoipPort)
	}
	geoipListen := cfg.GeoIP.Listen
	if geoipListen == "" {
		geoipListen = cfg.Listener.Address
	}

	routerCfg := geoip.RouterConfig{
		Listen:   geoipListen,
		Port:     geoipPort,
		Username: cfg.Listener.Username,
		Password: cfg.Listener.Password,
	}

	router := geoip.NewRouter(routerCfg, nil)

	// Register region pool dialers
	for _, region := range geoip.AllRegions() {
		poolTag := fmt.Sprintf("pool-%s", region)
		if dialer, ok := pool.GetDialer(poolTag); ok {
			router.SetPool(region, dialer)
			log.Printf("   GeoIP: registered pool %s for region /%s", poolTag, region)
		}
	}

	// Register global pool dialer (for requests without region path)
	if dialer, ok := pool.GetDialer(pool.Tag); ok {
		router.SetGlobalPool(dialer)
	}

	if err := router.Start(ctx); err != nil {
		m.logger.Warnf("failed to start GeoIP router: %v", err)
		return
	}

	m.mu.Lock()
	m.geoRouter = router
	m.mu.Unlock()
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
	outboundErrRe := regexp.MustCompile(`initialize outbound\[(\d+)\]`)

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

		instance, err := box.New(box.Options{Context: boxCtx, Options: opts})
		if err == nil {
			if attempt > 0 {
				log.Printf("✅ sing-box instance created after removing %d invalid outbound(s)", attempt)
			}
			return &builtInstance{box: instance, ctx: boxCtx, options: opts}, nil
		}

		// Check if this is an outbound initialization error we can recover from
		matches := outboundErrRe.FindStringSubmatch(err.Error())
		if matches == nil {
			return nil, fmt.Errorf("create sing-box instance: %w", err)
		}

		idx, convErr := strconv.Atoi(matches[1])
		if convErr != nil || idx < 0 || idx >= len(opts.Outbounds) {
			return nil, fmt.Errorf("create sing-box instance: %w", err)
		}

		badTag := opts.Outbounds[idx].Tag
		log.Printf("⚠️  Outbound '%s' failed sing-box validation: %v (removing and retrying)", badTag, err)

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
func (m *Manager) waitForHealthCheck(timeout time.Duration) error {
	if m.monitorMgr == nil || m.minAvailableNodes <= 0 {
		return nil
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
		<-ticker.C
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
func (m *Manager) ensureMonitor(ctx context.Context) error {
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

	var serverToStart *monitor.Server
	if m.monitorCfg.Enabled {
		if m.monitorServer == nil {
			serverToStart = monitor.NewServer(m.monitorCfg, monitorMgr, log.Default())
			m.monitorServer = serverToStart
		}
		// Set config early so WebUI has data before Start() completes
		if m.monitorServer != nil && m.cfg != nil {
			m.monitorServer.SetConfig(m.cfg)
		}
		// Set NodeManager for config CRUD endpoints
		if m.monitorServer != nil {
			m.monitorServer.SetNodeManager(m)
		}
		// Note: StartPeriodicHealthCheck is called after nodes are registered in Start()
	}
	m.mu.Unlock()

	if serverToStart != nil {
		serverToStart.Start(ctx)
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
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return config.NodeConfig{}, err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cfg == nil {
		return config.NodeConfig{}, errConfigUnavailable
	}

	normalized, err := m.prepareNodeLocked(node, "")
	if err != nil {
		return config.NodeConfig{}, err
	}

	// Determine source: if subscriptions exist, new nodes go to nodes.txt (subscription source)
	// Otherwise, if nodes_file exists, use file source; else inline
	if len(m.cfg.Subscriptions) > 0 {
		normalized.Source = config.NodeSourceSubscription
	} else if m.cfg.NodesFile != "" {
		normalized.Source = config.NodeSourceFile
	} else {
		normalized.Source = config.NodeSourceInline
	}

	m.cfg.Nodes = append(m.cfg.Nodes, normalized)
	if err := m.cfg.Save(); err != nil {
		m.cfg.Nodes = m.cfg.Nodes[:len(m.cfg.Nodes)-1]
		return config.NodeConfig{}, fmt.Errorf("save config: %w", err)
	}
	return normalized, nil
}

// UpdateNode updates an existing node by name and saves the config.
func (m *Manager) UpdateNode(ctx context.Context, name string, node config.NodeConfig) (config.NodeConfig, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return config.NodeConfig{}, err
		}
	}

	name = strings.TrimSpace(name)
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cfg == nil {
		return config.NodeConfig{}, errConfigUnavailable
	}

	idx := m.nodeIndexLocked(name)
	if idx == -1 {
		return config.NodeConfig{}, monitor.ErrNodeNotFound
	}

	normalized, err := m.prepareNodeLocked(node, name)
	if err != nil {
		return config.NodeConfig{}, err
	}

	// Preserve the original source
	normalized.Source = m.cfg.Nodes[idx].Source

	prev := m.cfg.Nodes[idx]
	m.cfg.Nodes[idx] = normalized
	if err := m.cfg.Save(); err != nil {
		m.cfg.Nodes[idx] = prev
		return config.NodeConfig{}, fmt.Errorf("save config: %w", err)
	}
	if prev.NodeKey() != normalized.NodeKey() {
		if err := m.cfg.RemoveNodeAuthOverride(prev); err != nil {
			return config.NodeConfig{}, fmt.Errorf("remove previous node auth override: %w", err)
		}
	}
	return normalized, nil
}

// DeleteNode removes a node by name and saves the config.
func (m *Manager) DeleteNode(ctx context.Context, name string) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	name = strings.TrimSpace(name)
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cfg == nil {
		return errConfigUnavailable
	}

	idx := m.nodeIndexLocked(name)
	if idx == -1 {
		return monitor.ErrNodeNotFound
	}

	backup := cloneNodes(m.cfg.Nodes)
	deleted := m.cfg.Nodes[idx]
	m.cfg.Nodes = append(m.cfg.Nodes[:idx], m.cfg.Nodes[idx+1:]...)
	if err := m.cfg.Save(); err != nil {
		m.cfg.Nodes = backup
		return fmt.Errorf("save config: %w", err)
	}
	if err := m.cfg.RemoveNodeAuthOverride(deleted); err != nil {
		return fmt.Errorf("remove node auth override: %w", err)
	}
	return nil
}

// TriggerReload reloads the sing-box instance with current config.
func (m *Manager) TriggerReload(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	m.mu.RLock()
	cfgCopy := m.copyConfigLocked()
	portMap := m.cfg.BuildPortMap() // Preserve existing port assignments
	m.mu.RUnlock()

	if cfgCopy == nil {
		return errConfigUnavailable
	}
	return m.ReloadWithPortMap(cfgCopy, portMap)
}

// ReloadWithPortMap gracefully switches to a new configuration, preserving port assignments.
func (m *Manager) ReloadWithPortMap(newCfg *config.Config, portMap map[string]uint16) error {
	if newCfg == nil {
		return errors.New("new config is nil")
	}

	// Apply persisted/runtime mappings and validate even when the previous mode
	// did not expose dedicated ports.
	if err := newCfg.NormalizeWithPortMap(portMap); err != nil {
		return fmt.Errorf("normalize config with port map: %w", err)
	}

	return m.Reload(newCfg)
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
			// Find next available port
			newPort := conflictPort + 1
			address := cfg.MultiPort.Address
			if address == "" {
				address = "0.0.0.0"
			}
			for usedPorts[newPort] || !config.IsPortAvailable(address, newPort) {
				newPort++
				if newPort > 65535 {
					log.Printf("❌ No available port found for node %q", cfg.Nodes[idx].Name)
					return false
				}
			}
			log.Printf("⚠️  Port %d in use, reassigning node %q to port %d", conflictPort, cfg.Nodes[idx].Name, newPort)
			cfg.Nodes[idx].Port = newPort
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

func (m *Manager) copyConfigLocked() *config.Config {
	if m.cfg == nil {
		return nil
	}
	cloned := *m.cfg
	cloned.Nodes = cloneNodes(m.cfg.Nodes)
	// Clone Subscriptions slice to avoid shared backing array issues
	if len(m.cfg.Subscriptions) > 0 {
		cloned.Subscriptions = make([]string, len(m.cfg.Subscriptions))
		copy(cloned.Subscriptions, m.cfg.Subscriptions)
	}
	cloned.SetFilePath(m.cfg.FilePath())
	return &cloned
}

func (m *Manager) nodeIndexLocked(name string) int {
	for idx, node := range m.cfg.Nodes {
		if node.Name == name {
			return idx
		}
	}
	return -1
}

func (m *Manager) portInUseLocked(port uint16, currentName string) bool {
	if port == 0 {
		return false
	}
	for _, node := range m.cfg.Nodes {
		if node.Name == currentName {
			continue
		}
		if node.Port == port {
			return true
		}
	}
	return false
}

func (m *Manager) prepareNodeLocked(node config.NodeConfig, currentName string) (config.NodeConfig, error) {
	node.Name = strings.TrimSpace(node.Name)
	node.URI = strings.TrimSpace(node.URI)

	if node.URI == "" {
		return config.NodeConfig{}, fmt.Errorf("%w: URI 不能为空", monitor.ErrInvalidNode)
	}

	// Extract name from URI if not provided
	if node.Name == "" {
		if currentName != "" {
			node.Name = currentName
		} else {
			node.Name = config.ExtractNodeName(node.URI)
		}
		// Fallback to auto-generated name
		if node.Name == "" {
			node.Name = fmt.Sprintf("node-%d", len(m.cfg.Nodes)+1)
		}
	}

	// Check for name conflict (excluding current node when updating)
	if idx := m.nodeIndexLocked(node.Name); idx != -1 {
		if currentName == "" || m.cfg.Nodes[idx].Name != currentName {
			return config.NodeConfig{}, fmt.Errorf("%w: 节点 %s 已存在", monitor.ErrNodeConflict, node.Name)
		}
	}

	// Handle dedicated-port specifics in both multi-port and hybrid modes.
	if m.cfg.Mode == "multi-port" || m.cfg.Mode == "hybrid" {
		if node.Port == 0 && currentName != "" {
			if currentIndex := m.nodeIndexLocked(currentName); currentIndex >= 0 {
				node.Port = m.cfg.Nodes[currentIndex].Port
			}
		}
		if node.Port == 0 {
			candidateCfg := m.copyConfigLocked()
			candidateCfg.Nodes = append(candidateCfg.Nodes, node)
			if err := candidateCfg.NormalizeWithPortMap(m.cfg.BuildPortMap()); err != nil {
				return config.NodeConfig{}, fmt.Errorf("%w: 分配稳定端口失败: %v", monitor.ErrInvalidNode, err)
			}
			node.Port = candidateCfg.Nodes[len(candidateCfg.Nodes)-1].Port
		} else if m.portInUseLocked(node.Port, currentName) || (m.cfg.Mode == "hybrid" && node.Port == m.cfg.Listener.Port) {
			return config.NodeConfig{}, fmt.Errorf("%w: 端口 %d 已被占用", monitor.ErrNodeConflict, node.Port)
		}
		if node.Username == "" {
			node.Username = m.cfg.MultiPort.Username
			node.Password = m.cfg.MultiPort.Password
		}
	}

	return node, nil
}
