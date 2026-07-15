package boxmgr

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/geoip"
	"easy_proxies/internal/outbound/pool"

	"github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/option"
)

type ipRegionLookup interface {
	LookupIP(ip string) geoip.RegionInfo
}

type exitRegionResult struct {
	ExitIP string
	Region geoip.RegionInfo
	Err    error
}

func (m *Manager) refreshExitGeoIP(ctx context.Context, cfg *config.Config) error {
	if cfg == nil || !cfg.GeoIP.Enabled {
		return nil
	}
	lookup, err := m.ensureGeoLookup(cfg)
	if err != nil {
		return err
	}
	m.mu.RLock()
	instance := m.currentBox
	runtimeCtx := m.runtimeCtx
	runtimeOptions := m.runtimeOptions
	previousIPs := make(map[string]string, len(m.exitIPs))
	for tag, ip := range m.exitIPs {
		previousIPs[tag] = ip
	}
	m.mu.RUnlock()
	if instance == nil || runtimeCtx == nil {
		return fmt.Errorf("GeoIP classification requires a running box")
	}
	baseOutbounds, _ := splitRuntimeOutbounds(runtimeOptions)
	dialers := make(map[string]geoip.OutboundDialer, len(baseOutbounds))
	for tag := range baseOutbounds {
		if outbound, ok := instance.Outbound().Outbound(tag); ok {
			dialers[tag] = outbound
		}
	}
	results := discoverExitRegions(
		ctx,
		dialers,
		lookup,
		cfg.GeoIP.ExitIPURL,
		cfg.GeoIP.ExitIPTimeout,
		cfg.GeoIP.ExitIPConcurrency,
		previousIPs,
	)
	regionCounts := make(map[string]int)
	observed := 0
	for tag, result := range results {
		regionCounts[result.Region.Code]++
		if result.ExitIP != "" {
			observed++
		}
		if result.Err != nil {
			if result.ExitIP != "" {
				m.logger.Warnf("exit IP probe failed for %s; keeping %s: %v", tag, result.ExitIP, result.Err)
			} else {
				m.logger.Warnf("exit IP probe failed for %s: %v", tag, result.Err)
			}
		}
	}
	m.logger.Infof("classified %d/%d proxy exit IPs; regions=%v", observed, len(results), regionCounts)
	updatedOptions, err := m.installGeoPools(runtimeCtx, instance, runtimeOptions, cfg, results)
	if err != nil {
		return err
	}
	exitIPs := make(map[string]string, len(results))
	for tag, result := range results {
		if result.ExitIP != "" {
			exitIPs[tag] = result.ExitIP
		}
	}
	m.mu.Lock()
	if m.currentBox == instance {
		m.runtimeOptions = updatedOptions
		m.exitIPs = exitIPs
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) ensureGeoLookup(cfg *config.Config) (*geoip.Lookup, error) {
	path := cfg.GeoIP.DatabasePath
	if path == "" {
		return nil, fmt.Errorf("GeoIP database_path is empty")
	}
	updateInterval := time.Duration(0)
	if cfg.GeoIP.AutoUpdateEnabled {
		updateInterval = cfg.GeoIP.AutoUpdateInterval
		if updateInterval <= 0 {
			updateInterval = 24 * time.Hour
		}
	}
	m.mu.RLock()
	current := m.geoLookup
	currentPath := m.geoLookupPath
	currentInterval := m.geoAutoInterval
	m.mu.RUnlock()
	if current != nil && currentPath == path && currentInterval == updateInterval {
		return current, nil
	}
	lookup, err := geoip.NewWithAutoUpdate(path, updateInterval)
	if err != nil {
		return nil, fmt.Errorf("load GeoIP database: %w", err)
	}
	lookup.SetUpdateCallback(func() {
		m.handleGeoDatabaseUpdate(lookup)
	})
	m.mu.Lock()
	previous := m.geoLookup
	m.geoLookup = lookup
	m.geoLookupPath = path
	m.geoAutoInterval = updateInterval
	if m.exitIPs == nil {
		m.exitIPs = make(map[string]string)
	}
	m.mu.Unlock()
	if previous != nil {
		_ = previous.Close()
	}
	return lookup, nil
}

func (m *Manager) stopGeoLookup() {
	m.mu.Lock()
	lookup := m.geoLookup
	m.geoLookup = nil
	m.geoLookupPath = ""
	m.geoAutoInterval = 0
	m.exitIPs = nil
	m.mu.Unlock()
	if lookup != nil {
		_ = lookup.Close()
	}
}

// handleGeoDatabaseUpdate reclassifies the last observed exit IPs against the
// newly loaded MMDB. It deliberately does not probe the network again: the
// database changed, while the proxy exit observations did not.
func (m *Manager) handleGeoDatabaseUpdate(source *geoip.Lookup) {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()

	m.mu.RLock()
	if m.geoLookup != source || m.currentBox == nil || m.runtimeCtx == nil || m.cfg == nil {
		m.mu.RUnlock()
		return
	}
	instance := m.currentBox
	runtimeCtx := m.runtimeCtx
	runtimeOptions := m.runtimeOptions
	cfg := m.copyConfigLocked()
	exitIPs := make(map[string]string, len(m.exitIPs))
	for tag, ip := range m.exitIPs {
		exitIPs[tag] = ip
	}
	m.mu.RUnlock()

	results := classifyKnownExitIPs(exitIPs, source)
	if len(results) == 0 {
		m.logger.Infof("GeoIP database updated; no observed proxy exit IPs need reclassification")
		return
	}
	updatedOptions, err := m.installGeoPools(runtimeCtx, instance, runtimeOptions, cfg, results)
	if err != nil {
		m.logger.Warnf("failed to reclassify proxy pools after GeoIP update: %v", err)
		return
	}

	m.mu.Lock()
	if m.geoLookup != source || m.currentBox != instance {
		m.mu.Unlock()
		return
	}
	m.runtimeOptions = updatedOptions
	m.mu.Unlock()
	m.syncGeoRouterDialers()

	regionCounts := make(map[string]int)
	for _, result := range results {
		regionCounts[result.Region.Code]++
	}
	m.logger.Infof("reclassified %d proxy exit IPs after GeoIP database update; regions=%v", len(results), regionCounts)
}

func classifyKnownExitIPs(exitIPs map[string]string, lookup ipRegionLookup) map[string]exitRegionResult {
	results := make(map[string]exitRegionResult, len(exitIPs))
	for tag, exitIP := range exitIPs {
		if exitIP == "" {
			continue
		}
		results[tag] = exitRegionResult{
			ExitIP: exitIP,
			Region: lookup.LookupIP(exitIP),
		}
	}
	return results
}

func (m *Manager) syncGeoRouterDialers() {
	m.mu.RLock()
	router := m.geoRouter
	m.mu.RUnlock()
	if router == nil {
		return
	}
	for _, region := range geoip.AllRegions() {
		if dialer, ok := pool.GetDialer("pool-" + region); ok {
			router.SetPool(region, dialer)
		} else {
			router.RemovePool(region)
		}
	}
	if dialer, ok := pool.GetDialer(pool.Tag); ok {
		router.SetGlobalPool(dialer)
	}
}

func discoverExitRegions(
	ctx context.Context,
	dialers map[string]geoip.OutboundDialer,
	lookup ipRegionLookup,
	endpoint string,
	timeout time.Duration,
	concurrency int,
	previousIPs map[string]string,
) map[string]exitRegionResult {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if concurrency <= 0 {
		concurrency = 16
	}
	if concurrency > len(dialers) {
		concurrency = len(dialers)
	}
	results := make(map[string]exitRegionResult, len(dialers))
	if len(dialers) == 0 {
		return results
	}
	type job struct {
		tag    string
		dialer geoip.OutboundDialer
	}
	type result struct {
		tag   string
		value exitRegionResult
	}
	jobs := make(chan job)
	completed := make(chan result, len(dialers))
	var workers sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for work := range jobs {
				probeCtx, cancel := context.WithTimeout(ctx, timeout)
				exitIP, err := geoip.DiscoverExitIP(probeCtx, work.dialer, endpoint)
				cancel()
				if err != nil {
					exitIP = previousIPs[work.tag]
				}
				region := geoip.RegionInfo{Code: geoip.RegionOther, Country: "Unknown"}
				if exitIP != "" {
					region = lookup.LookupIP(exitIP)
				}
				completed <- result{tag: work.tag, value: exitRegionResult{ExitIP: exitIP, Region: region, Err: err}}
			}
		}()
	}
	go func() {
		tags := make([]string, 0, len(dialers))
		for tag := range dialers {
			tags = append(tags, tag)
		}
		sort.Strings(tags)
		for _, tag := range tags {
			jobs <- job{tag: tag, dialer: dialers[tag]}
		}
		close(jobs)
		workers.Wait()
		close(completed)
	}()
	for item := range completed {
		results[item.tag] = item.value
	}
	return results
}

func (m *Manager) installGeoPools(
	runtimeCtx context.Context,
	instance *box.Box,
	runtimeOptions option.Options,
	cfg *config.Config,
	results map[string]exitRegionResult,
) (option.Options, error) {
	_, oldPools := splitRuntimeOutbounds(runtimeOptions)
	globalOutbound, ok := oldPools[pool.Tag]
	if !ok {
		return option.Options{}, fmt.Errorf("global pool %s not found", pool.Tag)
	}
	globalOptions, ok := globalOutbound.Options.(*pool.Options)
	if !ok {
		return option.Options{}, fmt.Errorf("global pool has unexpected options %T", globalOutbound.Options)
	}
	updatedGlobal := *globalOptions
	updatedGlobal.Members = append([]string(nil), globalOptions.Members...)
	updatedGlobal.Metadata = make(map[string]pool.MemberMeta, len(globalOptions.Metadata))
	for tag, metadata := range globalOptions.Metadata {
		if result, exists := results[tag]; exists {
			metadata.ExitIP = result.ExitIP
			metadata.Region = result.Region.Code
			metadata.Country = result.Region.Country
		}
		updatedGlobal.Metadata[tag] = metadata
	}
	updatedGlobal.SkipStartupProbe = true
	desiredPools := map[string]option.Outbound{
		pool.Tag: {Type: pool.Type, Tag: pool.Tag, Options: &updatedGlobal},
	}
	regionMembers := make(map[string][]string)
	for _, tag := range updatedGlobal.Members {
		region := updatedGlobal.Metadata[tag].Region
		if region == "" {
			region = geoip.RegionOther
		}
		regionMembers[region] = append(regionMembers[region], tag)
	}
	for _, region := range geoip.AllRegions() {
		members := regionMembers[region]
		if len(members) == 0 {
			continue
		}
		metadata := make(map[string]pool.MemberMeta, len(members))
		for _, tag := range members {
			metadata[tag] = updatedGlobal.Metadata[tag]
		}
		tag := "pool-" + region
		desiredPools[tag] = option.Outbound{
			Type: pool.Type,
			Tag:  tag,
			Options: &pool.Options{
				Mode:              cfg.Pool.Mode,
				Members:           append([]string(nil), members...),
				FailureThreshold:  cfg.Pool.FailureThreshold,
				BlacklistDuration: cfg.Pool.BlacklistDuration,
				Metadata:          metadata,
				FailOpen:          cfg.Pool.FailOpen,
				SkipStartupProbe:  true,
			},
		}
	}

	type change struct {
		tag      string
		previous option.Outbound
		existed  bool
	}
	changes := make([]change, 0, len(desiredPools))
	rollback := func() {
		for idx := len(changes) - 1; idx >= 0; idx-- {
			item := changes[idx]
			if item.existed {
				_ = createRuntimeOutbound(runtimeCtx, instance, item.previous)
			} else {
				_ = instance.Outbound().Remove(item.tag)
			}
		}
	}
	tags := sortedMapKeys(desiredPools)
	sort.SliceStable(tags, func(i, j int) bool { return tags[i] == pool.Tag && tags[j] != pool.Tag })
	for _, tag := range tags {
		desired := desiredPools[tag]
		previous, existed := oldPools[tag]
		if err := createRuntimeOutbound(runtimeCtx, instance, desired); err != nil {
			rollback()
			return option.Options{}, fmt.Errorf("install GeoIP pool %s: %w", tag, err)
		}
		changes = append(changes, change{tag: tag, previous: previous, existed: existed})
	}
	for _, tag := range mapDifferenceKeys(oldPools, desiredPools) {
		if err := instance.Outbound().Remove(tag); err != nil {
			rollback()
			return option.Options{}, fmt.Errorf("remove stale GeoIP pool %s: %w", tag, err)
		}
	}

	updated := runtimeOptions
	updated.Outbounds = make([]option.Outbound, 0, len(runtimeOptions.Outbounds)+len(desiredPools))
	for _, outbound := range runtimeOptions.Outbounds {
		if outbound.Type != pool.Type {
			updated.Outbounds = append(updated.Outbounds, outbound)
		}
	}
	for _, tag := range sortedMapKeys(desiredPools) {
		updated.Outbounds = append(updated.Outbounds, desiredPools[tag])
	}
	return updated, nil
}
