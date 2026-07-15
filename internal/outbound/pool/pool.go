package pool

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"easy_proxies/internal/monitor"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	singlog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

const (
	// Type is the outbound type name exposed to sing-box.
	Type = "pool"
	// Tag is the default outbound tag used by builder.
	Tag = "proxy-pool"

	modeSequential = "sequential"
	modeRandom     = "random"
	modeBalance    = "balance"
)

// Options controls pool outbound behaviour.
type Options struct {
	Mode              string
	Members           []string
	FailureThreshold  int
	BlacklistDuration time.Duration
	Metadata          map[string]MemberMeta
	// FailOpen retries blacklisted nodes when the entire shared pool is down.
	// It is opt-in because silently reviving known-bad nodes is unsafe for
	// crawlers that require predictable failure handling.
	FailOpen bool
	// Dedicated marks a single-node pool backing a fixed inbound port. It
	// always attempts that exact node even when the shared pool has blacklisted
	// it; it must never clear the shared blacklist merely to make progress.
	Dedicated bool
	// DedicatedMembers maps a mixed inbound tag to the exact upstream member
	// backing that listener. Keeping this dispatch table in the stable global
	// pool avoids per-node route rules, so listeners can be added and removed
	// without rebuilding the sing-box router.
	DedicatedMembers map[string]string
}

// MemberMeta carries optional descriptive information for monitoring UI.
type MemberMeta struct {
	Name          string
	URI           string
	Mode          string
	ListenAddress string
	Port          uint16
	Region        string // GeoIP region code: "jp", "kr", "us", "hk", "tw", "other"
	Country       string // Full country name from GeoIP
}

// Register wires the pool outbound into the registry.
func Register(registry *outbound.Registry) {
	outbound.Register[Options](registry, Type, newPool)
}

type memberState struct {
	outbound adapter.Outbound
	tag      string
	entry    *monitor.EntryHandle
	shared   *sharedMemberState
	unwatch  func()
}

type memberSet struct {
	items []*memberState
	index map[*memberState]int
}

func newMemberSet(capacity int) memberSet {
	return memberSet{
		items: make([]*memberState, 0, capacity),
		index: make(map[*memberState]int, capacity),
	}
}

func (s *memberSet) add(member *memberState) {
	if _, exists := s.index[member]; exists {
		return
	}
	s.index[member] = len(s.items)
	s.items = append(s.items, member)
}

func (s *memberSet) remove(member *memberState) {
	idx, exists := s.index[member]
	if !exists {
		return
	}
	last := len(s.items) - 1
	replacement := s.items[last]
	s.items[idx] = replacement
	s.index[replacement] = idx
	s.items = s.items[:last]
	delete(s.index, member)
}

type poolOutbound struct {
	outbound.Adapter
	ctx         context.Context
	logger      singlog.ContextLogger
	manager     adapter.OutboundManager
	options     Options
	mode        string
	members     []*memberState
	memberByTag map[string]*memberState
	mu          sync.Mutex
	eligibleMu  sync.RWMutex
	eligibleTCP memberSet
	eligibleUDP memberSet
	rrCounter   atomic.Uint32
	rng         *rand.Rand
	rngMu       sync.Mutex // protects rng for random mode
	monitor     *monitor.Manager
	closed      atomic.Bool
	initialized atomic.Bool
}

func newPool(ctx context.Context, _ adapter.Router, logger singlog.ContextLogger, tag string, options Options) (adapter.Outbound, error) {
	if len(options.Members) == 0 {
		return nil, E.New("pool requires at least one member")
	}
	manager := service.FromContext[adapter.OutboundManager](ctx)
	if manager == nil {
		return nil, E.New("missing outbound manager in context")
	}
	monitorMgr := monitor.FromContext(ctx)
	normalized := normalizeOptions(options)
	p := &poolOutbound{
		// Members are resolved from the already-populated runtime manager. Do not
		// expose them as manager dependencies: replacement pools otherwise leave
		// stale dependency edges that prevent drained node outbounds from being
		// removed after a node-level reload.
		Adapter:     outbound.NewAdapter(Type, tag, []string{N.NetworkTCP, N.NetworkUDP}, nil),
		ctx:         ctx,
		logger:      logger,
		manager:     manager,
		options:     normalized,
		mode:        normalized.Mode,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
		monitor:     monitorMgr,
		memberByTag: make(map[string]*memberState, len(normalized.Members)),
		eligibleTCP: newMemberSet(len(normalized.Members)),
		eligibleUDP: newMemberSet(len(normalized.Members)),
	}

	return p, nil
}

// Close only releases the external dialer registration. Connections already
// handed out by this pool keep their member/outbound references and can drain
// naturally after the runtime manager swaps in a replacement pool.
func (p *poolOutbound) Close() error {
	p.closed.Store(true)
	p.mu.Lock()
	members := append([]*memberState(nil), p.members...)
	p.mu.Unlock()
	for _, member := range members {
		if member.unwatch != nil {
			member.unwatch()
		}
	}
	unregisterDialer(p.Tag(), p)
	return nil
}

func normalizeOptions(options Options) Options {
	if options.FailureThreshold <= 0 {
		options.FailureThreshold = 3
	}
	if options.BlacklistDuration <= 0 {
		options.BlacklistDuration = 24 * time.Hour
	}
	if options.Metadata == nil {
		options.Metadata = make(map[string]MemberMeta)
	}
	switch strings.ToLower(options.Mode) {
	case modeRandom:
		options.Mode = modeRandom
	case modeBalance:
		options.Mode = modeBalance
	default:
		options.Mode = modeSequential
	}
	return options
}

func (p *poolOutbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	p.mu.Lock()
	err := p.initializeMembersLocked()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	// Registration is intentionally delayed until Start. box.New is used to
	// pre-validate reloads while the old instance is still serving traffic; it
	// must not replace live monitor callbacks or GeoIP dialers.
	registerDialer(p.Tag(), p)
	// 在初始化完成后，立即在后台触发健康检查
	if p.monitor != nil {
		go p.probeAllMembersOnStartup()
	}
	return nil
}

// initializeMembersLocked must be called with p.mu held
func (p *poolOutbound) initializeMembersLocked() error {
	if len(p.members) > 0 {
		p.initialized.Store(true)
		return nil // Already initialized
	}

	members := make([]*memberState, 0, len(p.options.Members))
	for _, tag := range p.options.Members {
		detour, loaded := p.manager.Outbound(tag)
		if !loaded {
			return E.New("pool member not found: ", tag)
		}

		// Acquire shared state (creates if not exists, reuses if already created)
		state := acquireSharedState(tag)

		member := &memberState{
			outbound: detour,
			tag:      tag,
			shared:   state,
			entry:    state.entryHandle(),
		}
		p.memberByTag[tag] = member
		member.unwatch = state.subscribeBlacklist(func(blacklisted bool) {
			p.setMemberEligible(member, p.options.Dedicated || !blacklisted)
		})

		// Connect to existing monitor entry if available
		if p.monitor != nil {
			meta := p.options.Metadata[tag]
			info := monitor.NodeInfo{
				Tag:           tag,
				Name:          meta.Name,
				URI:           meta.URI,
				Mode:          meta.Mode,
				ListenAddress: meta.ListenAddress,
				Port:          meta.Port,
				Region:        meta.Region,
				Country:       meta.Country,
			}
			entry := p.monitor.Register(info)
			if entry != nil {
				state.attachEntry(entry)
				member.entry = entry
				entry.SetRelease(p.makeReleaseFunc(member))
				entry.SetBlacklistFn(p.makeBlacklistByTagFunc(member.tag))
				if probe := p.makeProbeFunc(member); probe != nil {
					entry.SetProbe(probe)
				}
			}
		}
		members = append(members, member)
	}
	p.members = members
	p.initialized.Store(true)
	p.logger.Info("pool initialized with ", len(members), " members")

	return nil
}

// probeAllMembersOnStartup performs initial health checks on all members
func (p *poolOutbound) probeAllMembersOnStartup() {
	destination, ok := p.monitor.DestinationForProbe()
	if !ok {
		p.logger.Warn("probe target not configured, skipping initial health check")
		// 没有配置探测目标时，标记所有节点为可用
		p.mu.Lock()
		for _, member := range p.members {
			if member.entry != nil {
				member.entry.MarkInitialCheckDone(true)
			}
			if member.shared != nil {
				member.shared.persist()
			}
		}
		p.mu.Unlock()
		return
	}

	p.logger.Info("starting initial health check for all nodes")

	p.mu.Lock()
	members := make([]*memberState, len(p.members))
	copy(members, p.members)
	p.mu.Unlock()

	// Concurrent probing with bounded workers
	const maxWorkers = 20
	type probeResult struct {
		member  *memberState
		success bool
		latency time.Duration
		err     error
	}

	results := make(chan probeResult, len(members))
	sem := make(chan struct{}, maxWorkers)

	for _, member := range members {
		sem <- struct{}{} // acquire worker slot
		go func(m *memberState) {
			defer func() { <-sem }() // release worker slot

			ctx, cancel := context.WithTimeout(p.ctx, 15*time.Second)
			defer cancel()

			start := time.Now()
			conn, err := m.outbound.DialContext(ctx, N.NetworkTCP, destination)
			if err != nil {
				results <- probeResult{member: m, err: err}
				return
			}

			_, err = httpProbe(conn, destination.AddrString())
			conn.Close()
			if err != nil {
				results <- probeResult{member: m, err: err}
				return
			}

			results <- probeResult{member: m, success: true, latency: time.Since(start)}
		}(member)
	}

	// Collect results
	availableCount := 0
	failedCount := 0
	for i := 0; i < len(members); i++ {
		res := <-results
		if res.err != nil {
			p.logger.Warn("initial probe failed for ", res.member.tag, ": ", res.err)
			failedCount++
			if res.member.entry != nil {
				res.member.entry.MarkInitialCheckDone(false)
			}
			if res.member.shared != nil {
				res.member.shared.recordFailure(res.err, 1, p.options.BlacklistDuration)
			} else if res.member.entry != nil {
				res.member.entry.RecordFailure(res.err)
			}
		} else {
			latencyMs := res.latency.Milliseconds()
			p.logger.Info("initial probe success for ", res.member.tag, ", latency: ", latencyMs, "ms")
			availableCount++
			if res.member.entry != nil {
				res.member.entry.RecordSuccessWithLatency(res.latency)
				res.member.entry.MarkInitialCheckDone(true)
			}
			if res.member.shared != nil {
				res.member.shared.forceRelease()
			}
		}
	}

	p.logger.Info("initial health check completed: ", availableCount, " available, ", failedCount, " failed")
}

func (p *poolOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	member, err := p.pickMember(ctx, network)
	if err != nil {
		return nil, err
	}
	p.incActive(member)
	conn, err := member.outbound.DialContext(ctx, network, destination)
	if err != nil {
		p.decActive(member)
		p.recordFailure(member, err)
		return nil, err
	}
	p.recordSuccess(member)
	return p.wrapConn(conn, member), nil
}

func (p *poolOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	member, err := p.pickMember(ctx, N.NetworkUDP)
	if err != nil {
		return nil, err
	}
	p.incActive(member)
	conn, err := member.outbound.ListenPacket(ctx, destination)
	if err != nil {
		p.decActive(member)
		p.recordFailure(member, err)
		return nil, err
	}
	p.recordSuccess(member)
	return p.wrapPacketConn(conn, member), nil
}

func (p *poolOutbound) pickMember(ctx context.Context, network string) (*memberState, error) {
	if !p.initialized.Load() {
		p.mu.Lock()
		if err := p.initializeMembersLocked(); err != nil {
			p.mu.Unlock()
			return nil, err
		}
		p.mu.Unlock()
	}
	if inbound := adapter.ContextFrom(ctx); inbound != nil {
		if dedicatedTag := p.options.DedicatedMembers[inbound.Inbound]; dedicatedTag != "" {
			member := p.memberByTag[dedicatedTag]
			if member != nil && supportsMemberNetwork(member, network) {
				return member, nil
			}
			return nil, E.New("dedicated proxy member not found: ", dedicatedTag)
		}
	}
	if member := p.selectHealthyMember(network); member != nil {
		return member, nil
	}
	if p.releaseIfAllBlacklisted() {
		if member := p.selectHealthyMember(network); member != nil {
			return member, nil
		}
	}
	return nil, E.New("no healthy proxy available")
}

func (p *poolOutbound) selectHealthyMember(network string) *memberState {
	// The event-maintained index is authoritative in steady state. The atomic
	// check closes the tiny transition window between setting shared blacklist
	// state and delivering its removal callback, without scanning the pool.
	for attempt := 0; attempt < 2; attempt++ {
		member := p.selectEligibleMember(network)
		if member == nil {
			return nil
		}
		if member.shared == nil || !member.shared.blacklistedFast.Load() {
			return member
		}
		p.setMemberEligible(member, false)
	}
	return nil
}

func supportsMemberNetwork(member *memberState, network string) bool {
	if member == nil || member.outbound == nil || network == "" {
		return member != nil
	}
	return common.Contains(member.outbound.Network(), network)
}

func (p *poolOutbound) setMemberEligible(member *memberState, eligible bool) {
	if member == nil || p.closed.Load() {
		return
	}
	p.eligibleMu.Lock()
	if eligible && supportsMemberNetwork(member, N.NetworkTCP) {
		p.eligibleTCP.add(member)
	} else {
		p.eligibleTCP.remove(member)
	}
	if eligible && supportsMemberNetwork(member, N.NetworkUDP) {
		p.eligibleUDP.add(member)
	} else {
		p.eligibleUDP.remove(member)
	}
	p.eligibleMu.Unlock()
}

func (p *poolOutbound) releaseIfAllBlacklisted() bool {
	if p.options.Dedicated || !p.options.FailOpen || len(p.members) == 0 {
		return false
	}
	now := time.Now()
	for _, member := range p.members {
		if member.shared == nil || !member.shared.isBlacklisted(now) {
			return false
		}
	}
	// All blacklisted, force release all
	for _, member := range p.members {
		if member.shared != nil {
			member.shared.forceRelease()
		}
	}
	if p.logger != nil {
		p.logger.Warn("all upstream proxies were blacklisted, releasing them for retry")
	}
	return true
}

func (p *poolOutbound) selectEligibleMember(network string) *memberState {
	p.eligibleMu.RLock()
	candidates := p.eligibleTCP.items
	if network == N.NetworkUDP {
		candidates = p.eligibleUDP.items
	}
	if len(candidates) == 0 {
		p.eligibleMu.RUnlock()
		return nil
	}
	var selected *memberState
	switch p.mode {
	case modeRandom:
		p.rngMu.Lock()
		idx := p.rng.Intn(len(candidates))
		p.rngMu.Unlock()
		selected = candidates[idx]
	case modeBalance:
		if len(candidates) == 1 {
			selected = candidates[0]
			break
		}
		p.rngMu.Lock()
		firstIndex := p.rng.Intn(len(candidates))
		secondIndex := p.rng.Intn(len(candidates) - 1)
		p.rngMu.Unlock()
		if secondIndex >= firstIndex {
			secondIndex++
		}
		first := candidates[firstIndex]
		second := candidates[secondIndex]
		if activeConnections(second) < activeConnections(first) {
			selected = second
		} else {
			selected = first
		}
	default:
		idx := int(p.rrCounter.Add(1)-1) % len(candidates)
		selected = candidates[idx]
	}
	p.eligibleMu.RUnlock()
	return selected
}

func activeConnections(member *memberState) int32 {
	if member == nil || member.shared == nil {
		return 0
	}
	return member.shared.activeCount()
}

func (p *poolOutbound) recordFailure(member *memberState, cause error) {
	if member.shared == nil {
		p.logger.Warn("proxy ", member.tag, " failure (no shared state): ", cause)
		return
	}
	failures, blacklisted, _ := member.shared.recordFailure(cause, p.options.FailureThreshold, p.options.BlacklistDuration)
	if blacklisted {
		p.logger.Warn("proxy ", member.tag, " blacklisted for ", p.options.BlacklistDuration, ": ", cause)
		log.Printf("[pool] %s blacklisted for %s: %v", member.tag, p.options.BlacklistDuration, cause)
	} else {
		p.logger.Warn("proxy ", member.tag, " failure ", failures, "/", p.options.FailureThreshold, ": ", cause)
		log.Printf("[pool] %s failure %d/%d: %v", member.tag, failures, p.options.FailureThreshold, cause)
	}
}

func (p *poolOutbound) recordProbeFailure(member *memberState, cause error) {
	if member.shared != nil {
		// An explicit active probe is authoritative; exclude the node from the
		// shared pool immediately instead of waiting for traffic failures.
		member.shared.recordFailure(cause, 1, p.options.BlacklistDuration)
	} else if member.entry != nil {
		member.entry.RecordFailure(cause)
	}
}

func (p *poolOutbound) recordSuccess(member *memberState) {
	if member.shared != nil {
		member.shared.recordSuccess()
	}
}

func (p *poolOutbound) wrapConn(conn net.Conn, member *memberState) net.Conn {
	return &trackedConn{Conn: conn, release: func() {
		p.decActive(member)
	}}
}

func (p *poolOutbound) wrapPacketConn(conn net.PacketConn, member *memberState) net.PacketConn {
	return &trackedPacketConn{PacketConn: conn, release: func() {
		p.decActive(member)
	}}
}

func (p *poolOutbound) makeReleaseFunc(member *memberState) func() {
	return func() {
		if member.shared != nil {
			member.shared.forceRelease()
		}
	}
}

// httpProbe performs an HTTP probe through the connection and measures TTFB.
// It sends a minimal HTTP request and waits for the first byte of response.
func httpProbe(conn net.Conn, host string) (time.Duration, error) {
	// Build HTTP request
	req := fmt.Sprintf("GET /generate_204 HTTP/1.1\r\nHost: %s\r\nConnection: close\r\nUser-Agent: Mozilla/5.0\r\n\r\n", host)

	// Try to set write deadline (ignore errors for connections that don't support it)
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	// Record time just before sending request
	start := time.Now()

	// Send HTTP request
	if _, err := conn.Write([]byte(req)); err != nil {
		return 0, fmt.Errorf("write request: %w", err)
	}

	// Try to set read deadline (ignore errors for connections that don't support it)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// Read first byte (TTFB - Time To First Byte)
	reader := bufio.NewReader(conn)
	_, err := reader.ReadByte()
	if err != nil {
		return 0, fmt.Errorf("read response: %w", err)
	}

	// Calculate TTFB
	ttfb := time.Since(start)
	return ttfb, nil
}

func (p *poolOutbound) makeProbeFunc(member *memberState) func(ctx context.Context) (time.Duration, error) {
	if p.monitor == nil {
		return nil
	}
	destination, ok := p.monitor.DestinationForProbe()
	if !ok {
		return nil
	}
	return func(ctx context.Context) (time.Duration, error) {
		start := time.Now()
		conn, err := member.outbound.DialContext(ctx, N.NetworkTCP, destination)
		if err != nil {
			if member.entry != nil {
				member.entry.MarkInitialCheckDone(false)
			}
			p.recordProbeFailure(member, err)
			return 0, err
		}
		defer conn.Close()

		// Perform HTTP probe to measure actual latency (TTFB)
		_, err = httpProbe(conn, destination.AddrString())
		if err != nil {
			if member.entry != nil {
				member.entry.MarkInitialCheckDone(false)
			}
			p.recordProbeFailure(member, err)
			return 0, err
		}

		// Total duration = dial time + HTTP probe
		duration := time.Since(start)
		if member.entry != nil {
			member.entry.RecordSuccessWithLatency(duration)
			member.entry.MarkInitialCheckDone(true)
		}
		// Clear pool blacklist on successful probe — a node that passes
		// health check should be available for selection immediately,
		// not remain blacklisted for the full duration (fixes #8, #9).
		if member.shared != nil {
			member.shared.forceRelease()
		}
		return duration, nil
	}
}

// makeBlacklistByTagFunc creates a blacklist function for manual ban via API
func (p *poolOutbound) makeBlacklistByTagFunc(tag string) func(time.Duration) {
	return func(duration time.Duration) {
		blacklistSharedMember(tag, duration)
	}
}

type trackedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

type trackedPacketConn struct {
	net.PacketConn
	once    sync.Once
	release func()
}

func (c *trackedPacketConn) Close() error {
	err := c.PacketConn.Close()
	c.once.Do(c.release)
	return err
}

func (p *poolOutbound) incActive(member *memberState) {
	if member.shared != nil {
		member.shared.incActive()
	}
}

func (p *poolOutbound) decActive(member *memberState) {
	if member.shared != nil {
		member.shared.decActive()
	}
}
