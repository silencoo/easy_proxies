package pool

import (
	"bufio"
	"context"
	"crypto/tls"
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
	modeLatency    = "latency"
)

// Options controls pool outbound behaviour.
type Options struct {
	Mode              string
	Members           []string
	FailureThreshold  int
	BlacklistDuration time.Duration
	TransientCooldown time.Duration
	RetryEnabled      bool
	RetryAttempts     int
	LatencySampleSize int
	LatencyTolerance  time.Duration
	Sticky            StickyOptions
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
	// SkipStartupProbe is used for metadata-only pool replacements after exit
	// GeoIP discovery; shared health state is already initialized at that point.
	SkipStartupProbe bool
}

// StickyOptions configures bounded session affinity for pooled traffic.
type StickyOptions struct {
	Enabled    bool
	TTL        time.Duration
	MaxEntries int
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
	ExitIP        string // Public egress IP observed through this node
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
	sticky      *stickyCache
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
	if normalized.Sticky.Enabled {
		p.sticky = newStickyCache(normalized.Sticky.TTL, normalized.Sticky.MaxEntries)
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
	if options.TransientCooldown <= 0 {
		options.TransientCooldown = time.Minute
	}
	if options.RetryAttempts <= 0 {
		options.RetryAttempts = 3
	} else if options.RetryAttempts > 10 {
		options.RetryAttempts = 10
	}
	if options.LatencySampleSize <= 0 {
		options.LatencySampleSize = 4
	} else if options.LatencySampleSize > 32 {
		options.LatencySampleSize = 32
	}
	if options.LatencyTolerance <= 0 {
		options.LatencyTolerance = 50 * time.Millisecond
	}
	if options.Sticky.TTL <= 0 {
		options.Sticky.TTL = 30 * time.Minute
	}
	if options.Sticky.MaxEntries <= 0 {
		options.Sticky.MaxEntries = 4096
	}
	if options.Metadata == nil {
		options.Metadata = make(map[string]MemberMeta)
	}
	switch strings.ToLower(options.Mode) {
	case modeRandom:
		options.Mode = modeRandom
	case modeBalance:
		options.Mode = modeBalance
	case modeLatency:
		options.Mode = modeLatency
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
				ExitIP:        meta.ExitIP,
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

func (p *poolOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	maxAttempts := p.maxAttempts(ctx)
	var tried map[string]struct{}
	if maxAttempts > 1 {
		tried = make(map[string]struct{}, maxAttempts)
	}
	stickyKey := p.stickyKeyFromContext(ctx)
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		member, err := p.pickMemberExcluding(ctx, network, tried, stickyKey)
		if err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("proxy dial failed after %d attempt(s): %w", attempt-1, lastErr)
			}
			return nil, err
		}
		p.incActive(member)
		conn, err := member.outbound.DialContext(ctx, network, destination)
		if err == nil {
			p.recordSuccess(member)
			return p.wrapConn(conn, member), nil
		}
		p.decActive(member)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		p.recordFailure(member, err)
		if attempt < maxAttempts {
			tried[member.tag] = struct{}{}
		}
		if stickyKey != "" {
			p.sticky.delete(stickyKey)
		}
		lastErr = err
	}
	return nil, fmt.Errorf("proxy dial failed after %d attempt(s): %w", maxAttempts, lastErr)
}

func (p *poolOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	maxAttempts := p.maxAttempts(ctx)
	var tried map[string]struct{}
	if maxAttempts > 1 {
		tried = make(map[string]struct{}, maxAttempts)
	}
	stickyKey := p.stickyKeyFromContext(ctx)
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		member, err := p.pickMemberExcluding(ctx, N.NetworkUDP, tried, stickyKey)
		if err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("proxy packet dial failed after %d attempt(s): %w", attempt-1, lastErr)
			}
			return nil, err
		}
		p.incActive(member)
		conn, err := member.outbound.ListenPacket(ctx, destination)
		if err == nil {
			p.recordSuccess(member)
			return p.wrapPacketConn(conn, member), nil
		}
		p.decActive(member)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		p.recordFailure(member, err)
		if attempt < maxAttempts {
			tried[member.tag] = struct{}{}
		}
		if stickyKey != "" {
			p.sticky.delete(stickyKey)
		}
		lastErr = err
	}
	return nil, fmt.Errorf("proxy packet dial failed after %d attempt(s): %w", maxAttempts, lastErr)
}

func (p *poolOutbound) pickMember(ctx context.Context, network string) (*memberState, error) {
	return p.pickMemberExcluding(ctx, network, nil, p.stickyKeyFromContext(ctx))
}

func (p *poolOutbound) pickMemberExcluding(ctx context.Context, network string, tried map[string]struct{}, stickyKey string) (*memberState, error) {
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
	if stickyKey != "" && p.sticky != nil {
		if tag, ok := p.sticky.get(stickyKey, time.Now()); ok {
			member := p.memberByTag[tag]
			_, wasTried := tried[tag]
			if !wasTried && p.memberEligible(member, network) {
				return member, nil
			}
			p.sticky.delete(stickyKey)
		}
	}
	if member := p.selectHealthyMemberExcluding(network, tried); member != nil {
		if stickyKey != "" && p.sticky != nil {
			p.sticky.set(stickyKey, member.tag, time.Now())
		}
		return member, nil
	}
	if p.releaseIfAllBlacklisted() {
		if member := p.selectHealthyMemberExcluding(network, tried); member != nil {
			if stickyKey != "" && p.sticky != nil {
				p.sticky.set(stickyKey, member.tag, time.Now())
			}
			return member, nil
		}
	}
	return nil, E.New("no healthy proxy available")
}

func (p *poolOutbound) selectHealthyMember(network string) *memberState {
	return p.selectHealthyMemberExcluding(network, nil)
}

func (p *poolOutbound) selectHealthyMemberExcluding(network string, tried map[string]struct{}) *memberState {
	// The event-maintained index is authoritative in steady state. The atomic
	// check closes the tiny transition window between setting shared blacklist
	// state and delivering its removal callback, without scanning the pool.
	for attempt := 0; attempt < 2; attempt++ {
		member := p.selectEligibleMemberExcluding(network, tried)
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

func (p *poolOutbound) maxAttempts(ctx context.Context) int {
	if !p.options.RetryEnabled || len(p.members) <= 1 {
		return 1
	}
	if inbound := adapter.ContextFrom(ctx); inbound != nil && p.options.DedicatedMembers[inbound.Inbound] != "" {
		return 1
	}
	attempts := p.options.RetryAttempts
	if attempts < 1 {
		attempts = 1
	}
	if attempts > len(p.members) {
		attempts = len(p.members)
	}
	return attempts
}

func (p *poolOutbound) stickyKeyFromContext(ctx context.Context) string {
	if p.sticky == nil {
		return ""
	}
	metadata := adapter.ContextFrom(ctx)
	if metadata == nil {
		return ""
	}
	user := strings.TrimSpace(metadata.User)
	source := ""
	if metadata.Source.IsValid() {
		source = metadata.Source.AddrString()
	}
	if user == "" && source == "" {
		return ""
	}
	return metadata.Inbound + "|" + user + "|" + source
}

func (p *poolOutbound) memberEligible(member *memberState, network string) bool {
	if member == nil || !supportsMemberNetwork(member, network) {
		return false
	}
	if member.shared != nil && member.shared.blacklistedFast.Load() {
		return false
	}
	p.eligibleMu.RLock()
	set := &p.eligibleTCP
	if network == N.NetworkUDP {
		set = &p.eligibleUDP
	}
	_, ok := set.index[member]
	p.eligibleMu.RUnlock()
	return ok
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
		if member.shared == nil || !member.shared.isBlocked(now) {
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
	return p.selectEligibleMemberExcluding(network, nil)
}

func (p *poolOutbound) selectEligibleMemberExcluding(network string, tried map[string]struct{}) *memberState {
	p.eligibleMu.RLock()
	candidates := p.eligibleTCP.items
	if network == N.NetworkUDP {
		candidates = p.eligibleUDP.items
	}
	if len(candidates) == 0 {
		p.eligibleMu.RUnlock()
		return nil
	}
	eligible := func(member *memberState) bool {
		if member == nil {
			return false
		}
		_, excluded := tried[member.tag]
		return !excluded
	}
	var selected *memberState
	switch p.mode {
	case modeRandom:
		p.rngMu.Lock()
		start := p.rng.Intn(len(candidates))
		p.rngMu.Unlock()
		for offset := 0; offset < len(candidates); offset++ {
			candidate := candidates[(start+offset)%len(candidates)]
			if eligible(candidate) {
				selected = candidate
				break
			}
		}
	case modeBalance:
		selected = p.selectBalancedCandidate(candidates, eligible)
	case modeLatency:
		selected = p.selectLatencyCandidate(candidates, eligible)
	default:
		for offset := 0; offset < len(candidates); offset++ {
			idx := int(p.rrCounter.Add(1)-1) % len(candidates)
			if eligible(candidates[idx]) {
				selected = candidates[idx]
				break
			}
		}
	}
	p.eligibleMu.RUnlock()
	return selected
}

func (p *poolOutbound) selectBalancedCandidate(candidates []*memberState, eligible func(*memberState) bool) *memberState {
	if len(candidates) == 0 {
		return nil
	}
	var first, second *memberState
	p.rngMu.Lock()
	start := p.rng.Intn(len(candidates))
	p.rngMu.Unlock()
	for offset := 0; offset < len(candidates); offset++ {
		candidate := candidates[(start+offset)%len(candidates)]
		if !eligible(candidate) {
			continue
		}
		if first == nil {
			first = candidate
			continue
		}
		second = candidate
		break
	}
	if second == nil || activeConnections(first) <= activeConnections(second) {
		return first
	}
	return second
}

// selectLatencyCandidate applies bounded power-of-k sampling instead of
// scanning the whole inventory or sending all traffic to the single globally
// fastest node. Candidates within the configured latency tolerance are
// balanced by active connection count.
func (p *poolOutbound) selectLatencyCandidate(candidates []*memberState, eligible func(*memberState) bool) *memberState {
	if len(candidates) == 0 {
		return nil
	}
	sampleSize := p.options.LatencySampleSize
	if sampleSize < 1 {
		sampleSize = 1
	}
	if sampleSize > len(candidates) {
		sampleSize = len(candidates)
	}
	p.rngMu.Lock()
	start := p.rng.Intn(len(candidates))
	p.rngMu.Unlock()

	var best *memberState
	for visited, index := 0, start; visited < len(candidates) && sampleSize > 0; visited, index = visited+1, (index+1)%len(candidates) {
		candidate := candidates[index]
		if !eligible(candidate) {
			continue
		}
		sampleSize--
		if betterLatencyCandidate(candidate, best, p.options.LatencyTolerance) {
			best = candidate
		}
	}
	return best
}

func betterLatencyCandidate(candidate, current *memberState, tolerance time.Duration) bool {
	if candidate == nil {
		return false
	}
	if current == nil {
		return true
	}
	candidateLatency := memberLatency(candidate)
	currentLatency := memberLatency(current)
	if candidateLatency <= 0 && currentLatency > 0 {
		return false
	}
	if candidateLatency > 0 && currentLatency <= 0 {
		return true
	}
	if candidateLatency > 0 && currentLatency > 0 {
		if candidateLatency+tolerance < currentLatency {
			return true
		}
		if currentLatency+tolerance < candidateLatency {
			return false
		}
	}
	if activeConnections(candidate) != activeConnections(current) {
		return activeConnections(candidate) < activeConnections(current)
	}
	return candidateLatency > 0 && (currentLatency <= 0 || candidateLatency < currentLatency)
}

func memberLatency(member *memberState) time.Duration {
	if member == nil || member.entry == nil {
		return 0
	}
	return member.entry.LastLatency()
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
	decision := member.shared.recordFailure(cause, p.options.FailureThreshold, p.options.BlacklistDuration, p.options.TransientCooldown)
	if decision.Cooldown {
		if p.logger != nil {
			p.logger.Warn("proxy ", member.tag, " cooling down for ", p.options.TransientCooldown, ": ", cause)
		}
		log.Printf("[pool] %s cooling down for %s: %v", member.tag, p.options.TransientCooldown, cause)
	} else if decision.Blacklisted {
		if p.logger != nil {
			p.logger.Warn("proxy ", member.tag, " blacklisted for ", p.options.BlacklistDuration, ": ", cause)
		}
		log.Printf("[pool] %s blacklisted for %s: %v", member.tag, p.options.BlacklistDuration, cause)
	} else {
		if p.logger != nil {
			p.logger.Warn("proxy ", member.tag, " failure ", decision.Failures, "/", p.options.FailureThreshold, ": ", cause)
		}
		log.Printf("[pool] %s failure %d/%d: %v", member.tag, decision.Failures, p.options.FailureThreshold, cause)
	}
}

func (p *poolOutbound) recordProbeFailure(member *memberState, cause error) {
	if member.entry != nil {
		member.entry.MarkInitialCheckDone(false)
	}
	if member.shared != nil {
		// An explicit active probe is authoritative; exclude the node from the
		// shared pool immediately instead of waiting for traffic failures.
		member.shared.recordFailure(cause, 1, p.options.BlacklistDuration, p.options.TransientCooldown)
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

func upgradeProbeConn(ctx context.Context, conn net.Conn, target monitor.ProbeTarget) (net.Conn, error) {
	if !target.TLS {
		return conn, nil
	}
	tlsConn := tls.Client(conn, &tls.Config{
		ServerName:         target.Host,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: target.SkipCertVerify, // Explicit user setting for HTTPS probes.
	})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return conn, fmt.Errorf("TLS handshake: %w", err)
	}
	return tlsConn, nil
}

// httpProbe performs an HTTP probe through the connection and measures TTFB.
func httpProbe(ctx context.Context, conn net.Conn, host string) (time.Duration, error) {
	// Build HTTP request
	req := fmt.Sprintf("GET /generate_204 HTTP/1.1\r\nHost: %s\r\nConnection: close\r\nUser-Agent: Mozilla/5.0\r\n\r\n", host)

	deadline := time.Now().Add(10 * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)

	// Record time just before sending request
	start := time.Now()

	// Send HTTP request
	if _, err := conn.Write([]byte(req)); err != nil {
		return 0, fmt.Errorf("write request: %w", err)
	}

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

// probeMember force-closes the raw connection when the deadline fires. Some
// sing-box wrappers ignore SetDeadline; closing the underlying connection is
// what releases blocked reads/handshakes and their file descriptors.
func (p *poolOutbound) probeMember(ctx context.Context, member *memberState, target monitor.ProbeTarget) (time.Duration, error) {
	start := time.Now()
	rawConn, err := member.outbound.DialContext(ctx, N.NetworkTCP, target.Destination)
	if err != nil {
		p.recordProbeFailure(member, err)
		return 0, err
	}
	stopWatch := watchProbeConnection(ctx, rawConn)
	defer stopWatch()
	defer rawConn.Close()

	conn, err := upgradeProbeConn(ctx, rawConn, target)
	if err != nil {
		p.recordProbeFailure(member, err)
		return 0, err
	}
	host := target.Host
	if target.Destination.Port != 80 && target.Destination.Port != 443 {
		host = target.Destination.AddrString()
	}
	if _, err = httpProbe(ctx, conn, host); err != nil {
		p.recordProbeFailure(member, err)
		return 0, err
	}

	duration := time.Since(start)
	if member.entry != nil {
		member.entry.RecordSuccessWithLatency(duration)
		member.entry.MarkInitialCheckDone(true)
	}
	if member.shared != nil {
		// A successful active probe is authoritative and is persisted through
		// shared state, making the node routable immediately.
		member.shared.forceRelease()
	}
	return duration, nil
}

func watchProbeConnection(ctx context.Context, conn net.Conn) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	return func() { close(done) }
}

func (p *poolOutbound) makeProbeFunc(member *memberState) func(ctx context.Context) (time.Duration, error) {
	if p.monitor == nil {
		return nil
	}
	return func(ctx context.Context) (time.Duration, error) {
		target, ok := p.monitor.DestinationForProbe()
		if !ok {
			return 0, fmt.Errorf("probe target is not configured")
		}
		return p.probeMember(ctx, member, target)
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
