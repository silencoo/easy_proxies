package pool

import (
	"sync"
	"sync/atomic"
	"time"

	"easy_proxies/internal/monitor"
)

// sharedMemberState holds failure/blacklist state shared across all pool instances.
// This enables hybrid mode where pool and multi-port modes share the same node state.
type sharedMemberState struct {
	tag              string
	mu               sync.Mutex
	failures         int
	blacklisted      bool
	blacklistedUntil time.Time
	cooldownUntil    time.Time
	restoredMonitor  monitor.PersistedHealthState
	monitorRestored  bool
	entry            atomic.Pointer[monitor.EntryHandle]
	active           atomic.Int32
	blacklistedFast  atomic.Bool
	watchMu          sync.Mutex
	watchers         map[uint64]func(bool)
	nextWatcher      uint64
	blacklistTimer   *time.Timer
	cooldownTimer    *time.Timer
}

var sharedStateStore sync.Map // map[tag]*sharedMemberState

// acquireSharedState returns the shared state for a tag, creating if needed.
func acquireSharedState(tag string) *sharedMemberState {
	if v, ok := sharedStateStore.Load(tag); ok {
		return v.(*sharedMemberState)
	}
	state := &sharedMemberState{tag: tag}
	if restored, ok := restoredMemberHealth(tag); ok {
		state.failures = restored.Failures
		state.restoredMonitor = restored.Monitor
		if restored.BlacklistedUntil.After(time.Now()) {
			state.blacklisted = true
			state.blacklistedUntil = restored.BlacklistedUntil
			state.blacklistedFast.Store(true)
		}
		if restored.CooldownUntil.After(time.Now()) {
			state.cooldownUntil = restored.CooldownUntil
			state.blacklistedFast.Store(true)
		}
	}
	actual, _ := sharedStateStore.LoadOrStore(tag, state)
	result := actual.(*sharedMemberState)
	if result == state && state.blacklisted {
		state.scheduleBlacklistExpiry(state.blacklistedUntil)
	}
	if result == state && state.cooldownUntil.After(time.Now()) {
		state.scheduleCooldownExpiry(state.cooldownUntil)
	}
	return result
}

// lookupSharedState returns the shared state if it exists.
func lookupSharedState(tag string) (*sharedMemberState, bool) {
	v, ok := sharedStateStore.Load(tag)
	if !ok {
		return nil, false
	}
	return v.(*sharedMemberState), true
}

// ResetSharedStateStore clears all shared state (used during config reload).
func ResetSharedStateStore() {
	sharedStateStore.Range(func(key, value any) bool {
		value.(*sharedMemberState).close()
		sharedStateStore.Delete(key)
		return true
	})
	ResetDialerRegistry()
}

func (s *sharedMemberState) attachEntry(entry *monitor.EntryHandle) {
	if entry == nil {
		return
	}
	s.entry.Store(entry)
	s.mu.Lock()
	if s.monitorRestored {
		s.mu.Unlock()
		return
	}
	s.monitorRestored = true
	restored := s.restoredMonitor
	if s.blacklisted && s.blacklistedUntil.After(time.Now()) {
		restored.BlacklistedUntil = s.blacklistedUntil
		restored.Available = false
	}
	if s.cooldownUntil.After(time.Now()) {
		restored.CooldownUntil = s.cooldownUntil
		restored.Available = false
	}
	s.mu.Unlock()
	entry.RestoreHealthState(restored)
}

func (s *sharedMemberState) entryHandle() *monitor.EntryHandle {
	return s.entry.Load()
}

type failureDecision struct {
	Failures    int
	Blacklisted bool
	Cooldown    bool
	Until       time.Time
}

// recordFailure separates short-lived transport faults from durable protocol
// failures. Transient failures immediately remove the node from pooled
// selection for cooldown, but never advance the long blacklist threshold.
func (s *sharedMemberState) recordFailure(cause error, threshold int, blacklistDuration, transientCooldown time.Duration) failureDecision {
	if threshold <= 0 {
		threshold = 1
	}
	if blacklistDuration <= 0 {
		blacklistDuration = 24 * time.Hour
	}
	if transientCooldown <= 0 {
		transientCooldown = time.Minute
	}
	now := time.Now()
	transient := isTransientError(cause)
	decision := failureDecision{}
	s.mu.Lock()
	wasBlocked := s.blacklisted || s.cooldownUntil.After(now)
	if transient && !s.blacklisted {
		decision.Failures = s.failures
		decision.Cooldown = true
		decision.Until = now.Add(transientCooldown)
		if decision.Until.After(s.cooldownUntil) {
			s.cooldownUntil = decision.Until
		}
		s.blacklistedFast.Store(true)
	} else {
		s.failures++
		decision.Failures = s.failures
		if s.failures >= threshold {
			decision.Blacklisted = true
			decision.Until = now.Add(blacklistDuration)
			s.failures = 0
			s.blacklisted = true
			s.blacklistedUntil = decision.Until
			s.cooldownUntil = time.Time{}
			s.blacklistedFast.Store(true)
			if s.cooldownTimer != nil {
				s.cooldownTimer.Stop()
				s.cooldownTimer = nil
			}
		}
	}
	isBlocked := s.blacklisted || s.cooldownUntil.After(now)
	s.mu.Unlock()
	if isBlocked && !wasBlocked {
		s.publishBlacklist(true)
	}
	if decision.Blacklisted {
		s.scheduleBlacklistExpiry(decision.Until)
	} else if decision.Cooldown {
		s.scheduleCooldownExpiry(decision.Until)
	}

	if entry := s.entry.Load(); entry != nil {
		entry.RecordFailure(cause)
		if decision.Blacklisted {
			entry.Blacklist(decision.Until)
		} else if decision.Cooldown {
			entry.Cooldown(decision.Until)
		}
	}
	s.persist()
	return decision
}

func (s *sharedMemberState) recordSuccess() {
	s.mu.Lock()
	s.failures = 0
	hadCooldown := s.cooldownUntil.After(time.Now())
	s.cooldownUntil = time.Time{}
	if s.cooldownTimer != nil {
		s.cooldownTimer.Stop()
		s.cooldownTimer = nil
	}
	blocked := s.blacklisted
	s.mu.Unlock()
	if !blocked {
		s.blacklistedFast.Store(false)
	}

	if entry := s.entry.Load(); entry != nil {
		entry.RecordSuccess()
		if hadCooldown {
			entry.ClearCooldown()
		}
	}
	if hadCooldown && !blocked {
		s.publishBlacklist(false)
	}
	s.persist()
}

// isBlacklisted checks if the node is currently blacklisted, auto-clearing if expired.
func (s *sharedMemberState) isBlacklisted(now time.Time) bool {
	s.mu.Lock()
	expired := s.blacklisted && now.After(s.blacklistedUntil)
	if expired {
		s.blacklisted = false
		s.blacklistedUntil = time.Time{}
		if s.blacklistTimer != nil {
			s.blacklistTimer.Stop()
			s.blacklistTimer = nil
		}
	}
	blacklisted := s.blacklisted
	blocked := s.blacklisted || s.cooldownUntil.After(now)
	s.mu.Unlock()
	s.blacklistedFast.Store(blocked)

	if expired {
		if entry := s.entry.Load(); entry != nil {
			entry.ClearBlacklist()
		}
		if !blocked {
			s.publishBlacklist(false)
		}
		s.persist()
	}
	return blacklisted
}

func (s *sharedMemberState) isCoolingDown(now time.Time) bool {
	s.mu.Lock()
	expired := !s.cooldownUntil.IsZero() && !s.cooldownUntil.After(now)
	if expired {
		s.cooldownUntil = time.Time{}
		if s.cooldownTimer != nil {
			s.cooldownTimer.Stop()
			s.cooldownTimer = nil
		}
	}
	cooling := s.cooldownUntil.After(now)
	blocked := s.blacklisted || cooling
	s.mu.Unlock()
	s.blacklistedFast.Store(blocked)
	if expired {
		if entry := s.entry.Load(); entry != nil {
			entry.ClearCooldown()
		}
		if !blocked {
			s.publishBlacklist(false)
		}
		s.persist()
	}
	return cooling
}

func (s *sharedMemberState) isBlocked(now time.Time) bool {
	blacklisted := s.isBlacklisted(now)
	cooling := s.isCoolingDown(now)
	return blacklisted || cooling
}

func (s *sharedMemberState) forceRelease() {
	s.mu.Lock()
	wasBlocked := s.blacklisted || s.cooldownUntil.After(time.Now())
	s.failures = 0
	s.blacklisted = false
	s.blacklistedUntil = time.Time{}
	s.cooldownUntil = time.Time{}
	if s.blacklistTimer != nil {
		s.blacklistTimer.Stop()
		s.blacklistTimer = nil
	}
	if s.cooldownTimer != nil {
		s.cooldownTimer.Stop()
		s.cooldownTimer = nil
	}
	s.mu.Unlock()
	s.blacklistedFast.Store(false)

	if entry := s.entry.Load(); entry != nil {
		entry.ClearBlacklist()
		entry.ClearCooldown()
	}
	if wasBlocked {
		s.publishBlacklist(false)
	}
	s.persist()
}

func (s *sharedMemberState) incActive() {
	s.active.Add(1)
	if entry := s.entry.Load(); entry != nil {
		entry.IncActive()
	}
}

func (s *sharedMemberState) decActive() {
	s.active.Add(-1)
	if entry := s.entry.Load(); entry != nil {
		entry.DecActive()
	}
}

func (s *sharedMemberState) activeCount() int32 {
	return s.active.Load()
}

func (s *sharedMemberState) persist() {
	if s == nil || s.tag == "" {
		return
	}
	s.mu.Lock()
	record := persistedMemberHealth{
		Failures: s.failures,
	}
	if s.blacklisted && s.blacklistedUntil.After(time.Now()) {
		record.BlacklistedUntil = s.blacklistedUntil
	}
	if s.cooldownUntil.After(time.Now()) {
		record.CooldownUntil = s.cooldownUntil
	}
	s.mu.Unlock()
	if entry := s.entry.Load(); entry != nil {
		record.Monitor = entry.ExportHealthState()
	}
	if !record.BlacklistedUntil.IsZero() {
		record.Monitor.BlacklistedUntil = record.BlacklistedUntil
		record.Monitor.Available = false
	}
	if !record.CooldownUntil.IsZero() {
		record.Monitor.CooldownUntil = record.CooldownUntil
		record.Monitor.Available = false
	}
	storeMemberHealth(s.tag, record)
}

func (s *sharedMemberState) subscribeBlacklist(callback func(bool)) func() {
	if callback == nil {
		return func() {}
	}
	s.watchMu.Lock()
	if s.watchers == nil {
		s.watchers = make(map[uint64]func(bool))
	}
	s.nextWatcher++
	id := s.nextWatcher
	s.watchers[id] = callback
	s.watchMu.Unlock()
	callback(s.isBlocked(time.Now()))
	return func() {
		s.watchMu.Lock()
		delete(s.watchers, id)
		s.watchMu.Unlock()
	}
}

func (s *sharedMemberState) publishBlacklist(blacklisted bool) {
	s.watchMu.Lock()
	callbacks := make([]func(bool), 0, len(s.watchers))
	for _, callback := range s.watchers {
		callbacks = append(callbacks, callback)
	}
	s.watchMu.Unlock()
	for _, callback := range callbacks {
		callback(blacklisted)
	}
}

func (s *sharedMemberState) scheduleBlacklistExpiry(until time.Time) {
	if until.IsZero() {
		return
	}
	delay := time.Until(until)
	if delay < 0 {
		delay = 0
	}
	s.mu.Lock()
	if s.blacklistTimer != nil {
		s.blacklistTimer.Stop()
	}
	s.blacklistTimer = time.AfterFunc(delay, func() {
		s.expireBlacklist(until)
	})
	s.mu.Unlock()
}

func (s *sharedMemberState) expireBlacklist(expected time.Time) {
	s.mu.Lock()
	if !s.blacklisted || !s.blacklistedUntil.Equal(expected) {
		s.mu.Unlock()
		return
	}
	if remaining := time.Until(expected); remaining > 0 {
		s.blacklistTimer = time.AfterFunc(remaining, func() {
			s.expireBlacklist(expected)
		})
		s.mu.Unlock()
		return
	}
	s.blacklisted = false
	s.blacklistedUntil = time.Time{}
	s.blacklistTimer = nil
	blocked := s.cooldownUntil.After(time.Now())
	s.mu.Unlock()
	s.blacklistedFast.Store(blocked)
	if entry := s.entry.Load(); entry != nil {
		entry.ClearBlacklist()
	}
	if !blocked {
		s.publishBlacklist(false)
	}
	s.persist()
}

func (s *sharedMemberState) scheduleCooldownExpiry(until time.Time) {
	if until.IsZero() {
		return
	}
	delay := time.Until(until)
	if delay < 0 {
		delay = 0
	}
	s.mu.Lock()
	if s.cooldownTimer != nil {
		s.cooldownTimer.Stop()
	}
	s.cooldownTimer = time.AfterFunc(delay, func() {
		s.expireCooldown(until)
	})
	s.mu.Unlock()
}

func (s *sharedMemberState) expireCooldown(expected time.Time) {
	s.mu.Lock()
	if !s.cooldownUntil.Equal(expected) {
		s.mu.Unlock()
		return
	}
	if remaining := time.Until(expected); remaining > 0 {
		s.cooldownTimer = time.AfterFunc(remaining, func() {
			s.expireCooldown(expected)
		})
		s.mu.Unlock()
		return
	}
	s.cooldownUntil = time.Time{}
	s.cooldownTimer = nil
	blocked := s.blacklisted
	s.mu.Unlock()
	s.blacklistedFast.Store(blocked)
	if entry := s.entry.Load(); entry != nil {
		entry.ClearCooldown()
	}
	if !blocked {
		s.publishBlacklist(false)
	}
	s.persist()
}

func (s *sharedMemberState) close() {
	s.mu.Lock()
	if s.blacklistTimer != nil {
		s.blacklistTimer.Stop()
		s.blacklistTimer = nil
	}
	if s.cooldownTimer != nil {
		s.cooldownTimer.Stop()
		s.cooldownTimer = nil
	}
	s.mu.Unlock()
	s.watchMu.Lock()
	s.watchers = nil
	s.watchMu.Unlock()
}

// blacklistSharedMember manually blacklists a node in pool shared state.
func blacklistSharedMember(tag string, duration time.Duration) {
	if state, ok := lookupSharedState(tag); ok {
		until := time.Now().Add(duration)
		state.mu.Lock()
		state.blacklisted = true
		state.blacklistedUntil = until
		state.cooldownUntil = time.Time{}
		if state.cooldownTimer != nil {
			state.cooldownTimer.Stop()
			state.cooldownTimer = nil
		}
		state.failures = 0
		state.blacklistedFast.Store(true)
		state.mu.Unlock()
		if entry := state.entry.Load(); entry != nil {
			entry.Blacklist(until)
			entry.ClearCooldown()
		}
		state.publishBlacklist(true)
		state.scheduleBlacklistExpiry(until)
		state.persist()
	}
}
