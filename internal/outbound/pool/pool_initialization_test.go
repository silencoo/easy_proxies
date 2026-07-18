package pool

import (
	"context"
	"testing"

	"easy_proxies/internal/monitor"

	"github.com/sagernet/sing-box/adapter"
	singlog "github.com/sagernet/sing-box/log"
)

type initializationOutboundManager struct {
	outbounds map[string]adapter.Outbound
}

func (*initializationOutboundManager) Start(adapter.StartStage) error { return nil }
func (*initializationOutboundManager) Close() error                   { return nil }

func (m *initializationOutboundManager) Outbounds() []adapter.Outbound {
	result := make([]adapter.Outbound, 0, len(m.outbounds))
	for _, outbound := range m.outbounds {
		result = append(result, outbound)
	}
	return result
}

func (m *initializationOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	outbound, ok := m.outbounds[tag]
	return outbound, ok
}

func (m *initializationOutboundManager) Default() adapter.Outbound { return nil }

func (m *initializationOutboundManager) Remove(tag string) error {
	delete(m.outbounds, tag)
	return nil
}

func (*initializationOutboundManager) Create(
	context.Context,
	adapter.Router,
	singlog.ContextLogger,
	string,
	string,
	any,
) error {
	return nil
}

func TestInitializeMembersFailureInstallsNoResources(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)

	monitorManager, err := monitor.NewManager(monitor.Config{})
	if err != nil {
		t.Fatal(err)
	}
	valid := &scriptedOutbound{}
	manager := &initializationOutboundManager{
		outbounds: map[string]adapter.Outbound{"present": valid},
	}
	proxyPool := &poolOutbound{
		manager:     manager,
		monitor:     monitorManager,
		options:     normalizeOptions(Options{Members: []string{"present", "missing"}}),
		memberByTag: make(map[string]*memberState),
		eligibleTCP: newMemberSet(2),
		eligibleUDP: newMemberSet(2),
	}

	if err := proxyPool.initializeMembersLocked(); err == nil {
		t.Fatal("initialization unexpectedly accepted a missing member")
	}
	if proxyPool.initialized.Load() || len(proxyPool.members) != 0 || len(proxyPool.memberByTag) != 0 {
		t.Fatalf("failed initialization retained pool members: initialized=%v members=%d map=%d",
			proxyPool.initialized.Load(), len(proxyPool.members), len(proxyPool.memberByTag))
	}
	state, exists := lookupSharedState("present")
	if exists {
		state.watchMu.Lock()
		watchers := len(state.watchers)
		state.watchMu.Unlock()
		t.Fatalf("failed initialization retained shared state with %d watcher(s)", watchers)
	}
	if snapshots := monitorManager.Snapshot(); len(snapshots) != 0 {
		t.Fatalf("failed initialization installed monitor callbacks/entries: %#v", snapshots)
	}
}

var _ adapter.OutboundManager = (*initializationOutboundManager)(nil)
