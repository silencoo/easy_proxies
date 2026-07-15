package pool

import (
	"context"
	"net"
	"sync"

	M "github.com/sagernet/sing/common/metadata"
)

// NetDialer provides standard Go net.Conn dialing through a pool outbound.
type NetDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

var dialerRegistry sync.Map // map[string]NetDialer

// poolDialerAdapter wraps a poolOutbound to satisfy NetDialer.
type poolDialerAdapter struct {
	pool *poolOutbound
}

func (a *poolDialerAdapter) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	dest := M.ParseSocksaddr(address)
	return a.pool.DialContext(ctx, network, dest)
}

// registerDialer adds a pool outbound to the global dialer registry.
func registerDialer(tag string, p *poolOutbound) {
	dialerRegistry.Store(tag, &poolDialerAdapter{pool: p})
}

// unregisterDialer removes a registration only when it still belongs to p.
// During runtime replacement the new pool registers before sing-box closes
// the old one, so an unconditional delete would erase the new registration.
func unregisterDialer(tag string, p *poolOutbound) {
	value, ok := dialerRegistry.Load(tag)
	if !ok {
		return
	}
	adapter, ok := value.(*poolDialerAdapter)
	if ok && adapter.pool == p {
		dialerRegistry.CompareAndDelete(tag, value)
	}
}

// GetDialer returns a NetDialer for the given pool tag.
func GetDialer(tag string) (NetDialer, bool) {
	v, ok := dialerRegistry.Load(tag)
	if !ok {
		return nil, false
	}
	return v.(NetDialer), true
}

// ResetDialerRegistry clears the dialer registry (called during config reload).
func ResetDialerRegistry() {
	dialerRegistry.Range(func(key, _ any) bool {
		dialerRegistry.Delete(key)
		return true
	})
}
