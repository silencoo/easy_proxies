package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type closeTestOutbound struct {
	adapter.Outbound
	dialStarted chan struct{}
	dialRelease chan struct{}
	dialCalls   atomic.Int32
	packetCalls atomic.Int32
}

func (o *closeTestOutbound) Network() []string {
	return []string{N.NetworkTCP, N.NetworkUDP}
}

func (o *closeTestOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.dialCalls.Add(1)
	if o.dialStarted != nil {
		select {
		case <-o.dialStarted:
		default:
			close(o.dialStarted)
		}
	}
	if o.dialRelease != nil {
		<-o.dialRelease // Deliberately ignore cancellation like a legacy dialer.
	}
	client, peer := net.Pipe()
	_ = peer.Close()
	return client, nil
}

func (o *closeTestOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	o.packetCalls.Add(1)
	return nil, errors.New("packet dial should not be reached")
}

func TestCloseDoesNotWaitForUncooperativeMemberDial(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)

	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	outbound := &closeTestOutbound{
		dialStarted: make(chan struct{}),
		dialRelease: release,
	}
	state := acquireSharedState("blocked-dial")
	member := &memberState{tag: "blocked-dial", outbound: outbound, shared: state}
	proxyPool := newIndexedTestPool(member, Options{})

	type dialResult struct {
		conn net.Conn
		err  error
	}
	result := make(chan dialResult, 1)
	go func() {
		conn, err := proxyPool.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.com:80"))
		result <- dialResult{conn: conn, err: err}
	}()
	select {
	case <-outbound.dialStarted:
	case <-time.After(time.Second):
		t.Fatal("member dial did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- proxyPool.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Close waited for an uncooperative member dial")
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case got := <-result:
		if got.conn != nil {
			_ = got.conn.Close()
			t.Fatal("dial admitted before Close published a connection after shutdown")
		}
		if !errors.Is(got.err, errPoolClosed) {
			t.Fatalf("late dial error = %v, want errPoolClosed", got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("member dial did not exit after release")
	}
	if active := state.activeCount(); active != 0 {
		t.Fatalf("late dial left active count at %d", active)
	}
}

func TestClosedPoolRejectsConnectionAndPacketDialsBeforeMemberCall(t *testing.T) {
	ResetSharedStateStore()
	t.Cleanup(ResetSharedStateStore)

	outbound := &closeTestOutbound{}
	state := acquireSharedState("closed-dial")
	member := &memberState{tag: "closed-dial", outbound: outbound, shared: state}
	proxyPool := newIndexedTestPool(member, Options{})
	if err := proxyPool.Close(); err != nil {
		t.Fatal(err)
	}

	if conn, err := proxyPool.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.com:80")); conn != nil || !errors.Is(err, errPoolClosed) {
		t.Fatalf("DialContext after Close = (%v, %v), want (nil, errPoolClosed)", conn, err)
	}
	if conn, err := proxyPool.ListenPacket(context.Background(), M.ParseSocksaddr("1.1.1.1:53")); conn != nil || !errors.Is(err, errPoolClosed) {
		t.Fatalf("ListenPacket after Close = (%v, %v), want (nil, errPoolClosed)", conn, err)
	}
	if calls := outbound.dialCalls.Load(); calls != 0 {
		t.Fatalf("closed pool made %d connection dial(s)", calls)
	}
	if calls := outbound.packetCalls.Load(); calls != 0 {
		t.Fatalf("closed pool made %d packet dial(s)", calls)
	}
}
