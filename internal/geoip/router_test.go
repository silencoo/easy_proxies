package geoip

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func setProxyBasicAuth(req *http.Request, username, password string) {
	encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	req.Header.Set("Proxy-Authorization", "Basic "+encoded)
}

func TestRouterSelectsRegionThroughStandardProxyCredentials(t *testing.T) {
	router := NewRouter(RouterConfig{}, nil)
	req := httptest.NewRequest(http.MethodConnect, "http://example.test", nil)
	setProxyBasicAuth(req, RegionJP, "")
	region, authorized := router.authorizeAndSelectRegion(req)
	if !authorized || region != RegionJP {
		t.Fatalf("anonymous selector = (%q, %t), want (%q, true)", region, authorized, RegionJP)
	}

	authenticated := NewRouter(RouterConfig{Username: "crawler", Password: "secret"}, nil)
	req = httptest.NewRequest(http.MethodConnect, "http://example.test", nil)
	setProxyBasicAuth(req, "crawler@"+RegionUS, "secret")
	region, authorized = authenticated.authorizeAndSelectRegion(req)
	if !authorized || region != RegionUS {
		t.Fatalf("authenticated selector = (%q, %t), want (%q, true)", region, authorized, RegionUS)
	}
	req = httptest.NewRequest(http.MethodConnect, "http://example.test", nil)
	setProxyBasicAuth(req, "crawler", "secret")
	region, authorized = authenticated.authorizeAndSelectRegion(req)
	if !authorized || region != "" {
		t.Fatalf("global credentials = (%q, %t), want global authorization", region, authorized)
	}
}

func TestRouterConfiguredUsernameMayEndWithRegionSuffix(t *testing.T) {
	router := NewRouter(RouterConfig{Username: "crawler@jp", Password: "secret"}, nil)

	globalRequest := httptest.NewRequest(http.MethodConnect, "http://example.test", nil)
	setProxyBasicAuth(globalRequest, "crawler@jp", "secret")
	region, authorized := router.authorizeAndSelectRegion(globalRequest)
	if !authorized || region != "" {
		t.Fatalf("exact configured username = (%q, %t), want global authorization", region, authorized)
	}

	regionRequest := httptest.NewRequest(http.MethodConnect, "http://example.test", nil)
	setProxyBasicAuth(regionRequest, "crawler@jp@us", "secret")
	region, authorized = router.authorizeAndSelectRegion(regionRequest)
	if !authorized || region != RegionUS {
		t.Fatalf("configured username selector = (%q, %t), want (%q, true)", region, authorized, RegionUS)
	}
}

func TestRouterNeverTreatsOriginPathAsRegionSelector(t *testing.T) {
	router := NewRouter(RouterConfig{}, nil)
	req := httptest.NewRequest(http.MethodGet, "http://example.test/jp/article", nil)
	originalPath := req.URL.Path
	region, authorized := router.authorizeAndSelectRegion(req)
	if !authorized || region != "" {
		t.Fatalf("origin request selected region %q, authorized=%t", region, authorized)
	}
	if req.URL.Path != originalPath {
		t.Fatalf("origin path changed from %q to %q", originalPath, req.URL.Path)
	}
}

type routerTestDialer struct{}

func (routerTestDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("not used")
}

func TestRouterRemovePool(t *testing.T) {
	router := NewRouter(RouterConfig{}, nil)
	dialer := routerTestDialer{}
	router.SetPool(RegionUS, dialer)
	if _, exists := router.pools[RegionUS]; !exists {
		t.Fatal("region pool was not registered")
	}

	router.RemovePool(RegionUS)
	if _, exists := router.pools[RegionUS]; exists {
		t.Fatal("stale region pool was not removed")
	}

	// Removing an already absent pool is intentionally idempotent.
	router.RemovePool(RegionUS)
}

type singleConnDialer struct {
	conn net.Conn
}

func (d *singleConnDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	if d.conn == nil {
		return nil, errors.New("connection already used")
	}
	conn := d.conn
	d.conn = nil
	return conn, nil
}

func TestConnectTunnelPreservesPipelinedBytesAndHalfCloses(t *testing.T) {
	routerTarget, targetPeer := newTCPConnPair(t)
	dialer := &singleConnDialer{conn: routerTarget}
	router := NewRouter(RouterConfig{}, nil)
	router.SetGlobalPool(dialer)
	server := httptest.NewServer(router)
	defer server.Close()
	defer targetPeer.Close()

	proxyAddress := strings.TrimPrefix(server.URL, "http://")
	clientRaw, err := net.Dial("tcp", proxyAddress)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	client := clientRaw.(*net.TCPConn)
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set client deadline: %v", err)
	}
	if err := targetPeer.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set target deadline: %v", err)
	}

	const requestPayload = "pipelined-tunnel-request"
	if _, err := fmt.Fprintf(client, "CONNECT example.test:443 HTTP/1.1\r\nHost: example.test:443\r\n\r\n%s", requestPayload); err != nil {
		t.Fatalf("write CONNECT request: %v", err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatalf("half-close client write side: %v", err)
	}

	clientReader := bufio.NewReader(client)
	statusLine, err := clientReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read CONNECT status: %v", err)
	}
	if statusLine != "HTTP/1.1 200 Connection Established\r\n" {
		t.Fatalf("unexpected CONNECT status %q", statusLine)
	}
	for {
		line, err := clientReader.ReadString('\n')
		if err != nil {
			t.Fatalf("read CONNECT headers: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}

	gotRequest, err := io.ReadAll(targetPeer)
	if err != nil {
		t.Fatalf("read tunneled request through half-close: %v", err)
	}
	if string(gotRequest) != requestPayload {
		t.Fatalf("tunneled request = %q, want %q", gotRequest, requestPayload)
	}

	const responsePayload = "response-after-request-eof"
	if _, err := io.WriteString(targetPeer, responsePayload); err != nil {
		t.Fatalf("write tunneled response: %v", err)
	}
	if err := targetPeer.CloseWrite(); err != nil {
		t.Fatalf("half-close target write side: %v", err)
	}

	gotResponse, err := io.ReadAll(clientReader)
	if err != nil {
		t.Fatalf("read tunneled response through half-close: %v", err)
	}
	if string(gotResponse) != responsePayload {
		t.Fatalf("tunneled response = %q, want %q", gotResponse, responsePayload)
	}
}

func TestRelayConnectTunnelBoundsHalfCloseDrain(t *testing.T) {
	handlerClient, clientPeer := newTCPConnPair(t)
	handlerTarget, targetPeer := newTCPConnPair(t)
	defer handlerClient.Close()
	defer clientPeer.Close()
	defer handlerTarget.Close()
	defer targetPeer.Close()

	done := make(chan struct{})
	const drainTimeout = 40 * time.Millisecond
	started := time.Now()
	go func() {
		relayConnectTunnel(handlerClient, handlerTarget, nil, drainTimeout)
		close(done)
	}()

	if err := clientPeer.CloseWrite(); err != nil {
		t.Fatalf("half-close client write side: %v", err)
	}
	if err := targetPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set target deadline: %v", err)
	}
	if _, err := io.ReadAll(targetPeer); err != nil {
		t.Fatalf("target did not receive forwarded EOF: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay remained blocked after its bounded drain timeout")
	}
	if elapsed := time.Since(started); elapsed < drainTimeout {
		t.Fatalf("relay returned before drain timeout: %v", elapsed)
	}
}

func TestRouterServerHasBoundedHTTPParsing(t *testing.T) {
	router := NewRouter(RouterConfig{Listen: "127.0.0.1"}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := router.Start(ctx); err != nil {
		t.Fatalf("start router: %v", err)
	}
	defer router.Stop()

	if router.server.ReadHeaderTimeout <= 0 {
		t.Fatal("router HTTP header parsing must be bounded")
	}
	if router.server.ReadTimeout != 0 || router.server.WriteTimeout != 0 {
		t.Fatal("streaming proxy bodies must not have a whole-request deadline")
	}
	if router.server.IdleTimeout <= 0 {
		t.Fatal("router HTTP idle timeout must be bounded")
	}
	if router.server.MaxHeaderBytes <= 0 || router.server.MaxHeaderBytes > 64<<10 {
		t.Fatalf("unexpected MaxHeaderBytes: %d", router.server.MaxHeaderBytes)
	}
}

type directRouterDialer struct{ dialer net.Dialer }

func (d *directRouterDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d.dialer.DialContext(ctx, network, address)
}

func TestRouterRelaysHTTPUpgradeBidirectionally(t *testing.T) {
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	upstreamDone := make(chan error, 1)
	go func() {
		connection, acceptErr := upstream.Accept()
		if acceptErr != nil {
			upstreamDone <- acceptErr
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(connection)
		request, readErr := http.ReadRequest(reader)
		if readErr != nil {
			upstreamDone <- readErr
			return
		}
		if request.Header.Get("Upgrade") != "websocket" || !headerHasToken(request.Header, "Connection", "upgrade") {
			upstreamDone <- fmt.Errorf("upgrade headers were not forwarded: %#v", request.Header)
			return
		}
		if request.Header.Get("Proxy-Authorization") != "" {
			upstreamDone <- errors.New("proxy authorization leaked to the origin")
			return
		}
		if _, writeErr := io.WriteString(connection, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"); writeErr != nil {
			upstreamDone <- writeErr
			return
		}
		payload := make([]byte, len("client-frame"))
		if _, readErr := io.ReadFull(reader, payload); readErr != nil {
			upstreamDone <- readErr
			return
		}
		if string(payload) != "client-frame" {
			upstreamDone <- fmt.Errorf("upstream payload = %q", payload)
			return
		}
		_, writeErr := io.WriteString(connection, "server-frame")
		upstreamDone <- writeErr
	}()

	router := NewRouter(RouterConfig{}, nil)
	router.SetGlobalPool(&directRouterDialer{})
	proxy := httptest.NewServer(router)
	defer proxy.Close()
	client, err := net.Dial("tcp", strings.TrimPrefix(proxy.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := fmt.Fprintf(client, "GET http://%s/socket HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nProxy-Authorization: Basic ignored\r\n\r\nclient-frame", upstream.Addr(), upstream.Addr()); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(client)
	status, err := reader.ReadString('\n')
	if err != nil || status != "HTTP/1.1 101 Switching Protocols\r\n" {
		t.Fatalf("upgrade status=%q err=%v", status, err)
	}
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			t.Fatal(readErr)
		}
		if line == "\r\n" {
			break
		}
	}
	payload := make([]byte, len("server-frame"))
	if _, err := io.ReadFull(reader, payload); err != nil || string(payload) != "server-frame" {
		t.Fatalf("client payload=%q err=%v", payload, err)
	}
	if err := <-upstreamDone; err != nil {
		t.Fatal(err)
	}
}

func TestRouterConcurrentStartStopPublishesListenerAtomically(t *testing.T) {
	router := NewRouter(RouterConfig{Listen: "127.0.0.1"}, nil)
	listenEntered := make(chan struct{})
	releaseListen := make(chan struct{})
	ownedListener := make(chan net.Listener, 1)
	router.listen = func(network, address string) (net.Listener, error) {
		close(listenEntered)
		<-releaseListen
		listener, err := net.Listen(network, address)
		if err == nil {
			ownedListener <- listener
		}
		return listener, err
	}

	startResult := make(chan error, 1)
	go func() {
		startResult <- router.Start(context.Background())
	}()
	<-listenEntered

	stopResult := make(chan error, 1)
	stopInvoked := make(chan struct{})
	go func() {
		close(stopInvoked)
		stopResult <- router.Stop()
	}()
	<-stopInvoked
	select {
	case err := <-stopResult:
		t.Fatalf("Stop returned before Start published its listener: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(releaseListen)
	if err := <-startResult; err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	listener := <-ownedListener
	if err := <-stopResult; err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if conn, err := listener.Accept(); err == nil {
		conn.Close()
		t.Fatal("listener remained open after concurrent Stop")
	}

	router.lifecycleMu.Lock()
	state := router.state
	server := router.server
	storedListener := router.listener
	router.lifecycleMu.Unlock()
	if state != routerStateStopped || server != nil || storedListener != nil {
		t.Fatalf("inconsistent stopped lifecycle: state=%d server=%v listener=%v", state, server, storedListener)
	}
	if err := router.Start(context.Background()); !errors.Is(err, ErrRouterStopped) {
		t.Fatalf("Start after Stop error = %v, want ErrRouterStopped", err)
	}
}

func TestRouterRejectsRepeatedStartAndStopIsConcurrentIdempotent(t *testing.T) {
	router := NewRouter(RouterConfig{Listen: "127.0.0.1"}, nil)
	if err := router.Start(context.Background()); err != nil {
		t.Fatalf("first Start failed: %v", err)
	}
	if err := router.Start(context.Background()); !errors.Is(err, ErrRouterAlreadyStarted) {
		t.Fatalf("second Start error = %v, want ErrRouterAlreadyStarted", err)
	}

	const callers = 16
	startStops := make(chan struct{})
	results := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			ready.Done()
			<-startStops
			results <- router.Stop()
		}()
	}
	ready.Wait()
	close(startStops)
	for i := 0; i < callers; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent Stop failed: %v", err)
		}
	}
	if err := router.Stop(); err != nil {
		t.Fatalf("repeated Stop failed: %v", err)
	}
	if err := router.Start(context.Background()); !errors.Is(err, ErrRouterStopped) {
		t.Fatalf("Start after Stop error = %v, want ErrRouterStopped", err)
	}
}

func TestRouterStopBeforeStartPermanentlyClosesLifecycle(t *testing.T) {
	router := NewRouter(RouterConfig{Listen: "127.0.0.1"}, nil)
	if err := router.Stop(); err != nil {
		t.Fatalf("Stop before Start failed: %v", err)
	}
	if err := router.Stop(); err != nil {
		t.Fatalf("repeated Stop before Start failed: %v", err)
	}
	if err := router.Start(context.Background()); !errors.Is(err, ErrRouterStopped) {
		t.Fatalf("Start after Stop error = %v, want ErrRouterStopped", err)
	}
}

func newTCPConnPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for TCP pair: %v", err)
	}
	defer listener.Close()

	type acceptResult struct {
		conn net.Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		conn, err := listener.Accept()
		accepted <- acceptResult{conn: conn, err: err}
	}()

	peerRaw, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial TCP pair: %v", err)
	}
	result := <-accepted
	if result.err != nil {
		peerRaw.Close()
		t.Fatalf("accept TCP pair: %v", result.err)
	}
	return result.conn.(*net.TCPConn), peerRaw.(*net.TCPConn)
}
