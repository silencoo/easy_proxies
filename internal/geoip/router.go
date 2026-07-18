package geoip

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	connectHalfCloseDrainTimeout = 30 * time.Second
	connectForcedCloseWait       = time.Second
	routerShutdownTimeout        = 5 * time.Second
)

var (
	ErrRouterAlreadyStarted = errors.New("geoip router already started")
	ErrRouterStopped        = errors.New("geoip router has been stopped")
)

type routerLifecycleState uint8

const (
	routerStateNew routerLifecycleState = iota
	routerStateRunning
	routerStateStopping
	routerStateStopped
)

// RouterConfig holds configuration for the GeoIP router
type RouterConfig struct {
	Listen   string
	Port     uint16
	Username string
	Password string
}

// PoolDialer is an interface for dialing through a specific pool
type PoolDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// Router handles HTTP proxy requests with region selection encoded in standard
// proxy credentials. Origin paths are never interpreted as routing metadata.
type Router struct {
	cfg        RouterConfig
	pools      map[string]PoolDialer          // region -> dialer
	global     PoolDialer                     // default pool for requests without region path
	transports map[PoolDialer]*http.Transport // cached transports per dialer
	mu         sync.RWMutex
	logger     *log.Logger

	lifecycleMu sync.Mutex
	state       routerLifecycleState
	listen      func(network, address string) (net.Listener, error)
	listener    net.Listener
	server      *http.Server
	serveDone   chan struct{}
	stopDone    chan struct{}
	stopErr     error
}

// NewRouter creates a new GeoIP router
func NewRouter(cfg RouterConfig, logger *log.Logger) *Router {
	if logger == nil {
		logger = log.Default()
	}
	return &Router{
		cfg:        cfg,
		pools:      make(map[string]PoolDialer),
		transports: make(map[PoolDialer]*http.Transport),
		logger:     logger,
		listen:     net.Listen,
	}
}

// SetPool registers a pool dialer for a specific region
func (r *Router) SetPool(region string, dialer PoolDialer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pools[region] = dialer
	r.resetTransportsLocked()
}

// RemovePool unregisters a region whose refreshed classification has no
// members. Requests to that region fall back to the global pool.
func (r *Router) RemovePool(region string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.pools[region]; !exists {
		return
	}
	delete(r.pools, region)
	r.resetTransportsLocked()
}

// SetGlobalPool sets the default pool for requests without region path
func (r *Router) SetGlobalPool(dialer PoolDialer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.global = dialer
	r.resetTransportsLocked()
}

func (r *Router) resetTransportsLocked() {
	for _, transport := range r.transports {
		transport.CloseIdleConnections()
	}
	r.transports = make(map[PoolDialer]*http.Transport)
}

// Config returns a race-free snapshot of the listener and authentication
// settings currently served by the router.
func (r *Router) Config() RouterConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cfg
}

// UpdateCredentials changes proxy authentication without rebinding the
// listener. This makes password rotation transactional for an existing port.
func (r *Router) UpdateCredentials(username, password string) {
	r.mu.Lock()
	r.cfg.Username = username
	r.cfg.Password = password
	r.mu.Unlock()
}

// IsRunning reports whether the listener is currently published.
func (r *Router) IsRunning() bool {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	return r.state == routerStateRunning
}

// Start starts the GeoIP router HTTP server
func (r *Router) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.lifecycleMu.Lock()
	switch r.state {
	case routerStateRunning:
		r.lifecycleMu.Unlock()
		return ErrRouterAlreadyStarted
	case routerStateStopping, routerStateStopped:
		r.lifecycleMu.Unlock()
		return ErrRouterStopped
	}

	r.mu.RLock()
	cfg := r.cfg
	r.mu.RUnlock()
	addr := net.JoinHostPort(strings.Trim(strings.TrimSpace(cfg.Listen), "[]"), fmt.Sprint(cfg.Port))
	listen := r.listen
	if listen == nil {
		listen = net.Listen
	}
	// Keep the lifecycle lock while binding and publishing the server. Stop and
	// concurrent Start calls can therefore never observe a partially-started
	// router or consume shutdown before the listener is owned by the router.
	listener, err := listen("tcp", addr)
	if err != nil {
		r.lifecycleMu.Unlock()
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	if err := ctx.Err(); err != nil {
		_ = listener.Close()
		r.lifecycleMu.Unlock()
		return err
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
		// Forward-proxy request and response bodies may legitimately stream for
		// hours. Header and transport handshake deadlines bound setup without
		// terminating an established download, upload, SSE stream, or upgrade.
		ReadTimeout:    0,
		WriteTimeout:   0,
		IdleTimeout:    90 * time.Second,
		MaxHeaderBytes: 64 << 10,
	}
	serveDone := make(chan struct{})
	r.listener = listener
	r.server = server
	r.serveDone = serveDone
	r.state = routerStateRunning
	r.lifecycleMu.Unlock()

	go func() {
		r.logger.Printf("🌐 GeoIP Router started on %s", addr)
		r.logger.Println("   Region selector: proxy username <region>, or <username>@<region> when authentication is configured")
		err := server.Serve(listener)
		close(serveDone)
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			r.logger.Printf("GeoIP router error: %v", err)
		}
		// Normalize an unexpected Serve exit into the same stopped state and
		// cleanup path as an explicit Stop.
		_ = r.Stop()
	}()

	if ctxDone := ctx.Done(); ctxDone != nil {
		go func() {
			select {
			case <-ctxDone:
				_ = r.Stop()
			case <-serveDone:
			}
		}()
	}

	return nil
}

// Stop stops the GeoIP router
func (r *Router) Stop() error {
	r.lifecycleMu.Lock()
	switch r.state {
	case routerStateStopped:
		err := r.stopErr
		r.lifecycleMu.Unlock()
		return err
	case routerStateStopping:
		done := r.stopDone
		r.lifecycleMu.Unlock()
		<-done
		r.lifecycleMu.Lock()
		err := r.stopErr
		r.lifecycleMu.Unlock()
		return err
	}

	r.state = routerStateStopping
	stopDone := make(chan struct{})
	r.stopDone = stopDone
	server := r.server
	listener := r.listener
	serveDone := r.serveDone
	r.lifecycleMu.Unlock()

	stopErr := stopRouterServer(server, listener, serveDone)
	r.mu.Lock()
	r.resetTransportsLocked()
	r.mu.Unlock()

	r.lifecycleMu.Lock()
	r.server = nil
	r.listener = nil
	r.serveDone = nil
	r.stopErr = stopErr
	r.state = routerStateStopped
	close(stopDone)
	r.lifecycleMu.Unlock()
	return stopErr
}

func stopRouterServer(server *http.Server, listener net.Listener, serveDone <-chan struct{}) error {
	var stopErr error
	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), routerShutdownTimeout)
		stopErr = server.Shutdown(ctx)
		cancel()
		if errors.Is(stopErr, http.ErrServerClosed) {
			stopErr = nil
		}
		if stopErr != nil {
			if err := server.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				stopErr = errors.Join(stopErr, err)
			}
		}
	}
	if listener != nil {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			stopErr = errors.Join(stopErr, err)
		}
	}
	if serveDone != nil {
		timer := time.NewTimer(routerShutdownTimeout)
		select {
		case <-serveDone:
			timer.Stop()
		case <-timer.C:
			stopErr = errors.Join(stopErr, errors.New("timed out waiting for geoip router server to stop"))
		}
	}
	return stopErr
}

func proxyCredentials(req *http.Request) (username, password string, ok bool) {
	auth := req.Header.Get("Proxy-Authorization")
	if auth == "" {
		return "", "", false
	}
	const prefix = "Basic "
	if !strings.HasPrefix(auth, prefix) {
		return "", "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(auth[len(prefix):])
	if err != nil {
		return "", "", false
	}
	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func splitRegionUsername(username string) (base, region string) {
	lower := strings.ToLower(username)
	for _, candidate := range AllRegions() {
		if lower == candidate {
			return "", candidate
		}
		suffix := "@" + candidate
		if strings.HasSuffix(lower, suffix) {
			return username[:len(username)-len(suffix)], candidate
		}
	}
	return username, ""
}

// authorizeAndSelectRegion validates standard HTTP proxy credentials and
// extracts an optional region selector. With configured authentication, use
// <username>@<region> and the configured password. Without authentication,
// a region username (for example jp) selects that pool.
func (r *Router) authorizeAndSelectRegion(req *http.Request) (string, bool) {
	r.mu.RLock()
	cfg := r.cfg
	r.mu.RUnlock()
	username, password, provided := proxyCredentials(req)
	authRequired := cfg.Username != "" || cfg.Password != ""
	if !authRequired {
		base, region := splitRegionUsername(username)
		if provided && base != "" {
			return "", true
		}
		return region, true
	}
	if !provided {
		return "", false
	}
	// An exact configured username always selects the global pool. This check
	// must run before suffix parsing because legitimate usernames (for example
	// an account named "crawler@jp") may themselves end in a region suffix.
	if subtle.ConstantTimeCompare([]byte(username), []byte(cfg.Username)) == 1 &&
		subtle.ConstantTimeCompare([]byte(password), []byte(cfg.Password)) == 1 {
		return "", true
	}
	base := username
	region := ""
	lower := strings.ToLower(username)
	for _, candidate := range AllRegions() {
		suffix := "@" + candidate
		if strings.HasSuffix(lower, suffix) {
			base = username[:len(username)-len(suffix)]
			region = candidate
			break
		}
	}
	if region == "" || subtle.ConstantTimeCompare([]byte(base), []byte(cfg.Username)) != 1 ||
		subtle.ConstantTimeCompare([]byte(password), []byte(cfg.Password)) != 1 {
		return "", false
	}
	return region, true
}

// ServeHTTP handles incoming HTTP proxy requests
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	region, authorized := r.authorizeAndSelectRegion(req)
	if !authorized {
		w.Header().Set("Proxy-Authenticate", `Basic realm="Proxy"`)
		http.Error(w, "Proxy authentication required", http.StatusProxyAuthRequired)
		return
	}

	// Standard proxy requests always carry the destination in Host/authority.
	// Never consume URL.Path: it belongs to the origin server and may legally
	// begin with a region-like segment such as /jp/.
	targetHost := req.Host

	// Get the appropriate pool
	r.mu.RLock()
	var dialer PoolDialer
	if region != "" {
		dialer = r.pools[region]
	}
	if dialer == nil {
		dialer = r.global
	}
	r.mu.RUnlock()

	if dialer == nil {
		http.Error(w, "No proxy pool available", http.StatusServiceUnavailable)
		return
	}

	if req.Method == http.MethodConnect {
		r.handleConnect(w, req, dialer, targetHost)
	} else {
		r.handleHTTP(w, req, dialer, targetHost)
	}
}

// handleConnect handles HTTPS CONNECT tunneling
func (r *Router) handleConnect(w http.ResponseWriter, req *http.Request, dialer PoolDialer, targetHost string) {
	ctx, cancel := context.WithTimeout(req.Context(), 30*time.Second)
	targetConn, err := dialer.DialContext(ctx, "tcp", targetHost)
	cancel()
	if err != nil {
		http.Error(w, "Failed to connect", http.StatusBadGateway)
		return
	}
	defer targetConn.Close()

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return
	}

	clientConn, readWriter, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, fmt.Sprintf("Hijack failed: %v", err), http.StatusInternalServerError)
		return
	}
	defer clientConn.Close()

	// net/http may leave ReadTimeout/WriteTimeout deadlines on a hijacked
	// connection. A CONNECT tunnel owns its lifetime, so clear those deadlines.
	if err := clientConn.SetDeadline(time.Time{}); err != nil {
		return
	}
	if err := targetConn.SetDeadline(time.Time{}); err != nil {
		return
	}

	// Use the hijacker's buffered reader: a client is allowed to pipeline tunnel
	// bytes in the same packet as the CONNECT request.
	if _, err := readWriter.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := readWriter.Flush(); err != nil {
		return
	}

	relayConnectTunnel(clientConn, targetConn, readWriter.Reader, connectHalfCloseDrainTimeout)
}

// relayConnectTunnel copies a CONNECT stream in both directions. When one
// direction reaches EOF (or otherwise stops), it half-closes the destination's
// write side so the peer can observe EOF while the reverse direction drains.
// The drain is bounded so an uncooperative peer cannot retain the handler and
// its goroutines forever after one side has already finished.
func relayConnectTunnel(clientConn, targetConn net.Conn, clientReader *bufio.Reader, drainTimeout time.Duration) {
	if clientReader == nil {
		clientReader = bufio.NewReader(clientConn)
	}

	done := make(chan struct{}, 2)
	copyDirection := func(dst net.Conn, src io.Reader, srcConn net.Conn) {
		_, _ = io.Copy(dst, src)
		closeWrite(dst)
		closeRead(srcConn)
		done <- struct{}{}
	}

	go copyDirection(targetConn, clientReader, clientConn)
	go copyDirection(clientConn, targetConn, targetConn)

	<-done
	timer := time.NewTimer(drainTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
	}

	// Closing a real network connection interrupts pending reads and writes.
	// Set an immediate deadline first as an additional safeguard for wrappers.
	now := time.Now()
	_ = clientConn.SetDeadline(now)
	_ = targetConn.SetDeadline(now)
	_ = clientConn.Close()
	_ = targetConn.Close()

	forcedWait := time.NewTimer(connectForcedCloseWait)
	defer forcedWait.Stop()
	select {
	case <-done:
	case <-forcedWait.C:
	}
}

func closeWrite(conn net.Conn) {
	if halfCloser, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = halfCloser.CloseWrite()
	}
}

func closeRead(conn net.Conn) {
	if halfCloser, ok := conn.(interface{ CloseRead() error }); ok {
		_ = halfCloser.CloseRead()
	}
}

// getTransport returns a cached http.Transport for the given dialer, creating one if needed.
func (r *Router) getTransport(dialer PoolDialer) *http.Transport {
	r.mu.RLock()
	t, ok := r.transports[dialer]
	r.mu.RUnlock()
	if ok {
		return t
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Double-check after acquiring write lock
	if t, ok = r.transports[dialer]; ok {
		return t
	}
	t = &http.Transport{
		DialContext:           dialer.DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	r.transports[dialer] = t
	return t
}

// handleHTTP handles regular HTTP requests
func (r *Router) handleHTTP(w http.ResponseWriter, req *http.Request, dialer PoolDialer, targetHost string) {
	// Clone both request and URL: mutating req.URL would corrupt middleware or
	// access-log state retained by net/http after this handler returns.
	targetURL := *req.URL
	if targetURL.Host == "" {
		targetURL.Host = targetHost
	}
	if targetURL.Scheme == "" {
		targetURL.Scheme = "http"
	}

	outReq := req.Clone(req.Context())
	outReq.URL = &targetURL
	outReq.RequestURI = ""
	outReq.Host = req.Host
	requestedUpgrade := headerUpgradeType(req.Header)
	removeHopByHopHeaders(outReq.Header)
	if requestedUpgrade != "" {
		outReq.Header.Set("Connection", "Upgrade")
		outReq.Header.Set("Upgrade", requestedUpgrade)
	}

	// Use cached transport with connection pooling
	transport := r.getTransport(dialer)

	resp, err := transport.RoundTrip(outReq)
	if err != nil {
		http.Error(w, "Request failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusSwitchingProtocols {
		r.handleUpgradeResponse(w, requestedUpgrade, resp)
		return
	}

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	removeHopByHopHeaders(w.Header())

	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func headerUpgradeType(header http.Header) string {
	if !headerHasToken(header, "Connection", "upgrade") {
		return ""
	}
	return strings.TrimSpace(header.Get("Upgrade"))
}

func headerHasToken(header http.Header, name, token string) bool {
	for _, value := range header.Values(name) {
		for _, candidate := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(candidate), token) {
				return true
			}
		}
	}
	return false
}

func (r *Router) handleUpgradeResponse(w http.ResponseWriter, requestedUpgrade string, response *http.Response) {
	responseUpgrade := strings.TrimSpace(response.Header.Get("Upgrade"))
	if requestedUpgrade == "" || responseUpgrade == "" || !strings.EqualFold(requestedUpgrade, responseUpgrade) {
		http.Error(w, "Invalid upgrade response", http.StatusBadGateway)
		return
	}
	backend, ok := response.Body.(io.ReadWriteCloser)
	if !ok {
		http.Error(w, "Upgrade transport is unavailable", http.StatusBadGateway)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return
	}
	client, buffered, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer client.Close()
	defer backend.Close()
	_ = client.SetDeadline(time.Time{})

	response.Header.Del("Proxy-Authenticate")
	response.Header.Del("Proxy-Authorization")
	response.Header.Set("Connection", "Upgrade")
	response.Header.Set("Upgrade", responseUpgrade)
	if _, err := fmt.Fprintf(buffered, "HTTP/1.1 %s\r\n", response.Status); err != nil {
		return
	}
	if err := response.Header.Write(buffered); err != nil {
		return
	}
	if _, err := buffered.WriteString("\r\n"); err != nil {
		return
	}
	if err := buffered.Flush(); err != nil {
		return
	}

	relayUpgradeTunnel(client, backend, buffered.Reader)
}

func relayUpgradeTunnel(client net.Conn, backend io.ReadWriteCloser, clientReader io.Reader) {
	if clientReader == nil {
		clientReader = client
	}
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(backend, clientReader)
		if closer, ok := backend.(interface{ CloseWrite() error }); ok {
			_ = closer.CloseWrite()
		}
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, backend)
		closeWrite(client)
		done <- struct{}{}
	}()
	<-done
	_ = client.Close()
	_ = backend.Close()
	select {
	case <-done:
	case <-time.After(connectForcedCloseWait):
	}
}

func removeHopByHopHeaders(header http.Header) {
	for _, connectionValue := range header.Values("Connection") {
		for _, token := range strings.Split(connectionValue, ",") {
			if token = strings.TrimSpace(token); token != "" {
				header.Del(token)
			}
		}
	}
	for _, name := range []string{
		"Connection",
		"Proxy-Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Te",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
	} {
		header.Del(name)
	}
}
