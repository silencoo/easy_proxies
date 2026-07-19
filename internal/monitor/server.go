package monitor

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/geoip"
)

//go:embed assets/*
var embeddedFS embed.FS

// Session represents a user session with expiration.
type Session struct {
	Token     string
	CreatedAt time.Time
	ExpiresAt time.Time
}

// NodeManager exposes config node CRUD and reload operations.
type NodeManager interface {
	ListConfigNodes(ctx context.Context) ([]config.NodeConfig, error)
	CreateNode(ctx context.Context, node config.NodeConfig) (config.NodeConfig, error)
	UpdateNode(ctx context.Context, name string, node config.NodeConfig) (config.NodeConfig, error)
	DeleteNode(ctx context.Context, name string) error
	TriggerReload(ctx context.Context) error
	ConfigSnapshot() (*config.Config, uint64)
	CommitConfig(
		ctx context.Context,
		expectedRevision uint64,
		candidate *config.Config,
		persist func(*config.Config) (rollback func() error, err error),
	) error
}

// Sentinel errors for node operations.
var (
	ErrNodeNotFound                       = errors.New("节点不存在")
	ErrNodeConflict                       = errors.New("节点名称或端口已存在")
	ErrInvalidNode                        = errors.New("无效的节点配置")
	ErrSubscriptionConfigRevisionConflict = errors.New("subscription configuration revision conflict")
)

// SubscriptionRefresher interface for subscription manager.
type SubscriptionRefresher interface {
	RefreshNow() error
	Status() SubscriptionStatus
	UpdateConfig(urls []string, enabled bool, interval time.Duration)
	UpdateConfigAndRefresh(urls []string, enabled bool, interval time.Duration, fetchConcurrency int, allowPrivateNetworks bool) error
	UpdateConfigAndRefreshAtRevision(urls []string, enabled bool, interval time.Duration, fetchConcurrency int, allowPrivateNetworks bool, expectedRevision uint64) error
}

const (
	maxSubscriptionConfigBodyBytes int64 = 512 * 1024
	maxSettingsBodyBytes           int64 = 512 * 1024
	maxAuthBodyBytes               int64 = 16 * 1024
	maxNodeConfigBodyBytes         int64 = 64 * 1024
)

type settingsUpdateRequest struct {
	ExternalIP       *string `json:"external_ip,omitempty"`
	ProbeTarget      *string `json:"probe_target,omitempty"`
	SkipCertVerify   *bool   `json:"skip_cert_verify,omitempty"`
	ProbeConcurrency *int    `json:"probe_concurrency,omitempty"`
	Mode             *string `json:"mode,omitempty"`
	Listener         *struct {
		Address  string `json:"address"`
		Port     uint16 `json:"port"`
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"listener,omitempty"`
	MultiPort *struct {
		Address        string  `json:"address"`
		BasePort       uint16  `json:"base_port"`
		Username       string  `json:"username"`
		Password       string  `json:"password"`
		PortMapFile    *string `json:"port_map_file"`
		PortReuseDelay string  `json:"port_reuse_delay"`
	} `json:"multi_port,omitempty"`
	Pool *struct {
		Mode              string `json:"mode"`
		FailureThreshold  int    `json:"failure_threshold"`
		BlacklistDuration string `json:"blacklist_duration"`
		FailOpen          *bool  `json:"fail_open"`
		RetryEnabled      *bool  `json:"retry_enabled"`
		RetryAttempts     int    `json:"retry_attempts"`
		TransientCooldown string `json:"transient_cooldown"`
		LatencySampleSize int    `json:"latency_sample_size"`
		LatencyTolerance  string `json:"latency_tolerance"`
		Sticky            *struct {
			Enabled    bool   `json:"enabled"`
			TTL        string `json:"ttl"`
			MaxEntries int    `json:"max_entries"`
		} `json:"sticky"`
	} `json:"pool,omitempty"`
	Management *struct {
		Listen           *string `json:"listen"`
		Password         *string `json:"password"`
		ProbeConcurrency *int    `json:"probe_concurrency"`
		TLSCertFile      *string `json:"tls_cert_file"`
		TLSKeyFile       *string `json:"tls_key_file"`
	} `json:"management,omitempty"`
	Log *struct {
		Output     string `json:"output"`
		MaxSize    int    `json:"max_size"`
		MaxBackups int    `json:"max_backups"`
		MaxAge     int    `json:"max_age"`
		Compress   bool   `json:"compress"`
	} `json:"log,omitempty"`
	GeoIP *struct {
		Enabled            bool   `json:"enabled"`
		DatabasePath       string `json:"database_path"`
		Listen             string `json:"listen"`
		Port               uint16 `json:"port"`
		ExitIPURL          string `json:"exit_ip_url"`
		ExitIPTimeout      string `json:"exit_ip_timeout"`
		ExitIPConcurrency  int    `json:"exit_ip_concurrency"`
		AutoUpdateEnabled  bool   `json:"auto_update_enabled"`
		AutoUpdateInterval string `json:"auto_update_interval"`
	} `json:"geoip,omitempty"`
}

// SubscriptionStatus represents subscription refresh status.
type SubscriptionStatus struct {
	LastRefresh   time.Time `json:"last_refresh"`
	NextRefresh   time.Time `json:"next_refresh"`
	NodeCount     int       `json:"node_count"`
	LastError     string    `json:"last_error,omitempty"`
	RefreshCount  int       `json:"refresh_count"`
	IsRefreshing  bool      `json:"is_refreshing"`
	NodesModified bool      `json:"nodes_modified"` // True if nodes.txt was modified since last refresh
}

// Server exposes HTTP endpoints for monitoring.
type Server struct {
	cfg            Config
	cfgMu          sync.RWMutex   // 保护动态配置字段
	cfgSrc         *config.Config // 可持久化的配置对象
	authGeneration uint64         // 实时管理密码的变更代次
	mgr            *Manager
	srv            *http.Server
	logger         *log.Logger

	// Session management
	sessionMu  sync.RWMutex
	sessions   map[string]*Session
	sessionTTL time.Duration
	sessionCtx context.Context
	sessionEnd context.CancelFunc
	sessionWG  sync.WaitGroup
	stopOnce   sync.Once
	authGuard  authRequestGuard

	depsMu       sync.RWMutex
	subRefresher SubscriptionRefresher
	nodeMgr      NodeManager

	trafficHTTPClient *http.Client
	trafficURL        string
}

const (
	maxActiveSessions           = 1024
	defaultAuthRateEntries      = 2048
	defaultAuthRateBurst        = 5
	defaultAuthRateRefill       = 30 * time.Second
	defaultAuthRateEntryTTL     = 15 * time.Minute
	defaultAuthMaxConcurrent    = 32
	managementReadHeaderTimeout = 5 * time.Second
	managementReadTimeout       = 30 * time.Second
	managementIdleTimeout       = 60 * time.Second
	managementMaxHeaderBytes    = 64 << 10
)

type authRateEntry struct {
	tokens     float64
	lastRefill time.Time
	lastSeen   time.Time
}

// authRequestGuard bounds both per-client authentication attempts and the
// total number of authentication handlers allowed to run concurrently. Its
// source key is the TCP peer address; forwarding headers are intentionally
// ignored because the management server does not establish trusted proxies.
type authRequestGuard struct {
	mu            sync.Mutex
	clients       map[string]*authRateEntry
	inFlight      int
	maxEntries    int
	burst         int
	refill        time.Duration
	entryTTL      time.Duration
	maxConcurrent int
}

func (g *authRequestGuard) defaultsLocked() {
	if g.clients == nil {
		g.clients = make(map[string]*authRateEntry)
	}
	if g.maxEntries <= 0 {
		g.maxEntries = defaultAuthRateEntries
	}
	if g.burst <= 0 {
		g.burst = defaultAuthRateBurst
	}
	if g.refill <= 0 {
		g.refill = defaultAuthRateRefill
	}
	if g.entryTTL <= 0 {
		g.entryTTL = defaultAuthRateEntryTTL
	}
	if g.maxConcurrent <= 0 {
		g.maxConcurrent = defaultAuthMaxConcurrent
	}
}

// begin reserves one authentication attempt and one global concurrency slot.
// The returned release function must be called exactly once when allowed.
func (g *authRequestGuard) begin(remoteAddr string, now time.Time) (release func(), retryAfter time.Duration, allowed bool) {
	key := authRemoteKey(remoteAddr)
	g.mu.Lock()
	g.defaultsLocked()
	g.pruneExpiredLocked(now)
	if g.inFlight >= g.maxConcurrent {
		g.mu.Unlock()
		return nil, time.Second, false
	}

	entry := g.clients[key]
	if entry == nil {
		if len(g.clients) >= g.maxEntries {
			g.evictOldestLocked()
		}
		entry = &authRateEntry{
			tokens:     float64(g.burst),
			lastRefill: now,
			lastSeen:   now,
		}
		g.clients[key] = entry
	} else if elapsed := now.Sub(entry.lastRefill); elapsed > 0 {
		entry.tokens += float64(elapsed) / float64(g.refill)
		if entry.tokens > float64(g.burst) {
			entry.tokens = float64(g.burst)
		}
		entry.lastRefill = now
	}
	entry.lastSeen = now
	if entry.tokens < 1 {
		missing := 1 - entry.tokens
		retryAfter = time.Duration(missing * float64(g.refill))
		if retryAfter < time.Second {
			retryAfter = time.Second
		}
		g.mu.Unlock()
		return nil, retryAfter, false
	}
	entry.tokens--
	g.inFlight++
	g.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			if g.inFlight > 0 {
				g.inFlight--
			}
			g.mu.Unlock()
		})
	}, 0, true
}

func (g *authRequestGuard) reset(remoteAddr string) {
	g.mu.Lock()
	delete(g.clients, authRemoteKey(remoteAddr))
	g.mu.Unlock()
}

func (g *authRequestGuard) pruneExpiredLocked(now time.Time) {
	for key, entry := range g.clients {
		if !entry.lastSeen.Add(g.entryTTL).After(now) {
			delete(g.clients, key)
		}
	}
}

func (g *authRequestGuard) evictOldestLocked() {
	var oldestKey string
	var oldestTime time.Time
	for key, entry := range g.clients {
		if oldestKey == "" || entry.lastSeen.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.lastSeen
		}
	}
	if oldestKey != "" {
		delete(g.clients, oldestKey)
	}
}

func authRemoteKey(remoteAddr string) string {
	host := requestHostname(remoteAddr)
	if addr, err := netip.ParseAddr(host); err == nil {
		return addr.Unmap().String()
	}
	// A malformed TCP peer address should not be able to grow the table with
	// attacker-controlled keys. Real net/http connections always provide an IP.
	return "unknown"
}

// NewServer constructs a server; it can be nil when disabled.
func NewServer(cfg Config, mgr *Manager, logger *log.Logger) *Server {
	if !cfg.Enabled || mgr == nil {
		return nil
	}
	if logger == nil {
		logger = log.Default()
	}

	sessionCtx, sessionEnd := context.WithCancel(context.Background())
	s := &Server{
		cfg:               cfg,
		mgr:               mgr,
		logger:            logger,
		sessions:          make(map[string]*Session),
		sessionTTL:        24 * time.Hour,
		sessionCtx:        sessionCtx,
		sessionEnd:        sessionEnd,
		trafficHTTPClient: http.DefaultClient,
		trafficURL:        "http://127.0.0.1:9092/traffic",
	}

	// Start session cleanup goroutine
	s.sessionWG.Add(1)
	go s.cleanupExpiredSessions()

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/assets/echarts.min.js", s.handleEChartsAsset)
	mux.HandleFunc("/api/auth", s.handleAuth)
	mux.HandleFunc("/api/settings", s.withAuth(s.handleSettings))
	mux.HandleFunc("/api/nodes", s.withAuth(s.handleNodes))
	mux.HandleFunc("/api/nodes/config", s.withAuth(s.handleConfigNodes))
	mux.HandleFunc("/api/nodes/config/", s.withAuth(s.handleConfigNodeItem))
	mux.HandleFunc("/api/nodes/probe-all", s.withAuth(s.handleProbeAll))
	mux.HandleFunc("/api/nodes/", s.withAuth(s.handleNodeAction))
	mux.HandleFunc("/api/debug", s.withAuth(s.handleDebug))
	mux.HandleFunc("/api/export", s.withAuth(s.handleExport))
	mux.HandleFunc("/api/subscription/status", s.withAuth(s.handleSubscriptionStatus))
	mux.HandleFunc("/api/subscription/refresh", s.withAuth(s.handleSubscriptionRefresh))
	mux.HandleFunc("/api/subscription/config", s.withAuth(s.handleSubscriptionConfig))
	mux.HandleFunc("/api/reload", s.withAuth(s.handleReload))
	mux.HandleFunc("/api/traffic", s.withAuth(s.handleTraffic))
	mux.HandleFunc("/api/logs", s.withAuth(s.handleLogs))
	s.srv = &http.Server{
		Addr:              cfg.Listen,
		Handler:           mux,
		ReadHeaderTimeout: managementReadHeaderTimeout,
		ReadTimeout:       managementReadTimeout,
		IdleTimeout:       managementIdleTimeout,
		MaxHeaderBytes:    managementMaxHeaderBytes,
	}
	return s
}

// SetSubscriptionRefresher sets the subscription refresher for API endpoints.
func (s *Server) SetSubscriptionRefresher(sr SubscriptionRefresher) {
	if s != nil {
		s.depsMu.Lock()
		s.subRefresher = sr
		s.depsMu.Unlock()
	}
}

// SetNodeManager enables config-node CRUD endpoints.
func (s *Server) SetNodeManager(nm NodeManager) {
	if s != nil {
		s.depsMu.Lock()
		s.nodeMgr = nm
		s.depsMu.Unlock()
	}
}

func (s *Server) subscriptionRefresher() SubscriptionRefresher {
	if s == nil {
		return nil
	}
	s.depsMu.RLock()
	defer s.depsMu.RUnlock()
	return s.subRefresher
}

func (s *Server) nodeManager() NodeManager {
	if s == nil {
		return nil
	}
	s.depsMu.RLock()
	defer s.depsMu.RUnlock()
	return s.nodeMgr
}

// SetConfig binds the persistable config object for settings API.
func (s *Server) SetConfig(cfg *config.Config) {
	if s == nil {
		return
	}
	s.cfgMu.Lock()
	passwordChanged := false
	ownedCfg := cfg.Clone()
	s.cfgSrc = ownedCfg
	if ownedCfg != nil {
		// The HTTP listener cannot be rebound from inside the request that is
		// changing it. Keep the current listener's authentication policy until a
		// process restart whenever the persisted listen address differs.
		managementRestartRequired := managementRuntimeChanged(s.cfg, ownedCfg.Management)
		if !managementRestartRequired {
			passwordChanged = s.cfg.Password != ownedCfg.Management.Password
			if passwordChanged {
				s.authGeneration++
			}
			s.cfg.Password = ownedCfg.Management.Password
		}
		s.cfg.ExternalIP = ownedCfg.ExternalIP
		s.cfg.ProbeTarget = ownedCfg.Management.ProbeTarget
		s.cfg.SkipCertVerify = ownedCfg.SkipCertVerify
		s.cfg.ProbeConcurrency = ownedCfg.ProbeConcurrencyOrDefault()
		if s.mgr != nil {
			_ = s.mgr.SetProbeTarget(ownedCfg.Management.ProbeTarget, ownedCfg.SkipCertVerify)
			s.mgr.SetProbeConcurrency(ownedCfg.ProbeConcurrencyOrDefault())
		}
		// Sync proxy credentials based on mode
		if ownedCfg.Mode == "multi-port" || ownedCfg.Mode == "hybrid" {
			s.cfg.ProxyUsername = ownedCfg.MultiPort.Username
			s.cfg.ProxyPassword = ownedCfg.MultiPort.Password
		} else {
			s.cfg.ProxyUsername = ownedCfg.Listener.Username
			s.cfg.ProxyPassword = ownedCfg.Listener.Password
		}
	}
	s.cfgMu.Unlock()
	if passwordChanged {
		s.invalidateSessions()
	}
}

func sameManagementListen(first, second string) bool {
	return strings.EqualFold(strings.TrimSpace(first), strings.TrimSpace(second))
}

func managementRuntimeChanged(runtime Config, candidate config.ManagementConfig) bool {
	return !sameManagementListen(runtime.Listen, candidate.Listen) ||
		filepath.Clean(strings.TrimSpace(runtime.TLSCertFile)) != filepath.Clean(strings.TrimSpace(candidate.TLSCertFile)) ||
		filepath.Clean(strings.TrimSpace(runtime.TLSKeyFile)) != filepath.Clean(strings.TrimSpace(candidate.TLSKeyFile))
}

func (s *Server) runtimeConfigSnapshot() Config {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg
}

// getSettings returns current dynamic settings (thread-safe).
func (s *Server) getSettings() (externalIP, probeTarget string, skipCertVerify bool, logCfg config.LogConfig) {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	logCfg = config.LogConfig{}
	if s.cfgSrc != nil {
		logCfg = s.cfgSrc.Log
	}
	return s.cfg.ExternalIP, s.cfg.ProbeTarget, s.cfg.SkipCertVerify, logCfg
}

// updateSettings updates dynamic settings and persists to config file.
func (s *Server) updateSettings(externalIP, probeTarget string, skipCertVerify bool, probeConcurrency int, logCfg *config.LogConfig, geoipEnabled bool) error {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()

	s.cfg.ExternalIP = externalIP
	s.cfg.ProbeTarget = probeTarget
	s.cfg.SkipCertVerify = skipCertVerify
	s.cfg.ProbeConcurrency = clampProbeConcurrency(probeConcurrency)

	if s.cfgSrc == nil {
		return errors.New("配置存储未初始化")
	}

	s.cfgSrc.ExternalIP = externalIP
	s.cfgSrc.Management.ProbeTarget = probeTarget
	s.cfgSrc.Management.ProbeConcurrency = clampProbeConcurrency(probeConcurrency)
	s.cfgSrc.SkipCertVerify = skipCertVerify
	s.mgr.SetProbeTarget(probeTarget, skipCertVerify)
	s.mgr.SetProbeConcurrency(probeConcurrency)

	// GeoIP settings
	s.cfgSrc.GeoIP.Enabled = geoipEnabled
	if geoipEnabled && s.cfgSrc.GeoIP.DatabasePath == "" {
		s.cfgSrc.GeoIP.DatabasePath = "./GeoLite2-Country.mmdb"
		s.cfgSrc.GeoIP.AutoUpdateEnabled = true
		s.cfgSrc.GeoIP.AutoUpdateInterval = 24 * time.Hour
	}

	if logCfg != nil {
		s.cfgSrc.Log.Output = logCfg.Output
		if logCfg.MaxSize > 0 {
			s.cfgSrc.Log.MaxSize = logCfg.MaxSize
		}
		if logCfg.MaxBackups > 0 {
			s.cfgSrc.Log.MaxBackups = logCfg.MaxBackups
		}
		if logCfg.MaxAge > 0 {
			s.cfgSrc.Log.MaxAge = logCfg.MaxAge
		}
		s.cfgSrc.Log.Compress = logCfg.Compress
	}

	if err := s.cfgSrc.SaveSettings(); err != nil {
		return fmt.Errorf("保存配置失败: %w", err)
	}
	return nil
}

// Start binds the management listener synchronously, then serves it in the
// background. Callers therefore see bind failures and can roll back startup.
func (s *Server) Start(ctx context.Context) error {
	if s == nil || s.srv == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	enabled := true
	if err := config.ValidateManagementConfig(config.ManagementConfig{
		Enabled:     &enabled,
		Listen:      s.cfg.Listen,
		Password:    s.cfg.Password,
		TLSCertFile: s.cfg.TLSCertFile,
		TLSKeyFile:  s.cfg.TLSKeyFile,
	}); err != nil {
		return err
	}
	var tlsConfig *tls.Config
	if strings.TrimSpace(s.cfg.TLSCertFile) != "" || strings.TrimSpace(s.cfg.TLSKeyFile) != "" {
		certificate, certErr := tls.LoadX509KeyPair(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
		if certErr != nil {
			return fmt.Errorf("load management TLS certificate: %w", certErr)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	}
	listener, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		s.Shutdown(shutdownCtx)
		cancel()
		return fmt.Errorf("listen on %s: %w", s.srv.Addr, err)
	}
	if tlsConfig != nil {
		s.srv.TLSConfig = tlsConfig
		listener = tls.NewListener(listener, tlsConfig)
	}
	s.logger.Printf("Starting monitor server on %s", s.cfg.Listen)
	go func() {
		if err := s.srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			s.logger.Printf("❌ Monitor server error: %v", err)
		}
	}()
	scheme := "http"
	if tlsConfig != nil {
		scheme = "https"
	}
	s.logger.Printf("✅ Monitor server started on %s://%s", scheme, s.cfg.Listen)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.Shutdown(shutdownCtx)
	}()
	return nil
}

// Shutdown stops the server gracefully.
func (s *Server) Shutdown(ctx context.Context) {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		if s.sessionEnd != nil {
			s.sessionEnd()
		}
		s.sessionWG.Wait()
	})
	if s.srv == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_ = s.srv.Shutdown(ctx)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	data, err := embeddedFS.ReadFile("assets/index.html")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	setManagementSecurityHeaders(w)
	w.Header().Set("X-Frame-Options", "DENY")
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(data)
}

func (s *Server) handleEChartsAsset(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/assets/echarts.min.js" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	data, err := embeddedFS.ReadFile("assets/echarts.min.js")
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	setManagementSecurityHeaders(w)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(data)
}

func setManagementSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONMethodNotAllowed(w, http.MethodGet)
		return
	}
	// 只返回初始检查通过的可用节点
	filtered := s.mgr.SnapshotFiltered(true)
	allNodes := s.mgr.Snapshot()
	totalNodes := len(allNodes)
	for index := range filtered {
		filtered[index].Region = displayRegion(filtered[index])
	}

	// Calculate region statistics
	regionStats := make(map[string]int)
	regionHealthy := make(map[string]int)
	for _, snap := range allNodes {
		region := displayRegion(snap)
		regionStats[region]++
		// Count healthy nodes per region
		if snap.InitialCheckDone && snap.Available && !snap.Blacklisted && !snap.CoolingDown {
			regionHealthy[region]++
		}
	}

	sweepActive, sweepDone, sweepTotal, sweepOK, sweepFail := s.mgr.ProbeSweepProgress()
	payload := map[string]any{
		"nodes":          filtered,
		"total_nodes":    totalNodes,
		"region_stats":   regionStats,
		"region_healthy": regionHealthy,
		"probe_sweep": map[string]any{
			"active":    sweepActive,
			"done":      sweepDone,
			"total":     sweepTotal,
			"available": sweepOK,
			"failed":    sweepFail,
		},
	}
	writeJSON(w, payload)
}

func (s *Server) handleDebug(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONMethodNotAllowed(w, http.MethodGet)
		return
	}
	snapshots := s.mgr.Snapshot()
	var totalCalls, totalSuccess int64
	debugNodes := make([]map[string]any, 0, len(snapshots))
	for _, snap := range snapshots {
		totalCalls += snap.SuccessCount + int64(snap.FailureCount)
		totalSuccess += snap.SuccessCount
		debugNodes = append(debugNodes, map[string]any{
			"tag":                snap.Tag,
			"name":               snap.Name,
			"mode":               snap.Mode,
			"port":               snap.Port,
			"failure_count":      snap.FailureCount,
			"success_count":      snap.SuccessCount,
			"active_connections": snap.ActiveConnections,
			"last_latency_ms":    snap.LastLatencyMs,
			"last_success":       snap.LastSuccess,
			"last_failure":       snap.LastFailure,
			"last_error":         snap.LastError,
			"blacklisted":        snap.Blacklisted,
			"blacklisted_until":  snap.BlacklistedUntil,
			"cooling_down":       snap.CoolingDown,
			"cooldown_until":     snap.CooldownUntil,
			"timeline":           snap.Timeline,
		})
	}
	var successRate float64
	if totalCalls > 0 {
		successRate = float64(totalSuccess) / float64(totalCalls) * 100
	}
	writeJSON(w, map[string]any{
		"nodes":         debugNodes,
		"total_calls":   totalCalls,
		"total_success": totalSuccess,
		"success_rate":  successRate,
	})
}

func (s *Server) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/nodes/"), "/")
	if len(parts) < 1 {
		writeJSONError(w, http.StatusBadRequest, "节点标识无效")
		return
	}
	tag := parts[0]
	if tag == "" {
		writeJSONError(w, http.StatusBadRequest, "节点标识无效")
		return
	}
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}
	switch action {
	case "probe":
		if r.Method != http.MethodPost {
			writeJSONMethodNotAllowed(w, http.MethodPost)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		latency, err := s.mgr.Probe(ctx, tag)
		if err != nil {
			writeJSONError(w, runtimeNodeErrorStatus(err, http.StatusBadGateway), SanitizeProbeError(err))
			return
		}
		latencyMs := latency.Milliseconds()
		if latencyMs == 0 && latency > 0 {
			latencyMs = 1 // Round up sub-millisecond latencies to 1ms
		}
		writeJSON(w, map[string]any{"message": "探测成功", "latency_ms": latencyMs})
	case "release":
		if r.Method != http.MethodPost {
			writeJSONMethodNotAllowed(w, http.MethodPost)
			return
		}
		if err := s.mgr.Release(tag); err != nil {
			writeJSONError(w, runtimeNodeErrorStatus(err, http.StatusConflict), err.Error())
			return
		}
		writeJSON(w, map[string]any{"message": "已解除拉黑"})
	case "blacklist":
		if r.Method != http.MethodPost {
			writeJSONMethodNotAllowed(w, http.MethodPost)
			return
		}
		var req struct {
			Duration string `json:"duration"` // e.g. "1h", "24h", "30m"
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Duration == "" {
			req.Duration = "24h"
		}
		duration, err := time.ParseDuration(req.Duration)
		if err != nil || duration <= 0 {
			duration = 24 * time.Hour
		}
		if err := s.mgr.ManualBlacklist(tag, duration); err != nil {
			writeJSONError(w, runtimeNodeErrorStatus(err, http.StatusConflict), err.Error())
			return
		}
		writeJSON(w, map[string]any{"message": fmt.Sprintf("已拉黑 %s", duration)})
	default:
		writeJSONError(w, http.StatusNotFound, "节点操作不存在")
	}
}

func runtimeNodeErrorStatus(err error, fallback int) int {
	switch {
	case errors.Is(err, context.Canceled):
		return http.StatusRequestTimeout
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	case strings.Contains(strings.ToLower(err.Error()), "not found"):
		return http.StatusNotFound
	default:
		return fallback
	}
}

// handleProbeAll joins the same process-wide single-flight sweep used by boot,
// periodic checks and reload validation. It streams aggregate progress rather
// than launching a second independent set of node probes.
func (s *Server) handleProbeAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONMethodNotAllowed(w, http.MethodPost)
		return
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	total := len(s.mgr.Snapshot())
	if total == 0 {
		emptyData, _ := json.Marshal(map[string]any{"type": "complete", "total": 0, "success": 0, "failed": 0})
		fmt.Fprintf(w, "data: %s\n\n", emptyData)
		flusher.Flush()
		return
	}

	// Send start event
	startData, _ := json.Marshal(map[string]any{"type": "start", "total": total})
	fmt.Fprintf(w, "data: %s\n\n", startData)
	flusher.Flush()

	done := make(chan struct{})
	go func() {
		s.mgr.ProbeAllNow(defaultProbeTimeout)
		close(done)
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	lastDone, lastTotal := -1, -1
	lastOK, lastFail := -1, -1
	sendProgress := func() (int, int, int, int) {
		_, current, currentTotal, okCount, failCount := s.mgr.ProbeSweepProgress()
		if current == lastDone && currentTotal == lastTotal && okCount == lastOK && failCount == lastFail {
			return current, currentTotal, okCount, failCount
		}
		lastDone, lastTotal, lastOK, lastFail = current, currentTotal, okCount, failCount
		percent := float64(0)
		if currentTotal > 0 {
			percent = float64(current) / float64(currentTotal) * 100
		}
		data, _ := json.Marshal(map[string]any{
			"type": "progress", "current": current, "total": currentTotal,
			"success": okCount, "failed": failCount, "progress": percent,
		})
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
		return current, currentTotal, okCount, failCount
	}

	for {
		select {
		case <-r.Context().Done():
			// The global health sweep deliberately continues after an SSE client
			// disconnects; periodic/reload callers may be waiting on the same run.
			return
		case <-ticker.C:
			sendProgress()
		case <-done:
			current, currentTotal, successCount, failedCount := sendProgress()
			if currentTotal == 0 {
				currentTotal = total
			}
			if current < currentTotal {
				current = currentTotal
			}
			completeData, _ := json.Marshal(map[string]any{
				"type": "complete", "total": currentTotal,
				"success": successCount, "failed": failedCount,
			})
			fmt.Fprintf(w, "data: %s\n\n", completeData)
			flusher.Flush()
			return
		}
	}
}

func writeJSON(w http.ResponseWriter, payload any) {
	setJSONHeaders(w)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeJSONStatus(w http.ResponseWriter, status int, payload any) {
	setJSONHeaders(w)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSONStatus(w, status, map[string]any{"error": message})
}

func writeJSONMethodNotAllowed(w http.ResponseWriter, allowed string) {
	w.Header().Set("Allow", allowed)
	writeJSONError(w, http.StatusMethodNotAllowed, "请求方法不受支持")
}

func setJSONHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store, max-age=0")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("Content-Type", "application/json")
}

func decodeStrictJSON(w http.ResponseWriter, r *http.Request, limit int64, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain exactly one JSON value")
		}
		return err
	}
	return nil
}

func writeStrictJSONError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeJSONError(w, http.StatusRequestEntityTooLarge, "请求体过大")
		return
	}
	writeJSONError(w, http.StatusBadRequest, "请求格式错误")
}

// withAuth 认证中间件，如果配置了密码则需要验证
func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 无密码模式仅允许可信的本机浏览器/原生客户端请求。
		if s.managementPassword() == "" {
			if !isSafePasswordlessManagementRequest(r) {
				writeJSONError(w, http.StatusForbidden, "拒绝跨站管理请求")
				return
			}
			next(w, r)
			return
		}

		// Bearer authentication is intended for native clients and is not subject
		// to browser cookie CSRF checks. Evaluate it before cookies so a native
		// request is not rejected merely because it also carries a stale cookie.
		authHeader := r.Header.Get("Authorization")
		if authHeader != "" {
			parts := strings.Fields(authHeader)
			if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") && s.validateSession(parts[1]) {
				next(w, r)
				return
			}
		}

		// Cookie-authenticated unsafe methods must originate from this exact
		// management origin. SameSite cookies alone do not stop a sibling origin
		// (including another localhost port) from submitting a form POST.
		cookie, err := r.Cookie("session_token")
		if err == nil && s.validateSession(cookie.Value) {
			if isUnsafeHTTPMethod(r.Method) && !hasSameManagementOrigin(r) {
				writeJSONError(w, http.StatusForbidden, "拒绝跨站管理请求")
				return
			}
			next(w, r)
			return
		}

		// 未授权
		writeJSONError(w, http.StatusUnauthorized, "未授权，请先登录")
	}
}

func isUnsafeHTTPMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return false
	default:
		return true
	}
}

func hasSameManagementOrigin(r *http.Request) bool {
	if r == nil || strings.TrimSpace(r.Host) == "" {
		return false
	}
	origins := r.Header.Values("Origin")
	if len(origins) != 1 {
		return false
	}
	origin, err := url.Parse(strings.TrimSpace(origins[0]))
	if err != nil || origin == nil || origin.IsAbs() == false || origin.User != nil || origin.Host == "" || origin.RawQuery != "" || origin.Fragment != "" {
		return false
	}
	expectedScheme := "http"
	if r.TLS != nil {
		expectedScheme = "https"
	}
	if !strings.EqualFold(origin.Scheme, expectedScheme) {
		return false
	}
	requestURL, err := url.Parse(expectedScheme + "://" + r.Host)
	if err != nil || requestURL.Host == "" || requestURL.User != nil {
		return false
	}
	return sameOriginAuthority(origin, requestURL)
}

func sameOriginAuthority(left, right *url.URL) bool {
	if left == nil || right == nil {
		return false
	}
	leftHost := strings.TrimSuffix(strings.ToLower(left.Hostname()), ".")
	rightHost := strings.TrimSuffix(strings.ToLower(right.Hostname()), ".")
	if leftHost == "" || leftHost != rightHost {
		return false
	}
	return effectiveOriginPort(left) == effectiveOriginPort(right)
}

func effectiveOriginPort(value *url.URL) string {
	if value == nil {
		return ""
	}
	if port := value.Port(); port != "" {
		return port
	}
	if strings.EqualFold(value.Scheme, "https") {
		return "443"
	}
	if strings.EqualFold(value.Scheme, "http") {
		return "80"
	}
	return ""
}

// isSafePasswordlessManagementRequest prevents ordinary web pages and DNS
// rebinding origins from driving an unauthenticated loopback management API.
// Native clients do not send browser Origin/Referer headers and remain usable.
func isSafePasswordlessManagementRequest(r *http.Request) bool {
	if r == nil || !isLoopbackHostname(requestHostname(r.Host)) {
		return false
	}
	if remoteHost := requestHostname(r.RemoteAddr); remoteHost != "" && !isLoopbackHostname(remoteHost) {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")), "cross-site") {
		return false
	}
	for _, header := range []string{"Origin", "Referer"} {
		raw := strings.TrimSpace(r.Header.Get(header))
		if raw == "" {
			continue
		}
		parsed, err := url.Parse(raw)
		if err != nil || !isLoopbackHostname(parsed.Hostname()) {
			return false
		}
	}
	return true
}

func requestHostname(hostport string) string {
	hostport = strings.TrimSpace(hostport)
	if host, _, err := net.SplitHostPort(hostport); err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(hostport, "[]")
}

func isLoopbackHostname(host string) bool {
	host = strings.TrimSuffix(strings.TrimSpace(host), ".")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// handleAuth 处理登录认证
func (s *Server) handleAuth(w http.ResponseWriter, r *http.Request) {
	configuredPassword, authGeneration := s.managementAuthSnapshot()
	// 如果没有配置密码，直接返回成功（不需要token）
	if configuredPassword == "" {
		writeJSON(w, map[string]any{"message": "无需密码", "no_password": true})
		return
	}

	if r.Method != http.MethodPost {
		writeJSONMethodNotAllowed(w, http.MethodPost)
		return
	}
	releaseAuth, retryAfter, allowed := s.authGuard.begin(r.RemoteAddr, time.Now())
	if !allowed {
		setJSONHeaders(w)
		retryAfterSeconds := int64((retryAfter + time.Second - 1) / time.Second)
		if retryAfterSeconds < 1 {
			retryAfterSeconds = 1
		}
		w.Header().Set("Retry-After", strconv.FormatInt(retryAfterSeconds, 10))
		writeJSONError(w, http.StatusTooManyRequests, "登录尝试过于频繁，请稍后再试")
		return
	}
	defer releaseAuth()

	var req struct {
		Password string `json:"password"`
	}

	if err := decodeStrictJSON(w, r, maxAuthBodyBytes, &req); err != nil {
		writeStrictJSONError(w, err)
		return
	}

	// 使用 constant-time 比较防止时序攻击
	if !secureCompareStrings(req.Password, configuredPassword) {
		// 添加随机延迟防止暴力破解
		time.Sleep(time.Duration(100+mathrand.Intn(200)) * time.Millisecond)
		writeJSONError(w, http.StatusUnauthorized, "密码错误")
		return
	}
	// Commit the session against the same authentication generation that was
	// checked above. Holding cfgMu through createSession makes password rotation
	// either invalidate this session afterwards or reject this stale request;
	// an old-password request cannot recreate a session after invalidation.
	s.cfgMu.RLock()
	if s.authGeneration != authGeneration || !secureCompareStrings(s.cfg.Password, configuredPassword) {
		s.cfgMu.RUnlock()
		writeJSONError(w, http.StatusUnauthorized, "认证配置已更新，请重新登录")
		return
	}
	session, err := s.createSession()
	s.cfgMu.RUnlock()
	if err != nil {
		s.logger.Printf("Failed to create session: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "服务器错误")
		return
	}
	s.authGuard.reset(r.RemoteAddr)

	// 设置 HttpOnly Cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    session.Token,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.managementTLSConfigured(),
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(s.sessionTTL.Seconds()),
	})

	writeJSON(w, map[string]any{
		"message": "登录成功",
		"token":   session.Token,
	})
}

// handleExport 导出所有可用代理池节点的代理 URI，每行一个。
// query 参数:
//   - scheme=http   (默认)
//   - scheme=socks5
//   - scheme=all    (同时导出 HTTP 和 SOCKS5)
//
// 在 pool/hybrid 模式下，还会导出 Pool 代理池入口和 GeoIP 分区路由入口。
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONMethodNotAllowed(w, http.MethodGet)
		return
	}

	scheme := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("scheme")))
	if scheme == "" {
		scheme = "http"
	}
	if scheme != "http" && scheme != "socks5" && scheme != "all" {
		writeJSONError(w, http.StatusBadRequest, "invalid scheme, use http/socks5/all")
		return
	}

	// 只导出初始检查通过的可用节点
	snapshots := s.mgr.SnapshotFiltered(true)
	var lines []string

	seen := make(map[string]bool)

	// 读取运行模式和监听配置
	s.cfgMu.RLock()
	mode := ""
	var listenerCfg config.ListenerConfig
	var multiPortCfg config.MultiPortConfig
	var geoipCfg config.GeoIPConfig
	externalIP := s.cfg.ExternalIP
	if s.cfgSrc != nil {
		mode = s.cfgSrc.Mode
		listenerCfg = s.cfgSrc.Listener
		multiPortCfg = s.cfgSrc.MultiPort
		geoipCfg = s.cfgSrc.GeoIP
	}
	s.cfgMu.RUnlock()

	// Pool 代理池入口（pool 或 hybrid 模式）
	if (mode == "pool" || mode == "hybrid") && listenerCfg.Port > 0 {
		poolAddr := exportAddress(listenerCfg.Address, externalIP)
		lines = append(lines, "# Pool 代理池入口")
		appendProxyURIs(&lines, seen, scheme, poolAddr, listenerCfg.Port, listenerCfg.Username, listenerCfg.Password)
	}

	// GeoIP 分区路由入口
	if geoipCfg.Enabled && geoipCfg.Port > 0 {
		geoListen := geoipCfg.Listen
		if geoListen == "" {
			geoListen = listenerCfg.Address
			if mode == "multi-port" {
				geoListen = multiPortCfg.Address
			}
		}
		geoAddr := exportAddress(geoListen, externalIP)
		geoUsername := listenerCfg.Username
		geoPassword := listenerCfg.Password
		if mode == "multi-port" {
			geoUsername = multiPortCfg.Username
			geoPassword = multiPortCfg.Password
		}
		lines = append(lines, "# GeoIP 分区路由入口（HTTP；全局池）")
		appendProxyURIs(&lines, seen, "http", geoAddr, geoipCfg.Port, geoUsername, geoPassword)
		for _, region := range geoip.AllRegions() {
			selectorUsername := region
			selectorPassword := ""
			if geoUsername != "" {
				selectorUsername = geoUsername + "@" + region
				selectorPassword = geoPassword
			}
			lines = append(lines, fmt.Sprintf("# GeoIP region=%s (Proxy-Authorization username=%s)", region, selectorUsername))
			appendProxyURIs(&lines, seen, "http", geoAddr, geoipCfg.Port, selectorUsername, selectorPassword)
		}
	}

	// Multi-port 独立节点
	if len(snapshots) > 0 && (mode == "hybrid" || mode == "multi-port" || mode == "") {
		lines = append(lines, "# Multi-port 独立节点")
	}
	for _, snap := range snapshots {
		// 只导出有监听地址和端口的节点
		if snap.ListenAddress == "" || snap.Port == 0 {
			continue
		}

		listenAddr := exportAddress(snap.ListenAddress, externalIP)
		appendProxyURIs(&lines, seen, scheme, listenAddr, snap.Port, snap.Username, snap.Password)
	}

	// 返回纯文本，每行一个 URI
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, max-age=0")
	w.Header().Set("Pragma", "no-cache")
	filename := "proxy_pool.txt"
	if scheme == "socks5" {
		filename = "proxy_pool_socks5.txt"
	} else if scheme == "all" {
		filename = "proxy_pool_all.txt"
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	_, _ = w.Write([]byte(strings.Join(lines, "\n")))
}

func appendProxyURIs(lines *[]string, seen map[string]bool, selection, host string, port uint16, username, password string) {
	schemes := []string{selection}
	if selection == "all" {
		schemes = []string{"http", "socks5"}
	}
	for _, scheme := range schemes {
		proxyURI, err := formatProxyURI(scheme, host, port, username, password)
		if err != nil || seen[proxyURI] {
			continue
		}
		*lines = append(*lines, proxyURI)
		seen[proxyURI] = true
	}
}

func formatProxyURI(scheme, host string, port uint16, username, password string) (string, error) {
	if scheme != "http" && scheme != "socks5" {
		return "", errors.New("unsupported proxy URI scheme")
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if port == 0 || !validExportHost(host) {
		return "", errors.New("invalid proxy endpoint")
	}
	parsed := &url.URL{Scheme: scheme, Host: net.JoinHostPort(host, strconv.Itoa(int(port)))}
	if username != "" {
		parsed.User = url.UserPassword(username, password)
	}
	return parsed.String(), nil
}

func exportAddress(listenAddress, externalIP string) string {
	listenAddress = strings.Trim(strings.TrimSpace(listenAddress), "[]")
	if listenAddress == "" || listenAddress == "0.0.0.0" || listenAddress == "::" {
		candidate := strings.Trim(strings.TrimSpace(externalIP), "[]")
		if validExportHost(candidate) {
			return candidate
		}
	}
	return listenAddress
}

func validExportHost(host string) bool {
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if host == "" || strings.ContainsAny(host, " \t\r\n/?#@\\") {
		return false
	}
	if net.ParseIP(host) != nil {
		return true
	}
	if strings.Contains(host, ":") || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
				(character >= '0' && character <= '9') || character == '-' || character == '_' {
				continue
			}
			return false
		}
	}
	return true
}

func parsePositiveSettingsDuration(value string) (time.Duration, error) {
	duration, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil || duration <= 0 {
		return 0, errors.New("duration must be positive")
	}
	return duration, nil
}

func validateProbeTarget(value string) error {
	_, ready, err := resolveProbeTarget(value, false)
	if err == nil && !ready {
		return errors.New("probe target cannot be empty")
	}
	return err
}

func (s *Server) handleSettingsPut(w http.ResponseWriter, r *http.Request) {
	var request settingsUpdateRequest
	if err := decodeStrictJSON(w, r, maxSettingsBodyBytes, &request); err != nil {
		writeStrictJSONError(w, err)
		return
	}

	runtimeConfig := s.runtimeConfigSnapshot()
	previousPassword := runtimeConfig.Password
	nodeMgr := s.nodeManager()
	if nodeMgr == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "节点管理未启用，无法安全应用设置")
		return
	}

	expectedRevision, err := parseSettingsIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		writeJSONError(w, http.StatusPreconditionRequired, "设置版本已缺失，请重新载入后再保存")
		return
	}
	candidate, revision := nodeMgr.ConfigSnapshot()
	if candidate == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "配置存储未初始化")
		return
	}
	if revision != expectedRevision {
		writeJSONError(w, http.StatusPreconditionFailed, "设置已被其他操作更新，请重新载入")
		return
	}
	previousLog := candidate.Log
	if err := applySettingsUpdate(candidate, request); err != nil {
		writeSettingsBadRequest(w, err.Error())
		return
	}
	err = nodeMgr.CommitConfig(r.Context(), revision, candidate, persistSettingsCandidate)
	if err != nil {
		_, currentRevision := nodeMgr.ConfigSnapshot()
		if currentRevision != revision {
			writeJSONError(w, http.StatusPreconditionFailed, "设置已被其他操作更新，请重新载入")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("应用配置失败: %v", err))
		return
	}
	committed, committedRevision := nodeMgr.ConfigSnapshot()
	if committed == nil {
		committed = candidate
	}
	w.Header().Set("ETag", settingsETag(committedRevision))
	s.SetConfig(committed)
	writeSettingsSuccess(w, committed, previousPassword, runtimeConfig, previousLog)
}

func settingsETag(revision uint64) string {
	return fmt.Sprintf(`"config-%d"`, revision)
}

func parseSettingsIfMatch(value string) (uint64, error) {
	value = strings.TrimSpace(value)
	if len(value) < len(`"config-0"`) || !strings.HasPrefix(value, `"config-`) || !strings.HasSuffix(value, `"`) {
		return 0, errors.New("invalid settings ETag")
	}
	return strconv.ParseUint(value[len(`"config-`):len(value)-1], 10, 64)
}

func applySettingsUpdate(candidate *config.Config, request settingsUpdateRequest) error {
	if candidate == nil {
		return errors.New("配置存储未初始化")
	}
	if request.ExternalIP != nil {
		candidate.ExternalIP = strings.TrimSpace(*request.ExternalIP)
	}
	if request.ProbeTarget != nil {
		candidate.Management.ProbeTarget = strings.TrimSpace(*request.ProbeTarget)
	}
	if err := validateProbeTarget(candidate.Management.ProbeTarget); err != nil {
		return fmt.Errorf("探测目标无效: %w", err)
	}
	if request.SkipCertVerify != nil {
		candidate.SkipCertVerify = *request.SkipCertVerify
	}
	if request.ProbeConcurrency != nil {
		if *request.ProbeConcurrency < 1 || *request.ProbeConcurrency > maxProbeConcurrency {
			return fmt.Errorf("探测并发数必须在 1 到 %d 之间", maxProbeConcurrency)
		}
		candidate.Management.ProbeConcurrency = *request.ProbeConcurrency
	}
	if request.Mode != nil {
		candidate.Mode = strings.TrimSpace(*request.Mode)
	}
	if request.Listener != nil {
		if strings.TrimSpace(request.Listener.Address) == "" || request.Listener.Port == 0 {
			return errors.New("统一入口地址和端口不能为空")
		}
		if (request.Listener.Username == "") != (request.Listener.Password == "") {
			return errors.New("统一入口用户名和密码必须同时设置或同时留空")
		}
		candidate.Listener.Address = strings.TrimSpace(request.Listener.Address)
		candidate.Listener.Port = request.Listener.Port
		candidate.Listener.Username = request.Listener.Username
		candidate.Listener.Password = request.Listener.Password
	}
	if request.MultiPort != nil {
		if strings.TrimSpace(request.MultiPort.Address) == "" || request.MultiPort.BasePort == 0 {
			return errors.New("多端口地址和起始端口不能为空")
		}
		if (request.MultiPort.Username == "") != (request.MultiPort.Password == "") {
			return errors.New("多端口用户名和密码必须同时设置或同时留空")
		}
		candidate.MultiPort.Address = strings.TrimSpace(request.MultiPort.Address)
		candidate.MultiPort.BasePort = request.MultiPort.BasePort
		candidate.MultiPort.Username = request.MultiPort.Username
		candidate.MultiPort.Password = request.MultiPort.Password
		if request.MultiPort.PortMapFile != nil {
			candidate.MultiPort.PortMapFile = strings.TrimSpace(*request.MultiPort.PortMapFile)
		}
		if request.MultiPort.PortReuseDelay != "" {
			duration, err := parsePositiveSettingsDuration(request.MultiPort.PortReuseDelay)
			if err != nil {
				return errors.New("端口复用等待时长格式无效")
			}
			candidate.MultiPort.PortReuseDelay = duration
		}
	}
	if request.Pool != nil {
		mode := strings.ToLower(strings.TrimSpace(request.Pool.Mode))
		if mode == "round-robin" || mode == "round_robin" {
			mode = "sequential"
		}
		switch mode {
		case "sequential", "random", "balance", "latency":
		default:
			return errors.New("不支持的调度模式")
		}
		if request.Pool.FailureThreshold < 1 || request.Pool.FailureThreshold > 100 {
			return errors.New("故障阈值必须在 1 到 100 之间")
		}
		if request.Pool.RetryAttempts < 1 || request.Pool.RetryAttempts > 10 {
			return errors.New("重试次数必须在 1 到 10 之间")
		}
		if request.Pool.LatencySampleSize < 1 || request.Pool.LatencySampleSize > 32 {
			return errors.New("延迟采样数必须在 1 到 32 之间")
		}
		blacklistDuration, err := parsePositiveSettingsDuration(request.Pool.BlacklistDuration)
		if err != nil {
			return errors.New("黑名单时长格式无效")
		}
		transientCooldown, err := parsePositiveSettingsDuration(request.Pool.TransientCooldown)
		if err != nil {
			return errors.New("临时冷却时长格式无效")
		}
		latencyTolerance, err := parsePositiveSettingsDuration(request.Pool.LatencyTolerance)
		if err != nil {
			return errors.New("延迟容差格式无效")
		}
		if request.Pool.Sticky == nil || request.Pool.Sticky.MaxEntries < 1 || request.Pool.Sticky.MaxEntries > 1_000_000 {
			return errors.New("会话保持容量必须在 1 到 1000000 之间")
		}
		stickyTTL, err := parsePositiveSettingsDuration(request.Pool.Sticky.TTL)
		if err != nil {
			return errors.New("会话保持 TTL 格式无效")
		}
		candidate.Pool.Mode = mode
		candidate.Pool.FailureThreshold = request.Pool.FailureThreshold
		candidate.Pool.BlacklistDuration = blacklistDuration
		candidate.Pool.RetryAttempts = request.Pool.RetryAttempts
		candidate.Pool.TransientCooldown = transientCooldown
		candidate.Pool.LatencySampleSize = request.Pool.LatencySampleSize
		candidate.Pool.LatencyTolerance = latencyTolerance
		candidate.Pool.Sticky.Enabled = request.Pool.Sticky.Enabled
		candidate.Pool.Sticky.TTL = stickyTTL
		candidate.Pool.Sticky.MaxEntries = request.Pool.Sticky.MaxEntries
		if request.Pool.FailOpen != nil {
			candidate.Pool.FailOpen = *request.Pool.FailOpen
		}
		if request.Pool.RetryEnabled != nil {
			retryEnabled := *request.Pool.RetryEnabled
			candidate.Pool.RetryEnabled = &retryEnabled
		}
	}
	if request.Management != nil {
		if request.Management.Listen != nil {
			candidate.Management.Listen = strings.TrimSpace(*request.Management.Listen)
		}
		if request.Management.Password != nil {
			candidate.Management.Password = *request.Management.Password
		}
		if request.Management.TLSCertFile != nil {
			candidate.Management.TLSCertFile = strings.TrimSpace(*request.Management.TLSCertFile)
		}
		if request.Management.TLSKeyFile != nil {
			candidate.Management.TLSKeyFile = strings.TrimSpace(*request.Management.TLSKeyFile)
		}
		if request.Management.ProbeConcurrency != nil {
			if *request.Management.ProbeConcurrency < 1 || *request.Management.ProbeConcurrency > maxProbeConcurrency {
				return fmt.Errorf("探测并发数必须在 1 到 %d 之间", maxProbeConcurrency)
			}
			candidate.Management.ProbeConcurrency = *request.Management.ProbeConcurrency
		}
	}
	if err := config.ValidateManagementConfig(candidate.Management); err != nil {
		return fmt.Errorf("管理端配置无效: %w", err)
	}
	if request.Log != nil {
		output := strings.ToLower(strings.TrimSpace(request.Log.Output))
		if output != "stdout" && output != "file" {
			return errors.New("日志输出必须为 stdout 或 file")
		}
		if request.Log.MaxSize < 1 || request.Log.MaxBackups < 1 || request.Log.MaxAge < 1 {
			return errors.New("日志轮转参数必须为正数")
		}
		candidate.Log.Output = output
		candidate.Log.MaxSize = request.Log.MaxSize
		candidate.Log.MaxBackups = request.Log.MaxBackups
		candidate.Log.MaxAge = request.Log.MaxAge
		candidate.Log.Compress = request.Log.Compress
	}
	if request.GeoIP != nil {
		candidate.GeoIP.Enabled = request.GeoIP.Enabled
		candidate.GeoIP.DatabasePath = strings.TrimSpace(request.GeoIP.DatabasePath)
		candidate.GeoIP.Listen = strings.TrimSpace(request.GeoIP.Listen)
		candidate.GeoIP.Port = request.GeoIP.Port
		candidate.GeoIP.AutoUpdateEnabled = request.GeoIP.AutoUpdateEnabled
		if request.GeoIP.ExitIPURL != "" {
			candidate.GeoIP.ExitIPURL = strings.TrimSpace(request.GeoIP.ExitIPURL)
		}
		if request.GeoIP.ExitIPConcurrency != 0 {
			if request.GeoIP.ExitIPConcurrency < 1 || request.GeoIP.ExitIPConcurrency > 1024 {
				return errors.New("GeoIP 出口探测并发数必须在 1 到 1024 之间")
			}
			candidate.GeoIP.ExitIPConcurrency = request.GeoIP.ExitIPConcurrency
		}
		if request.GeoIP.ExitIPTimeout != "" {
			duration, err := parsePositiveSettingsDuration(request.GeoIP.ExitIPTimeout)
			if err != nil {
				return errors.New("GeoIP 出口探测超时格式无效")
			}
			candidate.GeoIP.ExitIPTimeout = duration
		}
		if request.GeoIP.AutoUpdateInterval != "" {
			duration, err := parsePositiveSettingsDuration(request.GeoIP.AutoUpdateInterval)
			if err != nil {
				return errors.New("GeoIP 自动更新时间格式无效")
			}
			candidate.GeoIP.AutoUpdateInterval = duration
		}
	}
	return nil
}

func persistSettingsCandidate(candidate *config.Config) (func() error, error) {
	if candidate == nil || candidate.FilePath() == "" {
		return nil, errors.New("config file path is unknown")
	}
	return candidate.SaveSettingsTransaction()
}

func writeSettingsSuccess(w http.ResponseWriter, candidate *config.Config, previousPassword string, runtimeConfig Config, previousLog config.LogConfig) {
	managementRestartRequired := managementRuntimeChanged(runtimeConfig, candidate.Management)
	logRestartRequired := previousLog != candidate.Log
	needRestart := managementRestartRequired || logRestartRequired
	passwordChanged := !managementRestartRequired && previousPassword != candidate.Management.Password
	writeJSON(w, map[string]any{
		"message":          "设置已保存并生效",
		"external_ip":      candidate.ExternalIP,
		"probe_target":     candidate.Management.ProbeTarget,
		"skip_cert_verify": candidate.SkipCertVerify,
		"need_reload":      false,
		"need_restart":     needRestart,
		"auth_changed":     passwordChanged,
	})
}

func writeSettingsBadRequest(w http.ResponseWriter, message string) {
	writeJSONError(w, http.StatusBadRequest, message)
}

// handleSettings handles GET/PUT for dynamic settings (external_ip, probe_target, skip_cert_verify, log).
func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPut {
		s.handleSettingsPut(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		extIP, probeTarget, skipCertVerify, logCfg := s.getSettings()

		// Read full config for extended fields
		s.cfgMu.RLock()
		cfg := s.cfgSrc.Clone()
		s.cfgMu.RUnlock()
		if nodeMgr := s.nodeManager(); nodeMgr != nil {
			if committed, revision := nodeMgr.ConfigSnapshot(); committed != nil {
				cfg = committed
				extIP = committed.ExternalIP
				probeTarget = committed.Management.ProbeTarget
				skipCertVerify = committed.SkipCertVerify
				logCfg = committed.Log
				w.Header().Set("ETag", settingsETag(revision))
			}
		}

		probeConcurrency := 32
		if cfg != nil {
			probeConcurrency = cfg.ProbeConcurrencyOrDefault()
		}
		if s.mgr != nil {
			probeConcurrency = s.mgr.ProbeConcurrency()
		}
		resp := map[string]any{
			"external_ip":       extIP,
			"probe_target":      probeTarget,
			"skip_cert_verify":  skipCertVerify,
			"probe_concurrency": probeConcurrency,
			"log": map[string]any{
				"output":      logCfg.Output,
				"file":        logCfg.File,
				"max_size":    logCfg.MaxSize,
				"max_backups": logCfg.MaxBackups,
				"max_age":     logCfg.MaxAge,
				"compress":    logCfg.Compress,
			},
			"geoip": map[string]any{
				"enabled":              false,
				"database_path":        "",
				"listen":               "",
				"port":                 0,
				"exit_ip_url":          "",
				"exit_ip_timeout":      "",
				"exit_ip_concurrency":  0,
				"auto_update_enabled":  false,
				"auto_update_interval": "",
			},
		}
		if cfg != nil {
			resp["mode"] = cfg.Mode
			resp["listener"] = map[string]any{
				"address":  cfg.Listener.Address,
				"port":     cfg.Listener.Port,
				"username": cfg.Listener.Username,
				"password": cfg.Listener.Password,
			}
			resp["multi_port"] = map[string]any{
				"address":          cfg.MultiPort.Address,
				"base_port":        cfg.MultiPort.BasePort,
				"username":         cfg.MultiPort.Username,
				"password":         cfg.MultiPort.Password,
				"port_map_file":    cfg.MultiPort.PortMapFile,
				"port_reuse_delay": cfg.MultiPort.PortReuseDelay.String(),
			}
			resp["pool"] = map[string]any{
				"mode":                cfg.Pool.Mode,
				"failure_threshold":   cfg.Pool.FailureThreshold,
				"blacklist_duration":  cfg.Pool.BlacklistDuration.String(),
				"fail_open":           cfg.Pool.FailOpen,
				"retry_enabled":       cfg.Pool.RetryEnabledValue(),
				"retry_attempts":      cfg.Pool.RetryAttempts,
				"transient_cooldown":  cfg.Pool.TransientCooldown.String(),
				"latency_sample_size": cfg.Pool.LatencySampleSize,
				"latency_tolerance":   cfg.Pool.LatencyTolerance.String(),
				"sticky": map[string]any{
					"enabled":     cfg.Pool.Sticky.Enabled,
					"ttl":         cfg.Pool.Sticky.TTL.String(),
					"max_entries": cfg.Pool.Sticky.MaxEntries,
				},
			}
			resp["management"] = map[string]any{
				"listen":            cfg.Management.Listen,
				"password":          cfg.Management.Password,
				"probe_concurrency": cfg.ProbeConcurrencyOrDefault(),
				"tls_cert_file":     cfg.Management.TLSCertFile,
				"tls_key_file":      cfg.Management.TLSKeyFile,
			}
			resp["geoip"] = map[string]any{
				"enabled":              cfg.GeoIP.Enabled,
				"database_path":        cfg.GeoIP.DatabasePath,
				"listen":               cfg.GeoIP.Listen,
				"port":                 cfg.GeoIP.Port,
				"exit_ip_url":          cfg.GeoIP.ExitIPURL,
				"exit_ip_timeout":      cfg.GeoIP.ExitIPTimeout.String(),
				"exit_ip_concurrency":  cfg.GeoIP.ExitIPConcurrency,
				"auto_update_enabled":  cfg.GeoIP.AutoUpdateEnabled,
				"auto_update_interval": cfg.GeoIP.AutoUpdateInterval.String(),
			}
		}
		writeJSON(w, resp)
	case http.MethodPut:
		if s.subscriptionRefresher() == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "订阅刷新管理器未启用")
			return
		}
		var req struct {
			ExternalIP       string `json:"external_ip"`
			ProbeTarget      string `json:"probe_target"`
			SkipCertVerify   bool   `json:"skip_cert_verify"`
			ProbeConcurrency int    `json:"probe_concurrency"`
			Mode             string `json:"mode,omitempty"`
			Listener         *struct {
				Address  string `json:"address"`
				Port     uint16 `json:"port"`
				Username string `json:"username"`
				Password string `json:"password"`
			} `json:"listener,omitempty"`
			MultiPort *struct {
				Address        string  `json:"address"`
				BasePort       uint16  `json:"base_port"`
				Username       string  `json:"username"`
				Password       string  `json:"password"`
				PortMapFile    *string `json:"port_map_file"`
				PortReuseDelay string  `json:"port_reuse_delay"`
			} `json:"multi_port,omitempty"`
			Pool *struct {
				Mode              string `json:"mode"`
				FailureThreshold  int    `json:"failure_threshold"`
				BlacklistDuration string `json:"blacklist_duration"`
				FailOpen          *bool  `json:"fail_open"`
				RetryEnabled      *bool  `json:"retry_enabled"`
				RetryAttempts     int    `json:"retry_attempts"`
				TransientCooldown string `json:"transient_cooldown"`
				LatencySampleSize int    `json:"latency_sample_size"`
				LatencyTolerance  string `json:"latency_tolerance"`
				Sticky            *struct {
					Enabled    bool   `json:"enabled"`
					TTL        string `json:"ttl"`
					MaxEntries int    `json:"max_entries"`
				} `json:"sticky"`
			} `json:"pool,omitempty"`
			Management *struct {
				Listen           string `json:"listen"`
				Password         string `json:"password"`
				ProbeConcurrency int    `json:"probe_concurrency"`
			} `json:"management,omitempty"`
			Log *struct {
				Output     string `json:"output"`
				MaxSize    int    `json:"max_size"`
				MaxBackups int    `json:"max_backups"`
				MaxAge     int    `json:"max_age"`
				Compress   bool   `json:"compress"`
			} `json:"log"`
			GeoIP *struct {
				Enabled            bool   `json:"enabled"`
				DatabasePath       string `json:"database_path"`
				Listen             string `json:"listen"`
				Port               uint16 `json:"port"`
				ExitIPURL          string `json:"exit_ip_url"`
				ExitIPTimeout      string `json:"exit_ip_timeout"`
				ExitIPConcurrency  int    `json:"exit_ip_concurrency"`
				AutoUpdateEnabled  bool   `json:"auto_update_enabled"`
				AutoUpdateInterval string `json:"auto_update_interval"`
			} `json:"geoip"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "请求格式错误")
			return
		}

		extIP := strings.TrimSpace(req.ExternalIP)
		probeTarget := strings.TrimSpace(req.ProbeTarget)
		if err := validateProbeTarget(req.ProbeTarget); err != nil {
			writeSettingsBadRequest(w, "探测目标无效: "+err.Error())
			return
		}

		s.cfgMu.RLock()
		managementCandidate := config.ManagementConfig{}
		if s.cfgSrc != nil {
			managementCandidate = s.cfgSrc.Management
		}
		s.cfgMu.RUnlock()
		if req.Management != nil {
			if strings.TrimSpace(req.Management.Listen) != "" {
				managementCandidate.Listen = strings.TrimSpace(req.Management.Listen)
			}
			managementCandidate.Password = req.Management.Password
			if req.Management.ProbeConcurrency > 0 {
				managementCandidate.ProbeConcurrency = req.Management.ProbeConcurrency
			}
		}
		if err := config.ValidateManagementConfig(managementCandidate); err != nil {
			writeSettingsBadRequest(w, "管理端配置无效: "+err.Error())
			return
		}

		var logCfg *config.LogConfig
		if req.Log != nil {
			logCfg = &config.LogConfig{
				Output:     req.Log.Output,
				MaxSize:    req.Log.MaxSize,
				MaxBackups: req.Log.MaxBackups,
				MaxAge:     req.Log.MaxAge,
				Compress:   req.Log.Compress,
			}
		}

		var poolBlacklist, transientCooldown, latencyTolerance, stickyTTL time.Duration
		if req.Pool != nil {
			mode := strings.ToLower(strings.TrimSpace(req.Pool.Mode))
			if mode == "round-robin" || mode == "round_robin" {
				mode = "sequential"
			}
			switch mode {
			case "sequential", "random", "balance", "latency":
				req.Pool.Mode = mode
			default:
				writeSettingsBadRequest(w, "不支持的调度模式")
				return
			}
			if req.Pool.FailureThreshold < 1 || req.Pool.FailureThreshold > 100 {
				writeSettingsBadRequest(w, "故障阈值必须在 1 到 100 之间")
				return
			}
			if req.Pool.RetryAttempts < 1 || req.Pool.RetryAttempts > 10 {
				writeSettingsBadRequest(w, "重试次数必须在 1 到 10 之间")
				return
			}
			if req.Pool.LatencySampleSize < 1 || req.Pool.LatencySampleSize > 32 {
				writeSettingsBadRequest(w, "延迟采样数必须在 1 到 32 之间")
				return
			}
			var err error
			if poolBlacklist, err = parsePositiveSettingsDuration(req.Pool.BlacklistDuration); err != nil {
				writeSettingsBadRequest(w, "黑名单时长格式无效")
				return
			}
			if transientCooldown, err = parsePositiveSettingsDuration(req.Pool.TransientCooldown); err != nil {
				writeSettingsBadRequest(w, "临时冷却时长格式无效")
				return
			}
			if latencyTolerance, err = parsePositiveSettingsDuration(req.Pool.LatencyTolerance); err != nil {
				writeSettingsBadRequest(w, "延迟容差格式无效")
				return
			}
			if req.Pool.Sticky == nil {
				writeSettingsBadRequest(w, "缺少会话保持配置")
				return
			}
			if req.Pool.Sticky.MaxEntries < 1 || req.Pool.Sticky.MaxEntries > 1_000_000 {
				writeSettingsBadRequest(w, "会话保持容量必须在 1 到 1000000 之间")
				return
			}
			if stickyTTL, err = parsePositiveSettingsDuration(req.Pool.Sticky.TTL); err != nil {
				writeSettingsBadRequest(w, "会话保持 TTL 格式无效")
				return
			}
		}

		probeConcurrency := req.ProbeConcurrency
		if req.Management != nil && req.Management.ProbeConcurrency > 0 {
			probeConcurrency = req.Management.ProbeConcurrency
		}
		if probeConcurrency <= 0 {
			probeConcurrency = s.mgr.ProbeConcurrency()
		}
		if err := s.updateSettings(extIP, probeTarget, req.SkipCertVerify, probeConcurrency, logCfg, req.GeoIP != nil && req.GeoIP.Enabled); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Update extended settings
		passwordChanged := false
		s.cfgMu.Lock()
		if s.cfgSrc != nil {
			previousManagementPassword := s.cfgSrc.Management.Password
			if req.Mode != "" {
				s.cfgSrc.Mode = req.Mode
			}
			if req.Listener != nil {
				s.cfgSrc.Listener.Address = req.Listener.Address
				s.cfgSrc.Listener.Port = req.Listener.Port
				s.cfgSrc.Listener.Username = req.Listener.Username
				s.cfgSrc.Listener.Password = req.Listener.Password
			}
			if req.MultiPort != nil {
				s.cfgSrc.MultiPort.Address = req.MultiPort.Address
				s.cfgSrc.MultiPort.BasePort = req.MultiPort.BasePort
				s.cfgSrc.MultiPort.Username = req.MultiPort.Username
				s.cfgSrc.MultiPort.Password = req.MultiPort.Password
				if req.MultiPort.PortMapFile != nil {
					s.cfgSrc.MultiPort.PortMapFile = *req.MultiPort.PortMapFile
				}
				if req.MultiPort.PortReuseDelay != "" {
					if d, err := time.ParseDuration(req.MultiPort.PortReuseDelay); err == nil {
						s.cfgSrc.MultiPort.PortReuseDelay = d
					}
				}
			}
			if req.Pool != nil {
				s.cfgSrc.Pool.Mode = req.Pool.Mode
				s.cfgSrc.Pool.FailureThreshold = req.Pool.FailureThreshold
				if req.Pool.FailOpen != nil {
					s.cfgSrc.Pool.FailOpen = *req.Pool.FailOpen
				}
				if req.Pool.RetryEnabled != nil {
					s.cfgSrc.Pool.RetryEnabled = req.Pool.RetryEnabled
				}
				s.cfgSrc.Pool.RetryAttempts = req.Pool.RetryAttempts
				s.cfgSrc.Pool.BlacklistDuration = poolBlacklist
				s.cfgSrc.Pool.TransientCooldown = transientCooldown
				s.cfgSrc.Pool.LatencySampleSize = req.Pool.LatencySampleSize
				s.cfgSrc.Pool.LatencyTolerance = latencyTolerance
				s.cfgSrc.Pool.Sticky.Enabled = req.Pool.Sticky.Enabled
				s.cfgSrc.Pool.Sticky.TTL = stickyTTL
				s.cfgSrc.Pool.Sticky.MaxEntries = req.Pool.Sticky.MaxEntries
			}
			if req.Management != nil {
				if strings.TrimSpace(req.Management.Listen) != "" {
					s.cfgSrc.Management.Listen = strings.TrimSpace(req.Management.Listen)
				}
				s.cfgSrc.Management.Password = req.Management.Password
			}
			if req.GeoIP != nil {
				s.cfgSrc.GeoIP.DatabasePath = req.GeoIP.DatabasePath
				s.cfgSrc.GeoIP.Listen = req.GeoIP.Listen
				s.cfgSrc.GeoIP.Port = req.GeoIP.Port
				if req.GeoIP.ExitIPURL != "" {
					s.cfgSrc.GeoIP.ExitIPURL = req.GeoIP.ExitIPURL
				}
				if req.GeoIP.ExitIPConcurrency > 0 {
					s.cfgSrc.GeoIP.ExitIPConcurrency = req.GeoIP.ExitIPConcurrency
				}
				if req.GeoIP.ExitIPTimeout != "" {
					if d, err := time.ParseDuration(req.GeoIP.ExitIPTimeout); err == nil {
						s.cfgSrc.GeoIP.ExitIPTimeout = d
					}
				}
				s.cfgSrc.GeoIP.AutoUpdateEnabled = req.GeoIP.AutoUpdateEnabled
				if req.GeoIP.AutoUpdateInterval != "" {
					if d, err := time.ParseDuration(req.GeoIP.AutoUpdateInterval); err == nil {
						s.cfgSrc.GeoIP.AutoUpdateInterval = d
					}
				}
			}
			if err := s.cfgSrc.SaveSettings(); err != nil {
				s.cfgSrc.Management.Password = previousManagementPassword
				s.cfgMu.Unlock()
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}
			if req.Management != nil {
				passwordChanged = s.cfg.Password != req.Management.Password
				s.cfg.Password = req.Management.Password
			}
		}
		s.cfgMu.Unlock()
		if passwordChanged {
			s.invalidateSessions()
		}

		writeJSON(w, map[string]any{
			"message":          "设置已保存",
			"external_ip":      extIP,
			"probe_target":     probeTarget,
			"skip_cert_verify": req.SkipCertVerify,
			"need_reload":      true,
			"auth_changed":     passwordChanged,
		})
	default:
		writeJSONMethodNotAllowed(w, "GET, PUT")
	}
}

// handleSubscriptionStatus returns the current subscription refresh status.
func (s *Server) handleSubscriptionStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONMethodNotAllowed(w, http.MethodGet)
		return
	}

	refresher := s.subscriptionRefresher()
	if refresher == nil {
		writeJSON(w, map[string]any{
			"enabled": false,
			"message": "订阅刷新未启用",
		})
		return
	}

	status := refresher.Status()
	writeJSON(w, map[string]any{
		"enabled":        true,
		"last_refresh":   status.LastRefresh,
		"next_refresh":   status.NextRefresh,
		"node_count":     status.NodeCount,
		"last_error":     status.LastError,
		"refresh_count":  status.RefreshCount,
		"is_refreshing":  status.IsRefreshing,
		"nodes_modified": status.NodesModified,
	})
}

// handleSubscriptionRefresh triggers an immediate subscription refresh.
func (s *Server) handleSubscriptionRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONMethodNotAllowed(w, http.MethodPost)
		return
	}

	refresher := s.subscriptionRefresher()
	if refresher == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "订阅刷新未启用")
		return
	}

	if err := refresher.RefreshNow(); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	status := refresher.Status()
	writeJSON(w, map[string]any{
		"message":    "刷新成功",
		"node_count": status.NodeCount,
	})
}

// handleSubscriptionConfig handles GET/PUT for subscription configuration.
func (s *Server) handleSubscriptionConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		var urls []string
		var enabled bool
		var interval string
		fetchConcurrency := config.NormalizeSubscriptionFetchConcurrency(0)
		allowPrivateNetworks := false
		s.cfgMu.RLock()
		cfg := s.cfgSrc.Clone()
		s.cfgMu.RUnlock()
		if nodeMgr := s.nodeManager(); nodeMgr != nil {
			if committed, revision := nodeMgr.ConfigSnapshot(); committed != nil {
				cfg = committed
				w.Header().Set("ETag", settingsETag(revision))
			}
		}
		if cfg != nil {
			urls = append([]string(nil), cfg.Subscriptions...)
			enabled = cfg.SubscriptionRefresh.Enabled
			interval = cfg.SubscriptionRefresh.Interval.String()
			fetchConcurrency = config.NormalizeSubscriptionFetchConcurrency(cfg.SubscriptionRefresh.FetchConcurrency)
			allowPrivateNetworks = cfg.SubscriptionRefresh.AllowPrivateNetworks
		}
		writeJSON(w, map[string]any{
			"subscriptions":          urls,
			"enabled":                enabled,
			"interval":               interval,
			"fetch_concurrency":      fetchConcurrency,
			"allow_private_networks": allowPrivateNetworks,
		})

	case http.MethodPut:
		refresher := s.subscriptionRefresher()
		if refresher == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "订阅刷新未启用")
			return
		}
		var req struct {
			Subscriptions        []string `json:"subscriptions"`
			Enabled              bool     `json:"enabled"`
			Interval             string   `json:"interval"` // e.g. "1h", "30m"
			FetchConcurrency     *int     `json:"fetch_concurrency,omitempty"`
			AllowPrivateNetworks *bool    `json:"allow_private_networks,omitempty"`
		}
		if err := decodeStrictJSON(w, r, maxSubscriptionConfigBodyBytes, &req); err != nil {
			writeStrictJSONError(w, err)
			return
		}

		// Parse interval
		interval, err := time.ParseDuration(req.Interval)
		if err != nil || interval < 5*time.Minute {
			writeSettingsBadRequest(w, "订阅刷新间隔格式无效或小于 5 分钟")
			return
		}

		cleanURLs, err := config.ValidateSubscriptionURLs(req.Subscriptions)
		if err != nil {
			writeSettingsBadRequest(w, "订阅链接无效: "+err.Error())
			return
		}

		s.cfgMu.RLock()
		fetchConcurrency := config.NormalizeSubscriptionFetchConcurrency(0)
		allowPrivateNetworks := false
		if s.cfgSrc != nil {
			fetchConcurrency = config.NormalizeSubscriptionFetchConcurrency(s.cfgSrc.SubscriptionRefresh.FetchConcurrency)
			allowPrivateNetworks = s.cfgSrc.SubscriptionRefresh.AllowPrivateNetworks
		}
		s.cfgMu.RUnlock()
		if req.FetchConcurrency != nil {
			if *req.FetchConcurrency < 1 || *req.FetchConcurrency > 32 {
				writeSettingsBadRequest(w, "订阅抓取并发数必须在 1 到 32 之间")
				return
			}
			fetchConcurrency = *req.FetchConcurrency
		}
		if req.AllowPrivateNetworks != nil {
			allowPrivateNetworks = *req.AllowPrivateNetworks
		}
		nodeMgr := s.nodeManager()
		if nodeMgr == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "节点管理未启用，无法安全应用订阅设置")
			return
		}
		expectedRevision, err := parseSettingsIfMatch(r.Header.Get("If-Match"))
		if err != nil {
			writeJSONError(w, http.StatusPreconditionRequired, "订阅设置版本已缺失，请重新载入后再保存")
			return
		}
		_, revision := nodeMgr.ConfigSnapshot()
		if revision != expectedRevision {
			writeJSONError(w, http.StatusPreconditionFailed, "订阅设置已被其他操作更新，请重新载入")
			return
		}

		// The subscription manager owns the config+nodes+runtime transaction.
		// Nothing is pre-written here, so a failed fetch/reload leaves GET, disk,
		// and the active BoxManager on the same committed revision.
		if err := refresher.UpdateConfigAndRefreshAtRevision(cleanURLs, req.Enabled, interval, fetchConcurrency, allowPrivateNetworks, expectedRevision); err != nil {
			if errors.Is(err, ErrSubscriptionConfigRevisionConflict) {
				writeJSONError(w, http.StatusPreconditionFailed, "订阅设置已被其他操作更新，请重新载入")
				return
			}
			writeJSONError(w, http.StatusBadGateway, fmt.Sprintf("订阅更新失败: %v", err))
			return
		}
		if committed, committedRevision := nodeMgr.ConfigSnapshot(); committed != nil {
			s.SetConfig(committed)
			w.Header().Set("ETag", settingsETag(committedRevision))
		}

		status := refresher.Status()
		writeJSON(w, map[string]any{
			"message":                "订阅配置已更新并生效",
			"subscriptions":          cleanURLs,
			"enabled":                req.Enabled,
			"interval":               interval.String(),
			"fetch_concurrency":      fetchConcurrency,
			"allow_private_networks": allowPrivateNetworks,
			"node_count":             status.NodeCount,
		})

	default:
		writeJSONMethodNotAllowed(w, "GET, PUT")
	}
}

// nodePayload is the JSON request body for node CRUD operations.
type nodePayload struct {
	Name     string `json:"name"`
	URI      string `json:"uri"`
	Port     uint16 `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type nodeConfigResponse struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	URI      string            `json:"uri"`
	Port     uint16            `json:"port,omitempty"`
	Username string            `json:"username,omitempty"`
	Password string            `json:"password,omitempty"`
	Source   config.NodeSource `json:"source,omitempty"`
}

func maskedNodeURI(rawURI string) string {
	rawURI = strings.TrimSpace(rawURI)
	if rawURI == "" {
		return ""
	}
	if separator := strings.Index(rawURI, "://"); separator >= 0 {
		return rawURI[:separator+3] + "********"
	}
	return "********"
}

func newNodeConfigResponse(node config.NodeConfig, revealURI bool) nodeConfigResponse {
	uri := maskedNodeURI(node.URI)
	username := ""
	password := ""
	if revealURI {
		uri = node.URI
		username = node.Username
		password = node.Password
	}
	return nodeConfigResponse{
		ID:       node.NodeKey(),
		Name:     node.Name,
		URI:      uri,
		Port:     node.Port,
		Username: username,
		Password: password,
		Source:   node.Source,
	}
}

func (p nodePayload) toConfig() config.NodeConfig {
	return config.NodeConfig{
		Name:     p.Name,
		URI:      p.URI,
		Port:     p.Port,
		Username: p.Username,
		Password: p.Password,
	}
}

// handleConfigNodes handles GET (list) and POST (create) for config nodes.
func (s *Server) handleConfigNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		writeJSONMethodNotAllowed(w, "GET, POST")
		return
	}
	nodeMgr, ok := s.ensureNodeManager(w)
	if !ok {
		return
	}

	switch r.Method {
	case http.MethodGet:
		nodes, err := nodeMgr.ListConfigNodes(r.Context())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		responseNodes := make([]nodeConfigResponse, 0, len(nodes))
		for _, node := range nodes {
			responseNodes = append(responseNodes, newNodeConfigResponse(node, false))
		}
		writeJSON(w, map[string]any{"nodes": responseNodes})
	case http.MethodPost:
		var payload nodePayload
		if err := decodeStrictJSON(w, r, maxNodeConfigBodyBytes, &payload); err != nil {
			writeStrictJSONError(w, err)
			return
		}
		node, err := nodeMgr.CreateNode(r.Context(), payload.toConfig())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		writeJSONStatus(w, http.StatusCreated, map[string]any{"node": newNodeConfigResponse(node, false), "message": "节点已添加，请点击重载使配置生效"})
	}
}

// handleConfigNodeItem handles PUT (update) and DELETE for a specific config node.
func (s *Server) handleConfigNodeItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPut && r.Method != http.MethodDelete {
		writeJSONMethodNotAllowed(w, "GET, PUT, DELETE")
		return
	}
	nodeMgr, ok := s.ensureNodeManager(w)
	if !ok {
		return
	}

	namePart := strings.TrimPrefix(r.URL.Path, "/api/nodes/config/")
	nodeName, err := url.PathUnescape(namePart)
	if err != nil || nodeName == "" || strings.Contains(nodeName, "/") {
		writeJSONError(w, http.StatusBadRequest, "节点标识无效")
		return
	}

	switch r.Method {
	case http.MethodGet:
		nodes, err := nodeMgr.ListConfigNodes(r.Context())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		for _, node := range nodes {
			if node.NodeKey() == nodeName {
				writeJSON(w, map[string]any{"node": newNodeConfigResponse(node, true)})
				return
			}
		}
		writeJSONError(w, http.StatusNotFound, ErrNodeNotFound.Error())
	case http.MethodPut:
		var payload nodePayload
		if err := decodeStrictJSON(w, r, maxNodeConfigBodyBytes, &payload); err != nil {
			writeStrictJSONError(w, err)
			return
		}
		node, err := nodeMgr.UpdateNode(r.Context(), nodeName, payload.toConfig())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		writeJSON(w, map[string]any{"node": newNodeConfigResponse(node, false), "message": "节点已更新，请点击重载使配置生效"})
	case http.MethodDelete:
		if err := nodeMgr.DeleteNode(r.Context(), nodeName); err != nil {
			s.respondNodeError(w, err)
			return
		}
		writeJSON(w, map[string]any{"message": "节点已删除，请点击重载使配置生效"})
	}
}

// handleReload triggers a configuration reload.
func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONMethodNotAllowed(w, http.MethodPost)
		return
	}
	nodeMgr, ok := s.ensureNodeManager(w)
	if !ok {
		return
	}

	if err := nodeMgr.TriggerReload(r.Context()); err != nil {
		s.respondNodeError(w, err)
		return
	}
	writeJSON(w, map[string]any{
		"message": "重载成功，现有连接已被中断",
	})
}

func (s *Server) ensureNodeManager(w http.ResponseWriter) (NodeManager, bool) {
	nodeMgr := s.nodeManager()
	if nodeMgr == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "节点管理未启用")
		return nil, false
	}
	return nodeMgr, true
}

func (s *Server) respondNodeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrNodeNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrNodeConflict):
		status = http.StatusConflict
	case errors.Is(err, ErrInvalidNode):
		status = http.StatusBadRequest
	case errors.Is(err, context.Canceled):
		status = http.StatusRequestTimeout
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	}
	writeJSONError(w, status, err.Error())
}

// handleTraffic streams real-time traffic from sing-box Clash API as SSE.
// Clash API /traffic returns newline-delimited JSON; we convert to SSE for browser EventSource.
func (s *Server) handleTraffic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONMethodNotAllowed(w, http.MethodGet)
		return
	}

	trafficURL := s.trafficURL
	if trafficURL == "" {
		trafficURL = "http://127.0.0.1:9092/traffic"
	}
	client := s.trafficHTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, trafficURL, nil)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "流量统计接口配置无效")
		return
	}
	resp, err := client.Do(request)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		writeJSONError(w, http.StatusBadGateway, "无法连接到流量统计接口")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		status := http.StatusBadGateway
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented {
			status = http.StatusNotImplemented
		}
		writeJSONStatus(w, status, map[string]any{
			"error":           "流量统计接口不可用",
			"upstream_status": resp.StatusCode,
		})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "响应流不支持实时刷新")
		return
	}

	// Set SSE headers only after the upstream has accepted the request, so
	// pre-stream failures retain their proper JSON status and content type.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Scanner preserves NDJSON record boundaries even when the upstream splits a
	// JSON object across network reads or combines several objects in one read.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", line); err != nil {
			return
		}
		flusher.Flush()
	}
}

// handleLogs returns recent console log content from the in-memory ring buffer.
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONMethodNotAllowed(w, http.MethodGet)
		return
	}
	content := SharedLogBuffer.Content()
	writeJSON(w, map[string]any{"logs": content})
}

// Session management functions

func (s *Server) managementPassword() string {
	s.cfgMu.RLock()
	password := s.cfg.Password
	s.cfgMu.RUnlock()
	return password
}

func (s *Server) managementAuthSnapshot() (string, uint64) {
	s.cfgMu.RLock()
	password := s.cfg.Password
	generation := s.authGeneration
	s.cfgMu.RUnlock()
	return password, generation
}

func (s *Server) managementTLSConfigured() bool {
	s.cfgMu.RLock()
	configured := strings.TrimSpace(s.cfg.TLSCertFile) != "" && strings.TrimSpace(s.cfg.TLSKeyFile) != ""
	s.cfgMu.RUnlock()
	return configured
}

func (s *Server) invalidateSessions() {
	s.sessionMu.Lock()
	s.sessions = make(map[string]*Session)
	s.sessionMu.Unlock()
}

// generateSessionToken creates a cryptographically secure random token.
func (s *Server) generateSessionToken() (string, error) {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", fmt.Errorf("failed to generate session token: %w", err)
	}
	return hex.EncodeToString(tokenBytes), nil
}

// createSession creates a new session with expiration.
func (s *Server) createSession() (*Session, error) {
	token, err := s.generateSessionToken()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	session := &Session{
		Token:     token,
		CreatedAt: now,
		ExpiresAt: now.Add(s.sessionTTL),
	}

	s.sessionMu.Lock()
	s.pruneExpiredSessionsLocked(now)
	if len(s.sessions) >= maxActiveSessions {
		var oldestToken string
		var oldestTime time.Time
		for existingToken, existing := range s.sessions {
			if oldestToken == "" || existing.CreatedAt.Before(oldestTime) {
				oldestToken = existingToken
				oldestTime = existing.CreatedAt
			}
		}
		delete(s.sessions, oldestToken)
	}
	s.sessions[token] = session
	s.sessionMu.Unlock()

	return session, nil
}

// validateSession checks if a session token is valid and not expired.
func (s *Server) validateSession(token string) bool {
	s.sessionMu.RLock()
	session, exists := s.sessions[token]
	s.sessionMu.RUnlock()

	if !exists {
		return false
	}

	// Check if expired
	if time.Now().After(session.ExpiresAt) {
		s.sessionMu.Lock()
		delete(s.sessions, token)
		s.sessionMu.Unlock()
		return false
	}

	return true
}

// cleanupExpiredSessions periodically removes expired sessions.
func (s *Server) cleanupExpiredSessions() {
	defer s.sessionWG.Done()
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-s.sessionCtx.Done():
			return
		case now := <-ticker.C:
			s.sessionMu.Lock()
			s.pruneExpiredSessionsLocked(now)
			s.sessionMu.Unlock()
		}
	}
}

func (s *Server) pruneExpiredSessionsLocked(now time.Time) {
	for token, session := range s.sessions {
		if !session.ExpiresAt.After(now) {
			delete(s.sessions, token)
		}
	}
}

// secureCompareStrings performs constant-time string comparison to prevent timing attacks.
func secureCompareStrings(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
