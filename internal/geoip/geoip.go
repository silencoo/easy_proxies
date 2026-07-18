package geoip

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/ssruri"
	"easy_proxies/internal/ssuri"

	"github.com/oschwald/geoip2-golang"
)

// Region codes
const (
	RegionJP    = "jp"
	RegionKR    = "kr"
	RegionUS    = "us"
	RegionHK    = "hk"
	RegionTW    = "tw"
	RegionSG    = "sg"
	RegionOther = "other"
)

// Default GeoIP database download URL
const (
	DefaultGeoIPURL            = "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-Country.mmdb"
	maxGeoIPDatabaseSize int64 = 32 << 20
	geoIPDownloadTimeout       = 60 * time.Second
)

var geoIPDatabaseURL = DefaultGeoIPURL

var geoIPDownloadClient = &http.Client{Timeout: geoIPDownloadTimeout}

var ErrLookupClosed = errors.New("geoip lookup is closed")

// RegionInfo contains region details
type RegionInfo struct {
	Code    string // "jp", "kr", "us", "hk", "tw", "other"
	Country string // Full country name
	ISOCode string // ISO country code
}

// Lookup provides GeoIP lookup functionality
type Lookup struct {
	db             *geoip2.Reader
	mu             sync.RWMutex
	path           string
	updateInterval time.Duration
	dnsCache       map[string]RegionInfo
	cacheMu        sync.RWMutex
	callbackMu     sync.RWMutex
	onUpdate       func()

	lifecycleMu     sync.Mutex
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	lifecycleWG     sync.WaitGroup
	updateMu        sync.Mutex
	closed          bool
	autoStarted     bool
	closeDone       chan struct{}
	closeErr        error

	// updateFn is a lifecycle test seam. Production lookups leave it nil and
	// execute updateDatabase.
	updateFn func(context.Context) error
}

// EnsureDatabase checks if the GeoIP database exists, and downloads it if not
func EnsureDatabase(dbPath string) error {
	if dbPath == "" {
		return nil
	}

	// Check if file already exists and is valid
	info, err := os.Stat(dbPath)
	if err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("geoip database path is not a file: %s", dbPath)
		}
		if info.Size() > 0 {
			return nil // File exists and has content
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat geoip database: %w", err)
	}

	log.Printf("📥 GeoIP database not found at %s, downloading...", dbPath)

	// Create parent directory if needed
	dir := filepath.Dir(dbPath)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("create directory: %w", err)
		}
	}

	downloadCtx, cancelDownload := context.WithTimeout(context.Background(), geoIPDownloadTimeout)
	defer cancelDownload()
	req, err := http.NewRequestWithContext(downloadCtx, http.MethodGet, geoIPDatabaseURL, nil)
	if err != nil {
		return fmt.Errorf("create download request: %w", err)
	}

	resp, err := geoIPDownloadClient.Do(req)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed: unexpected status %s", resp.Status)
	}
	if err := validateGeoIPContentLength(resp.ContentLength, maxGeoIPDatabaseSize); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	// Download to temporary file
	tempFile, err := os.CreateTemp(dir, ".geoip-*.mmdb")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	cleanup := true
	defer func() {
		if tempFile != nil {
			tempFile.Close()
		}
		if cleanup {
			os.Remove(tempPath)
		}
	}()

	if _, err := copyGeoIPDatabase(tempFile, resp, maxGeoIPDatabaseSize); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	if err := downloadCtx.Err(); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	// Sync and close temp file
	if err := tempFile.Sync(); err != nil {
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	tempFile = nil

	// Validate MMDB format
	if err := validateMMDB(tempPath); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}
	// Parse the complete database before disturbing the active mapping. The
	// lightweight metadata check above catches truncated downloads; this catches
	// structurally invalid MMDB files.
	candidate, err := geoip2.Open(tempPath)
	if err != nil {
		return fmt.Errorf("open downloaded database: %w", err)
	}
	if err := candidate.Close(); err != nil {
		return fmt.Errorf("close downloaded database: %w", err)
	}
	if err := downloadCtx.Err(); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, dbPath); err != nil {
		return fmt.Errorf("rename failed: %w", err)
	}
	cleanup = false

	log.Printf("✅ GeoIP database downloaded successfully to %s", dbPath)
	return nil
}

// progressWriter tracks download progress
type progressWriter struct {
	total       int64
	downloaded  int64
	lastPercent int64
	lastLog     time.Time
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n := len(b)
	p.downloaded += int64(n)

	now := time.Now()
	if p.total > 0 {
		percent := p.downloaded * 100 / p.total
		if percent >= 100 || percent >= p.lastPercent+10 || now.Sub(p.lastLog) >= 3*time.Second {
			log.Printf("   Progress: %d%% (%d/%d bytes)", percent, p.downloaded, p.total)
			p.lastPercent = percent
			p.lastLog = now
		}
	} else if now.Sub(p.lastLog) >= 3*time.Second {
		log.Printf("   Downloaded: %d bytes", p.downloaded)
		p.lastLog = now
	}

	return n, nil
}

// copyGeoIPDatabase enforces the same hard response limit for initial
// downloads and periodic updates. Content-Length is only an early rejection;
// the limited reader remains authoritative for chunked, compressed, or
// dishonest responses.
func copyGeoIPDatabase(destination io.Writer, response *http.Response, limit int64) (int64, error) {
	if destination == nil || response == nil || response.Body == nil {
		return 0, errors.New("invalid GeoIP download response")
	}
	if limit <= 0 {
		return 0, errors.New("invalid GeoIP database size limit")
	}
	if err := validateGeoIPContentLength(response.ContentLength, limit); err != nil {
		return 0, err
	}

	progress := &progressWriter{total: response.ContentLength}
	reader := io.LimitReader(response.Body, limit+1)
	written, err := io.Copy(destination, io.TeeReader(reader, progress))
	if err != nil {
		return written, err
	}
	if written > limit {
		return written, fmt.Errorf("GeoIP database exceeds %d bytes", limit)
	}
	if response.ContentLength >= 0 && written != response.ContentLength {
		return written, fmt.Errorf("incomplete download (%d/%d bytes)", written, response.ContentLength)
	}
	return written, nil
}

func validateGeoIPContentLength(contentLength, limit int64) error {
	if limit <= 0 {
		return errors.New("invalid GeoIP database size limit")
	}
	if contentLength > limit {
		return fmt.Errorf("GeoIP database exceeds %d bytes", limit)
	}
	return nil
}

// validateMMDB performs basic validation of MMDB file format
func validateMMDB(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < 1024 {
		return fmt.Errorf("file too small (%d bytes)", info.Size())
	}

	// Check for MaxMind metadata in the last 8KB
	const tailSize int64 = 8192
	readSize := tailSize
	if info.Size() < readSize {
		readSize = info.Size()
	}
	if _, err := file.Seek(-readSize, io.SeekEnd); err != nil {
		return err
	}
	buf := make([]byte, readSize)
	if _, err := io.ReadFull(file, buf); err != nil && err != io.ErrUnexpectedEOF {
		return err
	}
	if !bytes.Contains(buf, []byte("MaxMind.com")) {
		return fmt.Errorf("missing MaxMind metadata")
	}

	return nil
}

// New creates a new GeoIP lookup instance
func New(dbPath string) (*Lookup, error) {
	return NewWithAutoUpdate(dbPath, 0)
}

// NewWithAutoUpdate creates a new GeoIP lookup instance with auto-update support
func NewWithAutoUpdate(dbPath string, updateInterval time.Duration) (*Lookup, error) {
	if dbPath == "" {
		return newLookup(nil, "", 0), nil
	}

	// Ensure database exists (download if needed)
	if err := EnsureDatabase(dbPath); err != nil {
		return nil, fmt.Errorf("ensure database: %w", err)
	}

	db, err := geoip2.Open(dbPath)
	if err != nil {
		return nil, err
	}

	lookup := newLookup(db, dbPath, updateInterval)

	// Start auto-update goroutine if interval is set
	if updateInterval > 0 {
		lookup.startAutoUpdate()
		log.Printf("🔄 GeoIP auto-update enabled (interval: %v)", updateInterval)
	}

	return lookup, nil
}

func newLookup(db *geoip2.Reader, path string, updateInterval time.Duration) *Lookup {
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	return &Lookup{
		db:              db,
		path:            path,
		updateInterval:  updateInterval,
		dnsCache:        make(map[string]RegionInfo),
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
	}
}

func (l *Lookup) lifecycleContextLocked() context.Context {
	if l.lifecycleCtx == nil {
		l.lifecycleCtx, l.lifecycleCancel = context.WithCancel(context.Background())
	}
	return l.lifecycleCtx
}

func (l *Lookup) startAutoUpdate() {
	if l == nil || l.updateInterval <= 0 {
		return
	}
	l.lifecycleMu.Lock()
	if l.closed || l.autoStarted {
		l.lifecycleMu.Unlock()
		return
	}
	l.autoStarted = true
	ctx := l.lifecycleContextLocked()
	l.lifecycleWG.Add(1)
	l.lifecycleMu.Unlock()
	go l.autoUpdateLoop(ctx)
}

// autoUpdateLoop periodically updates the GeoIP database.
func (l *Lookup) autoUpdateLoop(ctx context.Context) {
	defer l.lifecycleWG.Done()
	ticker := time.NewTicker(l.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := l.runUpdate(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrLookupClosed) {
				log.Printf("⚠️  GeoIP auto-update failed: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// Update downloads and reloads the GeoIP database
func (l *Lookup) Update() error {
	return l.runUpdate()
}

func (l *Lookup) beginUpdate() (context.Context, func(), error) {
	if l == nil {
		return nil, nil, ErrLookupClosed
	}
	l.lifecycleMu.Lock()
	if l.closed {
		l.lifecycleMu.Unlock()
		return nil, nil, ErrLookupClosed
	}
	ctx := l.lifecycleContextLocked()
	l.lifecycleWG.Add(1)
	l.lifecycleMu.Unlock()
	return ctx, l.lifecycleWG.Done, nil
}

func (l *Lookup) runUpdate() error {
	ctx, finish, err := l.beginUpdate()
	if err != nil {
		return err
	}
	defer finish()

	updateErr := func() error {
		l.updateMu.Lock()
		defer l.updateMu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		update := l.updateFn
		if update == nil {
			update = l.updateDatabase
		}
		return update(ctx)
	}()
	if updateErr != nil {
		return updateErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.notifyUpdate()
	return nil
}

func (l *Lookup) updateDatabase(ctx context.Context) error {
	log.Printf("🔄 Updating GeoIP database...")
	if strings.TrimSpace(l.path) == "" {
		return errors.New("GeoIP database path is empty")
	}

	// Download to temporary file
	tempPath := l.path + ".update"
	if err := downloadDatabaseContext(ctx, tempPath); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	defer os.Remove(tempPath) // Clean up temp file
	if err := ctx.Err(); err != nil {
		return err
	}

	// Validate the downloaded database
	if err := validateMMDB(tempPath); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}
	candidate, err := geoip2.Open(tempPath)
	if err != nil {
		return fmt.Errorf("open downloaded database: %w", err)
	}
	if err := candidate.Close(); err != nil {
		return fmt.Errorf("close downloaded database: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// On Windows the active MMDB is memory-mapped and cannot be replaced while
	// open. Hold the write lock so lookups pause briefly, close the old mapping,
	// atomically replace the file, then reopen the configured path.
	l.mu.Lock()
	oldDB := l.db
	if oldDB != nil {
		if err := oldDB.Close(); err != nil {
			l.mu.Unlock()
			return fmt.Errorf("close old database: %w", err)
		}
	}
	l.db = nil
	if err := config.ReplaceFileAtomic(tempPath, l.path); err != nil {
		reopened, reopenErr := geoip2.Open(l.path)
		l.db = reopened
		l.mu.Unlock()
		if reopenErr != nil {
			return fmt.Errorf("replace database: %w; reopen previous database: %v", err, reopenErr)
		}
		return fmt.Errorf("replace database: %w", err)
	}
	newDB, err := geoip2.Open(l.path)
	if err != nil {
		l.mu.Unlock()
		return fmt.Errorf("open replaced database: %w", err)
	}
	l.db = newDB
	l.mu.Unlock()
	l.cacheMu.Lock()
	l.dnsCache = make(map[string]RegionInfo)
	l.cacheMu.Unlock()

	log.Printf("✅ GeoIP database updated successfully")
	return nil
}

// SetUpdateCallback installs a callback invoked after a successful database
// swap. The latest callback replaces the previous one.
func (l *Lookup) SetUpdateCallback(callback func()) {
	if l == nil {
		return
	}
	l.lifecycleMu.Lock()
	defer l.lifecycleMu.Unlock()
	if l.closed {
		return
	}
	l.callbackMu.Lock()
	l.onUpdate = callback
	l.callbackMu.Unlock()
}

func (l *Lookup) notifyUpdate() {
	if l == nil {
		return
	}
	l.lifecycleMu.Lock()
	if l.closed {
		l.lifecycleMu.Unlock()
		return
	}
	l.callbackMu.RLock()
	callback := l.onUpdate
	l.callbackMu.RUnlock()
	l.lifecycleMu.Unlock()
	if callback != nil {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					log.Printf("⚠️  GeoIP update callback panicked: %v", recovered)
				}
			}()
			callback()
		}()
	}
}

// downloadDatabase downloads the GeoIP database to the specified path
func downloadDatabase(dbPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), geoIPDownloadTimeout)
	defer cancel()
	return downloadDatabaseContext(ctx, dbPath)
}

func downloadDatabaseContext(ctx context.Context, dbPath string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Create parent directory if needed
	dir := filepath.Dir(dbPath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("create directory: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, geoIPDatabaseURL, nil)
	if err != nil {
		return fmt.Errorf("create download request: %w", err)
	}

	resp, err := geoIPDownloadClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	if err := validateGeoIPContentLength(resp.ContentLength, maxGeoIPDatabaseSize); err != nil {
		return err
	}

	// Create temp file
	tempFile, err := os.CreateTemp(dir, ".geoip-download-*.mmdb")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	cleanup := true
	defer func() {
		if tempFile != nil {
			tempFile.Close()
		}
		if cleanup {
			os.Remove(tempPath)
		}
	}()

	if _, err := copyGeoIPDatabase(tempFile, resp, maxGeoIPDatabaseSize); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Sync and close
	if err := tempFile.Sync(); err != nil {
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}
	tempFile = nil
	if err := ctx.Err(); err != nil {
		return err
	}

	// Rename to target path
	if err := config.ReplaceFileAtomic(tempPath, dbPath); err != nil {
		return err
	}
	cleanup = false

	return nil
}

// Close closes the GeoIP database and stops auto-update
func (l *Lookup) Close() error {
	if l == nil {
		return nil
	}
	l.lifecycleMu.Lock()
	if l.closed {
		done := l.closeDone
		l.lifecycleMu.Unlock()
		if done != nil {
			<-done
		}
		l.lifecycleMu.Lock()
		err := l.closeErr
		l.lifecycleMu.Unlock()
		return err
	}
	l.closed = true
	l.closeDone = make(chan struct{})
	closeDone := l.closeDone
	if l.lifecycleCancel != nil {
		l.lifecycleCancel()
	}
	l.callbackMu.Lock()
	l.onUpdate = nil
	l.callbackMu.Unlock()
	l.lifecycleMu.Unlock()

	// beginUpdate registers each update before Close can mark the lifecycle
	// closed. Waiting here therefore covers the periodic loop, queued updates,
	// the active download, database replacement, and its callback.
	l.lifecycleWG.Wait()

	l.mu.Lock()
	var closeErr error
	if l.db != nil {
		closeErr = l.db.Close()
		l.db = nil
	}
	l.mu.Unlock()

	l.lifecycleMu.Lock()
	l.closeErr = closeErr
	close(closeDone)
	l.lifecycleMu.Unlock()
	return closeErr
}

// IsEnabled returns true if GeoIP lookup is available
func (l *Lookup) IsEnabled() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.db != nil
}

// LookupIP returns region info for an IP address
func (l *Lookup) LookupIP(ipStr string) RegionInfo {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.db == nil {
		return RegionInfo{Code: RegionOther, Country: "Unknown", ISOCode: ""}
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return RegionInfo{Code: RegionOther, Country: "Unknown", ISOCode: ""}
	}

	record, err := l.db.Country(ip)
	if err != nil {
		return RegionInfo{Code: RegionOther, Country: "Unknown", ISOCode: ""}
	}

	isoCode := record.Country.IsoCode
	country := record.Country.Names["en"]
	if country == "" {
		country = isoCode
	}

	return RegionInfo{
		Code:    isoCodeToRegion(isoCode),
		Country: country,
		ISOCode: isoCode,
	}
}

// LookupURI extracts server from URI and returns region info
func (l *Lookup) LookupURI(uri string) RegionInfo {
	host := extractHostFromURI(uri)
	if host == "" {
		return RegionInfo{Code: RegionOther, Country: "Unknown", ISOCode: ""}
	}

	// Check DNS cache first
	l.cacheMu.RLock()
	if cached, ok := l.dnsCache[host]; ok {
		l.cacheMu.RUnlock()
		return cached
	}
	l.cacheMu.RUnlock()

	// Resolve hostname to IP if needed
	ip := net.ParseIP(host)
	if ip == nil {
		// It's a hostname, try to resolve with timeout
		resolver := &net.Resolver{}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ips, err := resolver.LookupIPAddr(ctx, host)
		if err != nil || len(ips) == 0 {
			result := RegionInfo{Code: RegionOther, Country: "Unknown", ISOCode: ""}
			// Cache failed lookups too to avoid repeated timeouts
			l.cacheMu.Lock()
			l.dnsCache[host] = result
			l.cacheMu.Unlock()
			return result
		}
		host = ips[0].IP.String()
	}

	result := l.LookupIP(host)

	// Cache the result
	l.cacheMu.Lock()
	l.dnsCache[extractHostFromURI(uri)] = result
	l.cacheMu.Unlock()

	return result
}

// extractHostFromURI extracts the host/IP from various proxy URI formats
func extractHostFromURI(uri string) string {
	lowerURI := strings.ToLower(uri)

	// VMess: typically base64-encoded JSON — must be handled specially
	if strings.HasPrefix(lowerURI, "vmess://") {
		return extractVMessHost(uri)
	}

	// Shadowsocks: SIP002 and legacy whole-payload base64 formats.
	if strings.HasPrefix(lowerURI, "ss://") || strings.HasPrefix(lowerURI, "shadowsocks://") {
		return extractSSHost(uri)
	}

	// SSR: base64 encoded
	if strings.HasPrefix(lowerURI, "ssr://") || strings.HasPrefix(lowerURI, "shadowsocksr://") {
		return extractSSRHost(uri)
	}

	// All other standard URL-parseable schemes
	standardSchemes := []string{
		"vless://", "trojan://",
		"hysteria://", "hysteria2://", "hy2://",
		"anytls://", "tuic://",
		"socks5://", "socks5h://", "socks://",
		"http://", "https://",
	}
	for _, scheme := range standardSchemes {
		if strings.HasPrefix(lowerURI, scheme) {
			parsed, err := url.Parse(uri)
			if err != nil {
				return ""
			}
			return parsed.Hostname()
		}
	}

	// Fallback: try url.Parse for any unknown scheme
	parsed, err := url.Parse(uri)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

// extractVMessHost extracts the server address from a vmess:// URI.
// VMess URIs come in two formats:
//  1. Base64 JSON: vmess://base64({"add":"1.2.3.4", ...})
//  2. URL format:  vmess://uuid@host:port?...
func extractVMessHost(uri string) string {
	// Strip scheme (case-insensitive)
	idx := strings.Index(uri, "://")
	if idx < 0 {
		return ""
	}
	encoded := uri[idx+3:]
	if encoded == "" {
		return ""
	}

	// Strip fragment (#name) if present — it breaks base64 decode
	if hashIdx := strings.Index(encoded, "#"); hashIdx >= 0 {
		encoded = encoded[:hashIdx]
	}

	// Try base64 JSON format first (most common)
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(encoded)
	}
	if err != nil {
		decoded, err = base64.RawURLEncoding.DecodeString(encoded)
	}
	if err == nil && len(decoded) > 0 {
		var obj struct {
			Add string `json:"add"`
		}
		if json.Unmarshal(decoded, &obj) == nil && obj.Add != "" {
			return obj.Add
		}
	}

	// Fallback: try URL format vmess://uuid@host:port?...
	parsed, err := url.Parse(uri)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

func extractSSHost(uri string) string {
	parsed, err := ssuri.Parse(uri)
	if err != nil {
		return ""
	}
	return parsed.Server
}

func extractSSRHost(uri string) string {
	parsed, err := ssruri.Parse(uri)
	if err != nil {
		return ""
	}
	return parsed.Server
}

// isoCodeToRegion maps ISO country codes to our region codes
func isoCodeToRegion(isoCode string) string {
	switch strings.ToUpper(isoCode) {
	case "JP":
		return RegionJP
	case "KR":
		return RegionKR
	case "US":
		return RegionUS
	case "HK":
		return RegionHK
	case "TW":
		return RegionTW
	case "SG":
		return RegionSG
	default:
		return RegionOther
	}
}

// AllRegions returns all supported region codes
func AllRegions() []string {
	return []string{RegionJP, RegionKR, RegionUS, RegionHK, RegionTW, RegionSG, RegionOther}
}

// RegionName returns the display name for a region code
func RegionName(code string) string {
	switch code {
	case RegionJP:
		return "Japan"
	case RegionKR:
		return "Korea"
	case RegionUS:
		return "USA"
	case RegionHK:
		return "Hong Kong"
	case RegionTW:
		return "Taiwan"
	case RegionSG:
		return "Singapore"
	case RegionOther:
		return "Other"
	default:
		return "Unknown"
	}
}

// RegionEmoji returns the flag emoji for a region code
func RegionEmoji(code string) string {
	switch code {
	case RegionJP:
		return "🇯🇵"
	case RegionKR:
		return "🇰🇷"
	case RegionUS:
		return "🇺🇸"
	case RegionHK:
		return "🇭🇰"
	case RegionTW:
		return "🇹🇼"
	case RegionSG:
		return "🇸🇬"
	case RegionOther:
		return "🌍"
	default:
		return "❓"
	}
}
