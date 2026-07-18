package config

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var subscriptionErrorURLPattern = regexp.MustCompile(`(?i)https?://[^\s"'<>]+`)

const (
	defaultSubscriptionFetchConcurrency = 16
	maxSubscriptionFetchConcurrency     = 32
	maxSubscriptionBodySize             = 10 * 1024 * 1024
	MaxSubscriptionURLs                 = 128
	MaxSubscriptionURLLength            = 8 * 1024
	MaxSubscriptionURLBytes             = 256 * 1024
	MaxSubscriptionNodesPerSource       = 25_000
	MaxSubscriptionNodesTotal           = 50_000
	MaxSubscriptionNodeURIBytes         = 16 * 1024
	MaxSubscriptionNodeNameBytes        = 1024
	MaxSubscriptionNodeBytesPerSource   = 24 * 1024 * 1024
	MaxSubscriptionNodeBytesTotal       = 48 * 1024 * 1024
)

// SubscriptionFetchStats describes one batch of subscription requests.
type SubscriptionFetchStats struct {
	RequestedURLs int
	UniqueURLs    int
	Successful    int
	Failed        int
	Empty         int
	Nodes         int
	DedupedURLs   int
	DedupedNodes  int
	LastError     error
}

// SubscriptionFetchOptions controls bounded concurrent subscription loading.
type SubscriptionFetchOptions struct {
	Timeout              time.Duration
	Concurrency          int
	AllowPrivateNetworks bool
	Client               *http.Client
	Loggerf              func(format string, args ...any)
}

// SubscriptionSourceResult is the result for one stable, unique URL identity.
// Key is a one-way identifier and is safe to use as an in-memory cache key.
type SubscriptionSourceResult struct {
	Key   string
	Nodes []NodeConfig
	Err   error
}

type subscriptionURLSpec struct {
	raw string
	key string
}

// NormalizeSubscriptionFetchConcurrency applies the documented default and cap.
func NormalizeSubscriptionFetchConcurrency(value int) int {
	if value <= 0 {
		return defaultSubscriptionFetchConcurrency
	}
	if value > maxSubscriptionFetchConcurrency {
		return maxSubscriptionFetchConcurrency
	}
	return value
}

// RedactURL removes credentials, path data, query values, and fragments before
// a subscription URL is included in logs or user-visible errors.
func RedactURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "<invalid-url>"
	}
	redacted := strings.ToLower(u.Scheme) + "://<redacted-host>"
	if port := u.Port(); port != "" {
		redacted += ":" + port
	}
	if u.Path != "" && u.Path != "/" {
		redacted += "/..."
	} else if u.Path == "/" {
		redacted += "/"
	}
	if u.RawQuery != "" {
		redacted += "?redacted=1"
	}
	return redacted
}

// FetchSubscriptionSources returns stable input-order results for each unique
// subscription URL. Fetches run concurrently, but result ordering never depends
// on network completion order.
func FetchSubscriptionSources(ctx context.Context, urls []string, opts SubscriptionFetchOptions) ([]SubscriptionSourceResult, SubscriptionFetchStats) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	validatedURLs, validationErr := ValidateSubscriptionURLs(urls)
	if validationErr != nil {
		stats := SubscriptionFetchStats{RequestedURLs: len(urls), Failed: 1, LastError: validationErr}
		return []SubscriptionSourceResult{{Key: subscriptionURLKey("invalid-subscription-batch"), Err: validationErr}}, stats
	}
	specs, deduped := dedupeSubscriptionURLs(validatedURLs)
	stats := SubscriptionFetchStats{
		RequestedURLs: len(urls),
		UniqueURLs:    len(specs),
		DedupedURLs:   deduped,
	}
	if len(specs) == 0 {
		return nil, stats
	}

	client := opts.Client
	ownedClient := false
	if client == nil {
		client = newSubscriptionHTTPClient(timeout, opts.AllowPrivateNetworks)
		ownedClient = true
	}
	if ownedClient {
		defer client.CloseIdleConnections()
	}
	concurrency := NormalizeSubscriptionFetchConcurrency(opts.Concurrency)
	if concurrency > len(specs) {
		concurrency = len(specs)
	}

	type indexedResult struct {
		index int
		nodes []NodeConfig
		err   error
	}
	jobs := make(chan int)
	completed := make(chan indexedResult, len(specs))
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for worker := 0; worker < concurrency; worker++ {
		go func() {
			defer workers.Done()
			for index := range jobs {
				nodes, err := fetchSubscriptionWithClient(ctx, client, specs[index].raw, timeout, opts.AllowPrivateNetworks)
				completed <- indexedResult{index: index, nodes: nodes, err: err}
			}
		}()
	}
	go func() {
		for index := range specs {
			jobs <- index
		}
		close(jobs)
		workers.Wait()
		close(completed)
	}()

	results := make([]SubscriptionSourceResult, len(specs))
	for result := range completed {
		spec := specs[result.index]
		results[result.index] = SubscriptionSourceResult{
			Key:   spec.key,
			Nodes: cloneSubscriptionNodes(result.nodes),
			Err:   result.err,
		}
	}

	// Apply aggregate semantic limits in stable configured order. The response
	// byte cap protects the HTTP read, while these caps prevent many compact
	// nodes or expanded Clash fields from exhausting memory during reload.
	totalNodes := 0
	totalBytes := 0
	for index := range results {
		if results[index].Err != nil {
			continue
		}
		nodeBytes := subscriptionNodeBytes(results[index].Nodes)
		if totalNodes+len(results[index].Nodes) > MaxSubscriptionNodesTotal || totalBytes+nodeBytes > MaxSubscriptionNodeBytesTotal {
			results[index].Nodes = nil
			results[index].Err = errors.New("subscription aggregate exceeds configured safety limits")
			continue
		}
		totalNodes += len(results[index].Nodes)
		totalBytes += nodeBytes
	}

	// Aggregate and log in configured order, keeping output deterministic.
	for index, result := range results {
		redacted := RedactURL(specs[index].raw)
		switch {
		case result.Err != nil:
			stats.Failed++
			stats.LastError = result.Err
			if opts.Loggerf != nil {
				opts.Loggerf("subscription fetch failed for %s: %v", redacted, result.Err)
			}
		case len(result.Nodes) == 0:
			stats.Empty++
			if opts.Loggerf != nil {
				opts.Loggerf("subscription %s returned no usable nodes", redacted)
			}
		default:
			stats.Successful++
			stats.Nodes += len(result.Nodes)
			if opts.Loggerf != nil {
				opts.Loggerf("loaded %d nodes from subscription %s", len(result.Nodes), redacted)
			}
		}
	}

	return results, stats
}

// FetchSubscriptionNodes concurrently loads all sources and deduplicates nodes
// by their canonical stable identity.
func FetchSubscriptionNodes(ctx context.Context, urls []string, opts SubscriptionFetchOptions) ([]NodeConfig, SubscriptionFetchStats) {
	results, stats := FetchSubscriptionSources(ctx, urls, opts)
	allNodes := make([]NodeConfig, 0, stats.Nodes)
	for _, result := range results {
		if result.Err == nil {
			allNodes = append(allNodes, result.Nodes...)
		}
	}
	allNodes, stats.DedupedNodes = DedupeNodesByStableIdentity(allNodes)
	return allNodes, stats
}

// DedupeNodesByStableIdentity preserves first-seen order and removes nodes that
// differ only by display metadata such as fragment/name or URL query ordering.
func DedupeNodesByStableIdentity(nodes []NodeConfig) ([]NodeConfig, int) {
	seen := make(map[string]struct{}, len(nodes))
	unique := make([]NodeConfig, 0, len(nodes))
	deduped := 0
	for _, original := range nodes {
		node := original
		node.URI = strings.TrimSpace(node.URI)
		if node.URI == "" {
			deduped++
			continue
		}
		key := node.NodeKey()
		if key == "" {
			key = node.URI
		}
		if _, exists := seen[key]; exists {
			deduped++
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, node)
	}
	return unique, deduped
}

// LoadNodesFromFile reads the URI-per-line cache used as a restart fallback.
func LoadNodesFromFile(path string) ([]NodeConfig, error) {
	return loadNodesFromFile(path)
}

// loadNodesFromSubscription is retained for callers that load one source.
func loadNodesFromSubscription(subURL string, timeout time.Duration) ([]NodeConfig, error) {
	client := newSubscriptionHTTPClient(timeout, false)
	defer client.CloseIdleConnections()
	return fetchSubscriptionWithClient(context.Background(), client, subURL, timeout, false)
}

// ValidateSubscriptionURLs applies process-wide input limits before URLs are
// persisted or scheduled. It deliberately performs only structural validation;
// destination network policy is enforced at request and dial time.
func ValidateSubscriptionURLs(urls []string) ([]string, error) {
	clean := make([]string, 0, len(urls))
	totalBytes := 0
	for _, raw := range urls {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if len(clean) >= MaxSubscriptionURLs {
			return nil, fmt.Errorf("too many subscription URLs (maximum %d)", MaxSubscriptionURLs)
		}
		if len(raw) > MaxSubscriptionURLLength {
			return nil, fmt.Errorf("subscription URL exceeds %d bytes", MaxSubscriptionURLLength)
		}
		if containsControlCharacter(raw) {
			return nil, errors.New("subscription URL contains control characters")
		}
		totalBytes += len(raw)
		if totalBytes > MaxSubscriptionURLBytes {
			return nil, fmt.Errorf("subscription URLs exceed %d total bytes", MaxSubscriptionURLBytes)
		}
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Hostname() == "" {
			return nil, errors.New("invalid subscription URL")
		}
		scheme := strings.ToLower(parsed.Scheme)
		if scheme != "http" && scheme != "https" {
			return nil, fmt.Errorf("unsupported subscription scheme %q", parsed.Scheme)
		}
		if port := parsed.Port(); port != "" {
			value, err := strconv.Atoi(port)
			if err != nil || value < 1 || value > 65535 {
				return nil, errors.New("invalid subscription URL port")
			}
		}
		clean = append(clean, raw)
	}
	return clean, nil
}

func containsControlCharacter(value string) bool {
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}

func validateSubscriptionNodes(nodes []NodeConfig) error {
	if len(nodes) > MaxSubscriptionNodesPerSource {
		return fmt.Errorf("subscription contains too many nodes (maximum %d)", MaxSubscriptionNodesPerSource)
	}
	if subscriptionNodeBytes(nodes) > MaxSubscriptionNodeBytesPerSource {
		return fmt.Errorf("subscription nodes exceed %d total bytes", MaxSubscriptionNodeBytesPerSource)
	}
	for _, node := range nodes {
		if len(node.URI) > MaxSubscriptionNodeURIBytes {
			return fmt.Errorf("subscription node URI exceeds %d bytes", MaxSubscriptionNodeURIBytes)
		}
		if len(node.Name) > MaxSubscriptionNodeNameBytes {
			return fmt.Errorf("subscription node name exceeds %d bytes", MaxSubscriptionNodeNameBytes)
		}
	}
	return nil
}

func subscriptionNodeBytes(nodes []NodeConfig) int {
	total := 0
	for _, node := range nodes {
		total += len(node.Name) + len(node.URI)
	}
	return total
}

func dedupeSubscriptionURLs(urls []string) ([]subscriptionURLSpec, int) {
	seen := make(map[string]struct{}, len(urls))
	unique := make([]subscriptionURLSpec, 0, len(urls))
	deduped := 0
	for _, raw := range urls {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		key := subscriptionURLKey(raw)
		if _, exists := seen[key]; exists {
			deduped++
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, subscriptionURLSpec{raw: raw, key: key})
	}
	return unique, deduped
}

func subscriptionURLKey(raw string) string {
	identity := strings.TrimSpace(raw)
	if parsed, err := url.Parse(identity); err == nil {
		parsed.Scheme = strings.ToLower(parsed.Scheme)
		parsed.Host = strings.ToLower(parsed.Host)
		parsed.Fragment = ""
		parsed.RawQuery = parsed.Query().Encode()
		identity = parsed.String()
	}
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

var disallowedSubscriptionPrefixes = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"), // Shared address space and several metadata endpoints.
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001:db8::/32"),
}

func newSubscriptionHTTPClient(timeout time.Duration, allowPrivateNetworks bool) *http.Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	dialTimeout := minSubscriptionDuration(timeout, 10*time.Second)
	transport := &http.Transport{
		DialContext:           subscriptionDialContext(dialTimeout, allowPrivateNetworks),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   minSubscriptionDuration(timeout, 10*time.Second),
		ResponseHeaderTimeout: minSubscriptionDuration(timeout, 15*time.Second),
		ExpectContinueTimeout: 1 * time.Second,
	}
	// A proxy performs its own DNS resolution and could bypass the pinned-IP
	// dialer. Keep environment proxy support only when private destinations were
	// explicitly enabled by the administrator.
	if allowPrivateNetworks {
		transport.Proxy = http.ProxyFromEnvironment
	}
	return &http.Client{Transport: transport, Timeout: timeout}
}

func subscriptionDialContext(timeout time.Duration, allowPrivateNetworks bool) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	if allowPrivateNetworks {
		return dialer.DialContext
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, errors.New("invalid subscription destination")
		}
		addresses, err := resolveAllowedSubscriptionAddresses(ctx, host)
		if err != nil {
			return nil, err
		}
		var lastErr error
		for _, candidate := range addresses {
			connection, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(candidate.String(), port))
			if dialErr == nil {
				return connection, nil
			}
			lastErr = dialErr
		}
		if lastErr == nil {
			lastErr = errors.New("subscription destination has no public address")
		}
		return nil, lastErr
	}
}

func validateSubscriptionURLTarget(ctx context.Context, target *url.URL, allowPrivateNetworks bool) error {
	if target == nil || target.Hostname() == "" {
		return errors.New("subscription URL is missing host")
	}
	scheme := strings.ToLower(target.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("unsupported subscription scheme %q", target.Scheme)
	}
	if allowPrivateNetworks {
		return nil
	}
	_, err := resolveAllowedSubscriptionAddresses(ctx, target.Hostname())
	return err
}

func resolveAllowedSubscriptionAddresses(ctx context.Context, host string) ([]netip.Addr, error) {
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if host == "" {
		return nil, errors.New("subscription URL is missing host")
	}
	var addresses []netip.Addr
	if literal, err := netip.ParseAddr(host); err == nil {
		addresses = []netip.Addr{literal.Unmap()}
	} else {
		resolved, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("resolve subscription destination: %w", err)
		}
		addresses = resolved
	}
	allowed := make([]netip.Addr, 0, len(addresses))
	for _, address := range addresses {
		address = address.Unmap()
		if !isBlockedSubscriptionAddress(address) {
			allowed = append(allowed, address)
		}
	}
	if len(allowed) == 0 {
		return nil, errors.New("subscription destination is blocked by network policy")
	}
	return allowed, nil
}

func isBlockedSubscriptionAddress(address netip.Addr) bool {
	if !address.IsValid() || !address.IsGlobalUnicast() || address.IsLoopback() || address.IsPrivate() ||
		address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsMulticast() || address.IsUnspecified() {
		return true
	}
	for _, prefix := range disallowedSubscriptionPrefixes {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

func minSubscriptionDuration(first, second time.Duration) time.Duration {
	if first <= 0 || second < first {
		return second
	}
	return first
}

func fetchSubscriptionWithClient(ctx context.Context, client *http.Client, rawURL string, timeout time.Duration, allowPrivateNetworks bool) ([]NodeConfig, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, redactSubscriptionError("parse subscription URL", rawURL, err)
	}
	if strings.ToLower(parsed.Scheme) != "http" && strings.ToLower(parsed.Scheme) != "https" {
		return nil, fmt.Errorf("unsupported subscription scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("subscription URL is missing host")
	}

	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := validateSubscriptionURLTarget(requestCtx, parsed, allowPrivateNetworks); err != nil {
		return nil, redactSubscriptionError("validate subscription destination", rawURL, err)
	}
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, redactSubscriptionError("create subscription request", rawURL, err)
	}
	request.Header.Set("User-Agent", "clash-verge/v2.2.3")
	request.Header.Set("Accept", "*/*")

	if client == nil {
		client = newSubscriptionHTTPClient(timeout, allowPrivateNetworks)
		defer client.CloseIdleConnections()
	}
	requestClient := *client
	originalRedirectPolicy := client.CheckRedirect
	requestClient.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		if err := validateSubscriptionURLTarget(next.Context(), next.URL, allowPrivateNetworks); err != nil {
			return err
		}
		if originalRedirectPolicy != nil {
			return originalRedirectPolicy(next, via)
		}
		return nil
	}
	response, err := requestClient.Do(request)
	if err != nil {
		return nil, redactSubscriptionError("fetch subscription", rawURL, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("subscription returned status %d", response.StatusCode)
	}
	if response.ContentLength > maxSubscriptionBodySize {
		return nil, fmt.Errorf("subscription response exceeds %d bytes", maxSubscriptionBodySize)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, maxSubscriptionBodySize+1))
	if err != nil {
		return nil, redactSubscriptionError("read subscription response", rawURL, err)
	}
	if len(body) > maxSubscriptionBodySize {
		return nil, fmt.Errorf("subscription response exceeds %d bytes", maxSubscriptionBodySize)
	}
	nodes, err := parseSubscriptionContent(string(body))
	if err != nil {
		return nil, err
	}
	if err := validateSubscriptionNodes(nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

func redactSubscriptionError(operation, rawURL string, err error) error {
	if err == nil {
		return nil
	}
	redacted := RedactURL(rawURL)
	message := err.Error()
	for _, sensitive := range subscriptionURLForms(rawURL) {
		if sensitive != "" {
			message = strings.ReplaceAll(message, sensitive, redacted)
		}
	}
	// net/http reports the final redirect URL in *url.Error. That URL may be a
	// signed CDN target unrelated to the original subscription URL, so redact
	// every absolute HTTP(S) URL still present in the error as a final fence.
	message = subscriptionErrorURLPattern.ReplaceAllStringFunc(message, RedactURL)
	return fmt.Errorf("%s: %s", operation, message)
}

func subscriptionURLForms(raw string) []string {
	forms := []string{raw, strings.TrimSpace(raw)}
	if parsed, err := url.Parse(strings.TrimSpace(raw)); err == nil {
		forms = append(forms, parsed.String())
		requestURI := parsed.RequestURI()
		if len(requestURI) > 1 {
			forms = append(forms, requestURI)
		}
	}
	return forms
}

func cloneSubscriptionNodes(nodes []NodeConfig) []NodeConfig {
	if len(nodes) == 0 {
		return nil
	}
	return append([]NodeConfig(nil), nodes...)
}
