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
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	defaultSubscriptionFetchConcurrency = 16
	maxSubscriptionFetchConcurrency     = 32
	maxSubscriptionBodySize             = 10 * 1024 * 1024
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
	Timeout     time.Duration
	Concurrency int
	Client      *http.Client
	Loggerf     func(format string, args ...any)
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
	u.User = nil
	if u.Path != "" && u.Path != "/" {
		u.Path = "/..."
		u.RawPath = ""
	}
	if u.RawQuery != "" {
		u.RawQuery = "redacted=1"
	}
	u.Fragment = ""
	return u.String()
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
	specs, deduped := dedupeSubscriptionURLs(urls)
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
		client = newSubscriptionHTTPClient(timeout)
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
				nodes, err := fetchSubscriptionWithClient(ctx, client, specs[index].raw, timeout)
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
	return fetchSubscriptionWithClient(context.Background(), newSubscriptionHTTPClient(timeout), subURL, timeout)
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

func newSubscriptionHTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   minSubscriptionDuration(timeout, 10*time.Second),
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   minSubscriptionDuration(timeout, 10*time.Second),
		ResponseHeaderTimeout: minSubscriptionDuration(timeout, 15*time.Second),
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{Transport: transport, Timeout: timeout}
}

func minSubscriptionDuration(first, second time.Duration) time.Duration {
	if first <= 0 || second < first {
		return second
	}
	return first
}

func fetchSubscriptionWithClient(ctx context.Context, client *http.Client, rawURL string, timeout time.Duration) ([]NodeConfig, error) {
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
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("unsupported subscription scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("subscription URL is missing host")
	}

	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, redactSubscriptionError("create subscription request", rawURL, err)
	}
	request.Header.Set("User-Agent", "clash-verge/v2.2.3")
	request.Header.Set("Accept", "*/*")

	response, err := client.Do(request)
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
	return parseSubscriptionContent(string(body))
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
