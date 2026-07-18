package config

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchSubscriptionSourcesConcurrentStableDedupeAndRedaction(t *testing.T) {
	var requests atomic.Int32
	var active atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if active.Add(1) == 2 {
			releaseOnce.Do(func() { close(release) })
		}
		defer active.Add(-1)
		select {
		case <-release:
		case <-time.After(time.Second):
			http.Error(w, "not concurrent", http.StatusGatewayTimeout)
			return
		}
		switch request.URL.Path {
		case "/one":
			_, _ = w.Write([]byte("vless://id@example.com:443?security=tls&type=ws#first\n"))
		case "/two":
			_, _ = w.Write([]byte(strings.Join([]string{
				"vless://id@example.com:443?type=ws&security=tls#renamed",
				"trojan://pw@example.org:443#second",
			}, "\n")))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	var logs []string
	results, stats := FetchSubscriptionSources(context.Background(), []string{
		server.URL + "/one?token=secret&client=one",
		server.URL + "/one?client=one&token=secret#ignored",
		server.URL + "/two?token=second-secret",
	}, SubscriptionFetchOptions{
		Concurrency:          2,
		Timeout:              2 * time.Second,
		Client:               server.Client(),
		AllowPrivateNetworks: true,
		Loggerf: func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		},
	})

	if requests.Load() != 2 {
		t.Fatalf("expected two stable unique URL requests, got %d", requests.Load())
	}
	if len(results) != 2 || len(results[0].Nodes) != 1 || len(results[1].Nodes) != 2 {
		t.Fatalf("results did not preserve configured URL order: %#v", results)
	}
	if stats.RequestedURLs != 3 || stats.UniqueURLs != 2 || stats.DedupedURLs != 1 || stats.Successful != 2 {
		t.Fatalf("unexpected fetch stats: %+v", stats)
	}
	joined := strings.Join(logs, "\n")
	if strings.Contains(joined, "secret") || strings.Contains(joined, "client=one") {
		t.Fatalf("subscription log leaked URL data: %s", joined)
	}

	nodes, aggregateStats := FetchSubscriptionNodes(context.Background(), []string{
		server.URL + "/one",
		server.URL + "/two",
	}, SubscriptionFetchOptions{Concurrency: 2, Client: server.Client(), AllowPrivateNetworks: true})
	if len(nodes) != 2 || aggregateStats.DedupedNodes != 1 {
		t.Fatalf("expected stable node identity dedupe, got %d nodes and %+v", len(nodes), aggregateStats)
	}
}

func TestStartupSubscriptionCacheCommitsOnlyAfterExplicitSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "socks5://127.0.0.1:1082#fresh\n")
	}))
	defer server.Close()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	nodesPath := filepath.Join(dir, "nodes.txt")
	original := []byte("socks5://127.0.0.1:1081#last-known-good\n")
	if err := os.WriteFile(nodesPath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	configYAML := fmt.Sprintf("mode: pool\nsubscriptions:\n  - %q\nnodes_file: nodes.txt\nsubscription_refresh:\n  allow_private_networks: true\n", server.URL)
	if err := os.WriteFile(configPath, []byte(configYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(nodesPath); err != nil || string(got) != string(original) {
		t.Fatalf("Load overwrote last-known-good cache: err=%v data=%q", err, got)
	}
	if err := cfg.SaveSubscriptionCache(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(nodesPath); err != nil || !strings.Contains(string(got), "127.0.0.1:1082") {
		t.Fatalf("explicit cache commit missing fresh node: err=%v data=%q", err, got)
	}
}

func TestFetchSubscriptionStrictBodyLimit(t *testing.T) {
	tests := []struct {
		name      string
		size      int
		wantError bool
	}{
		{name: "exact limit accepted", size: maxSubscriptionBodySize},
		{name: "one byte over rejected", size: maxSubscriptionBodySize + 1, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(strings.Repeat("x", test.size)))
			}))
			defer server.Close()

			results, _ := FetchSubscriptionSources(context.Background(), []string{server.URL}, SubscriptionFetchOptions{
				Client:               server.Client(),
				AllowPrivateNetworks: true,
			})
			if len(results) != 1 {
				t.Fatalf("expected one result, got %d", len(results))
			}
			if gotError := results[0].Err != nil; gotError != test.wantError {
				t.Fatalf("error=%v, wantError=%v", results[0].Err, test.wantError)
			}
		})
	}
}

func TestFetchSubscriptionErrorDoesNotLeakURLSecret(t *testing.T) {
	const secret = "subscription-token-must-not-leak"
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("transport refused %s", request.URL.String())
	})}
	results, _ := FetchSubscriptionSources(context.Background(), []string{
		"https://user:password@example.invalid/private/path?token=" + secret,
	}, SubscriptionFetchOptions{Client: client})
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("expected one failed result: %#v", results)
	}
	message := results[0].Err.Error()
	if strings.Contains(message, secret) || strings.Contains(message, "password") || strings.Contains(message, "/private/path") {
		t.Fatalf("error leaked subscription URL: %s", message)
	}
}

func TestFetchSubscriptionCanceledContextAccountsForEveryURL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results, stats := FetchSubscriptionSources(ctx, []string{
		"http://one.invalid/sub",
		"http://two.invalid/sub",
		"http://three.invalid/sub",
	}, SubscriptionFetchOptions{Concurrency: 2})
	if len(results) != 3 || stats.Failed != 3 || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("canceled batch was not fully accounted for: results=%d stats=%+v", len(results), stats)
	}
}

func TestNormalizeSubscriptionFetchConcurrency(t *testing.T) {
	if got := NormalizeSubscriptionFetchConcurrency(0); got != 16 {
		t.Fatalf("default concurrency=%d", got)
	}
	if got := NormalizeSubscriptionFetchConcurrency(100); got != 32 {
		t.Fatalf("capped concurrency=%d", got)
	}
	if got := NormalizeSubscriptionFetchConcurrency(7); got != 7 {
		t.Fatalf("configured concurrency=%d", got)
	}
}

func TestSubscriptionNetworkPolicyBlocksPrivateTargetsByDefault(t *testing.T) {
	for _, rawURL := range []string{
		"http://127.0.0.1/subscription",
		"http://[::1]/subscription",
		"http://10.0.0.1/subscription",
		"http://169.254.169.254/latest/meta-data",
		"http://100.100.100.200/latest/meta-data",
	} {
		t.Run(rawURL, func(t *testing.T) {
			target, err := url.Parse(rawURL)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateSubscriptionURLTarget(context.Background(), target, false); err == nil {
				t.Fatalf("private destination %q was accepted", rawURL)
			}
		})
	}
}

func TestSubscriptionPrivateNetworkOptIn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "socks5://127.0.0.1:1080#local\n")
	}))
	defer server.Close()

	blocked, _ := FetchSubscriptionSources(context.Background(), []string{server.URL}, SubscriptionFetchOptions{})
	if len(blocked) != 1 || blocked[0].Err == nil {
		t.Fatalf("default policy did not block loopback: %#v", blocked)
	}
	allowed, _ := FetchSubscriptionSources(context.Background(), []string{server.URL}, SubscriptionFetchOptions{AllowPrivateNetworks: true})
	if len(allowed) != 1 || allowed[0].Err != nil || len(allowed[0].Nodes) != 1 {
		t.Fatalf("private-network opt-in did not fetch loopback subscription: %#v", allowed)
	}
}

func TestSubscriptionRedirectRevalidatesDestination(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     http.Header{"Location": []string{"http://169.254.169.254/latest/meta-data"}},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    request,
		}, nil
	})}
	results, _ := FetchSubscriptionSources(context.Background(), []string{"https://93.184.216.34/sub"}, SubscriptionFetchOptions{Client: client})
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("private redirect was not rejected: %#v", results)
	}
	if calls.Load() != 1 {
		t.Fatalf("redirect target reached transport; calls=%d", calls.Load())
	}
}

func TestSubscriptionRedirectErrorRedactsFinalSignedURL(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		target, err := url.Parse(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		target.User = url.UserPassword("user", "pass")
		target.Path = "/private/signed"
		target.RawQuery = "token=TOPSECRET"
		http.Redirect(w, request, target.String(), http.StatusFound)
	}))
	defer server.Close()

	client := server.Client()
	client.CheckRedirect = func(request *http.Request, _ []*http.Request) error {
		return fmt.Errorf("redirect rejected at %s", request.URL.String())
	}
	_, err := fetchSubscriptionWithClient(context.Background(), client, server.URL+"/start", time.Second, true)
	if err == nil {
		t.Fatal("expected redirect rejection")
	}
	message := err.Error()
	for _, secret := range []string{"TOPSECRET", "user:pass", "/private/signed", "token="} {
		if strings.Contains(message, secret) {
			t.Fatalf("redirect error leaked %q: %s", secret, message)
		}
	}
}

func TestSubscriptionInputLimits(t *testing.T) {
	tooMany := make([]string, MaxSubscriptionURLs+1)
	for index := range tooMany {
		tooMany[index] = fmt.Sprintf("https://example.com/sub/%d", index)
	}
	if _, err := ValidateSubscriptionURLs(tooMany); err == nil {
		t.Fatal("URL count limit was not enforced")
	}
	if _, err := ValidateSubscriptionURLs([]string{"https://example.com/" + strings.Repeat("x", MaxSubscriptionURLLength)}); err == nil {
		t.Fatal("URL length limit was not enforced")
	}
	if err := validateSubscriptionNodes(make([]NodeConfig, MaxSubscriptionNodesPerSource+1)); err == nil {
		t.Fatal("node count limit was not enforced")
	}
	if err := validateSubscriptionNodes([]NodeConfig{{Name: strings.Repeat("n", MaxSubscriptionNodeNameBytes+1), URI: "socks5://127.0.0.1:1"}}); err == nil {
		t.Fatal("node name limit was not enforced")
	}
	if err := validateSubscriptionNodes([]NodeConfig{{Name: "node", URI: strings.Repeat("u", MaxSubscriptionNodeURIBytes+1)}}); err == nil {
		t.Fatal("node URI limit was not enforced")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
