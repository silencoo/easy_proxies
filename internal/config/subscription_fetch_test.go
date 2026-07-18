package config

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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
		Concurrency: 2,
		Timeout:     2 * time.Second,
		Client:      server.Client(),
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
	}, SubscriptionFetchOptions{Concurrency: 2, Client: server.Client()})
	if len(nodes) != 2 || aggregateStats.DedupedNodes != 1 {
		t.Fatalf("expected stable node identity dedupe, got %d nodes and %+v", len(nodes), aggregateStats)
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

			results, _ := FetchSubscriptionSources(context.Background(), []string{server.URL}, SubscriptionFetchOptions{Client: server.Client()})
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
