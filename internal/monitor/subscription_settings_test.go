package monitor

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"easy_proxies/internal/config"
)

type subscriptionSettingsStub struct {
	calls            int
	fetchConcurrency int
}

func (s *subscriptionSettingsStub) RefreshNow() error { return nil }
func (s *subscriptionSettingsStub) Status() SubscriptionStatus {
	return SubscriptionStatus{NodeCount: 2}
}
func (s *subscriptionSettingsStub) UpdateConfig([]string, bool, time.Duration) {}
func (s *subscriptionSettingsStub) UpdateConfigAndRefresh(_ []string, _ bool, _ time.Duration, fetchConcurrency int) error {
	s.calls++
	s.fetchConcurrency = fetchConcurrency
	return nil
}

func loadSubscriptionSettingsConfig(t *testing.T, fetchConcurrency int) *config.Config {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := []byte("mode: pool\nsubscription_refresh:\n  fetch_concurrency: " + strconv.Itoa(fetchConcurrency) + "\nnodes:\n  - name: local\n    uri: socks5://127.0.0.1:1\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return cfg
}

func TestSubscriptionSettingsExposeAndPersistFetchConcurrency(t *testing.T) {
	cfg := loadSubscriptionSettingsConfig(t, 7)
	stub := &subscriptionSettingsStub{}
	server := &Server{cfgSrc: cfg, subRefresher: stub}

	getRecorder := httptest.NewRecorder()
	server.handleSubscriptionConfig(getRecorder, httptest.NewRequest(http.MethodGet, "/api/subscription/config", nil))
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("GET status = %d, body=%s", getRecorder.Code, getRecorder.Body.String())
	}
	var getPayload map[string]any
	if err := json.Unmarshal(getRecorder.Body.Bytes(), &getPayload); err != nil {
		t.Fatalf("decode GET response: %v", err)
	}
	if got := int(getPayload["fetch_concurrency"].(float64)); got != 7 {
		t.Fatalf("GET fetch_concurrency = %d, want 7", got)
	}

	putBody := bytes.NewBufferString(`{"subscriptions":["https://example.invalid/sub"],"enabled":true,"interval":"30m","fetch_concurrency":9}`)
	putRecorder := httptest.NewRecorder()
	server.handleSubscriptionConfig(putRecorder, httptest.NewRequest(http.MethodPut, "/api/subscription/config", putBody))
	if putRecorder.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, body=%s", putRecorder.Code, putRecorder.Body.String())
	}
	if stub.calls != 1 || stub.fetchConcurrency != 9 {
		t.Fatalf("refresh call = (%d, %d), want (1, 9)", stub.calls, stub.fetchConcurrency)
	}
	if cfg.SubscriptionRefresh.FetchConcurrency != 9 {
		t.Fatalf("persisted fetch concurrency = %d, want 9", cfg.SubscriptionRefresh.FetchConcurrency)
	}
}

func TestSubscriptionSettingsRejectInvalidFetchConcurrency(t *testing.T) {
	cfg := loadSubscriptionSettingsConfig(t, 7)
	stub := &subscriptionSettingsStub{}
	server := &Server{cfgSrc: cfg, subRefresher: stub}
	body := bytes.NewBufferString(`{"subscriptions":[],"enabled":false,"interval":"1h","fetch_concurrency":33}`)
	recorder := httptest.NewRecorder()

	server.handleSubscriptionConfig(recorder, httptest.NewRequest(http.MethodPut, "/api/subscription/config", body))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", recorder.Code, recorder.Body.String())
	}
	if stub.calls != 0 {
		t.Fatalf("refresh called %d times for invalid settings", stub.calls)
	}
	if cfg.SubscriptionRefresh.FetchConcurrency != 7 {
		t.Fatalf("invalid request changed concurrency to %d", cfg.SubscriptionRefresh.FetchConcurrency)
	}
}
