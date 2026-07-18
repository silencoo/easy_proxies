package monitor

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"easy_proxies/internal/config"
)

type subscriptionSettingsStub struct {
	calls                int
	fetchConcurrency     int
	allowPrivateNetworks bool
	expectedRevision     uint64
	refreshErr           error
}

func (s *subscriptionSettingsStub) RefreshNow() error { return nil }
func (s *subscriptionSettingsStub) Status() SubscriptionStatus {
	return SubscriptionStatus{NodeCount: 2}
}
func (s *subscriptionSettingsStub) UpdateConfig([]string, bool, time.Duration) {}
func (s *subscriptionSettingsStub) UpdateConfigAndRefresh(_ []string, _ bool, _ time.Duration, fetchConcurrency int, allowPrivateNetworks bool) error {
	s.calls++
	s.fetchConcurrency = fetchConcurrency
	s.allowPrivateNetworks = allowPrivateNetworks
	return s.refreshErr
}
func (s *subscriptionSettingsStub) UpdateConfigAndRefreshAtRevision(_ []string, _ bool, _ time.Duration, fetchConcurrency int, allowPrivateNetworks bool, expectedRevision uint64) error {
	s.calls++
	s.fetchConcurrency = fetchConcurrency
	s.allowPrivateNetworks = allowPrivateNetworks
	s.expectedRevision = expectedRevision
	return s.refreshErr
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

func TestSubscriptionSettingsExposeAndForwardFetchConcurrency(t *testing.T) {
	cfg := loadSubscriptionSettingsConfig(t, 7)
	stub := &subscriptionSettingsStub{}
	nodeManager := &settingsTransactionNodeManager{cfg: cfg.Clone(), revision: 4}
	server := &Server{cfgSrc: cfg, subRefresher: stub, nodeMgr: nodeManager}

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
	if getRecorder.Header().Get("Cache-Control") != "no-store, max-age=0" {
		t.Fatalf("GET Cache-Control = %q", getRecorder.Header().Get("Cache-Control"))
	}
	if getRecorder.Header().Get("ETag") != settingsETag(4) {
		t.Fatalf("GET ETag = %q", getRecorder.Header().Get("ETag"))
	}

	putBody := bytes.NewBufferString(`{"subscriptions":["https://example.invalid/sub"],"enabled":true,"interval":"30m","fetch_concurrency":9,"allow_private_networks":true}`)
	putRecorder := httptest.NewRecorder()
	putRequest := httptest.NewRequest(http.MethodPut, "/api/subscription/config", putBody)
	putRequest.Header.Set("If-Match", settingsETag(4))
	server.handleSubscriptionConfig(putRecorder, putRequest)
	if putRecorder.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, body=%s", putRecorder.Code, putRecorder.Body.String())
	}
	if putRecorder.Header().Get("ETag") != settingsETag(4) {
		t.Fatalf("PUT ETag = %q", putRecorder.Header().Get("ETag"))
	}
	if stub.calls != 1 || stub.fetchConcurrency != 9 {
		t.Fatalf("refresh call = (%d, %d), want (1, 9)", stub.calls, stub.fetchConcurrency)
	}
	if stub.expectedRevision != 4 {
		t.Fatalf("refresh expected revision = %d, want 4", stub.expectedRevision)
	}
	if !stub.allowPrivateNetworks {
		t.Fatal("allow_private_networks was not forwarded")
	}
	if cfg.SubscriptionRefresh.FetchConcurrency != 7 {
		t.Fatalf("handler mutated config before transaction commit: %d", cfg.SubscriptionRefresh.FetchConcurrency)
	}
}

func TestSubscriptionSettingsMapsFinalRevisionConflictToPreconditionFailed(t *testing.T) {
	cfg := loadSubscriptionSettingsConfig(t, 7)
	stub := &subscriptionSettingsStub{refreshErr: ErrSubscriptionConfigRevisionConflict}
	nodeManager := &settingsTransactionNodeManager{cfg: cfg.Clone(), revision: 11}
	server := &Server{cfgSrc: cfg, subRefresher: stub, nodeMgr: nodeManager}
	body := strings.NewReader(`{"subscriptions":["https://example.invalid/sub"],"enabled":true,"interval":"30m"}`)
	request := httptest.NewRequest(http.MethodPut, "/api/subscription/config", body)
	request.Header.Set("If-Match", settingsETag(11))
	recorder := httptest.NewRecorder()

	server.handleSubscriptionConfig(recorder, request)

	if recorder.Code != http.StatusPreconditionFailed {
		t.Fatalf("status=%d, want 412; body=%s", recorder.Code, recorder.Body.String())
	}
	if stub.calls != 1 || stub.expectedRevision != 11 {
		t.Fatalf("refresh calls=%d expected_revision=%d", stub.calls, stub.expectedRevision)
	}
}

func TestSubscriptionSettingsRequireMatchingRevisionETag(t *testing.T) {
	cfg := loadSubscriptionSettingsConfig(t, 7)
	stub := &subscriptionSettingsStub{}
	nodeManager := &settingsTransactionNodeManager{cfg: cfg.Clone(), revision: 9}
	server := &Server{cfgSrc: cfg, subRefresher: stub, nodeMgr: nodeManager}
	body := `{"subscriptions":["https://example.invalid/sub"],"enabled":true,"interval":"30m"}`

	missing := httptest.NewRecorder()
	server.handleSubscriptionConfig(missing, httptest.NewRequest(http.MethodPut, "/api/subscription/config", strings.NewReader(body)))
	if missing.Code != http.StatusPreconditionRequired || stub.calls != 0 {
		t.Fatalf("missing ETag status=%d calls=%d body=%s", missing.Code, stub.calls, missing.Body.String())
	}

	staleRequest := httptest.NewRequest(http.MethodPut, "/api/subscription/config", strings.NewReader(body))
	staleRequest.Header.Set("If-Match", settingsETag(8))
	stale := httptest.NewRecorder()
	server.handleSubscriptionConfig(stale, staleRequest)
	if stale.Code != http.StatusPreconditionFailed || stub.calls != 0 {
		t.Fatalf("stale ETag status=%d calls=%d body=%s", stale.Code, stub.calls, stale.Body.String())
	}
}

func TestSubscriptionSettingsStrictJSONAndBodyLimits(t *testing.T) {
	for _, test := range []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "unknown field", body: `{"subscriptions":[],"enabled":false,"interval":"1h","unexpected":true}`, wantStatus: http.StatusBadRequest},
		{name: "trailing JSON", body: `{"subscriptions":[],"enabled":false,"interval":"1h"}{}`, wantStatus: http.StatusBadRequest},
		{name: "oversized body", body: strings.Repeat(" ", int(maxSubscriptionConfigBodyBytes)+1), wantStatus: http.StatusRequestEntityTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := loadSubscriptionSettingsConfig(t, 7)
			stub := &subscriptionSettingsStub{}
			server := &Server{cfgSrc: cfg, subRefresher: stub}
			recorder := httptest.NewRecorder()
			server.handleSubscriptionConfig(recorder, httptest.NewRequest(http.MethodPut, "/api/subscription/config", strings.NewReader(test.body)))
			if recorder.Code != test.wantStatus {
				t.Fatalf("status=%d, want=%d; body=%s", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			if stub.calls != 0 {
				t.Fatalf("invalid request triggered %d refresh calls", stub.calls)
			}
			if recorder.Header().Get("Cache-Control") != "no-store, max-age=0" {
				t.Fatalf("Cache-Control = %q", recorder.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestSubscriptionSettingsPutRequiresLiveRefresher(t *testing.T) {
	cfg := loadSubscriptionSettingsConfig(t, 7)
	server := &Server{cfgSrc: cfg}
	recorder := httptest.NewRecorder()
	body := strings.NewReader(`{"subscriptions":["https://example.invalid/sub"],"enabled":true,"interval":"1h"}`)

	server.handleSubscriptionConfig(recorder, httptest.NewRequest(http.MethodPut, "/api/subscription/config", body))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want=%d; body=%s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}
	if cfg.SubscriptionRefresh.FetchConcurrency != 7 {
		t.Fatalf("unavailable refresher changed configuration: %d", cfg.SubscriptionRefresh.FetchConcurrency)
	}
}

func TestSubscriptionSettingsRejectTooManyURLs(t *testing.T) {
	urls := make([]string, config.MaxSubscriptionURLs+1)
	for index := range urls {
		urls[index] = "https://example.com/sub/" + strconv.Itoa(index)
	}
	payload, err := json.Marshal(map[string]any{"subscriptions": urls, "enabled": true, "interval": "1h"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := loadSubscriptionSettingsConfig(t, 7)
	stub := &subscriptionSettingsStub{}
	server := &Server{cfgSrc: cfg, subRefresher: stub}
	recorder := httptest.NewRecorder()
	server.handleSubscriptionConfig(recorder, httptest.NewRequest(http.MethodPut, "/api/subscription/config", bytes.NewReader(payload)))
	if recorder.Code != http.StatusBadRequest || stub.calls != 0 {
		t.Fatalf("oversized URL set status=%d calls=%d body=%s", recorder.Code, stub.calls, recorder.Body.String())
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

func TestSubscriptionSettingsRejectInvalidIntervalWithoutMutation(t *testing.T) {
	cfg := loadSubscriptionSettingsConfig(t, 7)
	stub := &subscriptionSettingsStub{}
	server := &Server{cfgSrc: cfg, subRefresher: stub}
	recorder := httptest.NewRecorder()

	server.handleSubscriptionConfig(recorder, httptest.NewRequest(http.MethodPut, "/api/subscription/config", strings.NewReader(`{"subscriptions":[],"enabled":false,"interval":"invalid"}`)))

	if recorder.Code != http.StatusBadRequest || stub.calls != 0 {
		t.Fatalf("status=%d calls=%d body=%s", recorder.Code, stub.calls, recorder.Body.String())
	}
	if cfg.SubscriptionRefresh.Interval != time.Hour {
		t.Fatalf("invalid interval changed config to %s", cfg.SubscriptionRefresh.Interval)
	}
}
