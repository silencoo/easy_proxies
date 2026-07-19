package monitor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIndexForbidsEmbedding(t *testing.T) {
	server := &Server{}
	recorder := httptest.NewRecorder()
	server.handleIndex(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	if got := recorder.Header().Get("Content-Security-Policy"); !strings.Contains(got, "default-src 'self'") || !strings.Contains(got, "frame-ancestors 'none'") {
		t.Fatalf("Content-Security-Policy = %q", got)
	}
	if got := recorder.Header().Get("X-Frame-Options"); got != "DENY" {
		t.Fatalf("X-Frame-Options = %q", got)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q", got)
	}
}

func TestEmbeddedWebUIUsesBundledECharts(t *testing.T) {
	data, err := embeddedFS.ReadFile("assets/index.html")
	if err != nil {
		t.Fatalf("read embedded WebUI: %v", err)
	}
	html := string(data)
	if !strings.Contains(html, `<script src="/assets/echarts.min.js"></script>`) {
		t.Fatal("embedded WebUI does not load the bundled ECharts asset")
	}
	if strings.Contains(html, "cdn.jsdelivr.net") {
		t.Fatal("embedded WebUI still depends on the ECharts CDN")
	}

	server := &Server{}
	recorder := httptest.NewRecorder()
	server.handleEChartsAsset(recorder, httptest.NewRequest(http.MethodGet, "/assets/echarts.min.js", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("asset status = %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/javascript") {
		t.Fatalf("asset Content-Type = %q", got)
	}
	if recorder.Body.Len() < 100_000 {
		t.Fatalf("bundled ECharts asset is unexpectedly small: %d bytes", recorder.Body.Len())
	}
}

func TestEmbeddedWebUIHasMonochromeIconsAndLanguageSwitcher(t *testing.T) {
	data, err := embeddedFS.ReadFile("assets/index.html")
	if err != nil {
		t.Fatalf("read embedded WebUI: %v", err)
	}
	html := string(data)

	required := []string{
		`--bg-base: #090909`,
		`class="icon-sprite"`,
		`id="settingLanguage"`,
		`const TRANSLATIONS`,
		`'监控看板': 'Dashboard'`,
		`'系统设置': 'System settings'`,
		`localStorage.setItem('uiLanguage'`,
	}
	for _, value := range required {
		if !strings.Contains(html, value) {
			t.Errorf("embedded WebUI is missing %q", value)
		}
	}

	forbidden := []string{
		"github.com/jasonwong1991/easy_proxies",
		"api.github.com",
		"githubStars",
		"⚡", "➕", "⚠", "⏹", "▶", "↺",
		"🇯🇵", "🇰🇷", "🇺🇸", "🇭🇰", "🇹🇼", "🇸🇬",
	}
	for _, value := range forbidden {
		if strings.Contains(html, value) {
			t.Errorf("embedded WebUI still contains %q", value)
		}
	}
}

func TestEmbeddedWebUIHasScalableNodeOperations(t *testing.T) {
	data, err := embeddedFS.ReadFile("assets/index.html")
	if err != nil {
		t.Fatalf("read embedded WebUI: %v", err)
	}
	html := string(data)

	required := []string{
		`id="nodeSearch"`,
		`id="nodePageSize"`,
		`id="configNodeSearch"`,
		`id="debugNodeSearch"`,
		`data-sort-table="nodes"`,
		`data-sort-table="debug"`,
		`function sortNodeTable(key)`,
		`function sortDebugTable(key)`,
		`const REGION_NAME_PATTERNS`,
		`['resident',`,
		`function getNodeRegion(node)`,
		`function getChartNodeDisplayName(node)`,
		`.replace(/[\u{1F1E6}-\u{1F1FF}]{2}/gu, '')`,
		`sorted.map(getChartNodeDisplayName)`,
		`CHART_FONT_FAMILY`,
		`function renderConsoleLogs(payload)`,
		`.log-warn`,
		`.log-error`,
		`class="setting-input sensitive-textarea masked"`,
		`function toggleSensitiveField(fieldId, button)`,
		`regionStatsCache = data.region_stats || {}`,
		`regionHealthyCache = data.region_healthy || {}`,
		`subscriptionSettingsLoaded && currentSubSnapshot !== _savedSubSnapshot`,
		`if (isAutoRefresh) startAutoRefresh()`,
		`trafficRetryAttempts >= 3`,
		`Math.min(60000, 2000 * Math.pow(2`,
		`function maskNodeURI(uri)`,
		`if (!node.available) return 1;`,
		`else if (!n.available) { badge = 'badge-error';`,
		`fetch('/api/nodes/config/' + encodeURIComponent(id))`,
		`function localizedAPIMessage(message, fallback='请求失败')`,
		`async function readAPIJSON(response, fallback='请求失败')`,
		`const managementPasswordChanged =`,
		`managementPasswordChanged && subChanged`,
		`'If-Match': subscriptionETag`,
		`coreAuthChanged = !!result.auth_changed`,
	}
	for _, value := range required {
		if !strings.Contains(html, value) {
			t.Errorf("embedded WebUI is missing %q", value)
		}
	}

	forbidden := []string{
		`<textarea id="consoleLogs"`,
	}
	for _, value := range forbidden {
		if strings.Contains(html, value) {
			t.Errorf("embedded WebUI still contains %q", value)
		}
	}
	if strings.Contains(html, `else if (n.failure_count >= 1)`) {
		t.Error("node status still treats historical failures as a current outage")
	}
}

func TestEmbeddedWebUIExposesAdaptivePoolSettings(t *testing.T) {
	data, err := embeddedFS.ReadFile("assets/index.html")
	if err != nil {
		t.Fatalf("read embedded WebUI: %v", err)
	}
	html := string(data)
	required := []string{
		`value="latency"`,
		`id="settingPoolRetry"`,
		`id="settingPoolCooldown"`,
		`id="settingPoolSticky"`,
		`id="settingStickyTTL"`,
		`id="settingStickyMax"`,
		`function togglePoolStrategySettings()`,
		`n.cooling_down`,
		`'冷却 Cooling': 'Cooling down'`,
	}
	for _, value := range required {
		if !strings.Contains(html, value) {
			t.Errorf("embedded WebUI is missing %q", value)
		}
	}
}

func TestEmbeddedWebUIExposesInputConcurrencySettings(t *testing.T) {
	data, err := embeddedFS.ReadFile("assets/index.html")
	if err != nil {
		t.Fatalf("read embedded WebUI: %v", err)
	}
	html := string(data)
	for _, value := range []string{
		`id="settingProbeConcurrency"`,
		`id="settingSubFetchConcurrency"`,
		`id="settingSubAllowPrivate"`,
		`fetch_concurrency: subFetchConcurrency`,
		`allow_private_networks: subAllowPrivate`,
		`'订阅抓取并发数': 'Subscription fetch concurrency'`,
		`'允许访问内网订阅地址（高风险）': 'Allow private-network subscription URLs (high risk)'`,
	} {
		if !strings.Contains(html, value) {
			t.Errorf("embedded WebUI is missing %q", value)
		}
	}
}
