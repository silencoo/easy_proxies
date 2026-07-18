package monitor

import (
	"strings"
	"testing"
)

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
		`CHART_FONT_FAMILY`,
		`function renderConsoleLogs(payload)`,
		`.log-warn`,
		`.log-error`,
		`class="setting-input sensitive-textarea masked"`,
		`function toggleSensitiveField(fieldId, button)`,
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
