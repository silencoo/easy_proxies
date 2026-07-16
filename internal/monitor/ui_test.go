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
