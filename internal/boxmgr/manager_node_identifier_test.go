package boxmgr

import (
	"errors"
	"strings"
	"testing"

	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
)

func TestNodeIndexByIdentifierDisambiguatesDuplicateDisplayNames(t *testing.T) {
	cfg := &config.Config{Nodes: []config.NodeConfig{
		{Name: "duplicate", URI: "socks5://127.0.0.1:1081#first"},
		{Name: "duplicate", URI: "socks5://127.0.0.1:1082#second"},
	}}
	if got := nodeIndexByIdentifier(cfg, cfg.Nodes[1].NodeKey()); got != 1 {
		t.Fatalf("stable identifier selected index %d, want 1", got)
	}
}

func TestPrepareNodeRejectsOversizedAndControlCharacterInputs(t *testing.T) {
	cfg := &config.Config{Mode: "pool"}
	for _, test := range []config.NodeConfig{
		{URI: "socks5://127.0.0.1:1080#ok", Name: "bad\nname"},
		{URI: "socks5://127.0.0.1:1080/" + strings.Repeat("x", config.MaxSubscriptionNodeURIBytes)},
		{URI: "not-a-proxy", Name: "bad-uri"},
	} {
		if _, err := prepareNode(cfg, test, -1); !errors.Is(err, monitor.ErrInvalidNode) {
			t.Fatalf("prepareNode(%q) error=%v, want ErrInvalidNode", test.Name, err)
		}
	}
}

func TestPrepareNodeRejectsDuplicateStableNodeIdentity(t *testing.T) {
	cfg := &config.Config{Mode: "pool", Nodes: []config.NodeConfig{
		{Name: "first", URI: "socks5://user:pass@127.0.0.1:1080?b=2&a=1#first"},
		{Name: "second", URI: "socks5://127.0.0.1:1081#second"},
	}}
	duplicate := config.NodeConfig{
		Name: "different display name",
		URI:  "socks5://user:pass@127.0.0.1:1080?a=1&b=2#renamed",
	}
	if _, err := prepareNode(cfg, duplicate, -1); !errors.Is(err, monitor.ErrNodeConflict) {
		t.Fatalf("create duplicate error=%v, want ErrNodeConflict", err)
	}
	if _, err := prepareNode(cfg, duplicate, 1); !errors.Is(err, monitor.ErrNodeConflict) {
		t.Fatalf("update collision error=%v, want ErrNodeConflict", err)
	}
}
