package builder

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"easy_proxies/internal/config"
	poolout "easy_proxies/internal/outbound/pool"
)

func TestPrintProxyLinksOmitsCredentials(t *testing.T) {
	var output bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	previousPrefix := log.Prefix()
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})

	cfg := &config.Config{
		Mode: "hybrid",
		Listener: config.ListenerConfig{
			Address:  "127.0.0.1",
			Port:     2323,
			Username: "pool-test-user",
			Password: "pool-test-password",
		},
		MultiPort: config.MultiPortConfig{
			Address:  "127.0.0.1",
			Username: "fallback-test-user",
			Password: "fallback-test-password",
		},
		Nodes: []config.NodeConfig{
			{
				Name:     "first-node",
				Port:     24001,
				Username: "node-test-user",
				Password: "node-test-password",
			},
			{Name: "fallback-node", Port: 24002},
		},
	}
	metadata := map[string]poolout.MemberMeta{
		"node-one": {Name: "first-node"},
	}

	printProxyLinks(cfg, metadata)
	logged := output.String()
	for _, credential := range []string{
		"pool-test-user",
		"pool-test-password",
		"node-test-user",
		"node-test-password",
		"fallback-test-user",
		"fallback-test-password",
	} {
		if strings.Contains(logged, credential) {
			t.Fatalf("startup proxy links exposed a credential")
		}
	}
	for _, endpoint := range []string{
		"http://127.0.0.1:2323",
		"socks5://127.0.0.1:2323",
		"http://127.0.0.1:24001",
		"socks5://127.0.0.1:24002",
	} {
		if !strings.Contains(logged, endpoint) {
			t.Fatalf("startup proxy links missing endpoint %q", endpoint)
		}
	}
	if count := strings.Count(logged, "Authentication: configured (credentials omitted)"); count != 3 {
		t.Fatalf("configured authentication status count = %d, want 3", count)
	}
}
