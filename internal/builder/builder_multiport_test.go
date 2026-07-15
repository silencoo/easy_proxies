package builder

import (
	"testing"
	"time"

	"easy_proxies/internal/config"
	poolout "easy_proxies/internal/outbound/pool"

	"github.com/sagernet/sing-box/option"
)

func TestBuildMultiPortUsesPerNodeCredentialsAndDedicatedDispatch(t *testing.T) {
	cfg := &config.Config{
		Mode: "multi-port",
		MultiPort: config.MultiPortConfig{
			Address:  "127.0.0.1",
			BasePort: 32000,
			Username: "global-user",
			Password: "global-password",
		},
		Pool: config.PoolConfig{
			Mode:              "sequential",
			FailureThreshold:  3,
			BlacklistDuration: time.Hour,
		},
		Nodes: []config.NodeConfig{{
			Name:     "upstream",
			URI:      "socks5://proxy-user:proxy-password@127.0.0.1:1080#upstream",
			Port:     32000,
			Username: "node-user",
			Password: "node-password",
		}},
	}

	opts, err := Build(cfg)
	if err != nil {
		t.Fatalf("build multi-port options: %v", err)
	}
	if len(opts.Inbounds) != 1 {
		t.Fatalf("expected one inbound, got %d", len(opts.Inbounds))
	}
	mixed, ok := opts.Inbounds[0].Options.(*option.HTTPMixedInboundOptions)
	if !ok {
		t.Fatalf("expected mixed inbound options, got %T", opts.Inbounds[0].Options)
	}
	if len(mixed.Users) != 1 {
		t.Fatalf("expected one inbound user, got %d", len(mixed.Users))
	}
	if mixed.Users[0].Username != "node-user" || mixed.Users[0].Password != "node-password" {
		t.Fatalf("inbound used wrong credentials: %#v", mixed.Users[0])
	}

	foundDispatcher := false
	for _, outbound := range opts.Outbounds {
		poolOptions, ok := outbound.Options.(*poolout.Options)
		if ok && outbound.Tag == poolout.Tag {
			foundDispatcher = true
			if len(poolOptions.Members) != 1 || len(poolOptions.DedicatedMembers) != 1 {
				t.Fatalf("unexpected dispatcher options: members=%d dedicated=%d", len(poolOptions.Members), len(poolOptions.DedicatedMembers))
			}
			if poolOptions.DedicatedMembers[opts.Inbounds[0].Tag] != poolOptions.Members[0] {
				t.Fatal("dedicated inbound was not mapped to its exact member")
			}
		}
	}
	if !foundDispatcher {
		t.Fatal("multi-port build did not create the stable dispatcher pool")
	}
	if opts.Route == nil || opts.Route.Final != poolout.Tag || len(opts.Route.Rules) != 0 {
		t.Fatalf("multi-port routing is not using the stable final pool: %#v", opts.Route)
	}
}
