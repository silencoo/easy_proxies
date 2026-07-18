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
			metadata := poolOptions.Metadata[poolOptions.Members[0]]
			if metadata.Username != "node-user" || metadata.Password != "node-password" {
				t.Fatalf("monitor metadata lost effective per-node credentials: %#v", metadata)
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

func TestBuildDefersGeoIPUntilOutboundExitCanBeProbed(t *testing.T) {
	cfg := &config.Config{
		Mode: "pool",
		Listener: config.ListenerConfig{
			Address: "127.0.0.1",
			Port:    2323,
		},
		Pool: config.PoolConfig{Mode: "sequential"},
		GeoIP: config.GeoIPConfig{
			Enabled:      true,
			DatabasePath: "does-not-exist-and-must-not-be-opened-during-build.mmdb",
		},
		Nodes: []config.NodeConfig{{
			Name: "server-location-is-not-exit-location",
			URI:  "socks5://203.0.113.9:1080#node",
		}},
	}
	opts, err := Build(cfg)
	if err != nil {
		t.Fatalf("builder unexpectedly performed GeoIP I/O: %v", err)
	}
	for _, outbound := range opts.Outbounds {
		poolOptions, ok := outbound.Options.(*poolout.Options)
		if !ok || outbound.Tag != poolout.Tag {
			continue
		}
		for _, metadata := range poolOptions.Metadata {
			if metadata.Region != "other" || metadata.Country != "Unknown" || metadata.ExitIP != "" {
				t.Fatalf("builder classified the proxy server instead of deferring to exit probe: %#v", metadata)
			}
		}
		return
	}
	t.Fatal("global pool not found")
}
