package config

import (
	"testing"
	"time"
)

func TestNormalizePoolConfigAppliesAdaptiveDefaults(t *testing.T) {
	cfg := &Config{}
	if err := cfg.normalizePoolConfig(); err != nil {
		t.Fatal(err)
	}
	if cfg.Pool.Mode != "sequential" || !cfg.Pool.RetryEnabledValue() || cfg.Pool.RetryAttempts != 3 {
		t.Fatalf("unexpected pool defaults: %#v", cfg.Pool)
	}
	if cfg.Pool.TransientCooldown != time.Minute || cfg.Pool.LatencySampleSize != 4 || cfg.Pool.LatencyTolerance != 50*time.Millisecond {
		t.Fatalf("unexpected adaptive defaults: %#v", cfg.Pool)
	}
	if cfg.Pool.Sticky.TTL != 30*time.Minute || cfg.Pool.Sticky.MaxEntries != 4096 {
		t.Fatalf("unexpected sticky defaults: %#v", cfg.Pool.Sticky)
	}
}

func TestNormalizePoolConfigAcceptsRoundRobinAliasAndRejectsUnknown(t *testing.T) {
	cfg := &Config{Pool: PoolConfig{Mode: "round-robin"}}
	if err := cfg.normalizePoolConfig(); err != nil || cfg.Pool.Mode != "sequential" {
		t.Fatalf("round-robin alias was not normalized: mode=%q err=%v", cfg.Pool.Mode, err)
	}
	cfg.Pool.Mode = "fastest-at-all-costs"
	if err := cfg.normalizePoolConfig(); err == nil {
		t.Fatal("unknown pool mode was accepted")
	}
}
