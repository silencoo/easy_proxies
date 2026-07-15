package geoip

import (
	"context"
	"errors"
	"net"
	"testing"
)

type routerTestDialer struct{}

func (routerTestDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("not used")
}

func TestRouterRemovePool(t *testing.T) {
	router := NewRouter(RouterConfig{}, nil)
	dialer := routerTestDialer{}
	router.SetPool(RegionUS, dialer)
	if _, exists := router.pools[RegionUS]; !exists {
		t.Fatal("region pool was not registered")
	}

	router.RemovePool(RegionUS)
	if _, exists := router.pools[RegionUS]; exists {
		t.Fatal("stale region pool was not removed")
	}

	// Removing an already absent pool is intentionally idempotent.
	router.RemovePool(RegionUS)
}
