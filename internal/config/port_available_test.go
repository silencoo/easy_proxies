package config

import (
	"net"
	"testing"
)

func TestIsPortAvailableSupportsIPv6Addresses(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	if IsPortAvailable("::1", port) {
		listener.Close()
		t.Fatal("bound IPv6 port reported available")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if !IsPortAvailable("[::1]", port) {
		t.Fatal("released IPv6 port reported unavailable")
	}
}
