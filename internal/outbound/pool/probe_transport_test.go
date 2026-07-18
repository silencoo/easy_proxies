package pool

import (
	"context"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"easy_proxies/internal/monitor"
)

func TestProbeConnectionWatchdogClosesBlockedConnection(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	stop := watchProbeConnection(ctx, client)
	readDone := make(chan error, 1)
	go func() {
		var buf [1]byte
		_, err := client.Read(buf[:])
		readDone <- err
	}()
	cancel()
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("blocked read unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("watchdog did not close blocked connection")
	}
	stop()
}

func TestUpgradeProbeConnVerifiesHTTPSCertificate(t *testing.T) {
	server := httptest.NewTLSServer(nil)
	defer server.Close()
	address := server.Listener.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	strictConn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := upgradeProbeConn(ctx, strictConn, monitor.ProbeTarget{Host: "localhost", TLS: true}); err == nil {
		strictConn.Close()
		t.Fatal("strict TLS probe accepted httptest's untrusted certificate")
	}
	strictConn.Close()

	insecureConn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	upgraded, err := upgradeProbeConn(ctx, insecureConn, monitor.ProbeTarget{Host: "localhost", TLS: true, SkipCertVerify: true})
	if err != nil {
		insecureConn.Close()
		t.Fatalf("explicit skip_cert_verify did not permit TLS probe: %v", err)
	}
	upgraded.Close()
}
