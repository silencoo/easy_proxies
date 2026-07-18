package monitor

import (
	"context"
	"io"
	"log"
	"testing"
	"time"
)

func newSessionTestServer(t *testing.T) *Server {
	t.Helper()
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Stop)
	server := NewServer(Config{Enabled: true, Listen: "127.0.0.1:0"}, manager, log.New(io.Discard, "", 0))
	if server == nil {
		t.Fatal("NewServer returned nil")
	}
	return server
}

func TestServerShutdownStopsSessionJanitorAndIsIdempotent(t *testing.T) {
	server := newSessionTestServer(t)
	server.Shutdown(context.Background())
	server.Shutdown(context.Background())
	select {
	case <-server.sessionCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("session janitor context was not cancelled")
	}
}

func TestSessionStoreIsBoundedAndPrunesExpiredEntries(t *testing.T) {
	server := newSessionTestServer(t)
	defer server.Shutdown(context.Background())

	server.sessionMu.Lock()
	server.sessions["expired"] = &Session{
		Token:     "expired",
		CreatedAt: time.Now().Add(-2 * time.Hour),
		ExpiresAt: time.Now().Add(-time.Hour),
	}
	server.sessionMu.Unlock()

	for range maxActiveSessions + 50 {
		if _, err := server.createSession(); err != nil {
			t.Fatal(err)
		}
	}

	server.sessionMu.RLock()
	defer server.sessionMu.RUnlock()
	if len(server.sessions) != maxActiveSessions {
		t.Fatalf("session count = %d, want %d", len(server.sessions), maxActiveSessions)
	}
	if _, exists := server.sessions["expired"]; exists {
		t.Fatal("expired session was not pruned")
	}
}
