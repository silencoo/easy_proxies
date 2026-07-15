package geoip

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	M "github.com/sagernet/sing/common/metadata"
)

type directTestDialer struct{}

func (directTestDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, destination.String())
}

func TestDiscoverExitIPThroughOutbound(t *testing.T) {
	for _, testCase := range []struct {
		name string
		body string
		want string
	}{
		{name: "plain IPv4", body: "8.8.8.8\n", want: "8.8.8.8"},
		{name: "JSON IPv6", body: `{"ip":"2001:4860:4860::8888"}`, want: "2001:4860:4860::8888"},
		{name: "origin list", body: `{"origin":"1.1.1.1, 8.8.4.4"}`, want: "1.1.1.1"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(testCase.body))
			}))
			defer server.Close()
			got, err := DiscoverExitIP(context.Background(), directTestDialer{}, server.URL)
			if err != nil {
				t.Fatalf("discover exit IP: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("got %s want %s", got, testCase.want)
			}
		})
	}
}

func TestDiscoverExitIPRejectsInvalidResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ip":"not-an-ip"}`))
	}))
	defer server.Close()
	if _, err := DiscoverExitIP(context.Background(), directTestDialer{}, server.URL); err == nil {
		t.Fatal("expected invalid response to fail")
	}
}

func TestLookupUpdateCallback(t *testing.T) {
	lookup := &Lookup{}
	called := 0
	lookup.SetUpdateCallback(func() { called++ })
	lookup.notifyUpdate()
	if called != 1 {
		t.Fatalf("update callback called %d times, want 1", called)
	}

	lookup.SetUpdateCallback(func() { panic("callback failure") })
	lookup.notifyUpdate()
}
