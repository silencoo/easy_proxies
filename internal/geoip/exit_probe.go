package geoip

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"

	M "github.com/sagernet/sing/common/metadata"
)

const maxExitIPResponseSize = 16 * 1024

// OutboundDialer is the subset of a sing-box outbound needed for exit-IP
// discovery. The HTTP request is dialed through one specific proxy node.
type OutboundDialer interface {
	DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error)
}

// DiscoverExitIP requests an IP-echo endpoint through dialer and returns the
// observed exit address. Plain-text responses and common JSON shapes such as
// {"ip":"..."}, {"query":"..."}, and {"origin":"..."} are accepted.
func DiscoverExitIP(ctx context.Context, dialer OutboundDialer, endpoint string) (string, error) {
	if dialer == nil {
		return "", fmt.Errorf("exit IP dialer is nil")
	}
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", fmt.Errorf("exit IP endpoint is empty")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("create exit IP request: %w", err)
	}
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, M.ParseSocksaddr(address))
		},
	}
	defer transport.CloseIdleConnections()
	response, err := transport.RoundTrip(request)
	if err != nil {
		return "", fmt.Errorf("request exit IP: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("exit IP endpoint returned %s", response.Status)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxExitIPResponseSize+1))
	if err != nil {
		return "", fmt.Errorf("read exit IP response: %w", err)
	}
	if len(body) > maxExitIPResponseSize {
		return "", fmt.Errorf("exit IP response exceeds %d bytes", maxExitIPResponseSize)
	}
	ip, err := parseExitIP(body)
	if err != nil {
		return "", err
	}
	return ip.String(), nil
}

func parseExitIP(body []byte) (netip.Addr, error) {
	text := strings.TrimSpace(string(body))
	if address, ok := parseIPCandidate(text); ok {
		return address, nil
	}
	var document map[string]any
	if json.Unmarshal(body, &document) == nil {
		for _, key := range []string{"ip", "query", "origin", "address"} {
			value, _ := document[key].(string)
			if address, ok := parseIPCandidate(value); ok {
				return address, nil
			}
		}
	}
	return netip.Addr{}, fmt.Errorf("exit IP endpoint returned no valid IP address")
}

func parseIPCandidate(value string) (netip.Addr, bool) {
	for _, candidate := range strings.Split(value, ",") {
		candidate = strings.TrimSpace(candidate)
		candidate = strings.Trim(candidate, "[]")
		if address, err := netip.ParseAddr(candidate); err == nil {
			return address.Unmap(), true
		}
	}
	return netip.Addr{}, false
}
