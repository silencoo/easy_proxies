package boxmgr

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"easy_proxies/internal/config"
)

const (
	defaultGeoIPRouterPort uint16 = 1221
	maxTCPPort                    = 65535
)

var errNoAvailableGeoIPRouterPort = errors.New("no available GeoIP router port")

// selectGeoIPRouterPort returns a deterministic, non-zero port that does not
// collide with another listener owned by this process. It only considers
// configuration state; binding the selected address remains Router.Start's
// responsibility.
func selectGeoIPRouterPort(cfg *config.Config) (uint16, error) {
	if cfg == nil {
		return 0, errors.New("select GeoIP router port: nil config")
	}

	reserved := make(map[uint16]struct{}, len(cfg.Nodes)+2)
	reservePort(reserved, cfg.Listener.Port)

	if cfg.Mode == "multi-port" || cfg.Mode == "hybrid" {
		for _, node := range cfg.Nodes {
			reservePort(reserved, node.Port)
		}
	}

	if cfg.ManagementEnabled() && strings.TrimSpace(cfg.Management.Listen) != "" {
		managementPort, err := listenPort(cfg.Management.Listen)
		if err != nil {
			return 0, fmt.Errorf("select GeoIP router port: %w", err)
		}
		reservePort(reserved, managementPort)
	}

	return chooseGeoIPRouterPort(cfg.GeoIP.Port, reserved)
}

// chooseGeoIPRouterPort scans every valid TCP port at most once. The cursor is
// an int so a preferred port of 65535 can never overflow to port zero.
func chooseGeoIPRouterPort(preferred uint16, reserved map[uint16]struct{}) (uint16, error) {
	if preferred == 0 {
		preferred = defaultGeoIPRouterPort
	}

	if _, occupied := reserved[preferred]; !occupied {
		return preferred, nil
	}

	for candidate := int(preferred) + 1; candidate <= maxTCPPort; candidate++ {
		port := uint16(candidate)
		if _, occupied := reserved[port]; !occupied {
			return port, nil
		}
	}
	for candidate := 1; candidate < int(preferred); candidate++ {
		port := uint16(candidate)
		if _, occupied := reserved[port]; !occupied {
			return port, nil
		}
	}

	return 0, errNoAvailableGeoIPRouterPort
}

func reservePort(reserved map[uint16]struct{}, port uint16) {
	if port != 0 {
		reserved[port] = struct{}{}
	}
}

func listenPort(listen string) (uint16, error) {
	_, portText, err := net.SplitHostPort(strings.TrimSpace(listen))
	if err != nil {
		return 0, fmt.Errorf("invalid management listen address: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > maxTCPPort {
		return 0, fmt.Errorf("invalid management listen port %q", portText)
	}
	return uint16(port), nil
}
