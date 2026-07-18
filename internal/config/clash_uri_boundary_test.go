package config

import (
	"net/url"
	"testing"

	"easy_proxies/internal/ssruri"
	"easy_proxies/internal/ssuri"
)

func TestConvertClashProxyEscapesUserInfoAndBracketsIPv6(t *testing.T) {
	const (
		host     = "2001:db8::42"
		username = "id@part:/?#%"
		password = "p@ss:/?#%"
	)
	tests := []struct {
		proxy        clashProxy
		wantUser     string
		wantPassword string
	}{
		{proxy: clashProxy{Type: "vmess", UUID: username}, wantUser: username},
		{proxy: clashProxy{Type: "vless", UUID: username}, wantUser: username},
		{proxy: clashProxy{Type: "trojan", Password: password}, wantUser: password},
		{proxy: clashProxy{Type: "anytls", Password: password}, wantUser: password},
		{proxy: clashProxy{Type: "hysteria2", Password: password}, wantUser: password},
		{proxy: clashProxy{Type: "tuic", UUID: username, Password: password}, wantUser: username, wantPassword: password},
	}

	for _, test := range tests {
		t.Run(test.proxy.Type, func(t *testing.T) {
			test.proxy.Name = "IPv6 node"
			test.proxy.Server = host
			test.proxy.Port = 443
			uri := convertClashProxyToURI(test.proxy)
			if uri == "" {
				t.Fatal("conversion unexpectedly skipped a valid node")
			}
			parsed, err := url.Parse(uri)
			if err != nil {
				t.Fatalf("url.Parse(%q) error = %v", uri, err)
			}
			if parsed.Host != "[2001:db8::42]:443" || parsed.Hostname() != host || parsed.Port() != "443" {
				t.Fatalf("endpoint = %q", parsed.Host)
			}
			if parsed.User == nil {
				t.Fatal("generated URI is missing userinfo")
			}
			if parsed.User.Username() != test.wantUser {
				t.Fatalf("username = %q, want %q", parsed.User.Username(), test.wantUser)
			}
			gotPassword, hasPassword := parsed.User.Password()
			if test.wantPassword != "" {
				if !hasPassword || gotPassword != test.wantPassword {
					t.Fatalf("password = %q (%v), want %q", gotPassword, hasPassword, test.wantPassword)
				}
			} else if hasPassword {
				t.Fatalf("unexpected password in userinfo: %q", gotPassword)
			}
		})
	}
}

func TestConvertClashShadowsocksFormatsSupportIPv6AndSpecialPassword(t *testing.T) {
	const password = "p@ss:/?#%"
	ssURI := convertClashProxyToURI(clashProxy{
		Name: "ss", Type: "ss", Server: "2001:db8::7", Port: 8388,
		Cipher: "aes-256-gcm", Password: password,
	})
	parsedSS, err := ssuri.Parse(ssURI)
	if err != nil {
		t.Fatalf("ssuri.Parse(%q) error = %v", ssURI, err)
	}
	if parsedSS.Server != "2001:db8::7" || parsedSS.Port != 8388 || parsedSS.Password != password {
		t.Fatalf("parsed Shadowsocks URI = %+v", parsedSS)
	}

	ssrURI := convertClashProxyToURI(clashProxy{
		Name: "ssr", Type: "ssr", Server: "2001:db8::8", Port: 8389,
		Cipher: "aes-256-gcm", Password: password, Protocol: "origin", Obfs: "plain",
	})
	parsedSSR, err := ssruri.Parse(ssrURI)
	if err != nil {
		t.Fatalf("ssruri.Parse(%q) error = %v", ssrURI, err)
	}
	if parsedSSR.Server != "2001:db8::8" || parsedSSR.Port != 8389 || parsedSSR.Password != password {
		t.Fatalf("parsed ShadowsocksR URI = %+v", parsedSSR)
	}
}

func TestConvertClashProxyRejectsMissingAndOutOfRangePorts(t *testing.T) {
	proxyTypes := []string{"vmess", "vless", "trojan", "anytls", "ss", "hysteria2", "tuic", "ssr", "hysteria"}
	for _, proxyType := range proxyTypes {
		for _, port := range []int{0, -1, 65536} {
			t.Run(proxyType+"/"+portTestName(port), func(t *testing.T) {
				proxy := clashProxy{
					Name: "bad", Type: proxyType, Server: "example.com", Port: flexInt(port),
					UUID: "uuid", Password: "password", Cipher: "aes-256-gcm", Protocol: "origin", Obfs: "plain",
				}
				if uri := convertClashProxyToURI(proxy); uri != "" {
					t.Fatalf("converted invalid port %d to %q", port, uri)
				}
			})
		}
	}
}

func TestConvertClashHysteria2RejectsInvalidPortSets(t *testing.T) {
	for _, ports := range []string{"0", "65536", "20000-10000", "10000-70000", "10000,,20000", "bad"} {
		t.Run(ports, func(t *testing.T) {
			uri := convertClashProxyToURI(clashProxy{
				Name: "bad", Type: "hysteria2", Server: "example.com", Port: 443, Password: "secret", Ports: ports,
			})
			if uri != "" {
				t.Fatalf("converted invalid port set %q to %q", ports, uri)
			}
		})
	}
}

func portTestName(port int) string {
	if port < 0 {
		return "negative"
	}
	if port == 0 {
		return "missing"
	}
	return "overflow"
}
