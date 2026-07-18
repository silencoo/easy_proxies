package monitor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
)

var probeURLPattern = regexp.MustCompile(`(?i)[a-z][a-z0-9+.-]*://[^\s]+`)
var opaqueHostPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+={0,2}$`)

func likelyOpaqueURIHost(u *url.URL) bool {
	if u == nil || u.User != nil || u.Port() != "" {
		return false
	}
	host := u.Hostname()
	return len(host) >= 24 && !strings.Contains(host, ".") && net.ParseIP(host) == nil && opaqueHostPattern.MatchString(host)
}

// nodeBrief returns only protocol and endpoint. Userinfo, path, query and
// fragment can contain UUIDs, tokens or subscription credentials and are never
// copied into diagnostics.
func nodeBrief(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Hostname() == "" {
		return ""
	}
	if likelyOpaqueURIHost(u) {
		return strings.ToLower(u.Scheme)
	}
	host := u.Hostname()
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if port := u.Port(); port != "" {
		return fmt.Sprintf("%s@%s:%s", strings.ToLower(u.Scheme), host, port)
	}
	return fmt.Sprintf("%s@%s", strings.ToLower(u.Scheme), host)
}

// SanitizeProbeError removes credentials and opaque URI components from an
// error before it is persisted in health-state.yaml or exposed by the API.
func SanitizeProbeError(err error) string {
	if err == nil {
		return ""
	}
	return probeURLPattern.ReplaceAllStringFunc(err.Error(), func(raw string) string {
		trimmed := strings.TrimRight(raw, ".,;)]}")
		suffix := raw[len(trimmed):]
		u, parseErr := url.Parse(trimmed)
		if parseErr != nil || u.Scheme == "" || u.Host == "" {
			return "[redacted-uri]" + suffix
		}
		if likelyOpaqueURIHost(u) {
			return strings.ToLower(u.Scheme) + "://[redacted]" + suffix
		}
		return strings.ToLower(u.Scheme) + "://" + u.Host + suffix
	})
}

func classifyProbeError(err error) (category, summary string) {
	if err == nil {
		return "", ""
	}
	var netErr net.Error
	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "127.127.127.1"):
		return "addr_invalid", "节点地址无效（可能是订阅解析异常或占位地址）"
	case strings.Contains(lower, "http-upgrade"), strings.Contains(lower, "httpupgrade"),
		strings.Contains(lower, "unexpected status"), strings.Contains(lower, "v2ray-"):
		return "transport_handshake", "传输层握手失败（节点服务端异常或被 CDN 拦截）"
	case strings.Contains(lower, "tls:"), strings.Contains(lower, "tls handshake"),
		strings.Contains(lower, "certificate"), strings.Contains(lower, "x509:"):
		return "tls_failed", "TLS 握手或证书验证失败"
	case strings.Contains(lower, "unknown version"), strings.Contains(lower, "malformed"):
		return "proto_mismatch", "协议响应不符合预期"
	case strings.Contains(lower, "connection refused"):
		return "dial_refused", "节点端口拒绝连接"
	case strings.Contains(lower, "no route to host"), strings.Contains(lower, "network is unreachable"):
		return "dial_no_route", "节点网络不可达"
	case strings.Contains(lower, "dial tcp"), strings.Contains(lower, "dial udp"):
		if errors.As(err, &netErr) && netErr.Timeout() || strings.Contains(lower, "timeout") {
			return "dial_timeout", "连接节点超时"
		}
		return "dial_failed", "连接节点失败"
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout(),
		strings.Contains(lower, "i/o timeout"), strings.Contains(lower, "deadline exceeded"):
		return "read_timeout", "节点响应超时或链路不稳定"
	case strings.Contains(lower, "eof"), strings.Contains(lower, "reset by peer"),
		strings.Contains(lower, "broken pipe"), strings.Contains(lower, "connection reset"):
		return "conn_reset", "探测连接被对端中断"
	case errors.Is(err, context.Canceled):
		return "cancelled", "探测被取消"
	default:
		return "other", "探测失败（未知原因）"
	}
}

// FormatProbeFailure produces a credential-free, single-line diagnostic.
func FormatProbeFailure(tag, uri string, err error) string {
	category, summary := classifyProbeError(err)
	var b strings.Builder
	b.WriteString(tag)
	if brief := nodeBrief(uri); brief != "" {
		b.WriteString(" [")
		b.WriteString(brief)
		b.WriteByte(']')
	}
	b.WriteString(" (")
	b.WriteString(category)
	b.WriteString(") ")
	b.WriteString(summary)
	if detail := SanitizeProbeError(err); detail != "" {
		b.WriteString(" | ")
		b.WriteString(detail)
	}
	return b.String()
}
