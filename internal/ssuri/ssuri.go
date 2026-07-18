// Package ssuri parses Shadowsocks SIP002 and legacy share links.
package ssuri

import (
	"encoding/base64"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

const defaultPort = 8388

// URI is the credential-bearing parsed form of a Shadowsocks share link.
// Callers must not include this value in logs or errors.
type URI struct {
	Method   string
	Password string
	Server   string
	Port     int
	Query    url.Values
	Fragment string
}

// Parse accepts SIP002 links, plain userinfo compatibility links, and the
// legacy whole-payload form: ss://base64(method:password@host:port).
func Parse(raw string) (URI, error) {
	raw = strings.TrimSpace(raw)
	schemeEnd := strings.Index(raw, "://")
	if schemeEnd < 0 {
		return URI{}, errors.New("shadowsocks URI missing scheme")
	}
	scheme := strings.ToLower(raw[:schemeEnd])
	if scheme != "ss" && scheme != "shadowsocks" {
		return URI{}, errors.New("unsupported Shadowsocks URI scheme")
	}

	rest := raw[schemeEnd+3:]
	fragment := ""
	if hash := strings.IndexByte(rest, '#'); hash >= 0 {
		// The fragment is display-only. A malformed provider name must not make
		// an otherwise usable proxy endpoint disappear from the pool.
		if decoded, err := url.PathUnescape(rest[hash+1:]); err == nil && utf8.ValidString(decoded) {
			fragment = decoded
		}
		rest = rest[:hash]
	}

	query := url.Values{}
	if question := strings.IndexByte(rest, '?'); question >= 0 {
		parsed, err := url.ParseQuery(rest[question+1:])
		if err != nil {
			return URI{}, errors.New("invalid Shadowsocks query")
		}
		query = parsed
		rest = rest[:question]
	}
	if rest == "" {
		return URI{}, errors.New("Shadowsocks URI missing payload")
	}

	if strings.Contains(rest, "@") {
		return parseSIP002(rest, query, fragment)
	}
	return parseLegacy(rest, query, fragment)
}

func parseSIP002(rest string, query url.Values, fragment string) (URI, error) {
	at := strings.LastIndexByte(rest, '@')
	if at <= 0 || at == len(rest)-1 {
		return URI{}, errors.New("Shadowsocks URI must include userinfo and host")
	}
	method, password, err := parseUserInfo(rest[:at])
	if err != nil {
		return URI{}, err
	}
	server, port, err := parseHostPort(rest[at+1:])
	if err != nil {
		return URI{}, err
	}
	return URI{Method: method, Password: password, Server: server, Port: port, Query: query, Fragment: fragment}, nil
}

func parseLegacy(encoded string, query url.Values, fragment string) (URI, error) {
	decoded, err := decodeBase64(encoded)
	if err != nil || !utf8.Valid(decoded) {
		return URI{}, errors.New("invalid legacy Shadowsocks payload")
	}
	payload := string(decoded)
	at := strings.LastIndexByte(payload, '@')
	if at <= 0 || at == len(payload)-1 {
		return URI{}, errors.New("legacy Shadowsocks payload must contain userinfo and host")
	}
	method, password, err := parsePlainUserInfo(payload[:at])
	if err != nil {
		return URI{}, err
	}
	server, port, err := parseHostPort(payload[at+1:])
	if err != nil {
		return URI{}, err
	}
	return URI{Method: method, Password: password, Server: server, Port: port, Query: query, Fragment: fragment}, nil
}

func parseUserInfo(raw string) (string, string, error) {
	if decoded, err := decodeBase64(raw); err == nil && utf8.Valid(decoded) {
		if method, password, plainErr := parsePlainUserInfo(string(decoded)); plainErr == nil {
			return method, password, nil
		}
	}
	unescaped, err := url.PathUnescape(raw)
	if err != nil || !utf8.ValidString(unescaped) {
		return "", "", errors.New("invalid Shadowsocks userinfo")
	}
	return parsePlainUserInfo(unescaped)
}

func parsePlainUserInfo(userInfo string) (string, string, error) {
	method, password, ok := strings.Cut(userInfo, ":")
	if !ok {
		return "", "", errors.New("Shadowsocks userinfo must be method:password")
	}
	method = strings.TrimSpace(method)
	if method == "" {
		return "", "", errors.New("Shadowsocks method is required")
	}
	if password == "" {
		return "", "", errors.New("Shadowsocks password is required")
	}
	if !utf8.ValidString(method) || !utf8.ValidString(password) {
		return "", "", errors.New("invalid UTF-8 in Shadowsocks userinfo")
	}
	return method, password, nil
}

func parseHostPort(hostPort string) (string, int, error) {
	if hostPort == "" {
		return "", 0, errors.New("Shadowsocks host is required")
	}
	host, port := "", defaultPort
	if strings.HasPrefix(hostPort, "[") {
		end := strings.IndexByte(hostPort, ']')
		if end < 0 {
			return "", 0, errors.New("invalid Shadowsocks IPv6 host")
		}
		host = hostPort[1:end]
		remainder := hostPort[end+1:]
		if remainder != "" {
			if !strings.HasPrefix(remainder, ":") {
				return "", 0, errors.New("invalid Shadowsocks IPv6 host/port")
			}
			parsed, err := parsePort(remainder[1:])
			if err != nil {
				return "", 0, err
			}
			port = parsed
		}
	} else if strings.Count(hostPort, ":") > 1 {
		return "", 0, errors.New("Shadowsocks IPv6 host must use brackets")
	} else if splitHost, splitPort, err := net.SplitHostPort(hostPort); err == nil {
		host = splitHost
		parsed, err := parsePort(splitPort)
		if err != nil {
			return "", 0, err
		}
		port = parsed
	} else if strings.Contains(hostPort, ":") {
		hostText, portText, _ := strings.Cut(hostPort, ":")
		host = hostText
		parsed, err := parsePort(portText)
		if err != nil {
			return "", 0, err
		}
		port = parsed
	} else {
		host = hostPort
	}

	unescaped, err := url.PathUnescape(host)
	if err != nil || !utf8.ValidString(unescaped) || strings.TrimSpace(unescaped) == "" {
		return "", 0, errors.New("invalid Shadowsocks host")
	}
	if strings.ContainsAny(unescaped, "\r\n\x00") {
		return "", 0, errors.New("invalid control character in Shadowsocks host")
	}
	return unescaped, port, nil
}

func parsePort(text string) (int, error) {
	port, err := strconv.Atoi(text)
	if err != nil || port < 1 || port > 65535 {
		return 0, errors.New("invalid Shadowsocks port")
	}
	return port, nil
}

func decodeBase64(text string) ([]byte, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, errors.New("empty base64 value")
	}
	encodings := []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	}
	for _, encoding := range encodings {
		if decoded, err := encoding.DecodeString(text); err == nil {
			return decoded, nil
		}
	}
	return nil, errors.New("invalid base64 value")
}
