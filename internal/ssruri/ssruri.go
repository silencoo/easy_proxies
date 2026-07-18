// Package ssruri parses ShadowsocksR share links without logging payloads.
package ssruri

import (
	"encoding/base64"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

// URI is the parsed form of a ShadowsocksR share link.
type URI struct {
	Server        string
	Port          int
	Protocol      string
	Method        string
	Obfs          string
	Password      string
	ObfsParam     string
	ProtocolParam string
	Remarks       string
	Group         string
}

// Parse accepts padded/unpadded standard and URL-safe SSR payloads.
func Parse(raw string) (URI, error) {
	raw = strings.TrimSpace(raw)
	schemeEnd := strings.Index(raw, "://")
	if schemeEnd < 0 {
		return URI{}, errors.New("SSR URI missing scheme")
	}
	scheme := strings.ToLower(raw[:schemeEnd])
	if scheme != "ssr" && scheme != "shadowsocksr" {
		return URI{}, errors.New("unsupported SSR URI scheme")
	}
	payload := raw[schemeEnd+3:]
	outerFragment := ""
	if hash := strings.IndexByte(payload, '#'); hash >= 0 {
		if decoded, err := url.PathUnescape(payload[hash+1:]); err == nil && utf8.ValidString(decoded) {
			outerFragment = decoded
		}
		payload = payload[:hash]
	}

	decoded, err := decodeBase64(payload)
	if err != nil || !utf8.Valid(decoded) {
		return URI{}, errors.New("invalid SSR payload")
	}
	text := string(decoded)
	main, paramText, _ := strings.Cut(text, "/?")
	main = strings.TrimSuffix(main, "/")
	parts := strings.Split(main, ":")
	if len(parts) < 6 {
		return URI{}, errors.New("SSR payload has too few fields")
	}
	n := len(parts)
	server := strings.Trim(strings.Join(parts[:n-5], ":"), "[]")
	port, err := strconv.Atoi(parts[n-5])
	if err != nil || port < 1 || port > 65535 {
		return URI{}, errors.New("invalid SSR port")
	}
	if server == "" || strings.ContainsAny(server, "\r\n\x00") {
		return URI{}, errors.New("invalid SSR host")
	}
	passwordBytes, err := decodeBase64(parts[n-1])
	if err != nil || !utf8.Valid(passwordBytes) {
		return URI{}, errors.New("invalid SSR password")
	}
	result := URI{
		Server:   server,
		Port:     port,
		Protocol: parts[n-4],
		Method:   parts[n-3],
		Obfs:     parts[n-2],
		Password: string(passwordBytes),
		Remarks:  outerFragment,
	}
	if result.Protocol == "" || result.Method == "" || result.Obfs == "" {
		return URI{}, errors.New("SSR protocol, method and obfs are required")
	}
	for _, pair := range strings.Split(paramText, "&") {
		key, value, ok := strings.Cut(pair, "=")
		if !ok || value == "" {
			continue
		}
		decodedValue, ok := decodeSSRParameter(value)
		if !ok {
			continue
		}
		switch strings.ToLower(key) {
		case "obfsparam":
			result.ObfsParam = decodedValue
		case "protoparam":
			result.ProtocolParam = decodedValue
		case "remarks":
			result.Remarks = decodedValue
		case "group":
			result.Group = decodedValue
		}
	}
	return result, nil
}

func decodeSSRParameter(raw string) (string, bool) {
	candidates := []string{raw}
	if unescaped, err := url.PathUnescape(raw); err == nil && unescaped != raw {
		candidates = append(candidates, unescaped)
	}
	for _, candidate := range candidates {
		decoded, err := decodeBase64(candidate)
		if err == nil && utf8.Valid(decoded) {
			return string(decoded), true
		}
	}
	return "", false
}

func decodeBase64(text string) ([]byte, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, errors.New("empty base64 value")
	}
	for _, encoding := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	} {
		if decoded, err := encoding.DecodeString(text); err == nil {
			return decoded, nil
		}
	}
	return nil, errors.New("invalid base64 value")
}
