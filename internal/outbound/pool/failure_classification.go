package pool

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"strings"
	"syscall"
)

// isTransientError classifies typed network failures first. A narrow text
// fallback remains necessary because several proxy protocols wrap remote HTTP
// status and transport errors without exposing a structured cause.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ETIMEDOUT) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Timeout() {
		return true
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"429", "too many requests", "service unavailable", "http status 503",
		"status 503", "connection reset", "reset by peer", "temporarily unavailable",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}
