package pool

import (
	"errors"
	"testing"
)

func TestTransientHTTPStatusClassificationUsesStatusBoundaries(t *testing.T) {
	for _, message := range []string{"HTTP/1.1 429 Too Many Requests", "unexpected status: 503", "status code=429"} {
		if !isTransientError(errors.New(message)) {
			t.Errorf("expected transient classification for %q", message)
		}
	}
	for _, message := range []string{"node id 4291 failed", "connected to port 5030", "certificate serial 429"} {
		if isTransientError(errors.New(message)) {
			t.Errorf("numeric substring was misclassified as transient: %q", message)
		}
	}
}
