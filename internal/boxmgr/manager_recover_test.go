package boxmgr

import (
	"errors"
	"strings"
	"testing"

	"github.com/sagernet/sing-box"
)

func TestRecoverBoxInitializationConvertsPanicWithoutCredentialLeak(t *testing.T) {
	const secret = "proxy-password-must-not-leak"
	instance, err := recoverBoxInitialization(func() (*box.Box, error) {
		panic("initialize outbound[7]: vless://uuid:" + secret + "@example.com")
	})
	if instance != nil {
		t.Fatal("instance must be nil after panic")
	}
	if err == nil || !strings.Contains(err.Error(), "initialize outbound[7]") {
		t.Fatalf("error = %v, want safe outbound index", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "vless://") {
		t.Fatalf("error leaked panic payload: %q", err)
	}
}

func TestRecoverBoxInitializationRedactsUnindexedPanic(t *testing.T) {
	const secret = "proxy-password-must-not-leak"
	_, err := recoverBoxInitialization(func() (*box.Box, error) {
		panic(secret)
	})
	if err == nil || err.Error() != "sing-box panic during initialization" {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked panic payload: %q", err)
	}
}

func TestRecoverBoxInitializationPassesThroughResults(t *testing.T) {
	sentinel := errors.New("sentinel")
	_, err := recoverBoxInitialization(func() (*box.Box, error) { return nil, sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want sentinel", err)
	}
}

func TestSanitizeBoxInitializationErrorDropsCredentialPayload(t *testing.T) {
	const secret = "proxy-password-must-not-leak"
	indexed := sanitizeBoxInitializationError(errors.New("initialize outbound[12]: trojan://" + secret + "@example.com"))
	if indexed == nil || indexed.Error() != "sing-box failed to initialize outbound[12]" {
		t.Fatalf("indexed error = %v", indexed)
	}
	unindexed := sanitizeBoxInitializationError(errors.New("dial trojan://" + secret + "@example.com"))
	if unindexed == nil || unindexed.Error() != "sing-box initialization failed" {
		t.Fatalf("unindexed error = %v", unindexed)
	}
	for _, err := range []error{indexed, unindexed} {
		if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "trojan://") {
			t.Fatalf("sanitized error leaked credentials: %q", err)
		}
	}
}
