package builder

import (
	"strings"
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
)

const testVLESSUUID = "b831381d-6324-4d53-ad4f-8cda48b30811"

func TestBuildNodeOutboundVLESSPacketEncoding(t *testing.T) {
	for _, encoding := range []string{"xudp", "packetaddr", " XUDP "} {
		t.Run(encoding, func(t *testing.T) {
			uri := "vless://" + testVLESSUUID + "@example.com:443?packetEncoding=" + encoding + "#valid"
			outbound, err := buildNodeOutbound("valid-vless", uri, false)
			if err != nil {
				t.Fatalf("buildNodeOutbound() error = %v", err)
			}
			if outbound.Type != C.TypeVLESS {
				t.Fatalf("type = %q, want %q", outbound.Type, C.TypeVLESS)
			}
			opts, ok := outbound.Options.(*option.VLESSOutboundOptions)
			if !ok {
				t.Fatalf("options type = %T", outbound.Options)
			}
			if opts.PacketEncoding == nil {
				t.Fatal("PacketEncoding is nil")
			}
			want := strings.ToLower(strings.TrimSpace(encoding))
			if *opts.PacketEncoding != want {
				t.Fatalf("PacketEncoding = %q, want %q", *opts.PacketEncoding, want)
			}
		})
	}
}

func TestBuildNodeOutboundRejectsInvalidVLESSPacketEncodingWithoutCredentialLeak(t *testing.T) {
	const secret = "subscription-password-must-not-leak"
	uri := "vless://" + testVLESSUUID + ":" + secret + "@example.com:443?packetEncoding=invalid#bad"
	_, err := buildNodeOutbound("bad-vless", uri, false)
	if err == nil {
		t.Fatal("expected unsupported packetEncoding error")
	}
	if !strings.Contains(err.Error(), "unsupported VLESS packetEncoding") {
		t.Fatalf("error = %q", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), testVLESSUUID) {
		t.Fatalf("error leaked node credentials: %q", err)
	}
}

func TestBuildNodeOutboundSafeConvertsPanicWithoutPayloadLeak(t *testing.T) {
	_, err := recoverNodeBuild(func() (option.Outbound, error) {
		panic("socks5://user:top-secret@example.com:1080")
	})
	if err == nil {
		t.Fatal("expected recovered error")
	}
	if err.Error() != "node parser panicked" {
		t.Fatalf("unexpected error: %q", err)
	}
	if strings.Contains(err.Error(), "top-secret") {
		t.Fatalf("error leaked panic payload: %q", err)
	}
}

func TestBuildNodeOutboundSafeRedactsMalformedURI(t *testing.T) {
	const secret = "password-must-not-leak"
	_, err := buildNodeOutboundSafe("safe-node-tag", "socks5://user:"+secret+"@example.com%zz:1080", false)
	if err == nil {
		t.Fatal("expected malformed URI error")
	}
	if !strings.Contains(err.Error(), "safe-node-tag") {
		t.Fatalf("error lacks node context: %q", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "socks5://") {
		t.Fatalf("error leaked URI credentials: %q", err)
	}
}
