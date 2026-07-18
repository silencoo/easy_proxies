package config

import "testing"

func TestValidateManagementConfigRequiresPasswordOffLoopback(t *testing.T) {
	enabled := true
	disabled := false
	for _, test := range []struct {
		name    string
		cfg     ManagementConfig
		wantErr bool
	}{
		{name: "IPv4 loopback", cfg: ManagementConfig{Enabled: &enabled, Listen: "127.0.0.1:9091"}},
		{name: "IPv6 loopback", cfg: ManagementConfig{Enabled: &enabled, Listen: "[::1]:9091"}},
		{name: "localhost", cfg: ManagementConfig{Enabled: &enabled, Listen: "localhost:9091"}},
		{name: "wildcard with password and TLS", cfg: ManagementConfig{Enabled: &enabled, Listen: "0.0.0.0:9091", Password: "strong", TLSCertFile: "cert.pem", TLSKeyFile: "key.pem"}},
		{name: "disabled", cfg: ManagementConfig{Enabled: &disabled, Listen: "0.0.0.0:9091"}},
		{name: "TLS certificate without key", cfg: ManagementConfig{Enabled: &enabled, Listen: "127.0.0.1:9091", TLSCertFile: "cert.pem"}, wantErr: true},
		{name: "wildcard password over plaintext", cfg: ManagementConfig{Enabled: &enabled, Listen: "0.0.0.0:9091", Password: "strong"}, wantErr: true},
		{name: "IPv4 wildcard without password", cfg: ManagementConfig{Enabled: &enabled, Listen: "0.0.0.0:9091"}, wantErr: true},
		{name: "IPv6 wildcard without password", cfg: ManagementConfig{Enabled: &enabled, Listen: "[::]:9091"}, wantErr: true},
		{name: "external without password", cfg: ManagementConfig{Enabled: &enabled, Listen: "192.0.2.1:9091"}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateManagementConfig(test.cfg)
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateManagementConfig() error=%v, wantErr=%v", err, test.wantErr)
			}
		})
	}
}
