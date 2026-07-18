package config

import "testing"

func TestProbeConcurrencyOrDefault(t *testing.T) {
	tests := []struct {
		configured int
		want       int
	}{
		{configured: 0, want: 32},
		{configured: -1, want: 32},
		{configured: 1, want: 1},
		{configured: 256, want: 256},
		{configured: 2048, want: 1024},
	}
	for _, test := range tests {
		cfg := &Config{Management: ManagementConfig{ProbeConcurrency: test.configured}}
		if got := cfg.ProbeConcurrencyOrDefault(); got != test.want {
			t.Errorf("ProbeConcurrencyOrDefault(%d) = %d, want %d", test.configured, got, test.want)
		}
	}
}
