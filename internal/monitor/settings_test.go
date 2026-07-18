package monitor

import (
	"testing"
	"time"
)

func TestParsePositiveSettingsDuration(t *testing.T) {
	got, err := parsePositiveSettingsDuration(" 50ms ")
	if err != nil || got != 50*time.Millisecond {
		t.Fatalf("valid duration rejected: duration=%v err=%v", got, err)
	}
	for _, value := range []string{"", "0s", "-1m", "tomorrow"} {
		if _, err := parsePositiveSettingsDuration(value); err == nil {
			t.Fatalf("invalid duration %q accepted", value)
		}
	}
}
