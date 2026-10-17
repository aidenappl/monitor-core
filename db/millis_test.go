package db

import (
	"testing"
	"time"
)

func TestMillisValue(t *testing.T) {
	pst := time.FixedZone("PST", -8*3600)
	tests := []struct {
		name string
		in   time.Time
		want string
	}{
		{"keeps milliseconds", time.Date(2026, 10, 17, 12, 21, 48, 730_000_000, time.UTC), "2026-10-17 12:21:48.730"},
		{"pads to three digits", time.Date(2026, 10, 17, 12, 21, 48, 5_000_000, time.UTC), "2026-10-17 12:21:48.005"},
		{"whole second", time.Date(2026, 10, 17, 12, 21, 48, 0, time.UTC), "2026-10-17 12:21:48.000"},
		{"truncates below a millisecond", time.Date(2026, 10, 17, 12, 21, 48, 730_999_999, time.UTC), "2026-10-17 12:21:48.730"},
		{"converts to UTC", time.Date(2026, 10, 17, 4, 21, 48, 730_000_000, pst), "2026-10-17 12:21:48.730"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := Millis(tt.in).Value()
			if err != nil {
				t.Fatalf("Value: %v", err)
			}
			if v != tt.want {
				t.Errorf("Value() = %q, want %q", v, tt.want)
			}
		})
	}
}
