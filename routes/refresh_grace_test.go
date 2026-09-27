package routes

import (
	"testing"
	"time"

	"github.com/aidenappl/monitor-core/structs"
)

func TestWithinRefreshGrace(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		ago  time.Duration
		want bool
	}{
		{name: "just spent", ago: 0, want: true},
		{name: "29s ago", ago: 29 * time.Second, want: true},
		{name: "exactly at the boundary is inside", ago: 30 * time.Second, want: true},
		{name: "31s ago", ago: 31 * time.Second, want: false},
		{name: "future used_at (clock skew) is inside", ago: -5 * time.Second, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := withinRefreshGrace(now.Add(-tt.ago), now); got != tt.want {
				t.Errorf("withinRefreshGrace(now-%s) = %v, want %v", tt.ago, got, tt.want)
			}
		})
	}
}

func TestClassifyRefresh(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) *time.Time { v := now.Add(d); return &v }
	successor := int64(99)
	live := now.Add(24 * time.Hour)

	tests := []struct {
		name string
		row  structs.RefreshToken
		want refreshOutcome
	}{
		{
			name: "fresh → rotate",
			row:  structs.RefreshToken{ExpiresAt: live},
			want: refreshRotate,
		},
		{
			name: "replaced 10s ago → grace",
			row:  structs.RefreshToken{ExpiresAt: live, ReplacedBy: &successor, UsedAt: at(-10 * time.Second)},
			want: refreshGrace,
		},
		{
			name: "replaced exactly 30s ago → grace",
			row:  structs.RefreshToken{ExpiresAt: live, ReplacedBy: &successor, UsedAt: at(-30 * time.Second)},
			want: refreshGrace,
		},
		{
			name: "replaced 45s ago → reuse",
			row:  structs.RefreshToken{ExpiresAt: live, ReplacedBy: &successor, UsedAt: at(-45 * time.Second)},
			want: refreshReuse,
		},
		{
			name: "replaced with nil UsedAt (pre-134 row) → reuse",
			row:  structs.RefreshToken{ExpiresAt: live, ReplacedBy: &successor},
			want: refreshReuse,
		},
		{
			name: "revoked within window → revoked",
			row:  structs.RefreshToken{ExpiresAt: live, ReplacedBy: &successor, UsedAt: at(-5 * time.Second), RevokedAt: at(-1 * time.Second)},
			want: refreshRevoked,
		},
		{
			name: "revoked unspent → revoked",
			row:  structs.RefreshToken{ExpiresAt: live, RevokedAt: at(-time.Hour)},
			want: refreshRevoked,
		},
		{
			name: "expired → expired",
			row:  structs.RefreshToken{ExpiresAt: now.Add(-time.Second)},
			want: refreshExpired,
		},
		{
			name: "expired but spent within window → expired (no grace)",
			row:  structs.RefreshToken{ExpiresAt: now.Add(-time.Second), ReplacedBy: &successor, UsedAt: at(-5 * time.Second)},
			want: refreshExpired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := tt.row
			if got := classifyRefresh(&row, now); got != tt.want {
				t.Errorf("classifyRefresh = %s, want %s", got, tt.want)
			}
		})
	}
}
