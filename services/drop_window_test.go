package services

import (
	"testing"
	"time"
)

func TestDropWindow(t *testing.T) {
	base := time.Date(2026, 11, 6, 7, 47, 0, 0, time.UTC)
	tests := []struct {
		name      string
		drops     []time.Duration // offsets from base, one event each
		at        time.Duration
		wantCount int64
	}{
		{"nothing dropped", nil, 0, 0},
		{"inside the window", []time.Duration{0, time.Minute, 5 * time.Minute}, 10 * time.Minute, 3},
		{"aged out", []time.Duration{0, time.Minute}, 20 * time.Minute, 0},
		{"partly aged out", []time.Duration{0, 10 * time.Minute}, 16 * time.Minute, 1},
		{"bucket reused after a lap", []time.Duration{0, 15 * time.Minute}, 15 * time.Minute, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var w dropWindow
			for _, d := range tt.drops {
				w.record(base.Add(d), 1)
			}
			got, last := w.recent(base.Add(tt.at))
			if got != tt.wantCount {
				t.Errorf("recent = %d, want %d", got, tt.wantCount)
			}
			if len(tt.drops) == 0 && !last.IsZero() {
				t.Errorf("last = %v, want zero with no drops", last)
			}
			if n := len(tt.drops); n > 0 && !last.Equal(base.Add(tt.drops[n-1])) {
				t.Errorf("last = %v, want the latest drop", last)
			}
		})
	}
}

func TestQueueRecentDropsCountsBothPaths(t *testing.T) {
	q := NewQueue(1)
	q.Enqueue(nil) // fills the buffer
	q.Enqueue(nil) // overflow: dropped
	q.RecordDropped(4)

	count, last := q.RecentDrops()
	if count != 5 {
		t.Errorf("RecentDrops count = %d, want 5 (1 overflow + 4 recorded)", count)
	}
	if last.IsZero() || time.Since(last) > time.Minute {
		t.Errorf("RecentDrops last = %v, want just now", last)
	}
	if _, dropped, _ := q.Stats(); dropped != 5 {
		t.Errorf("lifetime dropped = %d, want 5", dropped)
	}
}
