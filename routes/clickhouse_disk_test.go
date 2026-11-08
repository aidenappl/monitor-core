package routes

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aidenappl/monitor-core/services"
)

func TestEvaluateDisks(t *testing.T) {
	tests := []struct {
		name    string
		disk    ClickHouseDisk
		wantLow bool
		wantPct float64
	}{
		{"healthy", ClickHouseDisk{FreeBytes: 50, TotalBytes: 100}, false, 50},
		{"just under the floor", ClickHouseDisk{FreeBytes: 9, TotalBytes: 100}, true, 9},
		{"exactly at the floor", ClickHouseDisk{FreeBytes: 10, TotalBytes: 100}, false, 10},
		{"full", ClickHouseDisk{FreeBytes: 0, TotalBytes: 100}, true, 0},
		{"no total reported", ClickHouseDisk{FreeBytes: 0, TotalBytes: 0}, false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evaluateDisks([]ClickHouseDisk{tt.disk}, CLICKHOUSE_DISK_LOW_FREE_PCT)[0]
			if got.Low != tt.wantLow || got.FreePct != tt.wantPct {
				t.Errorf("got low=%v pct=%v, want low=%v pct=%v", got.Low, got.FreePct, tt.wantLow, tt.wantPct)
			}
		})
	}
}

func TestHealthReportsRecentDropsAndDisk(t *testing.T) {
	resetDependencyProbe()
	t.Cleanup(resetDependencyProbe)

	prevQueue := Queue
	Queue = services.NewQueue(1)
	t.Cleanup(func() { Queue = prevQueue })

	decode := func() map[string]interface{} {
		rec := httptest.NewRecorder()
		HealthHandler(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
		var body map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode /health body: %v", err)
		}
		return body
	}

	body := decode()
	if body["last_drop_at"] != nil || body["dropped_recent"] != float64(0) {
		t.Errorf("before any drop: last_drop_at=%v dropped_recent=%v, want null and 0", body["last_drop_at"], body["dropped_recent"])
	}
	if body["dropped_window_minutes"] != float64(15) {
		t.Errorf("dropped_window_minutes = %v, want 15", body["dropped_window_minutes"])
	}
	if body["clickhouse_disk"] != nil {
		t.Errorf("clickhouse_disk = %v before any check, want null", body["clickhouse_disk"])
	}

	Queue.RecordDropped(3)
	clickhouseDisks.mu.Lock()
	clickhouseDisks.disks = evaluateDisks([]ClickHouseDisk{{Name: "default", FreeBytes: 5, TotalBytes: 100}}, CLICKHOUSE_DISK_LOW_FREE_PCT)
	clickhouseDisks.checkedAt = time.Now()
	clickhouseDisks.mu.Unlock()
	t.Cleanup(func() {
		clickhouseDisks.mu.Lock()
		clickhouseDisks.disks, clickhouseDisks.checkedAt = nil, time.Time{}
		clickhouseDisks.mu.Unlock()
	})

	body = decode()
	if body["dropped"] != float64(3) || body["dropped_recent"] != float64(3) {
		t.Errorf("dropped=%v dropped_recent=%v, want 3 and 3", body["dropped"], body["dropped_recent"])
	}
	if s, _ := body["last_drop_at"].(string); s == "" {
		t.Errorf("last_drop_at = %v, want a timestamp", body["last_drop_at"])
	}
	disk, _ := body["clickhouse_disk"].(map[string]interface{})
	if disk == nil || disk["low"] != true {
		t.Errorf("clickhouse_disk = %v, want low=true at 5%% free", body["clickhouse_disk"])
	}
}
