package routes

import (
	"context"
	"sync"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/telemetry"
)

// CLICKHOUSE_DISK_CHECK_EVERY is how often RunClickHouseDiskCheck reads
// system.disks. /health serves the last reading and never queries ClickHouse
// itself: it is unauthenticated and polled.
const CLICKHOUSE_DISK_CHECK_EVERY = 5 * time.Minute

// CLICKHOUSE_DISK_LOW_FREE_PCT is the free-space floor below which every check
// emits clickhouse.disk.low at warn — once per check, not once per incident, so
// an alert rule counting it keeps firing for as long as the disk stays low.
// On 2026-11-06 worker-6's disk filled, ClickHouse answered every insert with
// code 243 "not enough space", and 39,026 trailblaze events were dropped over
// eleven hours before anyone looked. Nothing in Monitor said the disk was the
// cause.
const CLICKHOUSE_DISK_LOW_FREE_PCT = 10.0

// ClickHouseDisk is one row of system.disks as /health reports it.
type ClickHouseDisk struct {
	Name       string  `json:"name"`
	FreeBytes  uint64  `json:"free_bytes"`
	TotalBytes uint64  `json:"total_bytes"`
	FreePct    float64 `json:"free_pct"`
	Low        bool    `json:"low"`
}

var clickhouseDisks struct {
	mu        sync.RWMutex
	disks     []ClickHouseDisk
	checkedAt time.Time
}

// clickhouseDiskHealth is the `clickhouse_disk` value on /health: nil until the
// first reading (and in a process that runs no data plane).
func clickhouseDiskHealth() map[string]any {
	clickhouseDisks.mu.RLock()
	defer clickhouseDisks.mu.RUnlock()
	if clickhouseDisks.checkedAt.IsZero() {
		return nil
	}
	low := false
	for _, d := range clickhouseDisks.disks {
		low = low || d.Low
	}
	return map[string]any{
		"disks":         clickhouseDisks.disks,
		"low":           low,
		"low_below_pct": CLICKHOUSE_DISK_LOW_FREE_PCT,
		"checked_at":    clickhouseDisks.checkedAt.UTC().Format(time.RFC3339),
	}
}

// evaluateDisks fills FreePct and Low. A disk reporting no total (some virtual
// disks do) is never low: there is no ratio to judge.
func evaluateDisks(disks []ClickHouseDisk, lowBelowPct float64) []ClickHouseDisk {
	for i := range disks {
		if disks[i].TotalBytes == 0 {
			continue
		}
		disks[i].FreePct = float64(disks[i].FreeBytes) / float64(disks[i].TotalBytes) * 100
		disks[i].Low = disks[i].FreePct < lowBelowPct
	}
	return disks
}

func readClickHouseDisks(ctx context.Context) ([]ClickHouseDisk, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := db.Conn.Query(ctx, "SELECT name, free_space, total_space FROM system.disks")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClickHouseDisk
	for rows.Next() {
		var d ClickHouseDisk
		if err := rows.Scan(&d.Name, &d.FreeBytes, &d.TotalBytes); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func checkClickHouseDisks(ctx context.Context) {
	disks, err := readClickHouseDisks(ctx)
	if err != nil {
		telemetry.WarnCoalesced(ctx, "clickhouse.disk.check", "clickhouse.disk.check_failed", err, nil)
		return
	}
	disks = evaluateDisks(disks, CLICKHOUSE_DISK_LOW_FREE_PCT)

	clickhouseDisks.mu.Lock()
	clickhouseDisks.disks = disks
	clickhouseDisks.checkedAt = time.Now()
	clickhouseDisks.mu.Unlock()

	for _, d := range disks {
		if d.Low {
			telemetry.Warn(ctx, "clickhouse.disk.low", map[string]any{
				"disk":          d.Name,
				"free_bytes":    d.FreeBytes,
				"total_bytes":   d.TotalBytes,
				"free_pct":      d.FreePct,
				"low_below_pct": CLICKHOUSE_DISK_LOW_FREE_PCT,
			})
		}
	}
}

// RunClickHouseDiskCheck reads system.disks now and every
// CLICKHOUSE_DISK_CHECK_EVERY until ctx is cancelled. Data plane only.
func RunClickHouseDiskCheck(ctx context.Context) {
	checkClickHouseDisks(ctx)
	ticker := time.NewTicker(CLICKHOUSE_DISK_CHECK_EVERY)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkClickHouseDisks(ctx)
		}
	}
}
