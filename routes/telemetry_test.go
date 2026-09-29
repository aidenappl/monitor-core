package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	monitor "github.com/aidenappl/go-monitor"
	"github.com/aidenappl/monitor-core/probe"
	"github.com/aidenappl/monitor-core/structs"
)

// TestProbeTransitionFiresOncePerChange: an operator clicking "probe" on a
// zone that stays down must not produce a warning per click — only the change
// of state is news. The row is updated after each probe exactly as
// HandleProbeZone records it.
func TestProbeTransitionFiresOncePerChange(t *testing.T) {
	rec := monitor.StartRecording()
	t.Cleanup(rec.Stop)

	zone := &structs.Zone{ID: 2, Slug: "appleby", Reachability: structs.ZoneReachabilityHealthy}
	sequence := []structs.ZoneReachability{
		structs.ZoneReachabilityUnreachable,
		structs.ZoneReachabilityUnreachable,
		structs.ZoneReachabilityUnreachable,
		structs.ZoneReachabilityHealthy,
		structs.ZoneReachabilityHealthy,
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/zones/2/probe", nil)
	for _, verdict := range sequence {
		reportProbeTransition(req, zone, probe.Result{
			Reachability: verdict,
			Detail:       "query_url: https://appleby-monitor-api.appleby.cloud/health could not be reached",
		})
		zone.Reachability = verdict
	}

	unhealthy := rec.Named("zone.probe.unhealthy")
	recovered := rec.Named("zone.probe.recovered")
	if len(unhealthy) != 1 || len(recovered) != 1 || len(rec.Events()) != 2 {
		t.Fatalf("5 probes produced %d events (%d unhealthy, %d recovered), want exactly one of each",
			len(rec.Events()), len(unhealthy), len(recovered))
	}
	if unhealthy[0].Level != monitor.LevelWarn || recovered[0].Level != monitor.LevelInfo {
		t.Errorf("levels = %s/%s, want warn/info", unhealthy[0].Level, recovered[0].Level)
	}
	data := unhealthy[0].Data.(map[string]any)
	if data["previous"] != "healthy" || data["reachability"] != "unreachable" || data["detail"] == "" || data["zone"] != "appleby" {
		t.Errorf("the unhealthy warning lacks the probe's detail: %v", data)
	}
}

func TestProbeTransitionLevels(t *testing.T) {
	tests := []struct {
		name     string
		previous structs.ZoneReachability
		now      structs.ZoneReachability
		wantName string
	}{
		{"first probe healthy says nothing new", structs.ZoneReachabilityUnknown, structs.ZoneReachabilityHealthy, ""},
		{"first probe unhealthy warns", structs.ZoneReachabilityUnknown, structs.ZoneReachabilityMismatched, "zone.probe.unhealthy"},
		{"one unhealthy state to another warns", structs.ZoneReachabilityUnreachable, structs.ZoneReachabilityDegraded, "zone.probe.unhealthy"},
		{"degraded to healthy is a recovery", structs.ZoneReachabilityDegraded, structs.ZoneReachabilityHealthy, "zone.probe.recovered"},
		{"unchanged healthy says nothing", structs.ZoneReachabilityHealthy, structs.ZoneReachabilityHealthy, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := monitor.StartRecording()
			defer rec.Stop()

			reportProbeTransition(httptest.NewRequest(http.MethodPost, "/admin/zones/1/probe", nil),
				&structs.Zone{ID: 1, Slug: "z1", Reachability: tt.previous},
				probe.Result{Reachability: tt.now})

			events := rec.Events()
			switch {
			case tt.wantName == "" && len(events) != 0:
				t.Errorf("emitted %s, want nothing", events[0].Name)
			case tt.wantName != "" && (len(events) != 1 || events[0].Name != tt.wantName):
				t.Errorf("events = %d, want one %s", len(events), tt.wantName)
			}
		})
	}
}

// TestIngestSummaryOnlyWhenSomethingMoved: the periodic summary is the only
// trace of healthy ingest, and an idle window costs nothing.
func TestIngestSummaryOnlyWhenSomethingMoved(t *testing.T) {
	tests := []struct {
		name      string
		prev, cur ingestTotals
		want      int
	}{
		{"idle window is silent", ingestTotals{enqueued: 10, flushed: 10}, ingestTotals{enqueued: 10, flushed: 10}, 0},
		{"traffic is summarised", ingestTotals{enqueued: 10, flushed: 10}, ingestTotals{enqueued: 900, flushed: 890}, 1},
		{"a window of refusals is summarised", ingestTotals{}, ingestTotals{authRejected: 3}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := monitor.StartRecording()
			defer rec.Stop()

			emitIngestSummary(t.Context(), tt.prev, tt.cur, 15*time.Minute)

			if got := len(rec.Named("ingest.summary.reported")); got != tt.want {
				t.Errorf("summary events = %d, want %d", got, tt.want)
			}
		})
	}
}
