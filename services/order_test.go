package services

import (
	"strings"
	"testing"
)

func TestParseOrder(t *testing.T) {
	tests := []struct {
		raw     string
		want    string
		wantErr bool
	}{
		{"", OrderDesc, false},
		{"desc", OrderDesc, false},
		{"DESC", OrderDesc, false},
		{" asc ", OrderAsc, false},
		{"asc", OrderAsc, false},
		{"ascending", "", true},
		{"timestamp", "", true},
		{"1", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := ParseOrder(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseOrder(%q) err = %v, wantErr %v", tt.raw, err, tt.wantErr)
			}
			if err != nil && !strings.HasPrefix(err.Error(), "order ") {
				t.Errorf("error %q does not name the order parameter", err)
			}
			if got != tt.want {
				t.Errorf("ParseOrder(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

// TestQueryEventsPageOrder pins that order flips BOTH phases of the two-phase
// page together — a page sorted ascending but bounded by the descending
// boundary would return the wrong rows — and that the single-phase form follows.
func TestQueryEventsPageOrder(t *testing.T) {
	tests := []struct {
		name    string
		order   string
		filters []Filter
		want    string
	}{
		{
			name:  "default is desc",
			order: "",
			want: eventsPagePrefix + "project = ? AND " +
				"timestamp >= (SELECT min(timestamp) FROM (SELECT timestamp FROM monitor.events WHERE project = ? ORDER BY timestamp DESC LIMIT 100 OFFSET 0)) " +
				"ORDER BY timestamp DESC LIMIT 100 OFFSET 0",
		},
		{
			name:  "desc",
			order: OrderDesc,
			want: eventsPagePrefix + "project = ? AND " +
				"timestamp >= (SELECT min(timestamp) FROM (SELECT timestamp FROM monitor.events WHERE project = ? ORDER BY timestamp DESC LIMIT 100 OFFSET 0)) " +
				"ORDER BY timestamp DESC LIMIT 100 OFFSET 0",
		},
		{
			name:  "asc flips the sort and the boundary",
			order: OrderAsc,
			want: eventsPagePrefix + "project = ? AND " +
				"timestamp <= (SELECT max(timestamp) FROM (SELECT timestamp FROM monitor.events WHERE project = ? ORDER BY timestamp ASC LIMIT 100 OFFSET 0)) " +
				"ORDER BY timestamp ASC LIMIT 100 OFFSET 0",
		},
		{
			name:    "asc single-phase with a data filter",
			order:   OrderAsc,
			filters: []Filter{{Field: "latency_ms", Operator: OpEq, Value: "5", IsData: true}},
			want:    eventsPagePrefix + "project = ? AND position(data, ?) > 0 AND " + wantGuardedString + " = ? ORDER BY timestamp ASC LIMIT 100 OFFSET 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withDefaultProject(t, "default")
			recorder := withRecordingConn(t)
			if _, err := QueryEvents(scopedContext("atlas"), QueryParams{Order: tt.order, Filters: tt.filters}); err != nil {
				t.Fatalf("QueryEvents: %v", err)
			}
			page, _ := recorder.find(t, func(s string) bool { return strings.HasPrefix(s, eventsPagePrefix) })
			if page != tt.want {
				t.Errorf("page sql =\n\t%s\nwant\n\t%s", page, tt.want)
			}
		})
	}
}
