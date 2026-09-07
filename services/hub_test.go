package services

import (
	"testing"

	"github.com/aidenappl/monitor-core/structs"
)

// TestMatchesFilters_UnknownKeyDoesNotMatch pins the whole filter contract, not
// just the unknown-key case, because the dangerous failure here is silent: an
// unhandled key used to fall straight through to `return true`, turning a
// narrow subscription into a firehose. Asserting the four known keys alongside
// it keeps this test the single place that documents what the hub will and
// will not deliver, so adding a filter key without a matching case in
// matchesFilters fails here rather than in production.
func TestMatchesFilters_UnknownKeyDoesNotMatch(t *testing.T) {
	withDefaultProject(t, "default")

	event := &structs.Event{
		Service: "monitor-core",
		Env:     "production",
		Level:   "error",
		Name:    "request.failed",
		Project: "atlas",
	}

	tests := []struct {
		name    string
		filters map[string]string
		want    bool
	}{
		// No filters at all is a deliberate subscribe-to-everything.
		{"no filters matches everything", nil, true},
		{"empty filter map matches everything", map[string]string{}, true},

		// The keys the switch actually handles.
		{"service matches", map[string]string{"service": "monitor-core"}, true},
		{"service mismatches", map[string]string{"service": "forta-api"}, false},
		{"env matches", map[string]string{"env": "production"}, true},
		{"env mismatches", map[string]string{"env": "staging"}, false},
		{"level matches", map[string]string{"level": "error"}, true},
		{"level mismatches", map[string]string{"level": "info"}, false},
		{"name matches", map[string]string{"name": "request.failed"}, true},
		{"name mismatches", map[string]string{"name": "request.ok"}, false},

		// project — the tenancy key. Unlike the four above it is never supplied
		// by the client: routes/stream.go sets it from the credential, so these
		// cases are what stands between a subscriber and another project's live
		// error feed.
		{"project matches", map[string]string{"project": "atlas"}, true},
		{"project mismatches", map[string]string{"project": "johnnies"}, false},
		{"project wins over every matching known key", map[string]string{
			"service": "monitor-core",
			"env":     "production",
			"level":   "error",
			"name":    "request.failed",
			"project": "johnnies",
		}, false},
		// An unresolved project must match nothing rather than fall through to
		// equality with an unstamped event — see scope.Matches.
		{"empty project matches nothing", map[string]string{"project": ""}, false},

		// Multiple known keys are ANDed, not ORed.
		{"all known keys match", map[string]string{
			"service": "monitor-core",
			"env":     "production",
			"level":   "error",
			"name":    "request.failed",
			"project": "atlas",
		}, true},
		{"one known key of several mismatches", map[string]string{
			"service": "monitor-core",
			"env":     "staging",
		}, false},

		// The regression this test exists for: an unrecognised key must match
		// nothing. "zone" is the example now that "project" has been added —
		// it is the dimension most likely to reach the stream route next.
		{"unknown key matches nothing", map[string]string{"zone": "trailblaze"}, false},
		{"unknown key wins over a matching known key", map[string]string{
			"service": "monitor-core",
			"zone":    "trailblaze",
		}, false},
		{"empty-string unknown key still matches nothing", map[string]string{"zone": ""}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesFilters(event, tt.filters); got != tt.want {
				t.Errorf("matchesFilters(event, %v) = %v, want %v", tt.filters, got, tt.want)
			}
		})
	}
}
