package probe

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aidenappl/monitor-core/structs"
)

// fakeZone stands in for a far-end monitor-core. Both endpoints are served from
// one handler so a test can make /health and /ready disagree, which is the whole
// shape of the degraded case.
func fakeZone(t *testing.T, healthStatus int, healthBody string, readyStatus int, readyBody string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(healthStatus)
			io.WriteString(w, healthBody)
		case "/ready":
			w.WriteHeader(readyStatus)
			io.WriteString(w, readyBody)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

const (
	healthyReady = `{"status":"ready","clickhouse_ok":true,"mariadb_ok":true}`
	notReady     = `{"status":"not_ready","failing":["clickhouse"],"clickhouse_ok":false}`
)

// TestProbeReportsMismatchedWhenTheFarEndIsAnotherZone is the test this package
// exists for.
//
// The far end is a perfectly healthy monitor-core answering 200 on both probes —
// a status-code check would call this green — and it is SOMEBODY ELSE. Every read
// made through a registry row in this state returns another tenant's data under
// this zone's name, with every reference syntactically valid and nothing logged.
func TestProbeReportsMismatchedWhenTheFarEndIsAnotherZone(t *testing.T) {
	srv := fakeZone(t, http.StatusOK, `{"status":"ok","role":"zone","zone":"zone-two"}`, http.StatusOK, healthyReady)

	result := probeEndpoint(context.Background(), srv.URL, "zone-one")

	if result.Reachability != structs.ZoneReachabilityMismatched {
		t.Fatalf("Reachability = %q, want %q", result.Reachability, structs.ZoneReachabilityMismatched)
	}
	if result.ReportedZone != "zone-two" {
		t.Errorf("ReportedZone = %q, want %q — the claim is the finding and must be kept verbatim", result.ReportedZone, "zone-two")
	}
	// Both names have to appear, because "mismatched" alone does not tell an
	// operator whether the URL or the row is the thing to fix.
	if !strings.Contains(result.Detail, "zone-two") || !strings.Contains(result.Detail, "zone-one") {
		t.Errorf("Detail = %q, want it to name both the reported and the expected zone", result.Detail)
	}
}

// TestProbeReportsMismatchedWhenTheFarEndIsTheControlPlane is the subtle half,
// and the one a slug comparison alone gets wrong.
//
// A control plane runs the same binary and carries the same MON_ZONE_SLUG
// default, so it reports the very slug the registry row expects — and it serves
// no events at all. It is also the most likely wrong URL to be typed, because
// MON_PUBLIC_URL is the control plane. Compare slugs before roles and this
// misconfiguration comes back HEALTHY, with every read returning nothing and
// reading as "this zone is quiet".
func TestProbeReportsMismatchedWhenTheFarEndIsTheControlPlane(t *testing.T) {
	srv := fakeZone(t, http.StatusOK, `{"status":"ok","role":"app","zone":"zone-one"}`, http.StatusOK, healthyReady)

	result := probeEndpoint(context.Background(), srv.URL, "zone-one")

	if result.Reachability != structs.ZoneReachabilityMismatched {
		t.Fatalf("Reachability = %q, want %q — the slug matches but a control plane serves no events",
			result.Reachability, structs.ZoneReachabilityMismatched)
	}
	if !strings.Contains(result.Detail, "CONTROL PLANE") {
		t.Errorf("Detail = %q, want it to say the target is a control plane", result.Detail)
	}
}

// TestProbeReportsHealthyWhenTheZoneMatches IS THE NEGATIVE CONTROL for the two
// tests above. Without it, a classify() that returned Mismatched unconditionally
// would pass both of them and fail nothing.
func TestProbeReportsHealthyWhenTheZoneMatches(t *testing.T) {
	srv := fakeZone(t, http.StatusOK, `{"status":"ok","role":"zone","zone":"zone-one"}`, http.StatusOK, healthyReady)

	result := probeEndpoint(context.Background(), srv.URL, "zone-one")

	if result.Reachability != structs.ZoneReachabilityHealthy {
		t.Fatalf("Reachability = %q, want %q (detail: %s)", result.Reachability, structs.ZoneReachabilityHealthy, result.Detail)
	}
	if result.ReportedZone != "zone-one" {
		t.Errorf("ReportedZone = %q, want %q", result.ReportedZone, "zone-one")
	}
	if result.ProbedAt.IsZero() {
		t.Error("ProbedAt is zero — a reachability with no timestamp asserts a freshness it does not have")
	}
}

// TestProbeReportsDegradedWhenReadyFails: the right zone, not serving. Degraded
// rather than healthy, and NOT unreachable — the identity was confirmed, so the
// operator is being told about a store, not about a URL.
func TestProbeReportsDegradedWhenReadyFails(t *testing.T) {
	srv := fakeZone(t, http.StatusOK, `{"status":"ok","role":"zone","zone":"zone-one"}`,
		http.StatusServiceUnavailable, notReady)

	result := probeEndpoint(context.Background(), srv.URL, "zone-one")

	if result.Reachability != structs.ZoneReachabilityDegraded {
		t.Fatalf("Reachability = %q, want %q", result.Reachability, structs.ZoneReachabilityDegraded)
	}
	if !strings.Contains(result.Detail, "clickhouse") {
		t.Errorf("Detail = %q, want it to name the failing store", result.Detail)
	}
}

// TestProbeReportsUnverifiedWithoutAZoneIdentity pins the value that costs the
// most to argue for. A 200 proves a monitor-core is listening; it does not prove
// it is THIS zone's. An older build, a proxy answering upstream's behalf, or an
// unrelated service all land here, and calling any of them healthy is how a row
// ends up pointed at the wrong box with a green tick beside it.
func TestProbeReportsUnverifiedWithoutAZoneIdentity(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"an older build with no zone key", `{"status":"ok","role":"zone"}`},
		{"an empty zone value", `{"status":"ok","role":"zone","zone":""}`},
		{"a 200 from something that is not monitor-core", `{"ok":true}`},
		{"a zone with no role", `{"status":"ok","zone":"zone-one"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := fakeZone(t, http.StatusOK, tt.body, http.StatusOK, healthyReady)

			result := probeEndpoint(context.Background(), srv.URL, "zone-one")

			if result.Reachability != structs.ZoneReachabilityUnverified {
				t.Fatalf("Reachability = %q, want %q (detail: %s)",
					result.Reachability, structs.ZoneReachabilityUnverified, result.Detail)
			}
		})
	}
}

// TestProbeReportsUnreachableOnANonOKHealth. monitor-core's /health answers 200
// in every role and every dependency state, so anything else came from something
// that is not this zone's monitor-core — a proxy, a load balancer, or nothing at
// all.
func TestProbeReportsUnreachableOnANonOKHealth(t *testing.T) {
	srv := fakeZone(t, http.StatusBadGateway, `{"status":"nope"}`, http.StatusOK, healthyReady)

	result := probeEndpoint(context.Background(), srv.URL, "zone-one")

	if result.Reachability != structs.ZoneReachabilityUnreachable {
		t.Fatalf("Reachability = %q, want %q", result.Reachability, structs.ZoneReachabilityUnreachable)
	}
	if !strings.Contains(result.Detail, "502") {
		t.Errorf("Detail = %q, want it to name the status that came back", result.Detail)
	}
}

// TestProbeReportsUnreachableWhenNothingAnswers — the ordinary outage. The zone
// stays in the registry and stays active; only its reachability moves.
func TestProbeReportsUnreachableWhenNothingAnswers(t *testing.T) {
	srv := fakeZone(t, http.StatusOK, `{"status":"ok","role":"zone","zone":"zone-one"}`, http.StatusOK, healthyReady)
	url := srv.URL
	srv.Close() // the port is now closed; the probe must survive it as a verdict

	result := probeEndpoint(context.Background(), url, "zone-one")

	if result.Reachability != structs.ZoneReachabilityUnreachable {
		t.Fatalf("Reachability = %q, want %q", result.Reachability, structs.ZoneReachabilityUnreachable)
	}
	if result.Detail == "" {
		t.Error("Detail is empty — an operator needs to know whether it was DNS, TLS or a refused connection")
	}
}

// TestProbeHonoursTheCallerDeadline proves the budget is real: the context this
// probe builds is derived from the caller's, so an admin request that is already
// over cannot be extended by ZONE_PROBE_TIMEOUT. A probe that ignored the context
// would block on the sleeping server instead.
func TestProbeHonoursTheCallerDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	start := time.Now()
	result := probeEndpoint(ctx, srv.URL, "zone-one")
	elapsed := time.Since(start)

	if result.Reachability != structs.ZoneReachabilityUnreachable {
		t.Fatalf("Reachability = %q, want %q", result.Reachability, structs.ZoneReachabilityUnreachable)
	}
	if elapsed >= ZONE_PROBE_TIMEOUT {
		t.Errorf("probe took %s — it ran to its own budget instead of the caller's %s deadline", elapsed, time.Millisecond)
	}
}

// TestZoneReportsUnconfiguredWithNoQueryURL. Not folded into unreachable: nobody
// failed to reach anything, and the fix is a registry row to finish filling in
// rather than a box to go and look at.
func TestZoneReportsUnconfiguredWithNoQueryURL(t *testing.T) {
	result := Zone(context.Background(), structs.Zone{Slug: "zone-one", QueryURL: "  "})

	if result.Reachability != structs.ZoneReachabilityUnconfigured {
		t.Fatalf("Reachability = %q, want %q", result.Reachability, structs.ZoneReachabilityUnconfigured)
	}
	if result.ProbedAt.IsZero() {
		t.Error("ProbedAt is zero — the row still records WHEN it was last looked at")
	}
}

// TestZoneRefusesAnInternalTarget is the negative control on the split between
// Zone and probeEndpoint.
//
// Every test above calls probeEndpoint, which has no SSRF guard, so without this
// one the suite would be equally green against an exported Zone that had lost its
// guard entirely — and that guard is what stops an administrator-supplied
// query_url turning this server into an internal port scanner. httptest listens
// on 127.0.0.1 over plain HTTP, so it is exactly the target tools.ValidateExternalURL
// must refuse.
func TestZoneRefusesAnInternalTarget(t *testing.T) {
	srv := fakeZone(t, http.StatusOK, `{"status":"ok","role":"zone","zone":"zone-one"}`, http.StatusOK, healthyReady)

	result := Zone(context.Background(), structs.Zone{Slug: "zone-one", QueryURL: srv.URL})

	if result.Reachability != structs.ZoneReachabilityUnreachable {
		t.Fatalf("Reachability = %q, want %q — Zone must not fetch a loopback target", result.Reachability, structs.ZoneReachabilityUnreachable)
	}
	if !strings.Contains(result.Detail, "refused to probe") {
		t.Errorf("Detail = %q, want it to say the target was refused rather than tried", result.Detail)
	}
}
