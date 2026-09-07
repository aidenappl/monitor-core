// Package probe answers one question about a registry row: is the box at the
// other end of this zone's query_url actually THIS ZONE?
//
// It exists because a zone row RECORDS infrastructure rather than creating any
// (see the header of db/migrations/125_zone_endpoints.sql). Nothing reconciles
// the row against reality, so the failure worth building a package around is not
// the missing URL — that one is loud, everything downstream breaks and says so.
// It is the URL that is syntactically perfect and points at the WRONG BOX: the
// dashboard then renders one zone's data under another zone's name, every
// reference stays valid, nothing errors and nothing logs.
//
// ⚠️ A PROBE THAT ONLY CHECKS FOR A 200 IS WORTHLESS HERE. Every zone runs the
// same binary and every one of them answers /health with 200, so a query_url
// aimed at zone-one would pass a status-code check perfectly while mislabelling
// every read made through it. What makes this useful is the comparison in
// classify(): what the far end SAYS it is, against what the registry EXPECTED it
// to be. structs.ZoneReachabilityMismatched is the verdict that pays for the
// whole package.
//
// Nothing here writes to the database. The caller persists the verdict through
// query.RecordZoneProbe, which keeps the measurement and the storage of it in
// different packages — a probe that could write its own result is a probe a
// handler could use to fake one.
package probe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/tools"
)

// ZONE_PROBE_TIMEOUT is the WHOLE budget for probing one zone — DNS, TLS, both
// requests and both body reads, together.
//
// It is short deliberately, and the reasoning is the one the persisted verdict
// exists for in the first place. This runs behind an admin action, synchronously,
// while a person waits; a zone that accepts TCP and then never answers would
// otherwise hold the request open until the server's WriteTimeout (60s), so ONE
// wedged zone could hang the page whose entire job is to warn about wedged
// zones. Three seconds is an order of magnitude above a healthy round trip
// (routes/health.go budgets 2s for a single store ping and that is already
// generous) and far below any human's patience.
//
// It is a TOTAL rather than a per-request budget so the ceiling holds no matter
// how the two requests below are re-ordered or added to later. /health is
// fetched first and decides the verdict on its own in every failing case, so a
// /health that eats the whole budget leaves nothing for /ready and loses
// nothing: there is no zone identity to be ready about.
const ZONE_PROBE_TIMEOUT = 3 * time.Second

// MAX_PROBE_BODY_BYTES caps what is read from either endpoint.
//
// The far end is, by construction, whatever an administrator typed — including
// something that is not monitor-core at all. An unbounded json.Decoder against a
// host that streams forever is a memory exhaustion the probe would inflict on
// itself, and no genuine /health document is anywhere near this size.
const MAX_PROBE_BODY_BYTES = 64 << 10

// ROLE_CONTROL_PLANE is the value monitor-core's /health reports when it runs as
// the control plane (env.RoleApp). It is compared as a literal rather than
// imported from env, because this is the value a REMOTE process reported over
// the wire — a wire format this package parses, not this process's own
// configuration. Coupling the two would make a local refactor of the role type
// silently change how a remote answer is interpreted.
const ROLE_CONTROL_PLANE = "app"

// Result is one probe's verdict, shaped to be handed straight to
// query.RecordZoneProbe.
type Result struct {
	// Reachability is the verdict. Never the zero value: classify() returns one
	// of the seven states on every path, including the ones that never made a
	// request.
	Reachability structs.ZoneReachability

	// Detail is for a human reading an admin page. It names what was tried and
	// what came back, because "unreachable" alone sends an operator looking at
	// the wrong layer — a TLS failure and a connection refused need different
	// people.
	Detail string

	// ReportedZone is what the far end CLAIMED to be, verbatim and untrusted. It
	// is empty when nothing answered or nothing identified itself, and it is kept
	// even when it disagrees with the registry — especially then, because the
	// disagreement is the finding.
	ReportedZone string

	// ProbedAt is when the verdict was reached. Stored beside the verdict because
	// a reachability without a timestamp asserts a freshness it does not have.
	ProbedAt time.Time
}

// healthDocument is the subset of monitor-core's GET /health this probe reads.
//
// `zone` is what makes identity checkable at all. A build that predates it
// answers 200 with no zone and lands on Unverified rather than Healthy, which is
// the correct reading: it proves a monitor-core is listening and proves nothing
// about WHICH one.
type healthDocument struct {
	Status string `json:"status"`
	Role   string `json:"role"`
	Zone   string `json:"zone"`
}

// readyDocument is the subset of GET /ready this probe reads. `failing` names
// the stores that did not answer, and is what turns a bare "degraded" into
// something actionable.
type readyDocument struct {
	Status  string   `json:"status"`
	Failing []string `json:"failing"`
}

// client refuses redirects rather than following them.
//
// A 302 could bounce this probe to a host the SSRF guard never saw, which would
// turn an administrator-supplied URL back into the arbitrary outbound fetch that
// guard exists to prevent. It is also a real signal in its own right: a zone's
// /health does not redirect, so something in front of the zone is answering, and
// treating that as "reachable" would be the mislabelling this package is here to
// catch, arriving through the proxy layer.
var client = &http.Client{
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return fmt.Errorf("refused to follow a redirect to %s", req.URL.Redacted())
	},
}

// Zone probes one registry row and returns what it found.
//
// It never returns an error: every failure mode IS a reachability value, and
// that is the point. A caller that had to distinguish "the probe failed" from
// "the zone is unreachable" would have two ways to say the same thing and would
// eventually persist the wrong one — while an unreachable zone that raised an
// error rather than a verdict would be a zone the admin page shows nothing about,
// which is the "no issues is the most misleading possible reading of a 500"
// failure wearing a different hat.
func Zone(ctx context.Context, zone structs.Zone) Result {
	// BOTH endpoints are evaluated, and the worse verdict wins.
	//
	// The asymmetry between them is counterintuitive and worth stating. A wrong
	// query_url is the LOUDER failure: an operator opens the zone and sees
	// another tenant's events — confusing, but visible and reversible. A wrong
	// ingest_url is the QUIET one. Every go-monitor producer POSTs there, so
	// their events are filed under another tenant from the moment the row is
	// saved, nothing on any screen says so, and the data is mixed permanently —
	// the 30-day TTL does not unmix it, and issue fingerprints derived under the
	// wrong project cannot be re-keyed afterwards.
	//
	// Neither is skipped because the other is missing: a zone with an ingest_url
	// and no query_url still has producers pointed somewhere, and that somewhere
	// is exactly what needs checking.
	queryResult := probeOne(ctx, "query_url", zone.QueryURL, zone.Slug)
	ingestResult := probeOne(ctx, "ingest_url", zone.IngestURL, zone.Slug)
	return worseOf(queryResult, ingestResult)
}

// probeOne resolves a single endpoint to a verdict, attributing the outcome to
// the field it came from.
//
// The label is not decoration: "unreachable" on an admin page sends an operator
// to the wrong layer if they cannot tell which of the two URLs failed, and the
// two are frequently different hosts behind different proxies.
func probeOne(ctx context.Context, label, rawURL, expectSlug string) Result {
	if strings.TrimSpace(rawURL) == "" {
		return Result{
			Reachability: structs.ZoneReachabilityUnconfigured,
			Detail:       fmt.Sprintf("no %s recorded for this zone — there is nothing to probe", label),
			ProbedAt:     time.Now().UTC(),
		}
	}

	// ⚠️ RE-VALIDATED HERE, not trusted from the write. The check that ran when
	// the row was saved is not a lasting property of the row: DNS can be
	// re-pointed under a stored hostname at any time, so a value that was public
	// on Tuesday can resolve into RFC1918 on Wednesday and turn this probe into an
	// internal port scanner. tools.ValidateExternalURL is the one definition of
	// that rule — see tools/Validate.tool.go — and this calls it rather than
	// re-deriving it.
	if err := tools.ValidateExternalURL(rawURL); err != nil {
		return Result{
			Reachability: structs.ZoneReachabilityUnreachable,
			Detail:       fmt.Sprintf("%s: refused to probe %s: %v", label, rawURL, err),
			ProbedAt:     time.Now().UTC(),
		}
	}

	r := probeEndpoint(ctx, rawURL, expectSlug)
	r.Detail = label + ": " + r.Detail
	return r
}

// probeSeverity orders the verdicts so two probes can be combined without
// inventing a rule at the call site.
//
// Mismatched outranks unreachable deliberately. An unreachable zone reports
// nothing and misleads nobody; a mismatched one answers every request
// confidently with another tenant's data. Ranking them the other way would let a
// healthy query_url mask an ingest_url pointed at the wrong zone, which is the
// precise combination this function exists to surface.
func probeSeverity(r structs.ZoneReachability) int {
	switch r {
	case structs.ZoneReachabilityHealthy:
		return 0
	case structs.ZoneReachabilityDegraded:
		return 1
	case structs.ZoneReachabilityUnverified:
		return 2
	case structs.ZoneReachabilityUnconfigured:
		return 3
	case structs.ZoneReachabilityUnreachable:
		return 4
	case structs.ZoneReachabilityMismatched:
		return 5
	}
	// An unrecognised verdict sorts ABOVE healthy rather than below it: a state
	// this function has not been taught about must never be the one that wins and
	// paints a row green.
	return 2
}

// worseOf returns whichever result an operator most needs to see.
func worseOf(a, b Result) Result {
	if probeSeverity(b.Reachability) > probeSeverity(a.Reachability) {
		return b
	}
	return a
}

// probeEndpoint is Zone without the SSRF guard, split out so the guard sits on
// exactly one path and the transport logic can be exercised against a local test
// server (which the guard would, correctly, refuse).
//
// ⚠️ NOTHING OUTSIDE THIS PACKAGE MAY CALL IT. The exported entry point is Zone,
// and probe/zone_test.go carries a test asserting Zone still refuses a loopback
// target — a negative control on exactly the bypass this split creates.
func probeEndpoint(ctx context.Context, baseURL, expectedSlug string) Result {
	ctx, cancel := context.WithTimeout(ctx, ZONE_PROBE_TIMEOUT)
	defer cancel()

	// tools.NormalizeEndpointURL already strips this at write time, so it only
	// matters for the row that predates that function — where "https://z/" plus
	// "/health" would compose "https://z//health", which some proxies answer and
	// some 404. Trimmed here rather than trusted from the column.
	baseURL = strings.TrimRight(baseURL, "/")

	health, status, detail := fetchJSON[healthDocument](ctx, baseURL+"/health")
	if health == nil {
		return Result{
			Reachability: structs.ZoneReachabilityUnreachable,
			Detail:       detail,
			ProbedAt:     time.Now().UTC(),
		}
	}
	// monitor-core's /health answers 200 in every role and every dependency state
	// — it is liveness, not readiness — so anything else came from something that
	// is not this zone's monitor-core, whatever its body happens to contain.
	if status < 200 || status > 299 {
		return Result{
			Reachability: structs.ZoneReachabilityUnreachable,
			Detail:       fmt.Sprintf("%s/health answered %d %s", baseURL, status, http.StatusText(status)),
			ProbedAt:     time.Now().UTC(),
		}
	}

	result := classify(*health, expectedSlug)
	if result.Reachability != structs.ZoneReachabilityHealthy {
		result.ProbedAt = time.Now().UTC()
		return result
	}

	// Identity is settled by this point: the right zone answered. /ready is asked
	// second and can only ever downgrade Healthy to Degraded, never re-open the
	// question of WHICH zone this is — so a zone that is the wrong one is reported
	// as mismatched whether its stores are up or not.
	ready, readyStatus, readyDetail := fetchJSON[readyDocument](ctx, baseURL+"/ready")
	if ready != nil && readyStatus != http.StatusOK && readyStatus != http.StatusServiceUnavailable {
		// 200 and 503 are the only two answers /ready gives. Anything else decoded
		// as JSON by luck rather than by contract, so its `status` field means
		// nothing and must not be read as readiness.
		ready, readyDetail = nil, fmt.Sprintf("%s/ready answered %d %s", baseURL, readyStatus, http.StatusText(readyStatus))
	}
	switch {
	case ready == nil:
		// The right zone, but its readiness could not be read. Reported as
		// degraded rather than healthy on purpose: this fails CLOSED, because
		// "confirmed the identity, could not confirm it is serving" is not a claim
		// that it is serving.
		result.Reachability = structs.ZoneReachabilityDegraded
		result.Detail = fmt.Sprintf("zone %q identified itself, but its readiness could not be read: %s", expectedSlug, readyDetail)
	case ready.Status != "ready":
		result.Reachability = structs.ZoneReachabilityDegraded
		result.Detail = fmt.Sprintf("zone %q is not ready", expectedSlug)
		if len(ready.Failing) > 0 {
			result.Detail += " (failing: " + strings.Join(ready.Failing, ", ") + ")"
		}
	}

	result.ProbedAt = time.Now().UTC()
	return result
}

// classify turns a parsed /health document into a verdict. It is the whole
// reason this package exists, and the order of its checks is load-bearing.
func classify(health healthDocument, expectedSlug string) Result {
	reported := strings.TrimSpace(health.Zone)

	// Answered 200, would not say which zone it is. NOT healthy — an older build
	// that predates the zone identity on /health, a reverse proxy answering on the
	// upstream's behalf, and an unrelated service that happens to return 200 all
	// land here, and treating any of them as healthy is exactly how a registry row
	// ends up pointed at the wrong box with a green tick beside it.
	if reported == "" {
		return Result{
			Reachability: structs.ZoneReachabilityUnverified,
			Detail:       "something answered /health but reported no zone identity — an older build, a proxy, or a service that is not monitor-core",
		}
	}
	if strings.TrimSpace(health.Role) == "" {
		return Result{
			Reachability: structs.ZoneReachabilityUnverified,
			Detail:       fmt.Sprintf("/health reported zone %q but no role — the answer is not a monitor-core health document", reported),
			ReportedZone: reported,
		}
	}

	// ⚠️ THE ROLE IS CHECKED BEFORE THE SLUG, and that ordering is the single
	// most easily lost line in this file.
	//
	// A control plane runs the SAME BINARY and holds the same MON_ZONE_SLUG
	// default, so it reports the very slug a zone row is likely to expect — and it
	// serves no events at all. It is also the most likely wrong URL to be typed:
	// MON_PUBLIC_URL is the control plane, so an operator filling in query_url
	// from the deployment they have in front of them reaches for it first. Compare
	// slugs before roles and that misconfiguration comes back HEALTHY, with every
	// read through it returning nothing and reading as "this zone is quiet".
	if health.Role == ROLE_CONTROL_PLANE {
		return Result{
			Reachability: structs.ZoneReachabilityMismatched,
			Detail: fmt.Sprintf("query_url points at a CONTROL PLANE (role=%q, zone %q), which serves no events — this row would read as an empty zone",
				health.Role, reported),
			ReportedZone: reported,
		}
	}

	// The finding this package is built around: it answered, it is real, and it is
	// somebody else.
	if reported != expectedSlug {
		return Result{
			Reachability: structs.ZoneReachabilityMismatched,
			Detail: fmt.Sprintf("query_url points at zone %q, but this registry row is zone %q — every read through it would return another tenant's data under this name",
				reported, expectedSlug),
			ReportedZone: reported,
		}
	}

	return Result{
		Reachability: structs.ZoneReachabilityHealthy,
		Detail:       fmt.Sprintf("zone %q answered /health as role %q", reported, health.Role),
		ReportedZone: reported,
	}
}

// fetchJSON GETs one endpoint and decodes it, returning (nil, 0, reason) when
// nothing usable came back. The reason is written for an operator, not for a log
// parser.
//
// The STATUS CODE IS RETURNED RATHER THAN JUDGED, because the two endpoints judge
// it differently and only their callers know which: /health answers 200 in every
// state, so anything else is a failure, while /ready answers 503 precisely when
// it has something to say. Folding that decision in here would force one rule
// onto both and lose the readiness signal, or accept a proxy's 502 as a health
// document.
func fetchJSON[T any](ctx context.Context, url string) (*T, int, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, fmt.Sprintf("could not build a request for %s: %v", url, err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		// context.DeadlineExceeded arrives here wrapped in a *url.Error, so the
		// budget is named explicitly rather than left as "context deadline
		// exceeded" — an operator reading that has no way to know it was 3s.
		if ctx.Err() == context.DeadlineExceeded {
			return nil, 0, fmt.Sprintf("%s did not answer within %s", url, ZONE_PROBE_TIMEOUT)
		}
		return nil, 0, fmt.Sprintf("%s could not be reached: %v", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, MAX_PROBE_BODY_BYTES))
	if err != nil {
		return nil, resp.StatusCode, fmt.Sprintf("%s answered %d but the body could not be read: %v", url, resp.StatusCode, err)
	}

	var doc T
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, resp.StatusCode, fmt.Sprintf("%s answered %d but not with a JSON document — this may not be a monitor-core", url, resp.StatusCode)
	}
	return &doc, resp.StatusCode, ""
}
