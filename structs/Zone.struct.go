package structs

import "time"

// ZoneStatus is a zone's lifecycle axis.
//
// There is no 'deleted' verb here because there is no delete. A zone row is
// retired by moving to ZoneStatusDeleted and is then kept forever, so the
// UNIQUE key on slug keeps the name permanently spent — see
// db/migrations/116_create_registry.sql for why reuse is the failure to design
// against rather than the convenience to allow.
type ZoneStatus string

const (
	ZoneStatusActive  ZoneStatus = "active"
	ZoneStatusDeleted ZoneStatus = "deleted"
)

// IsValid reports whether the status is one of the known values.
func (s ZoneStatus) IsValid() bool {
	switch s {
	case ZoneStatusActive, ZoneStatusDeleted:
		return true
	}
	return false
}

// ZoneReachability is what the last probe of a zone's query URL found.
//
// It is a SEPARATE AXIS FROM ZoneStatus and must stay one. Status is what the
// operator intends (this tenant is live / this tenant is retired); reachability
// is what is actually true of the box right now. Folding them into one column
// would mean an unreachable zone becoming indistinguishable from a retired one,
// and the whole point of the value is that a zone which cannot be reached STAYS
// LISTED and STAYS ACTIVE while being visibly broken. A zone that vanishes from
// a list because it stopped answering is the failure this type exists to
// prevent — on the errors page, "no issues" is the most misleading possible
// reading of a 500.
//
// Seven values rather than a boolean, because "did something answer" and "was it
// the zone I expected" are different questions. The ordering below is
// worst-to-best and matches the ENUM's declaration order in
// db/migrations/125_zone_endpoints.sql, which is what makes an ORDER BY over the
// column sort by severity.
type ZoneReachability string

const (
	// ZoneReachabilityUnknown — never probed. Carries no claim at all, and is the
	// value every row created before the probe existed still holds.
	ZoneReachabilityUnknown ZoneReachability = "unknown"

	// ZoneReachabilityUnconfigured — no query URL recorded, so there was nothing
	// to probe. Deliberately not folded into Unreachable: nobody failed to reach
	// anything, and the fix is a registry row to finish filling in rather than a
	// box to go and look at.
	ZoneReachabilityUnconfigured ZoneReachability = "unconfigured"

	// ZoneReachabilityUnreachable — the probe ran and got no usable answer:
	// connection refused, TLS failure, timeout, a redirect it refused to follow,
	// or a target the SSRF guard declined to fetch.
	ZoneReachabilityUnreachable ZoneReachability = "unreachable"

	// ZoneReachabilityUnverified — something answered 200 but would not say which
	// zone it is.
	//
	// ⚠️ THIS IS THE VALUE THAT EARNS THE WHOLE TYPE. A 200 from /health proves a
	// monitor-core is listening; it does not prove it is THIS zone's
	// monitor-core. An older build that predates the zone identity on /health, a
	// reverse proxy answering on the upstream's behalf, or a completely unrelated
	// service that happens to return 200 all land here. Treating any of them as
	// healthy is precisely how a registry row ends up pointed at the wrong box
	// with a green tick beside it.
	ZoneReachabilityUnverified ZoneReachability = "unverified"

	// ZoneReachabilityMismatched — it answered and identified itself as a
	// DIFFERENT zone, or as a control plane (which serves no events at all). The
	// registry row points somewhere real and wrong, which is the worst of the
	// reachable outcomes: every read through it returns another tenant's data
	// under this zone's name, with nothing syntactically invalid anywhere.
	ZoneReachabilityMismatched ZoneReachability = "mismatched"

	// ZoneReachabilityDegraded — the right zone, but its own /ready says it is not
	// serving (a dead store, or alerting switched off).
	ZoneReachabilityDegraded ZoneReachability = "degraded"

	// ZoneReachabilityHealthy — the right zone, and ready.
	ZoneReachabilityHealthy ZoneReachability = "healthy"
)

// IsValid reports whether the reachability is one of the known values.
func (r ZoneReachability) IsValid() bool {
	switch r {
	case ZoneReachabilityUnknown, ZoneReachabilityUnconfigured, ZoneReachabilityUnreachable,
		ZoneReachabilityUnverified, ZoneReachabilityMismatched, ZoneReachabilityDegraded,
		ZoneReachabilityHealthy:
		return true
	}
	return false
}

// IsUsable reports whether a zone in this state can be read through with any
// confidence that the answer belongs to this zone.
//
// Only Healthy qualifies, and Degraded deliberately does not: a zone whose own
// /ready is failing may be serving from a store that is behind, empty or down,
// and "the right box, answering wrongly" is not a better outcome than "no
// answer" — it is the same wrong chart with a 200 in front of it.
//
// Nothing in monitor-core gates a read on this today (Phase 1 is single-zone and
// reads never leave the process). It exists so that when fan-out lands, the
// predicate is written once, here, next to the values it interprets, rather than
// as an `== "healthy"` comparison copied into every call site.
func (r ZoneReachability) IsUsable() bool {
	return r == ZoneReachabilityHealthy
}

// Zone is one whole ClickHouse instance. Lives in MariaDB (zones), in
// monitor_auth rather than the `monitor` schema — the registry sits next to
// api_keys, which is what binds an event to a project.
//
// A zone is an INSTANCE, not a database inside one: the ClickHouse database is
// always the one named by CLICKHOUSE_DATABASE, for every zone. Slug therefore
// never reaches ClickHouse SQL as an identifier, and no zone-derived string is
// ever interpolated into a query — which is what keeps the whole dimension out
// of the injection surface that structs/columns.go exists to police.
//
// Slug is immutable. DisplayName is the mutable, non-unique label, and exists
// precisely so that renaming a team never creates pressure to rename the
// identifier underneath it.
//
// ⚠️ THE ROW RECORDS INFRASTRUCTURE; IT DOES NOT CREATE ANY. IngestURL and
// QueryURL name a stack, a ClickHouse, a MariaDB, a DNS record and a certificate
// that were all provisioned by hand. Nothing here reconciles the row against
// reality, so a row whose URLs point at another zone is syntactically perfect
// and semantically catastrophic — see the header of
// db/migrations/125_zone_endpoints.sql, and the Reachability field below, which
// is the only thing that can tell the two apart.
//
// ZoneStatus and ProjectStatus are deliberately separate types with currently
// identical memberships, for the reason columns.go gives about its four column
// sets: "is this zone live" and "is this project live" are two questions about
// one schema, and folding them into one type would mean that answering one of
// them differently in future silently re-answers the other.
type Zone struct {
	ID          int64      `json:"id"`
	Slug        string     `json:"slug"`
	DisplayName string     `json:"display_name"`
	Status      ZoneStatus `json:"status"`

	// IngestURL is the PUBLIC origin SDKs POST events to. Client-facing, so its
	// value ends up copied into other repos' configuration and is effectively
	// permanent.
	//
	// QueryURL is the CONTROL-PLANE hop this server reads a zone through, and the
	// target the reachability probe hits. Server-to-server, so it may be a
	// different hostname on a private DNS zone or behind a different proxy.
	//
	// Both are the empty string when unrecorded, which is a real state and means
	// exactly that — see the migration header for why the DDL cannot require them
	// and the API can.
	IngestURL string `json:"ingest_url"`
	QueryURL  string `json:"query_url"`

	// The last probe's verdict, kept together because they are only meaningful
	// together.
	//
	// LastProbeAt is nil when the zone has never been probed, and Reachability is
	// ZoneReachabilityUnknown in exactly that case — the two cannot disagree. Any
	// caller rendering Reachability without LastProbeAt is asserting a freshness
	// this row does not have: the value is a record of the last look, not a live
	// status, and nothing re-probes on a timer.
	//
	// ReportedZone is what the box at the other end SAID it was, kept verbatim
	// beside the slug this registry expected. It is untrusted remote text, not a
	// registry identifier, and it is the field that makes "reached something" and
	// "reached the right something" impossible to confuse.
	Reachability       ZoneReachability `json:"reachability"`
	ReachabilityDetail string           `json:"reachability_detail"`
	ReportedZone       string           `json:"reported_zone"`
	LastProbeAt        *time.Time       `json:"last_probe_at"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
