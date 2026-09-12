package structs

// The tenancy register: which dimension each table is partitioned by.
//
// WHY THIS EXISTS. Seven monitor.* tables shipped with no tenancy column at all
// — alert_rules, notification_channels, notification_policies, service_groups,
// dashboards, saved_views and service_repos — and the way that was eventually
// noticed was `grep "scope\."` returning zero hits across every one of their
// query files. Nothing declared what they SHOULD have been, so nothing could
// notice they were not.
//
// A table missing from this map fails query/tenancy_test.go. That is the point:
// adding a table becomes a DECISION about its tenancy rather than an omission
// that reads as one only in hindsight.
type Tenancy int

const (
	// TenancyProject — rows belong to exactly one project and must carry a
	// `project` column that every read, update and delete predicates on.
	TenancyProject Tenancy = iota

	// TenancyDerived — no column, scoped transitively through a project-bearing
	// parent whose key is globally unique. monitor.issue_timeline and
	// issue_links are keyed by issue id, and every handler resolves the issue
	// with a project-scoped lookup first. This is a real classification, not an
	// exemption: it says the scoping happens one hop away, and names where.
	TenancyDerived

	// TenancyZone — one row per zone, or rows belonging to a zone rather than to
	// a project. A zone's own database holds only its own.
	TenancyZone

	// TenancyInstall — install-wide, with no tenant dimension at all: identity,
	// sessions, settings and the migration ledger.
	TenancyInstall
)

func (t Tenancy) String() string {
	switch t {
	case TenancyProject:
		return "project"
	case TenancyDerived:
		return "derived"
	case TenancyZone:
		return "zone"
	case TenancyInstall:
		return "install"
	}
	return "unknown"
}

// TableTenancy classifies every MariaDB table this service owns.
//
// Keys are exactly the strings the query layer uses in its FROM clauses, so the
// test can walk the table-name constants in query/*.go and match them here. The
// `monitor.` prefix is part of the name for the issue/config schema and absent
// for monitor_auth, mirroring db/migrations and the query constants.
//
// ⚠️ ClickHouse is deliberately NOT in this map. Its tenancy is enforced by
// scope.ProjectPredicate at query-build time rather than by a column list, and
// mixing the two engines here would suggest a single mechanism covers both.
var TableTenancy = map[string]Tenancy{
	// ---- monitor_auth: identity and the registry ----
	"users":              TenancyInstall,
	"identities":         TenancyInstall,
	"refresh_tokens":     TenancyInstall,
	"sso_providers":      TenancyInstall,
	"sso_sessions":       TenancyInstall,
	"settings":           TenancyInstall,
	"migrations_applied": TenancyInstall,

	// The fleet map. `zones` is install-wide because the control plane's copy
	// describes every zone; `projects` belongs to a zone by zone_id.
	"zones":    TenancyInstall,
	"projects": TenancyZone,

	// api_keys reaches its project through project_id (117) rather than a slug,
	// and its queries join projects → zones to scope it — a project slug is
	// unique only per zone, so the slug alone names a set.
	"api_keys": TenancyProject,

	// ---- monitor: issues ----
	"monitor.issues":         TenancyProject,
	"monitor.issue_timeline": TenancyDerived,
	"monitor.issue_links":    TenancyDerived,

	// ---- monitor: configuration (the seven, 127-133) ----
	"monitor.service_repos":         TenancyProject,
	"monitor.alert_rules":           TenancyProject,
	"monitor.notification_channels": TenancyProject,
	"monitor.notification_policies": TenancyProject,
	"monitor.service_groups":        TenancyProject,
	"monitor.dashboards":            TenancyProject,
	"monitor.saved_views":           TenancyProject,
}
