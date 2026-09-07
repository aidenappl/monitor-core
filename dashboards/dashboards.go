// Package dashboards is empty, and deliberately so — read this before adding
// anything to it.
//
// Saved dashboard layouts moved from a ClickHouse ReplacingMergeTree to MariaDB
// in migration 123. Everything this package used to hold went with the table:
//
//	dashboards.Dashboard  → structs.Dashboard      (structs/Dashboard.struct.go)
//	dashboards.Init       → migration 123          (db/migrations/123_create_dashboards.sql)
//	Create/List/Get/
//	Update/Delete         → query.*Dashboard*      (query/dashboards.query.go)
//
// The handlers in routes/dashboards.go now call the query layer directly, which
// is the house shape — a package that did nothing but forward a handler to one
// SQL statement is the service layer the repo's rules say not to build.
// dashboards.Init is no longer called from main.go; the table is created by the
// migration runner, on every plane, before anything serves.
//
// THE FILE IS KEPT RATHER THAN DELETED so that a reader arriving from an old
// import, a stale grep or a linked line number lands on this explanation instead
// of on nothing. It can be removed once a release has passed, together with the
// legacy ClickHouse table cutover/config.go reads — the same "drop it once a
// release has passed" note that applies there.
//
// Do not reintroduce CRUD here. If a dashboard ever needs behaviour that is more
// than one query — a per-project scope, a config schema, an export — that is a
// reason to give this package a purpose again, and a reason to say so at the top
// of it. Forwarding is not.
package dashboards
