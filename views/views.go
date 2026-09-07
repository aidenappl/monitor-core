// Package views is empty, and deliberately so — read this before adding
// anything to it.
//
// Saved query/filter views moved from a ClickHouse MergeTree to MariaDB in
// migration 124. Everything this package used to hold went with the table:
//
//	views.View          → structs.SavedView    (structs/SavedView.struct.go)
//	views.Init          → migration 124        (db/migrations/124_create_saved_views.sql)
//	Create/List/Delete  → query.*SavedView*    (query/saved_views.query.go)
//
// The handlers in routes/views.go now call the query layer directly, which is the
// house shape — a package that did nothing but forward a handler to one SQL
// statement is the service layer the repo's rules say not to build. views.Init is
// no longer called from main.go; the table is created by the migration runner, on
// every plane, before anything serves.
//
// The type gained a name in the move: structs.SavedView, not structs.View. A bare
// `View` next to `Event` in that package reads like a projection of an event,
// which is not what it is.
//
// THE FILE IS KEPT RATHER THAN DELETED so that a reader arriving from an old
// import, a stale grep or a linked line number lands on this explanation instead
// of on nothing. It can be removed once a release has passed, together with the
// legacy ClickHouse table cutover/config.go reads — the same "drop it once a
// release has passed" note that applies there.
package views
