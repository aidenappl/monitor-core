package scope

import (
	"context"
	"errors"

	"github.com/aidenappl/monitor-core/env"
)

// This package owns ONE thing: the project a single request is allowed to see,
// and the ClickHouse predicate that enforces it.
//
// It is a package of its own rather than a few helpers in middleware because of
// who needs to read it. The value is written by an HTTP middleware, but it is
// consumed by services/ (the event and analytics builders), routes/ (the
// hand-written issue-event lookups) and middleware/ itself — three packages with
// no import path between them. Left in middleware, the query layer would have to
// import an HTTP package to build a WHERE clause; split into a second copy, the
// read path and the write path would each carry their own idea of "which
// project", which is the exact drift structs/columns.go was consolidated to end.
//
// Nothing here interpolates a caller-supplied string into SQL. The project slug
// travels as a bound argument, never as query text, so this whole dimension
// stays outside the injection surface that structs.SafeIdentifierRegex polices.

// contextKey is this package's private key type. Unexported so no other package
// can mint a value that collides with ProjectContextKey, and so the compiler,
// not a convention, is what keeps the read side and the write side on one key.
type contextKey string

// ProjectContextKey is where the authenticating middleware stashes the slug of
// the project the credential is bound to.
const ProjectContextKey contextKey = "mon-project"

// ErrNoProject is returned by ProjectPredicate when the context carries no
// project. It exists as a sentinel so a caller can tell "this request was never
// authenticated" apart from "this query is malformed" — the first is a routing
// mistake in main.go, the second is the caller's input.
var ErrNoProject = errors.New("no project in request context")

// WithProject returns a context carrying the resolved project slug.
//
// This deliberately mirrors WithActor/GetActor in middleware/session.go rather
// than inventing a second way to read an auth result back out of a request.
// Both answer the same shape of question — "what did the authenticating step
// decide about this caller?" — and a codebase with two idioms for that is a
// codebase where the next handler picks the wrong one.
func WithProject(ctx context.Context, project string) context.Context {
	return context.WithValue(ctx, ProjectContextKey, project)
}

// GetProject returns the project slug resolved by the authenticating middleware.
//
// The SLUG travels rather than the numeric project id because that is what both
// consumers actually want: ingest stamps it onto the event's LowCardinality
// column, and every read compares against that same column. Carrying the id
// would mean a lookup per request to turn it back into the thing we already had.
//
// An empty slug reports ok=false. "Present but empty" is not a tenant, and the
// one thing this must never do is hand a read path a value that matches the
// pre-column rows described on ProjectPredicate.
func GetProject(ctx context.Context) (string, bool) {
	project, ok := ctx.Value(ProjectContextKey).(string)
	if !ok || project == "" {
		return "", false
	}
	return project, true
}

// ProjectPredicate returns the SQL fragment and bound arguments that scope a
// read of monitor.events to the project on the context.
//
// It returns an ERROR rather than an empty predicate when the context carries no
// project, and every builder is written to propagate that. This is the whole
// safety property: an unscoped read is not something a caller can reach by
// omitting a parameter, it is something that cannot be constructed. A route
// registered outside QueryAuthMiddleware fails loudly with a 500 on its first
// request instead of quietly serving every project's events.
//
// ---------------------------------------------------------------------------
// THE EMPTY-STRING TRANSITION — dated affordance, added 2026-09-06.
//
// migrations/006_events_project.sql adds `project` to an existing table. Adding
// a column in ClickHouse is metadata-only: existing parts are not rewritten, so
// every row ingested before that migration reads back as the EMPTY STRING (not
// NULL — the column is not Nullable). Those rows are real history, and under a
// strict `project = ?` they all vanish the moment scoping lands, which is the
// dashboard going blank on deploy rather than at some later point anyone chose.
//
// The fill for them is migrations/manual/backfill_events_project.sql, which is
// an ALTER TABLE ... UPDATE — a ClickHouse MUTATION. Mutations are asynchronous
// and cannot go in migrations/, because that runner replays every file on EVERY
// boot and would queue a fresh table-wide rewrite per restart forever. So it is
// run BY HAND, and there is therefore a window — bounded only by when an
// operator gets to it — in which pre-column rows still exist.
//
// So the default project, and ONLY the default project, also matches the empty
// string. That is sound rather than lax: every pre-column row was written before
// any project existed, and the same reasoning that makes env.DefaultProjectSlug
// the right stamp for the env master key (Mimir's "anonymous", Loki's "fake" —
// the tenant dimension is never left null) makes it the right reading of a row
// that predates the dimension. A NON-default project matches exactly, so this
// can never widen a real tenant's view: the untagged rows are visible to the
// default project alone, which is where the backfill is about to put them.
//
// REMOVE THIS ARM when BOTH are true:
//  1. backfill_events_project.sql has been run to completion (system.mutations
//     reports is_done = 1 for it), and
//  2. 30 days have passed since 006 was deployed — 2026-10-06 at the earliest —
//     so the events TTL has recycled every part that could still hold an
//     unstamped row.
//
// After that the empty arm can only ever match rows that a BUG wrote with no
// project, and silently folding those into the default project is precisely the
// wrong response to that bug.
// ---------------------------------------------------------------------------
func ProjectPredicate(ctx context.Context) (string, []interface{}, error) {
	project, ok := GetProject(ctx)
	if !ok {
		return "", nil, ErrNoProject
	}

	if project == env.DefaultProjectSlug {
		return "(project = ? OR project = '')", []interface{}{project}, nil
	}

	return "project = ?", []interface{}{project}, nil
}

// Matches reports whether an event stamped eventProject is visible to a reader
// scoped to project. It is the in-memory twin of ProjectPredicate: the SSE hub
// decides membership per event in Go rather than in SQL, but it must decide it
// the SAME way.
//
// The two live in one function precisely because they would otherwise be two
// implementations of one rule in two packages — and the shape of that drift is
// nasty. The stored history would exclude a row that the live tail showed a
// second earlier, or worse, agree for months and diverge only during the
// empty-string transition window, when the arm below is what keeps pre-006
// events flowing to the default project's stream.
//
// The empty-string arm here is removed at the same moment as the one in
// ProjectPredicate, under the same two conditions stated there.
func Matches(project, eventProject string) bool {
	// An empty reader project is NOT a tenant, and must never be allowed to
	// match the empty stamp on a pre-006 row by simple string equality. That
	// pairing is the one combination where both sides are "unknown" and the
	// naive comparison reads them as "the same" — a subscription that resolved
	// no project would then receive precisely the untagged rows. GetProject
	// rejects the empty string for the same reason; this is the second lock on
	// the same door, because this function is also reachable from callers that
	// never went through GetProject.
	if project == "" {
		return false
	}
	if eventProject == project {
		return true
	}
	return eventProject == "" && project == env.DefaultProjectSlug
}
