package structs

import "regexp"

// This file is the SINGLE SOURCE OF TRUTH for what a caller is allowed to name
// in a query: the identifier pattern for free-form data keys, and the column
// allowlists for each place a column name can appear.
//
// It exists because these definitions used to be copy-pasted into
// services/query.go, services/analytics.go and alerts/evaluator.go, and the
// copies drifted. The two identifier regexes ended up disagreeing about dots,
// so the same nested data key was accepted by an alert rule and rejected by an
// analytics query. Extend these HERE and nowhere else — the moment a new event
// column (a "project" column is coming) has to be added to four copies, someone
// updates three and the fourth silently rejects it with no error anyone can
// trace back to a missing map entry.

// SafeIdentifierRegex validates a caller-supplied data key before it is
// interpolated into SQL text. ClickHouse gives us no placeholder to bind inside
// JSONExtractString(data, '<key>'), so the key goes in as a string literal and
// the regex is the only thing standing between a caller and the query text.
//
// Dots are permitted deliberately. Alert rules legitimately reference nested
// data keys such as "user.id", and the stricter no-dot variant this replaces
// would reject every existing rule that does. Allowing dots is not the weaker
// guard it looks like: neither variant admits a quote or a backslash, so
// neither is injectable. The divergence was a correctness bug, not a security
// hole — and the permissive side is the correct one.
var SafeIdentifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

// The sets below stay separate even where their memberships currently coincide.
// "Which columns may be filtered on", "which may be grouped by", "which an
// alert rule may reference" and "which are exposed as labels" are four
// different questions about the same schema; folding them into one set would
// mean that answering one of them differently in future silently re-answers the
// other three.

// A note on "project", which appears in all four sets below.
//
// Membership here grants the ability to NAME the column, never the ability to
// widen what a request can see. Every read of monitor.events is scoped by
// scope.ProjectPredicate, injected at each builder's chokepoint and ANDed with
// whatever the caller asked for — so `?project=someone-else` resolves to
// `project = 'mine' AND project = 'someone-else'` and returns nothing. The
// allowlist is what makes the dimension filterable, groupable and browsable
// within a project; the predicate is what makes it a boundary.

// QueryableColumns are the event columns a raw event query may filter on.
var QueryableColumns = map[string]bool{
	"service":    true,
	"project":    true,
	"env":        true,
	"job_id":     true,
	"request_id": true,
	"trace_id":   true,
	"user_id":    true,
	"name":       true,
	"level":      true,
}

// GroupByColumns are the event columns an analytics query may GROUP BY, and by
// extension the non-data fields it may aggregate over.
var GroupByColumns = map[string]bool{
	"service":    true,
	"project":    true,
	"env":        true,
	"job_id":     true,
	"request_id": true,
	"trace_id":   true,
	"user_id":    true,
	"name":       true,
	"level":      true,
}

// FilterColumns are the event columns an alert rule's filter conditions may
// reference.
//
// "project" is a partial exception to the note above, and the only one. A rule
// is evaluated from two places and they are scoped differently: the HTTP test
// endpoint passes a request context, so scope.ProjectPredicate DOES run and the
// note above holds in full there; the 15-second timer has no request, so nothing
// ANDs a boundary onto its query and TIMER evaluation remains zone-wide.
// Membership here is what lets such a rule OPT IN to one project; it does not
// confine a rule that does not. See the KNOWN GAP header on alerts/evaluator.go
// for the full statement, including why the two callers deliberately disagree.
var FilterColumns = map[string]bool{
	"service":    true,
	"project":    true,
	"env":        true,
	"job_id":     true,
	"request_id": true,
	"trace_id":   true,
	"user_id":    true,
	"name":       true,
	"level":      true,
}

// LabelColumns maps a label name exposed by the label-values endpoint to the
// event column it reads. It is deliberately narrower than QueryableColumns —
// the high-cardinality identifier columns (job_id, request_id, trace_id) are
// useless as a browsable value list. The map is an indirection rather than a
// set so a public label can be renamed without renaming the column underneath.
var LabelColumns = map[string]string{
	"service": "service",
	"project": "project",
	"env":     "env",
	"user_id": "user_id",
	"name":    "name",
	"level":   "level",
}
