package scope

import (
	"context"
	"errors"
	"testing"

	"github.com/aidenappl/monitor-core/env"
)

// withDefaultProject pins env.DefaultProjectSlug for the duration of a test.
// env.Load() never runs under `go test`, so the var is empty unless a test sets
// it — and the empty-string transition arm keys off it, so asserting against the
// real default here would be asserting against "".
func withDefaultProject(t *testing.T, slug string) {
	t.Helper()
	previous := env.DefaultProjectSlug
	env.DefaultProjectSlug = slug
	t.Cleanup(func() { env.DefaultProjectSlug = previous })
}

// TestProjectPredicateRefusesUnscopedContext is the test that matters most in
// this file. Every read builder propagates this error, so it is the mechanism
// that turns "a route was registered outside QueryAuthMiddleware" into a 500 on
// the first request instead of a query that quietly returns every project.
func TestProjectPredicateRefusesUnscopedContext(t *testing.T) {
	withDefaultProject(t, "default")

	tests := []struct {
		name string
		ctx  context.Context
	}{
		{"no value at all", context.Background()},
		{"empty slug is not a tenant", WithProject(context.Background(), "")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args, err := ProjectPredicate(tt.ctx)
			if !errors.Is(err, ErrNoProject) {
				t.Fatalf("err = %v, want ErrNoProject", err)
			}
			if sql != "" || args != nil {
				t.Errorf("returned a usable predicate (%q, %v) alongside the error; a caller that ignores err must not get a working query", sql, args)
			}
		})
	}
}

// TestProjectPredicateScopesExactlyForNonDefault pins that the empty-string
// affordance is confined to the default project. Widening it to every project
// would make every tenant able to see the untagged pre-migration rows, which is
// the leak this whole change exists to prevent.
func TestProjectPredicateScopesExactlyForNonDefault(t *testing.T) {
	withDefaultProject(t, "default")

	sql, args, err := ProjectPredicate(WithProject(context.Background(), "atlas"))
	if err != nil {
		t.Fatalf("ProjectPredicate: %v", err)
	}
	if sql != "project = ?" {
		t.Errorf("sql = %q, want an exact match with no empty-string arm", sql)
	}
	if len(args) != 1 || args[0] != "atlas" {
		t.Errorf("args = %v, want [atlas]", args)
	}
}

// TestProjectPredicateAdmitsUntaggedRowsForDefault covers the transition window
// documented on ProjectPredicate. Without this arm the dashboard goes blank on
// the deploy that lands scoping, because every row written before migration 006
// reads back as the empty string until the manual backfill has been run.
func TestProjectPredicateAdmitsUntaggedRowsForDefault(t *testing.T) {
	withDefaultProject(t, "default")

	sql, args, err := ProjectPredicate(WithProject(context.Background(), "default"))
	if err != nil {
		t.Fatalf("ProjectPredicate: %v", err)
	}
	if sql != "(project = ? OR project = '')" {
		t.Errorf("sql = %q, want the default project to also admit unstamped rows", sql)
	}
	if len(args) != 1 || args[0] != "default" {
		t.Errorf("args = %v, want [default]", args)
	}
}

// TestProjectPredicateFollowsAConfiguredDefault pins that the transition arm
// keys off env.DefaultProjectSlug rather than the literal "default". A
// deployment that set MON_DEFAULT_PROJECT to something else would otherwise get
// the strict predicate for its own default project and lose all its history.
func TestProjectPredicateFollowsAConfiguredDefault(t *testing.T) {
	withDefaultProject(t, "trailblaze")

	sql, _, err := ProjectPredicate(WithProject(context.Background(), "trailblaze"))
	if err != nil {
		t.Fatalf("ProjectPredicate: %v", err)
	}
	if sql != "(project = ? OR project = '')" {
		t.Errorf("sql = %q, want the configured default to carry the transition arm", sql)
	}

	sql, _, err = ProjectPredicate(WithProject(context.Background(), "default"))
	if err != nil {
		t.Fatalf("ProjectPredicate: %v", err)
	}
	if sql != "project = ?" {
		t.Errorf("sql = %q, want the literal \"default\" to be ordinary once it is not the configured default", sql)
	}
}

// TestMatchesAgreesWithProjectPredicate pins the in-memory twin against the SQL
// one. These two decide the same question in different languages — the live tail
// in Go, the stored query in ClickHouse — and a subscriber seeing an event that
// the events table would not return for them is precisely the drift this shares
// one function to avoid.
func TestMatchesAgreesWithProjectPredicate(t *testing.T) {
	withDefaultProject(t, "default")

	tests := []struct {
		name         string
		project      string
		eventProject string
		want         bool
	}{
		{"same project", "atlas", "atlas", true},
		{"different project", "atlas", "johnnies", false},
		{"non-default must not see untagged rows", "atlas", "", false},
		{"default sees its own", "default", "default", true},
		{"default sees untagged pre-006 rows", "default", "", true},
		{"default does not see another project", "default", "atlas", false},
		{"unresolved reader sees nothing", "", "atlas", false},
		{"unresolved reader does not match an untagged row", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Matches(tt.project, tt.eventProject); got != tt.want {
				t.Errorf("Matches(%q, %q) = %v, want %v", tt.project, tt.eventProject, got, tt.want)
			}
		})
	}
}

// TestGetProjectRoundTrip pins the accessor contract the middleware relies on.
func TestGetProjectRoundTrip(t *testing.T) {
	if project, ok := GetProject(WithProject(context.Background(), "atlas")); !ok || project != "atlas" {
		t.Errorf("GetProject = (%q, %v), want (atlas, true)", project, ok)
	}
	if _, ok := GetProject(context.Background()); ok {
		t.Error("GetProject on a bare context reported ok; an unauthenticated request must never look scoped")
	}
	if _, ok := GetProject(context.WithValue(context.Background(), ProjectContextKey, 42)); ok {
		t.Error("GetProject accepted a non-string value; a type confusion here would produce an empty scope")
	}
}
