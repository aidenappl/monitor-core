package issues

import (
	"sync"
	"testing"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/structs"
)

// Regression: production accumulated four distinct issue rows for a single
// fingerprint. The issues table is ReplacingMergeTree(updated_at) ORDER BY (id),
// so rows only collapse when they agree on id — minting uuid.New() per insert
// meant racing workers created permanently separate issues. Deriving the id from
// the fingerprint makes them converge.
func TestIssueIDFor_DerivedFromFingerprint(t *testing.T) {
	fp := generateFingerprint("default", "scraper-service", "scraper.run.failed", "boom", "")

	if a, b := issueIDFor(fp), issueIDFor(fp); a != b {
		t.Fatalf("issueIDFor not deterministic: %q vs %q", a, b)
	}
	if len(issueIDFor(fp)) != 36 {
		t.Errorf("expected a 36-char UUID, got %q", issueIDFor(fp))
	}

	other := generateFingerprint("default", "scraper-service", "scraper.run.failed", "different", "")
	if issueIDFor(fp) == issueIDFor(other) {
		t.Error("distinct fingerprints must not collide onto one issue id")
	}
}

// The whole point of the derived id is that concurrent creators agree without
// coordinating, including across processes where a mutex would not help.
func TestIssueIDFor_ConcurrentCreatorsAgree(t *testing.T) {
	fp := generateFingerprint("default", "svc", "evt", "message", "/p")

	const goroutines = 32
	ids := make([]string, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i] = issueIDFor(fp)
		}(i)
	}
	wg.Wait()

	for i, id := range ids {
		if id != ids[0] {
			t.Fatalf("goroutine %d derived %q, want %q — racing creators would split the issue", i, id, ids[0])
		}
	}
}

// Regression: normalizeMessage collapsed every digit run, so a Bad Gateway and a
// rate-limit merged into one issue whose displayed message flip-flopped to
// whichever occurrence arrived last. The offset is noise and must still collapse;
// the status code is the failure class and must survive.
func TestNormalizeMessage_PreservesStatusCodes(t *testing.T) {
	gateway := normalizeMessage("workday returned status 502 for lowes (offset 7820): upstream returned non-2xx response")
	rateLimited := normalizeMessage("workday returned status 429 for lowes (offset 560): upstream returned non-2xx response")

	if gateway == rateLimited {
		t.Fatalf("502 and 429 must not share a normalization, both gave %q", gateway)
	}
	if want := "workday returned status 502 for lowes (offset <N>): upstream returned non-2xx response"; gateway != want {
		t.Errorf("gateway = %q, want %q", gateway, want)
	}
	if want := "workday returned status 429 for lowes (offset <N>): upstream returned non-2xx response"; rateLimited != want {
		t.Errorf("rateLimited = %q, want %q", rateLimited, want)
	}
}

// Preserving status codes must not regress the original grouping behaviour: the
// same status at different offsets is still one issue.
func TestNormalizeMessage_SameStatusDifferentOffsetStillGroups(t *testing.T) {
	a := normalizeMessage("workday returned status 502 for lowes (offset 7820)")
	b := normalizeMessage("workday returned status 502 for lowes (offset 120)")
	if a != b {
		t.Errorf("same status at different offsets must group: %q vs %q", a, b)
	}
}

func TestNormalizeMessage_StatusCodeVariants(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"bare status", "status 500 received", "status 500 received"},
		{"code introducer", "failed with code 404", "failed with code 404"},
		{"status_code key", "status_code=503 upstream", "status_code=503 upstream"},
		{"http introducer", "HTTP 418 teapot", "HTTP 418 teapot"},
		{"not a status: two digits", "status 42 things", "status <N> things"},
		{"not a status: four digits", "status 4040 things", "status <N> things"},
		{"unrelated number still collapses", "took 502 milliseconds", "took <N> milliseconds"},
		{"retry count still collapses", "failed after 3 retries", "failed after <N> retries"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeMessage(tt.in); got != tt.want {
				t.Errorf("normalizeMessage(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// Regression: GET /issues/{id}/events filtered on service+name and discarded the
// fingerprint, so asking for one tenant's failures returned every other tenant's
// too. Membership must be decided by fingerprint.
func TestFingerprintForEvent_SeparatesTenantsSharingAnEventName(t *testing.T) {
	walmart := &structs.Event{
		Service: "scraper-service",
		Name:    "scraper.run.failed",
		Data:    map[string]any{"error": "workday list returned HTML for walmart (offset 0)"},
	}
	lowes := &structs.Event{
		Service: "scraper-service",
		Name:    "scraper.run.failed",
		Data:    map[string]any{"error": "workday list returned HTML for lowes (offset 0)"},
	}

	if FingerprintForEvent(walmart) == FingerprintForEvent(lowes) {
		t.Error("two tenants sharing service+name must not share a fingerprint")
	}
}

// The same failure at a different offset is the same issue, so its events must
// still be collected together.
func TestFingerprintForEvent_MatchesIssueCreationDerivation(t *testing.T) {
	event := &structs.Event{
		Service: "scraper-service",
		Name:    "scraper.run.failed",
		Data:    map[string]any{"error": "workday returned status 502 for lowes (offset 7820)"},
	}
	later := &structs.Event{
		Service: "scraper-service",
		Name:    "scraper.run.failed",
		Data:    map[string]any{"error": "workday returned status 502 for lowes (offset 40)"},
	}

	path := extractPath(event)
	want := generateFingerprint(event.Project, event.Service, event.Name, extractMessage(event, path), path)

	if got := FingerprintForEvent(event); got != want {
		t.Errorf("FingerprintForEvent diverges from issue-creation derivation: %q vs %q", got, want)
	}
	if FingerprintForEvent(event) != FingerprintForEvent(later) {
		t.Error("same failure at a different offset must stay one issue")
	}
}

// withDefaultProject pins env.DefaultProjectSlug for the duration of a test.
// env.Load() never runs under `go test`, so the var is empty unless a test sets
// it — and the empty-project arm of fingerprintProject keys off it, so asserting
// against the real default without setting it here would be asserting against "".
// Mirrors the helper of the same name in scope/scope_test.go.
func withDefaultProject(t *testing.T, slug string) {
	t.Helper()
	previous := env.DefaultProjectSlug
	env.DefaultProjectSlug = slug
	t.Cleanup(func() { env.DefaultProjectSlug = previous })
}

// Two projects reporting the identical failure must not share a fingerprint.
//
// Before project entered the derivation these inputs produced one string, and
// therefore one issue: one occurrence count summing both tenants' traffic, one
// message, and one status, so resolving it for one project resolved it for the
// other. Nothing surfaced the collision — the row simply described two tenants
// at once.
func TestGenerateFingerprint_ProjectSeparatesIdenticalFailures(t *testing.T) {
	acme := generateFingerprint("acme", "auth-service", "login.failed", "invalid credentials", "/login")
	globex := generateFingerprint("globex", "auth-service", "login.failed", "invalid credentials", "/login")

	if acme == globex {
		t.Fatal("two projects with identical service, name, message and path must not share a fingerprint")
	}

	// The other half of the property: project is the ONLY thing that differs
	// above, so holding it fixed must still group.
	again := generateFingerprint("acme", "auth-service", "login.failed", "invalid credentials", "/login")
	if acme != again {
		t.Errorf("same project and same failure must group: %q vs %q", acme, again)
	}
}

// THE trap this change exists to close, asserted at the level where it actually
// bit: the issue's PRIMARY KEY.
//
// monitor.issues carries both `id CHAR(36) PRIMARY KEY` and
// UNIQUE KEY uq_issues_fingerprint (fingerprint), and id is a pure function of
// the fingerprint. So adding UNIQUE (project, fingerprint) to the table would
// have protected nothing on its own: identical fingerprints across two projects
// mean identical ids, and MariaDB's INSERT ... ON DUPLICATE KEY UPDATE fires on
// the FIRST unique index violated — the primary key — folding project B's
// occurrence into project A's row before the composite key is ever consulted.
//
// Fingerprints differing is necessary but not sufficient to state that; what has
// to be true is that the derived IDS differ, which is what this pins.
func TestIssueIDFor_TwoProjectsDoNotCollideOnThePrimaryKey(t *testing.T) {
	acme := issueIDFor(generateFingerprint("acme", "auth-service", "login.failed", "invalid credentials", "/login"))
	globex := issueIDFor(generateFingerprint("globex", "auth-service", "login.failed", "invalid credentials", "/login"))

	if acme == globex {
		t.Fatalf("both projects derived issue id %q — the upsert would fold one project's occurrences into the other's row", acme)
	}
}

// The event-level derivation must carry the project through too. If it read only
// service/name/message, the ingest stamp (IssueIDForEvent) and the read-side
// membership test (FingerprintForEvent) would both go back to being zone-wide
// while generateFingerprint looked correct in isolation.
func TestFingerprintForEvent_UsesTheEventProject(t *testing.T) {
	withDefaultProject(t, "default")

	acme := &structs.Event{
		Project: "acme",
		Service: "auth-service",
		Name:    "login.failed",
		Level:   "error",
		Data:    map[string]any{"error": "invalid credentials", "path": "/login"},
	}
	globex := &structs.Event{
		Project: "globex",
		Service: "auth-service",
		Name:    "login.failed",
		Level:   "error",
		Data:    map[string]any{"error": "invalid credentials", "path": "/login"},
	}

	if FingerprintForEvent(acme) == FingerprintForEvent(globex) {
		t.Error("two projects' identical events must not share a fingerprint")
	}
	if IssueIDForEvent(acme) == IssueIDForEvent(globex) {
		t.Error("two projects' identical events must not be stamped with the same issue_id")
	}
}

// An event with no project is fingerprinted as the DEFAULT project, not as a
// project literally named "".
//
// Ingest never produces one — the credential always resolves a project — but a
// monitor.events row written before migrations/006_events_project.sql reads back
// as "" (adding a ClickHouse column is metadata-only), and routes/issues.go's
// legacy fallback scan re-fingerprints exactly those rows. Treating "" as a
// tenant of its own would put them in an issue nothing else can ever address;
// scope.ProjectPredicate already resolves the same empty string to the default
// on the read that produced them, and these two must agree.
func TestFingerprintProject_EmptyResolvesToTheDefaultProject(t *testing.T) {
	withDefaultProject(t, "default")

	unstamped := generateFingerprint("", "auth-service", "login.failed", "invalid credentials", "/login")
	explicit := generateFingerprint("default", "auth-service", "login.failed", "invalid credentials", "/login")

	if unstamped != explicit {
		t.Errorf("a pre-tenancy row must fingerprint as the default project: %q vs %q", unstamped, explicit)
	}

	// It is the DEFAULT, not a wildcard: an empty project must not collide with
	// some other tenant's issue.
	other := generateFingerprint("acme", "auth-service", "login.failed", "invalid credentials", "/login")
	if unstamped == other {
		t.Error("an empty project resolved onto a non-default tenant's fingerprint")
	}
}

// issueNamespace is declared immutable in three places (its own comment,
// db/migrations/111_create_issues.sql, and the root AGENTS.md) and it did not
// move when project entered the fingerprint — the re-key went through the
// fingerprint string, which is one axis, not two.
//
// This pins the literal so that a change is a deliberate edit to a failing test
// rather than a one-character diff nobody notices. Changing it re-keys every
// issue a SECOND time and, worse, makes even the legacy rows unreachable by
// derivation, so a stored fingerprint could no longer be checked against its own
// id.
func TestIssueNamespaceIsFrozen(t *testing.T) {
	const want = "6f1d8f6a-2a3e-5c47-9a4b-6d1c0f2e7b31"
	if got := issueNamespace.String(); got != want {
		t.Fatalf("issueNamespace = %q, want %q — changing it orphans every existing issue", got, want)
	}
}
