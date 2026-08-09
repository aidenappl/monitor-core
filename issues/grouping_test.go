package issues

import (
	"sync"
	"testing"

	"github.com/aidenappl/monitor-core/structs"
)

// Regression: production accumulated four distinct issue rows for a single
// fingerprint. The issues table is ReplacingMergeTree(updated_at) ORDER BY (id),
// so rows only collapse when they agree on id — minting uuid.New() per insert
// meant racing workers created permanently separate issues. Deriving the id from
// the fingerprint makes them converge.
func TestIssueIDFor_DerivedFromFingerprint(t *testing.T) {
	fp := generateFingerprint("scraper-service", "scraper.run.failed", "boom", "")

	if a, b := issueIDFor(fp), issueIDFor(fp); a != b {
		t.Fatalf("issueIDFor not deterministic: %q vs %q", a, b)
	}
	if len(issueIDFor(fp)) != 36 {
		t.Errorf("expected a 36-char UUID, got %q", issueIDFor(fp))
	}

	other := generateFingerprint("scraper-service", "scraper.run.failed", "different", "")
	if issueIDFor(fp) == issueIDFor(other) {
		t.Error("distinct fingerprints must not collide onto one issue id")
	}
}

// The whole point of the derived id is that concurrent creators agree without
// coordinating, including across processes where a mutex would not help.
func TestIssueIDFor_ConcurrentCreatorsAgree(t *testing.T) {
	fp := generateFingerprint("svc", "evt", "message", "/p")

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
	want := generateFingerprint(event.Service, event.Name, extractMessage(event, path), path)

	if got := FingerprintForEvent(event); got != want {
		t.Errorf("FingerprintForEvent diverges from issue-creation derivation: %q vs %q", got, want)
	}
	if FingerprintForEvent(event) != FingerprintForEvent(later) {
		t.Error("same failure at a different offset must stay one issue")
	}
}
