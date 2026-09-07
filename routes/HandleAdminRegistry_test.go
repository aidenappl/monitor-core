package routes

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/db"
	"github.com/gorilla/mux"
)

// adminZoneRows mirrors query.zoneColumns positionally. queryURL is a parameter
// because the probe test needs a target that is refused before any packet leaves
// the process — a test that made a real outbound request would be a test that
// fails on an aeroplane.
func adminZoneRows(queryURL string) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{
		"id", "slug", "display_name", "status",
		"ingest_url", "query_url",
		"reachability", "reachability_detail", "reported_zone", "last_probe_at",
		"created_at", "updated_at",
	}).AddRow(int64(1), "trailblaze", "Trailblaze", "active",
		"https://events.example.com", queryURL,
		"unknown", "", "", nil, now, now)
}

func adminProjectRows() *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{"id", "zone_id", "slug", "display_name", "status", "created_at", "updated_at"}).
		AddRow(int64(1), int64(1), "default", "Default", "active", now, now)
}

// withMockDB points db.SQL at a mock for one test and restores it afterwards.
// db.SQL is a process-wide global, so without the restore a later test would
// inherit a closed connection.
func withMockDB(t *testing.T) sqlmock.Sqlmock {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	original := db.SQL
	db.SQL = mockDB
	t.Cleanup(func() {
		db.SQL = original
		mockDB.Close()
	})
	return mock
}

// adminRequest builds a request with the {id} path variable already resolved,
// since these handlers are called directly rather than through the router.
func adminRequest(t *testing.T, method, path, body, id string) (*httptest.ResponseRecorder, *http.Request) {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if id != "" {
		r = mux.SetURLVars(r, map[string]string{"id": id})
	}
	return httptest.NewRecorder(), r
}

// TestUpdateZoneRefusesASlugChange — INVARIANT: SLUGS ARE IMMUTABLE, AT THE API.
//
// query.UpdateZoneRequest has no Slug field, so the write is already impossible;
// what this pins is that the caller is TOLD. A handler that silently dropped the
// key would answer 200 to "rename this zone", having renamed nothing, and the
// operator would go on using a name that does not exist while every event kept
// filing under the old one — a mistake that is invisible on this side and
// permanent on the other, because a slug is never reusable either.
//
// No statements are registered on the mock, so touching the database at all
// fails the test: the refusal must happen before any work is done.
func TestUpdateZoneRefusesASlugChange(t *testing.T) {
	mock := withMockDB(t)

	w, r := adminRequest(t, http.MethodPut, "/admin/zones/1", `{"slug":"renamed","display_name":"Renamed"}`, "1")
	HandleUpdateZone(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 — a slug change must be refused, not ignored", w.Code)
	}
	if !strings.Contains(strings.ToLower(w.Body.String()), "immutable") {
		t.Errorf("body = %q, want it to say the slug is immutable", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestUpdateZoneRefusesAStatusChange. Retirement has a guard — query.RetireZone
// refuses while the zone still owns active projects — and a status edit accepted
// here would be a second way to retire that skips it entirely. A guard with a
// bypass beside it is not a guard.
func TestUpdateZoneRefusesAStatusChange(t *testing.T) {
	withMockDB(t)

	w, r := adminRequest(t, http.MethodPut, "/admin/zones/1", `{"status":"deleted"}`, "1")
	HandleUpdateZone(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "retire") {
		t.Errorf("body = %q, want it to name the route that does retire a zone", w.Body.String())
	}
}

// TestUpdateZoneAcceptsADisplayNameChange IS THE NEGATIVE CONTROL for the two
// refusals above. Without it, a HandleUpdateZone that rejected every request —
// or one that never reached the database at all — would pass them both and fail
// nothing.
func TestUpdateZoneAcceptsADisplayNameChange(t *testing.T) {
	mock := withMockDB(t)

	// Resolved first (404 if absent), then updated, then re-read.
	mock.ExpectQuery("FROM zones WHERE id = .").WithArgs(int64(1)).WillReturnRows(adminZoneRows("https://zone.example.com"))
	mock.ExpectExec("UPDATE zones SET display_name = .").
		WithArgs("Renamed", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("FROM zones WHERE id = .").WithArgs(int64(1)).WillReturnRows(adminZoneRows("https://zone.example.com"))

	w, r := adminRequest(t, http.MethodPut, "/admin/zones/1", `{"display_name":"Renamed"}`, "1")
	HandleUpdateZone(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestUpdateZoneOnAMissingRowIs404 — the row is resolved before the UPDATE, so a
// zone that is not there says so, instead of an UPDATE matching nothing and a
// `"data": null` that reads as success.
func TestUpdateZoneOnAMissingRowIs404(t *testing.T) {
	mock := withMockDB(t)
	mock.ExpectQuery("FROM zones WHERE id = .").WithArgs(int64(9)).WillReturnRows(emptyZoneRows())

	w, r := adminRequest(t, http.MethodPut, "/admin/zones/9", `{"display_name":"X"}`, "9")
	HandleUpdateZone(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body: %s)", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRegistryPathIDRefusesANonNumericID. /admin/zones/undefined is a frontend
// bug stringifying a missing value into a URL; answering 404 would send whoever
// is debugging it looking for a row that was never named.
func TestRegistryPathIDRefusesANonNumericID(t *testing.T) {
	withMockDB(t)

	w, r := adminRequest(t, http.MethodPut, "/admin/zones/undefined", `{"display_name":"X"}`, "undefined")
	HandleUpdateZone(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a malformed id", w.Code)
	}
}

// TestCreateZoneRefusesAReservedSlug. The reserved list guards the FRONTEND's
// static route table: a zone or project called `settings` is shadowed by the
// app's own /settings page and is permanently unreachable, with the row present,
// the API serving it, and the only symptom a link that lands on the wrong screen.
// Creation is the only moment the name is still negotiable.
func TestCreateZoneRefusesAReservedSlug(t *testing.T) {
	mock := withMockDB(t)

	body := `{"slug":"settings","display_name":"Settings","ingest_url":"https://a.example.com","query_url":"https://b.example.com"}`
	w, r := adminRequest(t, http.MethodPost, "/admin/zones", body, "")
	HandleCreateZone(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "reserved") {
		t.Errorf("body = %q, want it to say the slug is reserved", w.Body.String())
	}
	// Nothing was registered on the mock, so a statement here would fail: the
	// refusal must land before the INSERT.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestCreateZoneRefusesABadEndpoint. query_url is an outbound fetch target
// supplied by an administrator — textbook SSRF input — so a plaintext or internal
// URL is refused with a 400 naming the field rather than stored and probed.
func TestCreateZoneRefusesABadEndpoint(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "plaintext ingest_url",
			body: `{"slug":"payments","display_name":"P","ingest_url":"http://a.example.com","query_url":"https://b.example.com"}`,
			want: "ingest_url",
		},
		{
			name: "internal query_url",
			body: `{"slug":"payments","display_name":"P","ingest_url":"https://a.example.com","query_url":"https://localhost"}`,
			want: "query_url",
		},
		{
			name: "credentials in the URL",
			body: `{"slug":"payments","display_name":"P","ingest_url":"https://a.example.com","query_url":"https://u:p@b.example.com"}`,
			want: "query_url",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withMockDB(t)

			w, r := adminRequest(t, http.MethodPost, "/admin/zones", tt.body, "")
			HandleCreateZone(w, r)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body: %s)", w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), tt.want) {
				t.Errorf("body = %q, want it to name %s", w.Body.String(), tt.want)
			}
		})
	}
}

// TestRetireZoneWithActiveProjectsIsAConflict — 409, not 500 and not 200.
//
// A retired zone whose projects are still live is a tenant nobody can reach and
// nobody can see: the events keep arriving and the switcher no longer offers the
// zone that would list them. The count travels in the message because it is the
// actionable half of the refusal.
func TestRetireZoneWithActiveProjectsIsAConflict(t *testing.T) {
	mock := withMockDB(t)

	mock.ExpectExec("UPDATE zones SET status").
		WithArgs("deleted", int64(1), "active", "active").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("FROM zones WHERE id = .").WithArgs(int64(1)).WillReturnRows(adminZoneRows("https://zone.example.com"))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(3))

	w, r := adminRequest(t, http.MethodPost, "/admin/zones/1/retire", "", "1")
	HandleRetireZone(w, r)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body: %s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "3 active") {
		t.Errorf("body = %q, want the count of what is blocking the retirement", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRetireZoneSucceedsWhenEmpty is the negative control for the conflict above:
// without it, a handler that answered 409 to every retirement would look correct.
// It also pins that retirement is an UPDATE — sqlmock fails any statement it was
// not told to expect, so a DELETE would fail here.
func TestRetireZoneSucceedsWhenEmpty(t *testing.T) {
	mock := withMockDB(t)

	mock.ExpectExec("UPDATE zones SET status").
		WithArgs("deleted", int64(1), "active", "active").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("FROM zones WHERE id = .").WithArgs(int64(1)).WillReturnRows(adminZoneRows("https://zone.example.com"))

	w, r := adminRequest(t, http.MethodPost, "/admin/zones/1/retire", "", "1")
	HandleRetireZone(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestUpdateProjectRefusesTheImmutableFields. Same rule as the zone side, plus
// zone_id: moving a project between zones would leave every event already filed
// under it pointing at the old one, so the history would simply detach.
func TestUpdateProjectRefusesTheImmutableFields(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"slug", `{"slug":"renamed"}`, "immutable"},
		{"zone_id", `{"zone_id":2}`, "moved between zones"},
		{"status", `{"status":"deleted"}`, "retire"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := withMockDB(t)

			w, r := adminRequest(t, http.MethodPut, "/admin/projects/1", tt.body, "1")
			HandleUpdateProject(w, r)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body: %s)", w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), tt.want) {
				t.Errorf("body = %q, want it to mention %q", w.Body.String(), tt.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// TestUpdateProjectAcceptsADisplayNameChange is the negative control for the
// three refusals above.
func TestUpdateProjectAcceptsADisplayNameChange(t *testing.T) {
	mock := withMockDB(t)

	mock.ExpectQuery("FROM projects WHERE id = .").WithArgs(int64(1)).WillReturnRows(adminProjectRows())
	mock.ExpectExec("UPDATE projects SET display_name = .").
		WithArgs("Renamed", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("FROM projects WHERE id = .").WithArgs(int64(1)).WillReturnRows(adminProjectRows())

	w, r := adminRequest(t, http.MethodPut, "/admin/projects/1", `{"display_name":"Renamed"}`, "1")
	HandleUpdateProject(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestProbeZoneAnswers200ForAnUnreachableZone — ⚠️ THE PROBE SUCCEEDED; the ZONE
// is the thing that is broken, and those are different failures.
//
// Reporting an unreachable zone as a 500 would leave the admin page unable to
// tell "the probe broke" from "the zone is broken", which is the same shape as an
// errors page rendering "no issues" for a failed query — the most misleading
// possible reading of a 500. The verdict must also be PERSISTED, or the next page
// load quietly disagrees with the one the operator just ran.
//
// The stored query_url here is an internal host, so tools.ValidateExternalURL
// refuses it before any packet leaves the process — the verdict is real and the
// test makes no network call.
func TestProbeZoneAnswers200ForAnUnreachableZone(t *testing.T) {
	mock := withMockDB(t)

	mock.ExpectQuery("FROM zones WHERE id = .").WithArgs(int64(1)).WillReturnRows(adminZoneRows("https://localhost"))
	mock.ExpectExec("UPDATE zones SET reachability = .+, reachability_detail = .+, reported_zone = .+, last_probe_at = .+, updated_at = updated_at").
		WithArgs("unreachable", sqlmock.AnyArg(), "", sqlmock.AnyArg(), int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("FROM zones WHERE id = .").WithArgs(int64(1)).WillReturnRows(adminZoneRows("https://localhost"))

	w, r := adminRequest(t, http.MethodPost, "/admin/zones/1/probe", "", "1")
	HandleProbeZone(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 — an unreachable zone is a successful probe (body: %s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "unreachable") {
		t.Errorf("body = %q, want the verdict in the response", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestProbeZoneOnAMissingRowIs404. The zone has to exist before there is anything
// to probe, and a probe of nothing must not read as a probe that found nothing.
func TestProbeZoneOnAMissingRowIs404(t *testing.T) {
	mock := withMockDB(t)

	mock.ExpectQuery("FROM zones WHERE id = .").WithArgs(int64(42)).WillReturnRows(emptyZoneRows())

	w, r := adminRequest(t, http.MethodPost, "/admin/zones/42/probe", "", "42")
	HandleProbeZone(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body: %s)", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// emptyZoneRows is the same column set with no rows — what a lookup of a zone
// that is not there returns.
func emptyZoneRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "slug", "display_name", "status",
		"ingest_url", "query_url",
		"reachability", "reachability_detail", "reported_zone", "last_probe_at",
		"created_at", "updated_at",
	})
}
