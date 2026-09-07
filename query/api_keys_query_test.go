package query

import (
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/structs"
)

// apiKeyRows mirrors the column ORDER of apiKeyColumns, which is what scanAPIKey
// unpacks positionally. Adding a column to one and not the other is a silent
// mis-scan (a slug landing in key_prefix, say), not a compile error, so the
// fixture is deliberately written out in full rather than derived.
func apiKeyRows() *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{
		"id", "name", "key_hash", "key_prefix", "scope",
		"project_id", "slug", "created_at", "last_used_at",
	}).AddRow("k-1", "Trailblaze Ingest", strings.Repeat("a", 64), "d88ee36053c5", "ingest",
		int64(3), "default", now, nil)
}

// TestListAPIKeysJoinsProject pins the thing the whole tenancy phase rests on:
// the credential and the project it files under are read together, in one query.
//
// This is what apikeys.refreshCache calls every 30 seconds to rebuild the entire
// auth cache, so a regression to an unjoined SELECT would not fail — it would
// hydrate every key with a zero project and stamp every event with an empty
// tenant, which nothing downstream errors on.
func TestListAPIKeysJoinsProject(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("FROM api_keys JOIN projects ON projects.id = api_keys.project_id").
		WillReturnRows(apiKeyRows())

	keys, err := ListAllAPIKeys(mockDB)
	if err != nil {
		t.Fatalf("ListAllAPIKeys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("got %d keys, want 1", len(keys))
	}
	if keys[0].ProjectID != 3 {
		t.Errorf("ProjectID = %d, want 3", keys[0].ProjectID)
	}
	if keys[0].ProjectSlug != "default" {
		t.Errorf("ProjectSlug = %q, want %q", keys[0].ProjectSlug, "default")
	}
	if keys[0].Scope != structs.ScopeIngest {
		t.Errorf("Scope = %q, want %q", keys[0].Scope, structs.ScopeIngest)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestCreateAPIKeyRequiresProject checks the field is refused before it reaches
// the database. fk_api_keys_project would reject a zero anyway, but as errno
// 1452 naming a constraint — this layer knows it was project_id and says so.
//
// No expectations are registered on the mock, so any statement issued fails the
// call; that is what distinguishes "refused by the guard" from "refused by SQL".
func TestCreateAPIKeyRequiresProject(t *testing.T) {
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	err = CreateAPIKey(mockDB, CreateAPIKeyRequest{
		ID: "k-1", Name: "ingest", KeyHash: "h", KeyPrefix: "p",
		Scope: structs.ScopeIngest, CreatedAt: time.Now(),
	})
	if err == nil {
		t.Fatal("CreateAPIKey with no project succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "project_id") {
		t.Errorf("CreateAPIKey = %v, want an error naming project_id", err)
	}
}

// TestCreateAPIKeyPersistsProject is the other half: a resolved project actually
// reaches the INSERT. Phase 0 shipped a Create that silently dropped a field it
// had been handed, so the binding is asserted as a bound argument rather than
// assumed from the struct.
func TestCreateAPIKeyPersistsProject(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	now := time.Now()
	mock.ExpectExec("INSERT INTO api_keys .*project_id.*").
		WithArgs("k-1", "ingest", "h", "p", "ingest", int64(3), now).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = CreateAPIKey(mockDB, CreateAPIKeyRequest{
		ID: "k-1", Name: "ingest", KeyHash: "h", KeyPrefix: "p",
		Scope: structs.ScopeIngest, ProjectID: 3, CreatedAt: now,
	})
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestListAPIKeysScopesToProject is the counterpart to the test above, and the
// two exist as a pair on purpose: the cache read must span every project and the
// user-facing read must not, so a change that collapses them into one function
// breaks exactly one of these.
func TestListAPIKeysScopesToProject(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("WHERE projects.slug = ?").
		WithArgs("atlas").
		WillReturnRows(apiKeyRows())

	if _, err := ListAPIKeys(mockDB, "atlas"); err != nil {
		t.Fatalf("ListAPIKeys: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("project predicate missing — the list would show every tenant's keys: %v", err)
	}
}

// TestListAllAPIKeysIsUnscoped proves the cache path carries NO project
// predicate. If it ever gained one, keys outside that project would stop
// authenticating — a total ingest outage for every other tenant, with a 401 as
// the only symptom.
func TestListAllAPIKeysIsUnscoped(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("^SELECT (?s).* FROM api_keys JOIN projects ON projects.id = api_keys.project_id ORDER BY").
		WillReturnRows(apiKeyRows())

	if _, err := ListAllAPIKeys(mockDB); err != nil {
		t.Fatalf("ListAllAPIKeys: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("cache read is no longer unscoped: %v", err)
	}
}

// TestDeleteAPIKeyIsProjectScoped guards a DESTRUCTIVE cross-tenant verb.
//
// Unscoped, an admin key bound to one project could delete another project's
// INGEST credential by id alone — silently stopping that tenant's services from
// reporting, with a 401 at the producer as the only symptom and nothing in
// Monitor to explain it. The predicate lives in the DELETE itself rather than in
// the handler that calls it, so a future caller that skips or reorders the
// lookup still cannot delete across tenants.
func TestDeleteAPIKeyIsProjectScoped(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec("DELETE FROM api_keys WHERE id = \\? AND project_id = \\(SELECT id FROM projects WHERE slug = \\?\\)").
		WithArgs("key-1", "atlas").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := DeleteAPIKey(mockDB, "atlas", "key-1"); err != nil {
		t.Fatalf("DeleteAPIKey: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("delete is not project-scoped — it can destroy another tenant's key: %v", err)
	}
}

// TestGetAPIKeyByIDIsProjectScoped pins the other half: a key outside the
// caller's project must read as ABSENT, so its existence cannot even be probed.
func TestGetAPIKeyByIDIsProjectScoped(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("WHERE api_keys.id = \\? AND projects.slug = \\?").
		WithArgs("key-1", "atlas").
		WillReturnRows(apiKeyRows())

	if _, err := GetAPIKeyByID(mockDB, "atlas", "key-1"); err != nil {
		t.Fatalf("GetAPIKeyByID: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("lookup is not project-scoped: %v", err)
	}
}
