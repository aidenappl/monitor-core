package bootstrap

import (
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// column orders mirror the query package scanners so sqlmock rows line up with
// query.scanUser / query.scanIdentity.
var backfillUserCols = []string{
	"id", "email", "email_verified", "name", "display_name",
	"role", "active", "updated_at", "inserted_at",
}

var backfillIdentityCols = []string{
	"id", "user_id", "provider", "provider_user_id", "provider_email",
	"email_verified", "password_hash", "identity_data", "last_login_at", "inserted_at",
}

func strptr(s string) *string { return &s }

// TestBackfillFortaUsers_Idempotent proves the core migration guarantee: the
// first run CREATES a user + forta identity, and a second run over the SAME list
// SKIPS (the (forta, subject) identity already exists) — no double-create.
func TestBackfillFortaUsers_Idempotent(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	now := time.Now()
	seeds := []FortaUserSeed{
		{Subject: "forta-sub-1", Email: "Ada@B.com", EmailVerified: true, Name: strptr("Ada"), Role: "admin"},
	}

	// --- Run 1: CREATE path (transactional) ---
	mock.ExpectBegin()
	// No existing (forta, subject) identity.
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs("forta", "forta-sub-1").
		WillReturnError(sql.ErrNoRows)
	// No existing user for the (lowercased) email.
	mock.ExpectQuery("SELECT .* FROM users").
		WithArgs("ada@b.com").
		WillReturnError(sql.ErrNoRows)
	// Create the user, then re-select it.
	mock.ExpectExec("INSERT INTO users").
		WillReturnResult(sqlmock.NewResult(100, 1))
	mock.ExpectQuery("SELECT .* FROM users").
		WithArgs(int64(100)).
		WillReturnRows(sqlmock.NewRows(backfillUserCols).
			AddRow(int64(100), "ada@b.com", true, "Ada", nil, "admin", true, now, now))
	// Create the forta identity, then re-select it.
	mock.ExpectExec("INSERT INTO identities").
		WillReturnResult(sqlmock.NewResult(200, 1))
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs(int64(200)).
		WillReturnRows(sqlmock.NewRows(backfillIdentityCols).
			AddRow(int64(200), int64(100), "forta", "forta-sub-1", "ada@b.com", true, nil, []byte("{}"), nil, now))
	mock.ExpectCommit()

	res, err := BackfillFortaUsers(dbMock, seeds)
	if err != nil {
		t.Fatalf("run 1: %v", err)
	}
	if res.Created != 1 || res.Linked != 0 || res.Skipped != 0 {
		t.Fatalf("run 1: expected created=1 linked=0 skipped=0, got %+v", res)
	}

	// --- Run 2: SKIP path (identity already exists) ---
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs("forta", "forta-sub-1").
		WillReturnRows(sqlmock.NewRows(backfillIdentityCols).
			AddRow(int64(200), int64(100), "forta", "forta-sub-1", "ada@b.com", true, nil, []byte("{}"), nil, now))
	mock.ExpectCommit()

	res, err = BackfillFortaUsers(dbMock, seeds)
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if res.Skipped != 1 || res.Created != 0 || res.Linked != 0 {
		t.Fatalf("run 2: expected created=0 linked=0 skipped=1, got %+v", res)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestBackfillFortaUsers_LinkExisting: a users row already exists for the email,
// so the forta identity is attached to it (LINKED), no user is created.
func TestBackfillFortaUsers_LinkExisting(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	now := time.Now()
	seeds := []FortaUserSeed{
		{Subject: "forta-sub-2", Email: "existing@b.com", EmailVerified: true, Role: "editor"},
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs("forta", "forta-sub-2").
		WillReturnError(sql.ErrNoRows)
	// Existing user for the email.
	mock.ExpectQuery("SELECT .* FROM users").
		WithArgs("existing@b.com").
		WillReturnRows(sqlmock.NewRows(backfillUserCols).
			AddRow(int64(9), "existing@b.com", true, nil, nil, "viewer", true, now, now))
	// Attach the forta identity, then re-select it. No INSERT INTO users.
	mock.ExpectExec("INSERT INTO identities").
		WillReturnResult(sqlmock.NewResult(300, 1))
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs(int64(300)).
		WillReturnRows(sqlmock.NewRows(backfillIdentityCols).
			AddRow(int64(300), int64(9), "forta", "forta-sub-2", "existing@b.com", true, nil, []byte("{}"), nil, now))
	mock.ExpectCommit()

	res, err := BackfillFortaUsers(dbMock, seeds)
	if err != nil {
		t.Fatalf("BackfillFortaUsers: %v", err)
	}
	if res.Linked != 1 || res.Created != 0 || res.Skipped != 0 {
		t.Fatalf("expected linked=1, got %+v", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestBackfillFortaUsers_InvalidRole: an unmapped/invalid role is rejected before
// any DB write.
func TestBackfillFortaUsers_InvalidRole(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	seeds := []FortaUserSeed{
		{Subject: "s", Email: "x@b.com", Role: "pending"},
	}
	if _, err := BackfillFortaUsers(dbMock, seeds); err == nil {
		t.Fatalf("expected error for invalid role, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (no queries should have run): %v", err)
	}
}
