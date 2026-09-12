package bootstrap

import (
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// The first boot is the ONLY boot that mints an install id, so a bug on that
// path is permanent: every later boot reads the same absent key and takes the
// same branch. The first version of this function treated query.GetSetting's
// sql.ErrNoRows — which it returns for ANY absent key — as a failure, so the id
// was never minted and /version reported an empty install_id indefinitely.
//
// That matters beyond a missing label: install_id is what makes "this registry
// row resolves to the control plane's own database" provable, when the slug
// check that would otherwise catch it passes because two misconfigured values
// agree with each other.
func TestEnsureInstallIDMintsOnFirstBoot(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("SELECT value FROM settings").
		WithArgs(InstallIDSetting).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO settings").
		WillReturnResult(sqlmock.NewResult(1, 1))

	id, err := EnsureInstallID(mockDB)
	if err != nil {
		t.Fatalf("an absent key is the first-boot case, not an error: %v", err)
	}
	if id == "" {
		t.Fatal("no install id was minted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// The second boot must return what the first one stored, and must NOT write.
// Minting a fresh id per boot would make the value useless for exactly the
// comparison it exists for.
func TestEnsureInstallIDIsStableAcrossBoots(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	const stored = "11111111-2222-3333-4444-555555555555"
	mock.ExpectQuery("SELECT value FROM settings").
		WithArgs(InstallIDSetting).
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(stored))
	// No ExpectExec: a write here would be a regression, and sqlmock fails on
	// any statement it was not told to expect.

	id, err := EnsureInstallID(mockDB)
	if err != nil {
		t.Fatalf("EnsureInstallID: %v", err)
	}
	if id != stored {
		t.Errorf("id = %q, want the stored %q", id, stored)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A real database error must still surface. Swallowing everything would turn a
// broken settings table into a silently absent identity.
func TestEnsureInstallIDPropagatesRealErrors(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("SELECT value FROM settings").
		WithArgs(InstallIDSetting).
		WillReturnError(sql.ErrConnDone)

	if _, err := EnsureInstallID(mockDB); err == nil {
		t.Fatal("a connection failure was reported as a successful mint")
	}
}
