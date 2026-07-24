package sso

import (
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func stateJSON(t *testing.T, sd StateData) string {
	t.Helper()
	b, err := json.Marshal(sd)
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}
	return string(b)
}

// TestConsumeState_SingleUse proves a state is accepted at most once: the first
// ConsumeState deletes the record and returns its data; a replay finds nothing
// and is rejected. This is the OAuth CSRF/replay gate.
func TestConsumeState_SingleUse(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	valid := stateJSON(t, StateData{
		Provider:  "google",
		Nonce:     "n1",
		Verifier:  "v1",
		ReturnURL: "/dashboard",
		ExpiresAt: time.Now().Add(5 * time.Minute),
	})

	// First call: found → returned, then DELETEd (single-use consume).
	mock.ExpectQuery("SELECT value FROM settings").
		WithArgs("sso_state:abc").
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(valid))
	mock.ExpectExec("DELETE FROM settings").
		WithArgs("sso_state:abc").
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Second call (replay): the row is gone.
	mock.ExpectQuery("SELECT value FROM settings").
		WithArgs("sso_state:abc").
		WillReturnError(errNoRows())

	sd, err := ConsumeState(dbMock, "abc")
	if err != nil {
		t.Fatalf("first ConsumeState: %v", err)
	}
	if sd.Provider != "google" || sd.Nonce != "n1" || sd.Verifier != "v1" || sd.ReturnURL != "/dashboard" {
		t.Fatalf("unexpected state data: %+v", sd)
	}

	if _, err := ConsumeState(dbMock, "abc"); err == nil {
		t.Fatalf("replay of a consumed state must be rejected, got nil error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestConsumeState_Expired: an expired record is consumed (deleted) and rejected.
func TestConsumeState_Expired(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	expired := stateJSON(t, StateData{
		Provider:  "google",
		ExpiresAt: time.Now().Add(-1 * time.Minute),
	})

	mock.ExpectQuery("SELECT value FROM settings").
		WithArgs("sso_state:old").
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(expired))
	mock.ExpectExec("DELETE FROM settings").
		WithArgs("sso_state:old").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if _, err := ConsumeState(dbMock, "old"); err == nil {
		t.Fatalf("expected expired state to be rejected, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestConsumeState_Unknown: an unknown state is rejected without a delete.
func TestConsumeState_Unknown(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	mock.ExpectQuery("SELECT value FROM settings").
		WithArgs("sso_state:nope").
		WillReturnError(errNoRows())

	if _, err := ConsumeState(dbMock, "nope"); err == nil {
		t.Fatalf("expected unknown state to be rejected, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
