package routes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/gorilla/mux"
)

// identityRows builds a single-identity result set for the identities SELECT.
func identityRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "user_id", "provider", "provider_user_id", "provider_email",
		"email_verified", "password_hash", "identity_data", "last_login_at", "inserted_at",
	}).AddRow(int64(5), int64(1), "password", "a@b.com", "a@b.com",
		true, []byte("hash"), []byte("{}"), nil, time.Now())
}

func unlinkRequest(t *testing.T, slug string) (*httptest.ResponseRecorder, *http.Request) {
	t.Helper()
	r := httptest.NewRequest(http.MethodDelete, "/auth/self/identities/"+slug, nil)
	r = mux.SetURLVars(r, map[string]string{"slug": slug})
	ctx := context.WithValue(r.Context(), middleware.UserContextKey, &structs.User{ID: 1, Email: "a@b.com", Active: true})
	return httptest.NewRecorder(), r.WithContext(ctx)
}

// TestUnlinkLastIdentityRefused proves the last-identity guard: when the user has
// exactly one identity, DELETE must 409 and MUST NOT delete the row.
func TestUnlinkLastIdentityRefused(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	orig := db.SQL
	db.SQL = mockDB
	defer func() { db.SQL = orig }()

	// 1. GetIdentityByUserAndProvider → the one identity exists.
	mock.ExpectQuery("SELECT .* FROM identities").WillReturnRows(identityRows())
	// 2. CountIdentitiesByUser → exactly one.
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
	// No ExpectExec: a DELETE here would be an unmet-expectations failure.

	w, r := unlinkRequest(t, "password")
	HandleUnlinkIdentity(w, r)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestUnlinkAllowedWhenMultiple proves the guard permits the unlink when the user
// keeps another identity: the row is deleted and the response is 200.
func TestUnlinkAllowedWhenMultiple(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	orig := db.SQL
	db.SQL = mockDB
	defer func() { db.SQL = orig }()

	mock.ExpectQuery("SELECT .* FROM identities").WillReturnRows(identityRows())
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(2))
	mock.ExpectExec("DELETE FROM identities").WillReturnResult(sqlmock.NewResult(0, 1))

	w, r := unlinkRequest(t, "password")
	HandleUnlinkIdentity(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// The raw OIDC claim envelope is stored for audit only; it must never reach a
// client, whichever handler serialises an Identity.
func TestIdentityJSONOmitsSecrets(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"identity data", "identity_data"},
		{"password hash", "password_hash"},
	}
	b, err := json.Marshal(structs.Identity{
		Provider:     "forta",
		PasswordHash: []byte("hash"),
		IdentityData: json.RawMessage(`{"nonce":"n","sid":"s"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if strings.Contains(string(b), tt.key) {
				t.Errorf("Identity JSON contains %q: %s", tt.key, b)
			}
		})
	}
}
