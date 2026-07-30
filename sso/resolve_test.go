package sso

import (
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	ssolib "github.com/aidenappl/go-forta/sso"
)

// errNoRows is what a QueryRow scan returns for an absent row; the query layer
// maps it to (nil, nil).
func errNoRows() error { return sql.ErrNoRows }

// identityCols / userCols mirror the column order the query package scans, so
// sqlmock rows line up with query.scanIdentity / query.scanUser.
var identityCols = []string{
	"id", "user_id", "provider", "provider_user_id", "provider_email",
	"email_verified", "password_hash", "identity_data", "last_login_at", "inserted_at",
}

var userCols = []string{
	"id", "email", "email_verified", "name", "display_name",
	"role", "active", "updated_at", "inserted_at",
}

// testProvider builds the LIBRARY's provider view directly.
//
// It does not go through LoadProvider or the row mapping, deliberately: this file
// tests the DECISION MATRIX — known identity, safe link, provision, refuse — and
// only four fields of the provider participate in it. Constructing the row and
// mapping it would couple every case in this file to the mapping's shape without
// testing anything more.
func testProvider(allowAutoLink, autoProvision bool) *ssolib.Provider {
	return &ssolib.Provider{
		Slug:          "google",
		Kind:          ssolib.KindOIDC,
		AllowAutoLink: allowAutoLink,
		AutoProvision: autoProvision,
	}
}

func ni(provider, subject, email string, verified bool) ssolib.Identity {
	return ssolib.Identity{
		Provider:      provider,
		Subject:       subject,
		Email:         email,
		EmailVerified: verified,
	}
}

// TestResolveIdentity_KnownIdentity: an existing (provider, subject) resolves to
// its owning user and touches the login time — no link, no provision.
func TestResolveIdentity_KnownIdentity(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	now := time.Now()
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs("google", "sub-123").
		WillReturnRows(sqlmock.NewRows(identityCols).
			AddRow(int64(7), int64(42), "google", "sub-123", "a@b.com", true, nil, []byte("{}"), nil, now))
	mock.ExpectQuery("SELECT .* FROM users").
		WithArgs(int64(42)).
		WillReturnRows(sqlmock.NewRows(userCols).
			AddRow(int64(42), "a@b.com", true, nil, nil, "viewer", true, now, now))
	mock.ExpectExec("UPDATE identities SET last_login_at").
		WillReturnResult(sqlmock.NewResult(0, 1))

	user, err := ResolveIdentity(dbMock, testProvider(true, true), ni("google", "sub-123", "a@b.com", true))
	if err != nil {
		t.Fatalf("ResolveIdentity: %v", err)
	}
	if user.ID != 42 {
		t.Fatalf("expected user 42, got %d", user.ID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestResolveIdentity_SafeLink_VerifiedBothSides: no identity yet, verified IdP
// email matches a verified existing user, provider allows linking → the new
// identity is linked onto that user.
func TestResolveIdentity_SafeLink_VerifiedBothSides(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	now := time.Now()
	// No identity for (provider, sub).
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs("google", "sub-new").
		WillReturnError(errNoRows())
	// Existing user with verified email.
	mock.ExpectQuery("SELECT .* FROM users").
		WithArgs("v@b.com").
		WillReturnRows(sqlmock.NewRows(userCols).
			AddRow(int64(9), "v@b.com", true, nil, nil, "viewer", true, now, now))
	// CreateIdentity insert + re-select.
	mock.ExpectExec("INSERT INTO identities").
		WillReturnResult(sqlmock.NewResult(15, 1))
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs(int64(15)).
		WillReturnRows(sqlmock.NewRows(identityCols).
			AddRow(int64(15), int64(9), "google", "sub-new", "v@b.com", true, nil, []byte("{}"), nil, now))

	user, err := ResolveIdentity(dbMock, testProvider(true, true), ni("google", "sub-new", "v@b.com", true))
	if err != nil {
		t.Fatalf("ResolveIdentity: %v", err)
	}
	if user.ID != 9 {
		t.Fatalf("expected linked user 9, got %d", user.ID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestResolveIdentity_NoLink_UnverifiedExistingUser: verified IdP email but the
// existing account is UNVERIFIED → must NOT link (and must NOT provision onto
// the colliding email) → error. This is the pre-account-takeover defense.
func TestResolveIdentity_NoLink_UnverifiedExistingUser(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	now := time.Now()
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs("google", "sub-x").
		WillReturnError(errNoRows())
	mock.ExpectQuery("SELECT .* FROM users").
		WithArgs("u@b.com").
		WillReturnRows(sqlmock.NewRows(userCols).
			AddRow(int64(3), "u@b.com", false /* NOT verified */, nil, nil, "viewer", true, now, now))
	// No INSERT expected — linking and provisioning are both refused.

	_, err = ResolveIdentity(dbMock, testProvider(true, true), ni("google", "sub-x", "u@b.com", true))
	if err == nil {
		t.Fatalf("expected error when existing account is unverified, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestResolveIdentity_NoLink_UnverifiedIdPEmail: the IdP email is NOT verified,
// so even a verified existing user is never auto-linked; with no such user it
// provisions a fresh pending account.
func TestResolveIdentity_NoLink_UnverifiedIdPEmail(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	now := time.Now()
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs("google", "sub-unv").
		WillReturnError(errNoRows())
	// Unverified IdP email → skip the safe-link branch entirely (no user lookup)
	// and go straight to provisioning. Provisioning is transactional.
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO users").
		WillReturnResult(sqlmock.NewResult(50, 1))
	mock.ExpectQuery("SELECT .* FROM users").
		WithArgs(int64(50)).
		WillReturnRows(sqlmock.NewRows(userCols).
			AddRow(int64(50), "new@b.com", false, nil, nil, "pending", true, now, now))
	mock.ExpectExec("INSERT INTO identities").
		WillReturnResult(sqlmock.NewResult(60, 1))
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs(int64(60)).
		WillReturnRows(sqlmock.NewRows(identityCols).
			AddRow(int64(60), int64(50), "google", "sub-unv", "new@b.com", false, nil, []byte("{}"), nil, now))
	mock.ExpectCommit()

	user, err := ResolveIdentity(dbMock, testProvider(true, true), ni("google", "sub-unv", "new@b.com", false))
	if err != nil {
		t.Fatalf("ResolveIdentity: %v", err)
	}
	if user.ID != 50 || user.Role != "pending" {
		t.Fatalf("expected provisioned pending user 50, got id=%d role=%s", user.ID, user.Role)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestResolveIdentity_Provision_UnknownProvider: no identity, no matching user →
// provision a pending account + identity.
func TestResolveIdentity_Provision_UnknownProvider(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	now := time.Now()
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs("google", "sub-fresh").
		WillReturnError(errNoRows())
	// Verified email but NO existing user → falls through to provision. The
	// user lookup happens before the tx; provisioning itself is transactional.
	mock.ExpectQuery("SELECT .* FROM users").
		WithArgs("fresh@b.com").
		WillReturnError(errNoRows())
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO users").
		WillReturnResult(sqlmock.NewResult(70, 1))
	mock.ExpectQuery("SELECT .* FROM users").
		WithArgs(int64(70)).
		WillReturnRows(sqlmock.NewRows(userCols).
			AddRow(int64(70), "fresh@b.com", true, nil, nil, "pending", true, now, now))
	mock.ExpectExec("INSERT INTO identities").
		WillReturnResult(sqlmock.NewResult(80, 1))
	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs(int64(80)).
		WillReturnRows(sqlmock.NewRows(identityCols).
			AddRow(int64(80), int64(70), "google", "sub-fresh", "fresh@b.com", true, nil, []byte("{}"), nil, now))
	mock.ExpectCommit()

	user, err := ResolveIdentity(dbMock, testProvider(true, true), ni("google", "sub-fresh", "fresh@b.com", true))
	if err != nil {
		t.Fatalf("ResolveIdentity: %v", err)
	}
	if user.ID != 70 || user.Role != "pending" {
		t.Fatalf("expected provisioned user 70/pending, got id=%d role=%s", user.ID, user.Role)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestResolveIdentity_Provision_Disabled: no identity, no user, provider forbids
// auto-provisioning → reject.
func TestResolveIdentity_Provision_Disabled(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	mock.ExpectQuery("SELECT .* FROM identities").
		WithArgs("google", "sub-none").
		WillReturnError(errNoRows())
	mock.ExpectQuery("SELECT .* FROM users").
		WithArgs("x@b.com").
		WillReturnError(errNoRows())

	_, err = ResolveIdentity(dbMock, testProvider(true, false /* autoProvision off */), ni("google", "sub-none", "x@b.com", true))
	if err == nil {
		t.Fatalf("expected error when auto-provisioning disabled, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
