package sso

import (
	"context"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// An account provisioned before UserInfo was fetched has email "". Its next
// sign-in fills the email (and name) in from the provider, never overwriting
// a value that is already set.
func TestResolveIdentity_KnownIdentityBackfillsEmptyEmail(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	now := time.Now()
	mock.ExpectQuery("SELECT .* FROM identities").
		WillReturnRows(sqlmock.NewRows(identityCols).
			AddRow(int64(7), int64(2), "forta", "sub-agent", nil, false, nil, []byte("{}"), nil, now))
	mock.ExpectQuery("SELECT .* FROM users").
		WillReturnRows(sqlmock.NewRows(userCols).AddRow(int64(2), "", false, nil, nil, "pending", true, now, now))
	mock.ExpectExec("UPDATE users SET email = \\?, name = \\?, email_verified = \\?").
		WithArgs("agents@appleby.cloud", "Agents", true, int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT .* FROM users").
		WillReturnRows(sqlmock.NewRows(userCols).AddRow(int64(2), "agents@appleby.cloud", true, "Agents", nil, "pending", true, now, now))
	mock.ExpectExec("UPDATE identities SET last_login_at").WillReturnResult(sqlmock.NewResult(0, 1))

	id := ni("forta", "sub-agent", "agents@appleby.cloud", true)
	name := "Agents"
	id.Name = &name
	user, err := ResolveIdentity(context.Background(), dbMock, testProvider(true, true), id)
	if err != nil {
		t.Fatalf("ResolveIdentity: %v", err)
	}
	if user.Email != "agents@appleby.cloud" {
		t.Fatalf("email = %q, want the backfilled address", user.Email)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A provider that asserts no email cannot provision: users.email is UNIQUE, so
// a blank row would block the next account too.
func TestResolveIdentity_ProvisionRefusesBlankEmail(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbMock.Close()

	mock.ExpectQuery("SELECT .* FROM identities").WillReturnError(errNoRows())

	_, err = ResolveIdentity(context.Background(), dbMock, testProvider(false, true), ni("forta", "sub-x", "", false))
	if err == nil || !strings.Contains(err.Error(), "asserted no email") {
		t.Fatalf("err = %v, want a refusal naming the missing email", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
