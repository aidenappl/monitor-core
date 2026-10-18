package routes

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	oidcsession "github.com/aidenappl/go-oidc-session"
	"github.com/aidenappl/monitor-core/jwt"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/structs"
)

func TestSSOAuthTime(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	earlier := now.Add(-2 * time.Hour)
	withClaims := func(raw string) oidcsession.Identity { return oidcsession.Identity{RawClaims: json.RawMessage(raw)} }
	tests := []struct {
		name     string
		identity oidcsession.Identity
		silent   bool
		want     time.Time
	}{
		{"idp auth_time wins", withClaims(`{"auth_time": 1799992800}`), false, earlier},
		{"idp auth_time wins even when silent", withClaims(`{"auth_time": 1799992800}`), true, earlier},
		{"future auth_time is clamped to now", withClaims(`{"auth_time": 1900000000}`), false, now},
		{"no claim, interactive: callback time", withClaims(`{"sub":"x"}`), false, now},
		{"no claim, silent: unknown", withClaims(`{"sub":"x"}`), true, time.Time{}},
		{"no claims at all, silent: unknown", oidcsession.Identity{}, true, time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ssoAuthTime(tt.identity, tt.silent, now); !got.Equal(tt.want) {
				t.Errorf("ssoAuthTime = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMintSessionTokensCarriesAuthTime: a refresh mints a new pair carrying the
// ORIGINAL authentication time on both tokens — refreshing is not
// re-authenticating, or a stolen session could keep itself "recent" forever.
func TestMintSessionTokensCarriesAuthTime(t *testing.T) {
	at := time.Now().Add(-3 * time.Hour).Truncate(time.Second)
	minted, err := mintSessionTokens(&structs.User{ID: 9, Role: "viewer"}, make([]byte, 16), at)
	if err != nil {
		t.Fatalf("mintSessionTokens: %v", err)
	}
	access, err := jwt.ValidateAccessTokenClaims(minted.access)
	if err != nil {
		t.Fatalf("access: %v", err)
	}
	refresh, err := jwt.ValidateRefreshTokenClaims(minted.refresh)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if !access.AuthenticatedAt().Equal(at) || !refresh.AuthenticatedAt().Equal(at) {
		t.Errorf("auth_time access=%v refresh=%v, want %v on both", access.AuthenticatedAt(), refresh.AuthenticatedAt(), at)
	}
}

// selfRequest is PUT /auth/self as user 9 with an access token whose
// authentication happened at authTime (zero = none recorded).
func selfRequest(t *testing.T, body string, authTime time.Time) *http.Request {
	t.Helper()
	tok, _, err := jwt.NewAccessToken(9, "viewer", "", authTime)
	if err != nil {
		t.Fatalf("NewAccessToken: %v", err)
	}
	r := httptest.NewRequest(http.MethodPut, "/auth/self", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+tok)
	return r.WithContext(context.WithValue(r.Context(), middleware.UserContextKey,
		&structs.User{ID: 9, Email: "sso@example.com", Active: true, Role: "viewer"}))
}

var identityColumnNames = []string{"id", "user_id", "provider", "provider_user_id", "provider_email",
	"email_verified", "password_hash", "identity_data", "last_login_at", "inserted_at"}

// TestAddPasswordRequiresRecentAuthentication pins ACCOUNT-01: an SSO-only
// account adding a password needs an authentication younger than
// REAUTH_MAX_AGE, else 403 with error_code 4011 "reauth_required" — refused
// before any write. A recent one passes the gate.
func TestAddPasswordRequiresRecentAuthentication(t *testing.T) {
	const body = `{"password":"a-long-enough-password-1"}`
	tests := []struct {
		name     string
		authTime time.Time
		wantGate bool
	}{
		{"no auth time (pre-claim token or re-minted family)", time.Time{}, true},
		{"authenticated 11 minutes ago", time.Now().Add(-11 * time.Minute), true},
		{"authenticated 2 minutes ago", time.Now().Add(-2 * time.Minute), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := withMockDB(t)
			mock.ExpectQuery("FROM identities").WillReturnRows(sqlmock.NewRows(identityColumnNames))
			if !tt.wantGate {
				// Past the gate: the identity insert is attempted. Failing it
				// keeps the test off the rest of the handler.
				mock.ExpectExec("INSERT INTO identities").WillReturnError(errors.New("stop here"))
			}

			rec := httptest.NewRecorder()
			HandleUpdateSelf(rec, selfRequest(t, body, tt.authTime))

			var resp struct {
				Message   string `json:"message"`
				ErrorCode int    `json:"error_code"`
			}
			_ = json.Unmarshal(rec.Body.Bytes(), &resp)
			gated := rec.Code == http.StatusForbidden && resp.ErrorCode == ERROR_CODE_REAUTH_REQUIRED && resp.Message == "reauth_required"
			if gated != tt.wantGate {
				t.Fatalf("status %d body %s; want gated=%v", rec.Code, rec.Body, tt.wantGate)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// TestChangePasswordStillNeedsCurrentPassword: an account that already has a
// password keeps the current-password check and needs no recent sign-in.
func TestChangePasswordStillNeedsCurrentPassword(t *testing.T) {
	mock := withMockDB(t)
	mock.ExpectQuery("FROM identities").WillReturnRows(sqlmock.NewRows(identityColumnNames).
		AddRow(int64(1), int64(9), "password", "sso@example.com", "sso@example.com", true,
			[]byte("$2a$12$invalidinvalidinvalidinvalidinvalidinvalidinvalidinv"), nil, nil, time.Now()))

	rec := httptest.NewRecorder()
	HandleUpdateSelf(rec, selfRequest(t, `{"password":"a-long-enough-password-1","current_password":"wrong"}`, time.Time{}))
	if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), "current password is incorrect") {
		t.Fatalf("status %d body %s; want 401 current password is incorrect", rec.Code, rec.Body)
	}
}
