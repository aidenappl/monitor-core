package middleware

import (
	"testing"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/jwt"
)

// A ZONE has no users table, so validateSessionToken must build the caller from
// the signed access token instead of looking them up. These tests pin that
// branch because getting it wrong is not a compile error and not a test failure
// anywhere else — it is either a zone that 401-loops every page (what happened
// on the appleby zone) or, in the other direction, a zone that trusts a token it
// should have rejected.
//
// ⚠️ These deliberately never touch db.SQL. That is the property under test: on
// a data plane the DB is not consulted at all, so a nil pool cannot panic here.
// If someone reintroduces a lookup on this path, these tests fail by panicking
// rather than by silently passing.

// withRole swaps the process-wide plane role for one test and restores it.
// env.MonRole is a package var read at boot; nothing else mutates it.
func withRole(t *testing.T, role env.Role) {
	t.Helper()
	previous := env.MonRole
	env.MonRole = role
	t.Cleanup(func() { env.MonRole = previous })
}

func TestZoneBuildsUserFromAccessTokenClaims(t *testing.T) {
	withRole(t, env.RoleZone)

	token, _, err := jwt.NewAccessToken(42, "editor")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	user := validateSessionToken(token)
	if user == nil {
		t.Fatal("zone rejected a validly signed access token — this is the 401 loop")
	}
	if user.ID != 42 {
		t.Errorf("ID = %d, want 42", user.ID)
	}
	// The role has to survive the trip or every write in a zone 403s on
	// RequireEditor, which reads exactly this field.
	if user.Role != "editor" {
		t.Errorf("Role = %q, want %q", user.Role, "editor")
	}
	if !user.Active {
		t.Error("Active = false; a zone cannot check the row, so the signed token is the assertion")
	}
}

func TestZoneRoleAbsentFailsClosed(t *testing.T) {
	withRole(t, env.RoleZone)

	// A token minted before Claims.Role existed. It must still authenticate —
	// otherwise deploying this change logs every live session out — but it must
	// not carry privilege it never asserted.
	token, _, err := jwt.NewAccessToken(7, "")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	user := validateSessionToken(token)
	if user == nil {
		t.Fatal("a role-less token was rejected outright; existing sessions would break on deploy")
	}
	if user.Role != "" {
		t.Errorf("Role = %q, want empty so RequireEditor/RequireAdmin refuse", user.Role)
	}
}

func TestZoneRejectsRefreshTokenAsAccess(t *testing.T) {
	withRole(t, env.RoleZone)

	// The zone trusts a signature, so the type pin is what stops a 7-day refresh
	// token being spent as a 15-minute access token — which would extend the
	// revocation gap this design accepts from minutes to a week.
	refresh, _, err := jwt.NewRefreshToken(42)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	if user := validateSessionToken(refresh); user != nil {
		t.Fatal("refresh token accepted as a session at a zone")
	}
}

func TestZoneRejectsGarbage(t *testing.T) {
	withRole(t, env.RoleZone)

	for _, token := range []string{"", "not-a-jwt", "a.b.c"} {
		if user := validateSessionToken(token); user != nil {
			t.Errorf("accepted %q as a session", token)
		}
	}
}
