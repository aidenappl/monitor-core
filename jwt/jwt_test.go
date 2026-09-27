package jwt

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aidenappl/monitor-core/env"
	jwtlib "github.com/golang-jwt/jwt/v5"
)

func TestMain(m *testing.M) {
	// The JWT package reads its signing key from env.JWTSigningKey. Set it
	// directly so the suite does not depend on Keyring or process env.
	env.JWTSigningKey = "test-signing-key-do-not-use-in-prod"
	os.Exit(m.Run())
}

// signManual builds a token with an arbitrary method / claims / key so the
// negative tests can craft adversarial tokens the public API would never mint.
func signManual(t *testing.T, method jwtlib.SigningMethod, claims Claims, key interface{}) string {
	t.Helper()
	tok := jwtlib.NewWithClaims(method, claims)
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatalf("signManual: %v", err)
	}
	return s
}

func baseClaims(typ string, exp time.Time) Claims {
	return Claims{
		RegisteredClaims: jwtlib.RegisteredClaims{
			Issuer:    issuer,
			ExpiresAt: jwtlib.NewNumericDate(exp),
			IssuedAt:  jwtlib.NewNumericDate(time.Now()),
		},
		UserID: 42,
		Type:   typ,
	}
}

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		mint     func() (string, time.Time, error)
		validate func(string) (int64, error)
	}{
		{
			"access",
			func() (string, time.Time, error) { return NewAccessToken(42, "admin", "") },
			func(tok string) (int64, error) { uid, _, err := ValidateAccessToken(tok); return uid, err },
		},
		{"refresh", func() (string, time.Time, error) { return NewRefreshToken(42) }, ValidateRefreshToken},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tok, _, err := tc.mint()
			if err != nil {
				t.Fatalf("mint: %v", err)
			}
			uid, err := tc.validate(tok)
			if err != nil {
				t.Fatalf("validate: %v", err)
			}
			if uid != 42 {
				t.Fatalf("got user id %d, want 42", uid)
			}
		})
	}
}

func TestExpiredRejected(t *testing.T) {
	tok := signManual(t, jwtlib.SigningMethodHS512,
		baseClaims("access", time.Now().Add(-time.Hour)),
		[]byte(env.JWTSigningKey))

	if _, err := ValidateToken(tok); err == nil {
		t.Fatal("expected expired token to be rejected")
	}
}

func TestWrongTypeRejected(t *testing.T) {
	// An access token must not pass refresh validation and vice-versa.
	access, _, err := NewAccessToken(42, "admin", "")
	if err != nil {
		t.Fatalf("mint access: %v", err)
	}
	if _, err := ValidateRefreshToken(access); err == nil {
		t.Fatal("access token accepted as refresh token")
	}

	refresh, _, err := NewRefreshToken(42)
	if err != nil {
		t.Fatalf("mint refresh: %v", err)
	}
	if _, _, err := ValidateAccessToken(refresh); err == nil {
		t.Fatal("refresh token accepted as access token")
	}
}

func TestAlgNoneRejected(t *testing.T) {
	// alg:none — an unsigned token. Must be rejected by the pinned parser.
	tok := signManual(t, jwtlib.SigningMethodNone,
		baseClaims("access", time.Now().Add(time.Hour)),
		jwtlib.UnsafeAllowNoneSignatureType)

	if _, err := ValidateToken(tok); err == nil {
		t.Fatal("alg:none token was accepted — algorithm pinning is broken")
	}
}

func TestAlgConfusionRejected(t *testing.T) {
	// Same secret bytes but a different HMAC alg (HS256). WithValidMethods pins
	// HS512, so this must be rejected even though the signature would verify.
	tok := signManual(t, jwtlib.SigningMethodHS256,
		baseClaims("access", time.Now().Add(time.Hour)),
		[]byte(env.JWTSigningKey))

	if _, err := ValidateToken(tok); err == nil {
		t.Fatal("HS256 token accepted where HS512 is pinned — alg confusion possible")
	}
}

func TestTamperedSignatureRejected(t *testing.T) {
	tok, _, err := NewAccessToken(42, "admin", "")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	// Flip the FIRST character of the signature segment. (Flipping the last
	// char is unreliable: base64url's final char carries unused bits, so a
	// different char can decode to the same signature bytes.)
	dot := strings.LastIndexByte(tok, '.')
	if dot < 0 || dot+1 >= len(tok) {
		t.Fatalf("unexpected token shape: %q", tok)
	}
	b := []byte(tok)
	if b[dot+1] == 'A' {
		b[dot+1] = 'B'
	} else {
		b[dot+1] = 'A'
	}
	if _, err := ValidateToken(string(b)); err == nil {
		t.Fatal("tampered signature was accepted")
	}
}

func TestWrongIssuerRejected(t *testing.T) {
	claims := baseClaims("access", time.Now().Add(time.Hour))
	claims.Issuer = "not-monitor"
	tok := signManual(t, jwtlib.SigningMethodHS512, claims, []byte(env.JWTSigningKey))
	if _, err := ValidateToken(tok); err == nil {
		t.Fatal("token with foreign issuer was accepted")
	}
}

// TestRefreshTokensUnique pins the jti: two refresh tokens for the same user
// minted back to back (same second) must differ, or their SHA-256 hashes collide
// on uq_refresh_tokens_hash and the refresh grace path's sibling insert fails.
func TestRefreshTokensUnique(t *testing.T) {
	a, _, err := NewRefreshToken(1)
	if err != nil {
		t.Fatalf("NewRefreshToken: %v", err)
	}
	b, _, err := NewRefreshToken(1)
	if err != nil {
		t.Fatalf("NewRefreshToken: %v", err)
	}
	if a == b {
		t.Fatal("two refresh tokens minted in the same second are identical")
	}
}

// TestAccessTokenFamilyID pins the fid claim logout scopes itself by: it must
// round-trip on access tokens, and a token minted without one (every token issued
// before the claim existed) must still validate, just with an empty FamilyID.
func TestAccessTokenFamilyID(t *testing.T) {
	tests := []struct {
		name string
		fid  string
	}{
		{name: "with family", fid: "00112233445566778899aabbccddeeff"},
		{name: "without family (pre-fid token)", fid: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tok, _, err := NewAccessToken(42, "admin", tt.fid)
			if err != nil {
				t.Fatalf("NewAccessToken: %v", err)
			}
			claims, err := ValidateAccessTokenClaims(tok)
			if err != nil {
				t.Fatalf("ValidateAccessTokenClaims: %v", err)
			}
			if claims.FamilyID != tt.fid {
				t.Errorf("FamilyID = %q, want %q", claims.FamilyID, tt.fid)
			}
			if claims.UserID != 42 || claims.Role != "admin" {
				t.Errorf("claims = (%d, %q), want (42, admin)", claims.UserID, claims.Role)
			}
		})
	}

	// A hand-built legacy token (no fid key in the payload at all) still passes.
	legacy := signManual(t, jwtlib.SigningMethodHS512, baseClaims("access", time.Now().Add(time.Minute)), []byte(env.JWTSigningKey))
	claims, err := ValidateAccessTokenClaims(legacy)
	if err != nil {
		t.Fatalf("legacy token rejected: %v", err)
	}
	if claims.FamilyID != "" {
		t.Errorf("legacy FamilyID = %q, want empty", claims.FamilyID)
	}
}
