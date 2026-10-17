package jwt

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/aidenappl/monitor-core/env"
	jwtlib "github.com/golang-jwt/jwt/v5"
)

const (
	issuer             = "monitor"
	accessTokenExpiry  = 15 * time.Minute
	refreshTokenExpiry = 7 * 24 * time.Hour
)

// Claims is Monitor's own session claim set. UserID is int64 to match the
// MariaDB users.id column. Type distinguishes short-lived access tokens from
// long-lived refresh tokens so one cannot be used in place of the other.
type Claims struct {
	jwtlib.RegisteredClaims
	UserID int64  `json:"user_id"`
	Type   string `json:"type"` // "access" or "refresh"

	// Role travels in the ACCESS token so a data plane can authorise a request
	// without a users table.
	//
	// A zone (MON_ROLE=zone) has its own MariaDB and no users in it — identity
	// lives on the control plane — so middleware.validateSessionToken cannot do
	// its usual GetUserByID there. It builds the user from these claims instead,
	// which means the role has to be one of them or every write in a zone 403s
	// on RequireEditor.
	//
	// Set on ACCESS tokens only. A refresh token is exchanged at the control
	// plane, which reads the live row — putting a role in it would just create a
	// second, staler copy of a fact that is already authoritative there.
	//
	// ⚠️ THIS IS A SNAPSHOT, and its staleness is bounded by the 15-minute access
	// expiry, not by the role change. Demote someone and a zone honours their old
	// role until their current access token dies. The control plane is unaffected
	// — it still reads the row on every request.
	//
	// `omitempty` keeps tokens minted before this field existed valid: they
	// simply carry no role, and a user built from one fails CLOSED (reads work,
	// RequireEditor and RequireAdmin refuse). The next refresh mints a token with
	// the role in it, so an existing session heals within one access lifetime
	// without anyone logging out.
	Role string `json:"role,omitempty"`

	// FamilyID is the hex refresh-token family this access token was minted
	// alongside, so logout can revoke THIS session rather than every session.
	//
	// It has to ride in the access token because the refresh cookie cannot reach
	// logout: mon-refresh-token is path-scoped to the refresh endpoint (and
	// monitor-web's proxy rewrites that path again), so POST /auth/logout never
	// sees it. Without this claim HandleLogout can only RevokeAllForUser, and
	// logging out on one device logs out every device.
	//
	// Set on ACCESS tokens only. A refresh token is resolved to its family by the
	// stored row, which is authoritative — a claim there would be a second copy.
	//
	// It is an identifier, not a credential: knowing a family id lets a caller do
	// nothing but name a session they already hold a valid access token for.
	//
	// `omitempty` keeps tokens minted before this field existed valid: they carry
	// no family, and logout falls back to RevokeAllForUser — the pre-fid
	// behaviour — for at most one access lifetime.
	FamilyID string `json:"fid,omitempty"`

	// AuthTime is when the user last actually AUTHENTICATED (unix seconds, the
	// OIDC `auth_time` name) — typed a password, or completed an IdP sign-in —
	// as opposed to when this token was minted. It rides on BOTH token types,
	// because a refresh mints a new access token and must carry the original
	// moment forward rather than reset it: refreshing is not re-authenticating.
	//
	// It is what step-up checks read (ACCOUNT-01: adding a password to an
	// SSO-only account requires an authentication younger than
	// routes.REAUTH_MAX_AGE). Signed like every other claim, so a client cannot
	// freshen it; the only way to move it is to sign in again.
	//
	// Zero (omitted) means "unknown" and is never recent: tokens minted before
	// this field, and a family re-minted from a live SSO session with no
	// sign-in behind it, both carry none.
	AuthTime int64 `json:"auth_time,omitempty"`
}

// AuthenticatedAt returns AuthTime as a time, zero when unknown.
func (c *Claims) AuthenticatedAt() time.Time {
	if c.AuthTime <= 0 {
		return time.Time{}
	}
	return time.Unix(c.AuthTime, 0)
}

// NewAccessToken mints a 15-minute HS512 access token for userID, carrying the
// role so a zone can authorise without a users table (see Claims.Role) and the
// hex refresh-token family so logout can be scoped to this session (see
// Claims.FamilyID). familyID may be empty. authTime is when the user last
// authenticated (Claims.AuthTime); zero omits it.
func NewAccessToken(userID int64, role string, familyID string, authTime time.Time) (string, time.Time, error) {
	return newToken(userID, role, familyID, authTime, "access", accessTokenExpiry)
}

// NewRefreshToken mints a 7-day HS512 refresh token for userID. No role: it is
// redeemed at the control plane, which reads the live row. It carries authTime
// so the rotation can hand it to the next access token unchanged.
func NewRefreshToken(userID int64, authTime time.Time) (string, time.Time, error) {
	return newToken(userID, "", "", authTime, "refresh", refreshTokenExpiry)
}

func newToken(userID int64, role string, familyID string, authTime time.Time, typ string, ttl time.Duration) (string, time.Time, error) {
	// ⚠️ A random jti makes every minted token unique. Without it the claims are
	// (issuer, user, type, second-granularity iat/exp), so two refresh tokens for
	// the same user minted in the same second are BYTE-IDENTICAL — same SHA-256,
	// and the second insert hits uq_refresh_tokens_hash. That is exactly the
	// concurrent-tab case the refresh grace window (routes/refresh_grace.go)
	// answers with a sibling token, so without this the grace path would 500.
	var jti [16]byte
	if _, err := rand.Read(jti[:]); err != nil {
		return "", time.Time{}, fmt.Errorf("failed to generate %s token id: %w", typ, err)
	}

	expiresAt := time.Now().Add(ttl)
	claims := Claims{
		RegisteredClaims: jwtlib.RegisteredClaims{
			ID:        hex.EncodeToString(jti[:]),
			Issuer:    issuer,
			ExpiresAt: jwtlib.NewNumericDate(expiresAt),
			IssuedAt:  jwtlib.NewNumericDate(time.Now()),
		},
		UserID:   userID,
		Type:     typ,
		Role:     role,
		FamilyID: familyID,
	}
	if !authTime.IsZero() {
		claims.AuthTime = authTime.Unix()
	}

	token := jwtlib.NewWithClaims(jwtlib.SigningMethodHS512, claims)
	signed, err := token.SignedString([]byte(env.JWTSigningKey))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to sign %s token: %w", typ, err)
	}
	return signed, expiresAt, nil
}

// ValidateToken parses and verifies a Monitor JWT.
//
// Security: the parser is pinned to HS512 via jwt.WithValidMethods, and the
// keyfunc re-checks the concrete signing method. Pinning the accepted algorithm
// is what defeats the "alg: none" / algorithm-confusion attack class — without
// it an attacker could present an unsigned token (alg=none) or trick an
// asymmetric verifier into treating the public key as an HMAC secret. The issuer
// is also verified so tokens minted by another service are rejected.
func ValidateToken(tokenStr string) (*Claims, error) {
	token, err := jwtlib.ParseWithClaims(
		tokenStr,
		&Claims{},
		func(t *jwtlib.Token) (interface{}, error) {
			if t.Method != jwtlib.SigningMethodHS512 {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return []byte(env.JWTSigningKey), nil
		},
		jwtlib.WithValidMethods([]string{"HS512"}),
		jwtlib.WithIssuer(issuer),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token claims")
	}

	if claims.Issuer != issuer {
		return nil, fmt.Errorf("invalid token issuer: %s", claims.Issuer)
	}

	return claims, nil
}

// ValidateAccessToken validates the token and requires Type == "access".
func ValidateAccessToken(tokenStr string) (int64, string, error) {
	claims, err := ValidateAccessTokenClaims(tokenStr)
	if err != nil {
		return 0, "", err
	}
	return claims.UserID, claims.Role, nil
}

// ValidateAccessTokenClaims is ValidateAccessToken returning the full claim set,
// for callers that need more than the user and role (logout reads FamilyID).
func ValidateAccessTokenClaims(tokenStr string) (*Claims, error) {
	claims, err := ValidateToken(tokenStr)
	if err != nil {
		return nil, err
	}
	if claims.Type != "access" {
		return nil, fmt.Errorf("expected access token, got %q", claims.Type)
	}
	return claims, nil
}

// ValidateRefreshToken validates the token and requires Type == "refresh".
func ValidateRefreshToken(tokenStr string) (int64, error) {
	claims, err := ValidateRefreshTokenClaims(tokenStr)
	if err != nil {
		return 0, err
	}
	return claims.UserID, nil
}

// ValidateRefreshTokenClaims is ValidateRefreshToken returning the full claim
// set, for the rotation that must carry AuthTime forward.
func ValidateRefreshTokenClaims(tokenStr string) (*Claims, error) {
	claims, err := ValidateToken(tokenStr)
	if err != nil {
		return nil, err
	}
	if claims.Type != "refresh" {
		return nil, fmt.Errorf("expected refresh token, got %q", claims.Type)
	}
	return claims, nil
}
