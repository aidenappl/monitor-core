package jwt

import (
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
}

// NewAccessToken mints a 15-minute HS512 access token for userID, carrying the
// role so a zone can authorise without a users table. See Claims.Role.
func NewAccessToken(userID int64, role string) (string, time.Time, error) {
	return newToken(userID, role, "access", accessTokenExpiry)
}

// NewRefreshToken mints a 7-day HS512 refresh token for userID. No role: it is
// redeemed at the control plane, which reads the live row.
func NewRefreshToken(userID int64) (string, time.Time, error) {
	return newToken(userID, "", "refresh", refreshTokenExpiry)
}

func newToken(userID int64, role string, typ string, ttl time.Duration) (string, time.Time, error) {
	expiresAt := time.Now().Add(ttl)
	claims := Claims{
		RegisteredClaims: jwtlib.RegisteredClaims{
			Issuer:    issuer,
			ExpiresAt: jwtlib.NewNumericDate(expiresAt),
			IssuedAt:  jwtlib.NewNumericDate(time.Now()),
		},
		UserID: userID,
		Type:   typ,
		Role:   role,
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
	claims, err := ValidateToken(tokenStr)
	if err != nil {
		return 0, "", err
	}
	if claims.Type != "access" {
		return 0, "", fmt.Errorf("expected access token, got %q", claims.Type)
	}
	return claims.UserID, claims.Role, nil
}

// ValidateRefreshToken validates the token and requires Type == "refresh".
func ValidateRefreshToken(tokenStr string) (int64, error) {
	claims, err := ValidateToken(tokenStr)
	if err != nil {
		return 0, err
	}
	if claims.Type != "refresh" {
		return 0, fmt.Errorf("expected refresh token, got %q", claims.Type)
	}
	return claims.UserID, nil
}
