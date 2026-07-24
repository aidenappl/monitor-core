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
}

// NewAccessToken mints a 15-minute HS512 access token for userID.
func NewAccessToken(userID int64) (string, time.Time, error) {
	return newToken(userID, "access", accessTokenExpiry)
}

// NewRefreshToken mints a 7-day HS512 refresh token for userID.
func NewRefreshToken(userID int64) (string, time.Time, error) {
	return newToken(userID, "refresh", refreshTokenExpiry)
}

func newToken(userID int64, typ string, ttl time.Duration) (string, time.Time, error) {
	expiresAt := time.Now().Add(ttl)
	claims := Claims{
		RegisteredClaims: jwtlib.RegisteredClaims{
			Issuer:    issuer,
			ExpiresAt: jwtlib.NewNumericDate(expiresAt),
			IssuedAt:  jwtlib.NewNumericDate(time.Now()),
		},
		UserID: userID,
		Type:   typ,
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
func ValidateAccessToken(tokenStr string) (int64, error) {
	claims, err := ValidateToken(tokenStr)
	if err != nil {
		return 0, err
	}
	if claims.Type != "access" {
		return 0, fmt.Errorf("expected access token, got %q", claims.Type)
	}
	return claims.UserID, nil
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
