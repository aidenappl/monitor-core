package routes

import (
	"net/http"
	"time"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/jwt"
)

const (
	cookieAccessToken  = "mon-access-token"
	cookieRefreshToken = "mon-refresh-token"
	cookieLoggedIn     = "mon-logged-in"

	// refreshCookiePath scopes the refresh cookie so it is only ever sent to the
	// refresh endpoint, shrinking its exposure surface.
	refreshCookiePath = "/auth/refresh"
)

// writeAuthCookies writes the three session cookies from already-minted tokens:
//   - mon-access-token  HttpOnly, Path=/, 15m
//   - mon-refresh-token HttpOnly, Path=/auth/refresh, 7d
//   - mon-logged-in     JS-readable ("true"), Path=/, 7d
//
// Secure is on unless env.CookieInsecure (local dev over http); SameSite=Lax so
// top-level navigations (SSO redirects) still carry the session.
func writeAuthCookies(w http.ResponseWriter, accessToken, refreshToken string, accessExpiry, refreshExpiry time.Time) {
	secure := !env.CookieInsecure

	http.SetCookie(w, &http.Cookie{
		Name:     cookieAccessToken,
		Value:    accessToken,
		Path:     "/",
		Domain:   env.CookieDomain,
		Expires:  accessExpiry,
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
	})

	http.SetCookie(w, &http.Cookie{
		Name:     cookieRefreshToken,
		Value:    refreshToken,
		Path:     refreshCookiePath,
		Domain:   env.CookieDomain,
		Expires:  refreshExpiry,
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
	})

	http.SetCookie(w, &http.Cookie{
		Name:     cookieLoggedIn,
		Value:    "true",
		Path:     "/",
		Domain:   env.CookieDomain,
		Expires:  refreshExpiry,
		HttpOnly: false, // frontend JS reads this to detect login state
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
	})
}

// setTokenCookies mints a fresh access + refresh token for userID, writes all
// three session cookies, and returns the RAW refresh token plus its expiry so
// the caller can persist its SHA-256 hash (the raw token is never stored). This
// is the convenience entry point for handlers that start a new session
// (native login/register, SSO callback — Phase 3). The rotating refresh flow in
// HandleRefresh mints its successor token itself and calls writeAuthCookies
// directly so it can persist the hash inside its rotation transaction.
func setTokenCookies(w http.ResponseWriter, userID int64) (rawRefresh string, refreshExpiry time.Time, err error) {
	access, accessExp, err := jwt.NewAccessToken(userID)
	if err != nil {
		return "", time.Time{}, err
	}
	refresh, refreshExp, err := jwt.NewRefreshToken(userID)
	if err != nil {
		return "", time.Time{}, err
	}
	writeAuthCookies(w, access, refresh, accessExp, refreshExp)
	return refresh, refreshExp, nil
}

// clearTokenCookies expires all three session cookies (MaxAge -1). Paths/Domain
// must match the ones used when setting them or the browser keeps the cookie.
func clearTokenCookies(w http.ResponseWriter) {
	secure := !env.CookieInsecure

	specs := []struct {
		name     string
		path     string
		httpOnly bool
	}{
		{cookieAccessToken, "/", true},
		{cookieRefreshToken, refreshCookiePath, true},
		{cookieLoggedIn, "/", false},
	}
	for _, s := range specs {
		http.SetCookie(w, &http.Cookie{
			Name:     s.name,
			Value:    "",
			Path:     s.path,
			Domain:   env.CookieDomain,
			MaxAge:   -1,
			Expires:  time.Unix(0, 0),
			HttpOnly: s.httpOnly,
			Secure:   secure,
			SameSite: http.SameSiteLaxMode,
		})
	}
}
