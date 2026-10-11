package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/sso"
)

func TestRefreshSSOGateWithoutCookieIsAbsent(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/auth/refresh", nil)
	s, gate := refreshSSOGate(httptest.NewRecorder(), r)
	if gate != ssoGateAbsent || s != nil {
		t.Fatalf("gate = %v, session = %v; want absent, nil — a password session must not touch the SSO store", gate, s)
	}
}

func TestSSOInteractionRequired(t *testing.T) {
	tests := []struct {
		code string
		want bool
	}{
		{"login_required", true},
		{"interaction_required", true},
		{"consent_required", true},
		{"account_selection_required", true},
		{"access_denied", false},
		{"server_error", false},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			if got := ssoInteractionRequired(tt.code); got != tt.want {
				t.Errorf("ssoInteractionRequired(%q) = %v, want %v", tt.code, got, tt.want)
			}
		})
	}
}

func TestSSOStateCookieBinding(t *testing.T) {
	rr := httptest.NewRecorder()
	setSSOStateCookie(rr, "state-123")
	cookie := rr.Result().Cookies()[0]

	tests := []struct {
		name   string
		cookie *http.Cookie
		state  string
		want   bool
	}{
		{"matching cookie", cookie, "state-123", true},
		{"no cookie (login finished in another browser)", nil, "state-123", false},
		{"different state", cookie, "state-456", false},
		{"empty state", cookie, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/auth/sso/callback", nil)
			if tt.cookie != nil {
				r.AddCookie(tt.cookie)
			}
			if got := ssoStateCookieMatches(r, tt.state); got != tt.want {
				t.Errorf("ssoStateCookieMatches = %v, want %v", got, tt.want)
			}
		})
	}
}

// middleware/csrf.go names the SSO cookie as a literal (importing sso there
// would cycle). This keeps the two in step.
func TestCSRFKnowsTheSSOSessionCookie(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/admin/x", nil)
	r.AddCookie(&http.Cookie{Name: sso.SessionCookie, Value: "v"})
	r.Header.Set("Authorization", "Bearer junk")
	rr := httptest.NewRecorder()
	middleware.CSRFMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rr, r)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("bearer + %s cookie without a CSRF token got %d, want 403 — csrf.go's cookie list is out of date", sso.SessionCookie, rr.Code)
	}
}
