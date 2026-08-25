package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// okHandler records whether the request got past the middleware.
func okHandler(reached *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		*reached = true
		w.WriteHeader(http.StatusOK)
	})
}

// TestCSRFExemptsBackchannelLogout is a REGRESSION TEST for a live production
// failure, and the shape of that failure is the lesson.
//
// ─────────────────────────────────────────────────────────────────────────────
// The back-channel logout endpoint had a passing unit test asserting it was
// mounted, reachable without a session, and rejected GET. All true. It was still
// completely broken in production, because that test exercised the ROUTER and
// this middleware runs in front of it.
//
// The notification is a server-to-server POST carrying no cookie, no Bearer
// token and no X-Api-Key, so it fell through every exemption and was refused 403
// "missing csrf cookie" — error_code 4030. Forta retried six times, marked the
// delivery exhausted, and revocation silently stayed at poll speed.
//
// So this test asserts the property THROUGH the middleware, which is the only
// place it can actually be observed.
// ─────────────────────────────────────────────────────────────────────────────
func TestCSRFExemptsBackchannelLogout(t *testing.T) {
	reached := false
	h := CSRFMiddleware(okHandler(&reached))

	// Exactly what Forta sends: form POST, no cookie, no auth headers.
	req := httptest.NewRequest(http.MethodPost, "/auth/sso/forta/backchannel-logout",
		strings.NewReader("logout_token=signed.jwt.here"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if !reached {
		t.Fatalf("CSRF middleware refused the back-channel logout POST (status %d, body %s).\n\n"+
			"Forta sends this with no cookie, no Bearer token and no X-Api-Key, so without an "+
			"exemption it can never pass. Every notification would be retried six times and marked "+
			"exhausted, and revocation would quietly stay at poll speed while the endpoint looked "+
			"like a broken receiver.", rr.Code, strings.TrimSpace(rr.Body.String()))
	}
}

// TestCSRFExemptsGitHubWebhook is the SAME failure shape as the test above, and
// it is here because that precedent is what caught it.
//
// GitHub deliveries are server-to-server POSTs with no cookie, no Bearer token
// and no X-Api-Key — identical to Forta's back-channel logout. Without an
// explicit exemption every delivery would be refused 403 before the handler ran,
// GitHub would retry, then disable the webhook, and PR links would silently stop
// updating while the endpoint looked like a broken receiver.
//
// Exempting is sound rather than a hole: CSRF defends against a browser
// auto-attaching a session cookie, and this route has no cookie auth at all. Its
// authentication is the HMAC-SHA256 signature over the body, which a cross-site
// form cannot produce.
func TestCSRFExemptsGitHubWebhook(t *testing.T) {
	reached := false
	h := CSRFMiddleware(okHandler(&reached))

	// Exactly what GitHub sends.
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github",
		strings.NewReader(`{"action":"closed"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-Hub-Signature-256", "sha256=deadbeef")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if !reached {
		t.Fatalf("CSRF middleware refused the GitHub webhook POST (status %d, body %s).\n\n"+
			"GitHub sends this with no cookie, no Bearer token and no X-Api-Key, so without an "+
			"exemption it can never pass. Deliveries would be retried, the webhook eventually "+
			"disabled, and linked PR state would quietly stop updating.",
			rr.Code, strings.TrimSpace(rr.Body.String()))
	}
}

// TestCSRFStillProtectsOtherUnsafeRequests pins that the exemption above is
// NARROW.
//
// ⚠️ The failure this guards against is an exemption predicate that is too
// loose — a `strings.Contains(path, "backchannel")`, or a prefix that swallows
// the whole /auth/sso/ tree. That would hand a CSRF bypass to real endpoints
// while every test about back-channel logout still passed.
func TestCSRFStillProtectsOtherUnsafeRequests(t *testing.T) {
	protected := []string{
		"/auth/self",
		"/admin/sso-providers",
		"/auth/sso/forta/login",
		"/auth/self/identities/forta",
		// Adjacent to the exempt path but NOT it.
		"/auth/sso/forta/backchannel-logout/extra",
		"/auth/sso/backchannel-logout-something",
		// Same, for the GitHub webhook exemption.
		"/webhooks/github/extra",
		"/webhooks/github-something",
		"/webhooks",
	}

	for _, path := range protected {
		t.Run(path, func(t *testing.T) {
			reached := false
			h := CSRFMiddleware(okHandler(&reached))

			req := httptest.NewRequest(http.MethodPost, path, nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			if reached {
				t.Errorf("POST %s passed CSRF with no cookie and no token — the back-channel "+
					"exemption is too broad and is now a CSRF bypass", path)
			}
		})
	}
}

// TestCSRFExemptionDoesNotDependOnMethod documents that the exemption is
// path-based, so a GET probe of the same URL is also let through here — the
// handler itself answers 405. Safe methods were already exempt above; this
// simply records that the two rules do not interact.
func TestCSRFExemptionDoesNotDependOnMethod(t *testing.T) {
	reached := false
	h := CSRFMiddleware(okHandler(&reached))

	req := httptest.NewRequest(http.MethodGet, "/auth/sso/forta/backchannel-logout", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if !reached {
		t.Fatalf("GET was blocked by CSRF (status %d); safe methods are unconditionally exempt", rr.Code)
	}
}
