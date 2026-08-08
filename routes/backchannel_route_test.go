package routes

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

// TestBackchannelLogoutRouteIsMountedAndPublic pins the ROUTING, which is the
// half of back-channel logout that go-forta's own tests cannot cover.
//
// ─────────────────────────────────────────────────────────────────────────────
// ⚠️ THE TWO WAYS TO GET THIS WRONG BOTH LOOK FINE LOCALLY.
//
//  1. NOT MOUNTING IT. Forta's sender then gets a 404, retries six times, marks
//     the delivery exhausted and gives up. Nothing in Monitor logs anything,
//     because nothing in Monitor was ever called. Revocation silently stays at
//     poll speed while the discovery document advertises push.
//
//  2. MOUNTING IT BEHIND SessionMiddleware. The caller is Forta, which holds no
//     Monitor session and no cookie, so every genuine notification is rejected
//     as unauthenticated — again invisibly, and again indistinguishable from
//     "the feature works, nothing has been revoked yet".
//
// So this asserts the route resolves, and that it resolves WITHOUT credentials.
// It deliberately does not assert a 200: a valid signed logout token would
// require a real IdP, and go-forta's backchannel_test.go already covers
// verification. What matters here is that the request REACHES the handler.
//
// ⚠️ AND THAT IS EXACTLY AS FAR AS THIS FILE CAN SEE. It builds a bare
// mux.Router, so it proves nothing about main.go's global middleware stack —
// which is where the endpoint actually broke in production on 2026-08-08:
// CSRFMiddleware refused the POST with 403 "missing csrf cookie" while every
// assertion below still passed. The middleware half lives in
// middleware/csrf_test.go, and a change to this endpoint's path must be made in
// BOTH places or one of them is testing a URL that no longer exists.
// ─────────────────────────────────────────────────────────────────────────────
func TestBackchannelLogoutRouteIsMountedAndPublic(t *testing.T) {
	r := mux.NewRouter()
	RegisterSSORoutes(r)

	const path = "/auth/sso/forta/backchannel-logout"

	t.Run("resolves_without_a_session", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, path,
			strings.NewReader("logout_token=not-a-real-token"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		var match mux.RouteMatch
		if !r.Match(req, &match) {
			t.Fatalf("POST %s does not match any route. Forta's sender would get a 404, "+
				"retry to exhaustion and give up — with nothing logged on this side, because "+
				"nothing on this side was ever reached.", path)
		}
		if match.MatchErr != nil {
			t.Fatalf("route matched with error %v", match.MatchErr)
		}
	})

	t.Run("rejects_GET", func(t *testing.T) {
		// §2.5 is a form POST. A GET is a probe or a misconfiguration and must not
		// be treated as a logout.
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if rr.Code == http.StatusOK {
			t.Errorf("GET %s returned 200; only POST is a logout notification", path)
		}
	})

	t.Run("is_not_under_the_admin_subrouter", func(t *testing.T) {
		// The admin subrouter applies SessionMiddleware + RequireAdmin. If the
		// logout endpoint ever drifts under it, Forta — which holds no session —
		// gets a 401 for every genuine notification.
		if strings.HasPrefix(path, "/admin") {
			t.Fatal("the back-channel logout endpoint is under /admin, which is session-gated. " +
				"Forta holds no Monitor session, so every notification would be rejected.")
		}
	})
}
