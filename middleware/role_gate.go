package middleware

import (
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/responder"
)

// readOnlyPOSTPaths are the /v1 routes that take a POST body but only READ.
// The analytics surface sends its query as JSON, so the method alone cannot
// tell a viewer's chart from a write. Anything not listed here and not a safe
// method is a write.
var readOnlyPOSTPaths = map[string]bool{
	"/v1/analytics":  true,
	"/v1/timeseries": true,
	"/v1/topn":       true,
	"/v1/gauge":      true,
	"/v1/compare":    true,
	// Routes a draft rule against the project's policies; stores and sends
	// nothing.
	"/v1/alert-rules/preview-routing": true,
}

// isWriteRequest reports whether r changes state on the /v1 surface.
func isWriteRequest(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	case http.MethodPost:
		if readOnlyPOSTPaths[r.URL.Path] {
			return false
		}
		// POST /v1/alert-rules/{id}/test evaluates the rule's query now and
		// returns the value; it sends nothing and stores nothing, so a viewer
		// may run it. (The CHANNEL test does send, and stays a write.)
		if strings.HasPrefix(r.URL.Path, "/v1/alert-rules/") && strings.HasSuffix(r.URL.Path, "/test") {
			return false
		}
		return true
	default:
		return true
	}
}

// requireSessionRole enforces the ROLE half of session authorisation on /v1.
// It runs only on the session branch of QueryAuthMiddleware, after
// SessionMiddleware has put the user in context — API-key callers are scoped
// by the key's own scope and never reach it.
//
// ─────────────────────────────────────────────────────────────────────────────
// ⚠️ UNTIL THIS EXISTED, ROLES GATED NOTHING ON /v1.
//
// RequireEditor and RejectPending were defined, documented ("roles gate verbs")
// and never wired onto the /v1 subrouter. So a `viewer` could mint admin-scope
// API keys, delete alert rules and notification channels, and revoke the ingest
// key a whole fleet reports through; and a `pending` account — which every SSO
// auto-provisioned sign-in starts as — could read and write every project's
// data the moment its session was issued. The approval gate was a client-side
// redirect and nothing else.
//
// The rules, in order:
//   - pending → 403 error_code 4004 on EVERY request (monitor-web routes it to
//     /pending). Reads included: approval is what grants access at all.
//   - a write by anyone but admin/editor → 403 error_code 4003
//     (→ /unauthorized). A token minted before Claims.Role existed carries no
//     role and fails CLOSED here, exactly as RequireEditor documents.
//   - everything else passes.
//
// ─────────────────────────────────────────────────────────────────────────────
func requireSessionRole(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := GetUserFromContext(r.Context())
		if !ok || user == nil {
			responder.Error(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if user.Role == "pending" {
			RecordFailure(w, http.StatusForbidden, "account pending approval", nil, map[string]any{
				"auth": "session", "reason": "role_pending",
			})
			responder.ErrorWithCode(w, http.StatusForbidden, "your account is pending admin approval", 4004)
			return
		}
		if isWriteRequest(r) && user.Role != "admin" && user.Role != "editor" {
			RecordFailure(w, http.StatusForbidden, "editor access required", nil, map[string]any{
				"auth": "session", "reason": "role_read_only", "role": user.Role,
			})
			responder.ErrorWithCode(w, http.StatusForbidden, "editor access required", 4003)
			return
		}
		next.ServeHTTP(w, r)
	})
}
