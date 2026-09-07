package middleware

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/apikeys"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/registry"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/structs"
)

// QueryAuthMiddleware authenticates query/analytics requests.
// Accepts: env-based master key, DB-stored admin-scoped keys, or a valid Monitor
// session (mon-access-token cookie or Bearer access JWT — validated exactly as
// SessionMiddleware does, including the SSO revocation checkpoint).
// Rejects: ingest-scoped keys (they are write-only).
//
// Every accepted branch injects a project, without exception. That is not a
// convenience for the handlers — it is the read-side half of the tenancy
// boundary, and scope.ProjectPredicate refuses to build a WHERE clause at all
// without it, so a branch added here that forgets to inject one produces a loud
// 500 on its first request rather than a silent cross-project read.
//
// WHERE the project comes from differs by branch, and the difference is the
// design rather than an inconsistency to iron out. The two API-key branches
// resolve it from the CREDENTIAL, for the same reason ingest resolves it from
// the api_keys row rather than the body: the single lookup that decides "may you
// read?" is the one that decides "whose data may you read?", so the two cannot be
// answered against different sources or drift apart. The session branch resolves
// it from the REQUEST, because a session has no credential-side answer to derive
// — see the asymmetry register on withSessionProject, which is the one place that
// argument is written down in full.
func QueryAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Api-Key")
		if key != "" {
			// Env-based master key always has full access.
			//
			// It has no api_keys row and so no binding of its own, and it reads
			// the DEFAULT project for exactly the reason the ingest path stamps
			// the default project with it: that is where its own events land, so
			// any other choice here would give the fleet's master credential a
			// view that excludes the events it wrote. Narrowing this to a
			// break-glass credential is the same follow-up ingest_auth.go names.
			//
			// The ?project selector is deliberately NOT honoured here. This
			// branch is credential-derived and stays that way; a `project` in the
			// query string reaches it only as an ordinary filter column, which is
			// ANDed onto the mandatory predicate and can therefore only ever
			// narrow the result, never widen it.
			if matchesEnvMasterKey(key) {
				ctx := WithActor(r.Context(), structs.SystemActor(EnvMasterKeyLabel))
				ctx = scope.WithProject(ctx, env.DefaultProjectSlug)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			// DB-stored keys — only admin scope can query.
			//
			// Resolved as a full Identity rather than a bare scope so the tenant
			// arrives with the authorisation decision, from the one row. An
			// admin key reads ONLY its own project: "admin" is a scope over
			// verbs (query vs. ingest), never over tenants. The selector is
			// ignored here too, for the reason above — this is a real boundary
			// and a request-supplied slug must never be able to move it.
			identity, ok := apikeys.ValidateWithIdentity(key)
			if ok && identity.Scope == apikeys.ScopeAdmin {
				ctx := WithActor(r.Context(), structs.APIKeyActor(identity.ID, identity.Name))
				ctx = scope.WithProject(ctx, identity.ProjectSlug)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			if ok && identity.Scope == apikeys.ScopeIngest {
				http.Error(w, "Forbidden: ingest keys cannot access query endpoints", http.StatusForbidden)
				return
			}
		}

		// Fall back to a Monitor-owned session. SessionMiddleware validates the
		// access JWT (cookie or Bearer), loads the active user, runs the SSO
		// checkpoint, injects the user into context, and 401s on failure.
		//
		// The wrap order matters and is the reason the project is not injected
		// before the call: it must be added to the context SessionMiddleware
		// built, on the way to the handler, not to the one handed to a middleware
		// that is still free to reject the request.
		SessionMiddleware(withSessionProject(next)).ServeHTTP(w, r)
	})
}

// PROJECT_SELECTOR_PARAM is the query-string parameter a session uses to choose
// which project it is reading.
//
// A QUERY PARAMETER — not a header, not a path segment — and the deciding reason
// is EventSource. Monitor has two SSE surfaces (the live event tail and the alert
// feed) and the browser's EventSource cannot set a custom header. A header-based
// selector would therefore work for every axios call and silently leave both
// streams tailing whichever project the fallback chose, forever, with no error on
// either side and nothing in the UI to distinguish a quiet project from a
// misdirected subscription. One mechanism that works for both surfaces beats two
// that agree only for as long as someone remembers to keep them in step.
//
// The ZONE, by contrast, is a path segment in monitor-web (/{zone}/errors?
// project=atlas). A zone selects which backend answers, so it has to survive a
// bookmark and drive the proxy's upstream choice; a project is a filter inside
// one backend and may reasonably become multi-valued. That is Sentry's shape —
// org slug in the path, project as a repeatable query param.
const PROJECT_SELECTOR_PARAM = "project"

// projectSelectable answers whether a slug names an active project in this
// install's zone. It is registry.HasActiveProject in production, and is a var
// only so this package's tests can pin an answer without a live MariaDB — the
// registry cache is package-private to registry/ and needs a database to
// populate.
//
// It is initialised at compile time rather than installed at boot the way
// SSOCheckpoint is, because the two hooks fail in opposite directions. A nil
// checkpoint means "no revocation check is configured", which is a legitimate
// state. A nil selector would mean "validate nothing", and this must fail CLOSED:
// an unloaded registry answers false and the request is refused, rather than the
// server trusting a client-supplied slug during exactly the window in which it
// knows least about the registry.
var projectSelectable = registry.HasActiveProject

// withSessionProject resolves the project a logged-in human is reading: the
// explicit selection on the request when there is one, the install's default
// otherwise.
//
// ---------------------------------------------------------------------------
// THE PROJECT ASYMMETRY — read this before "fixing" it.
//
// Three credentials reach Monitor and each resolves a project a different way.
// Two of those resolutions are TENANCY BOUNDARIES. This one is not, and that is
// deliberate rather than an oversight:
//
//  1. INGEST (X-Api-Key on POST /v1/events) — derived from the api_keys row and
//     OVERWRITTEN over whatever the client sent. Unforgeable. A real boundary.
//  2. API-KEY READS (an admin key on /v1/*) — derived from that key's own row.
//     An admin key reads ONLY its own project, because "admin" is a scope over
//     VERBS, never over tenants. Also a real boundary.
//  3. SESSION READS (this function) — a SELECTOR the user chooses, validated
//     against the registry. NOT a boundary.
//
// (3) reads as a flat contradiction of (1) until the missing premise is stated:
// Monitor has NO per-user project membership table. There is no row anywhere on
// this install that could say "this user may see payments but not billing" —
// every account belongs to the operator or a colleague of the operator — and a
// check that consults nothing is not a boundary. It is a decoration, and the
// danger of shipping one is that the next reader trusts it. Roles still gate
// VERBS (admin/editor/viewer/pending), which is the authorisation that genuinely
// exists here. Sentry and Grafana both work exactly this way: the org/tenant
// selector is a chooser, and membership is the thing that constrains it.
//
// THE CHANGE THAT TURNS THIS INTO A BOUNDARY IS A PER-USER MEMBERSHIP TABLE, AND
// THIS FUNCTION IS WHERE IT LANDS. Until that table exists, validating the
// selection against the registry is the whole of what can honestly be enforced,
// and claiming more in a comment would be worse than claiming nothing.
// ---------------------------------------------------------------------------
//
// An unknown or retired slug is REFUSED, never quietly replaced by the default.
// Falling back would answer the request with one project's events under the label
// the user asked for — a chart that is wrong while looking right, which is the
// worst of the three outcomes available (right answer, honest error, silent wrong
// answer). The refusal names the slug, so the switcher can say which one died
// rather than showing an empty dashboard.
//
// Note for the two registry routes (GET /v1/zones, GET /v1/zones/{zone}/projects):
// they sit on the same subrouter and therefore run through here, so a client must
// NOT append the selector to them. That is what keeps a bad selection
// recoverable — the switcher can always re-read the registry and offer the user
// something valid, even while every other page on that URL is 400ing.
func withSessionProject(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		selected, ok := selectedProject(w, r)
		if !ok {
			return
		}
		next.ServeHTTP(w, r.WithContext(scope.WithProject(r.Context(), selected)))
	})
}

// selectedProject reads and validates the selector, writing the refusal and
// reporting false when it cannot be honoured.
//
// A 400, not a 401 or a 403. There is no membership to fail, so nothing about
// this is an authorisation outcome — the caller named something that does not
// exist in this zone, which is a malformed request. Answering 403 would assert a
// boundary that the register above says plainly is not there, and would send
// monitor-web's error_code 4003 handler to /unauthorized for what is really a
// stale bookmark.
func selectedProject(w http.ResponseWriter, r *http.Request) (string, bool) {
	values := r.URL.Query()[PROJECT_SELECTOR_PARAM]

	// More than one is refused rather than resolved by taking the first. A
	// repeatable project param is the shape this scheme is meant to grow into,
	// and until it does, silently reading two and honouring one would be the same
	// "wrong answer that looks right" the fallback is refused for. Phase 1 is
	// single-select by decision; the error says so.
	if len(values) > 1 {
		responder.Error(w, http.StatusBadRequest, "only one project may be selected per request")
		return "", false
	}

	selected := ""
	if len(values) == 1 {
		selected = strings.TrimSpace(values[0])
	}

	// Absent, or present-but-blank, both mean "no selection". A blank value is
	// what an unset variable interpolated into a URL produces, and treating it as
	// a slug would refuse the request with a message naming the empty string.
	if selected == "" {
		return env.DefaultProjectSlug, true
	}

	if !projectSelectable(selected) {
		responder.Error(w, http.StatusBadRequest,
			fmt.Sprintf("unknown project %q in zone %q", selected, env.ZoneSlug))
		return "", false
	}

	return selected, true
}
