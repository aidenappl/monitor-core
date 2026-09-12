package main

import (
	"net/http"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/routes"
	"github.com/gorilla/mux"
)

// buildRouter constructs the HTTP surface for one role.
//
// ROUTE REGISTRATION IS THE GATE — not a check inside the handler, and not a
// middleware that rejects. A route whose handler reaches for something this
// process does not have (db.Conn in an `app` process, the ingest Queue in
// either) is NOT REGISTERED, so it answers 404.
//
// The alternative is worse than it looks. db.Conn is an INTERFACE: a nil one
// does not return an error, it panics on the method call, and net/http answers a
// panicking handler by closing the connection without a response at all. So a
// registered-but-broken route does not even give the caller a 500 to read — it
// gives them a dropped connection, from a server that is otherwise perfectly
// healthy. A 404 is the honest answer to "you asked the wrong plane", and it is
// the one a proxy in front of a split deployment can route on.
//
// The role is a parameter rather than a read of env.MonRole so that all three
// surfaces can be built and compared inside one test process — router_test.go
// pins the `both` surface against the deployed inventory, which is what makes
// "this change is a no-op in production" an assertion instead of a claim.
//
// REGISTRATION ORDER IS PRESERVED EXACTLY as it was before roles existed, and
// the gates are wrapped around contiguous runs rather than regrouping the routes
// by plane. gorilla/mux is first-match, and at least one pair here genuinely
// depends on it (PUT /v1/notification-policies/reorder must be registered before
// PUT /v1/notification-policies/{id}, which would otherwise swallow "reorder").
// Sorting these into two tidy blocks would be a silent behaviour change in the
// deployed configuration, which is the one thing this change must not be.
func buildRouter(role env.Role) *mux.Router {
	r := mux.NewRouter()
	r.Use(middleware.RequestIDMiddleware)
	r.Use(middleware.LoggingMiddleware)
	r.Use(middleware.MuxHeaderMiddleware)
	// Double-submit CSRF for cookie-authenticated browsers. Safe methods, Bearer
	// clients, and X-Api-Key clients (ingestion) are exempt, so this does not
	// affect the go-monitor ingest path or API-key query callers.
	r.Use(middleware.CSRFMiddleware)

	// Liveness vs readiness. /health always answers 200 — the container
	// HEALTHCHECK points at it, and restarting the process does not repair a
	// dead ClickHouse. /ready answers 503 naming the store that is down, so a
	// load balancer can route around a replica that would only drop what it
	// accepts. Do not "fix" /health to fail on dependencies; that is /ready.
	//
	// Both are registered in EVERY role. A probe that disappears when a process
	// is configured a particular way is a probe an orchestrator reads as "dead".
	// /health also reports the role, so the answer to "which plane is this?" is
	// reachable without shell access to the container.
	r.HandleFunc("/health", routes.HealthHandler).Methods(http.MethodGet)
	r.HandleFunc("/ready", routes.ReadyHandler).Methods(http.MethodGet)

	// /version rides beside them, in every role and unauthenticated, for the
	// same reason: it is what makes fleet drift answerable. CI redeploys one
	// container, so a zone can sit any number of commits and migrations behind
	// the control plane — and before this endpoint nothing, anywhere, reported a
	// build sha or a schema count.
	r.HandleFunc("/version", routes.VersionHandler).Methods(http.MethodGet)

	// ---- Control plane (MON_ROLE=app or both) --------------------------------
	//
	// Identity. Every route here either mints a Monitor session or is reached
	// through SessionMiddleware, and a zone does neither: sso.Install() and
	// bootstrap.EnsureAdminUser do not run there, so a zone has no provider
	// wiring and no seeded account to log in as. Registering these on a zone
	// would offer a login that can only ever fail, on the plane least able to
	// explain why.
	//
	// Note the asymmetry, which is deliberate: a zone still VERIFIES access
	// tokens, because QueryAuthMiddleware falls back to SessionMiddleware on the
	// /v1 subrouter below. Issuing and verifying are different jobs. The control
	// plane issues; both planes verify.
	if role.RunsControlPlane() {
		// Native session auth (Monitor-owned JWT). External IdPs are config rows,
		// mounted below via RegisterSSORoutes.
		//   /auth/login, /auth/register — public, CSRF-exempt (no session yet).
		//   /auth/refresh — public, CSRF-exempt (authenticates via the refresh cookie).
		//   /auth/logout, /auth/self* — behind SessionMiddleware.
		r.HandleFunc("/auth/login", routes.HandleLogin).Methods(http.MethodPost)
		if env.AllowRegistration {
			r.HandleFunc("/auth/register", routes.HandleRegister).Methods(http.MethodPost)
		}
		r.HandleFunc("/auth/refresh", routes.HandleRefresh).Methods(http.MethodPost)
		r.HandleFunc("/auth/logout", middleware.Protected(routes.HandleLogout)).Methods(http.MethodPost)

		// Current-user profile + linked sign-in methods (all Protected).
		r.HandleFunc("/auth/self", middleware.Protected(routes.HandleGetSelf)).Methods(http.MethodGet)
		r.HandleFunc("/auth/self", middleware.Protected(routes.HandleUpdateSelf)).Methods(http.MethodPut)
		r.HandleFunc("/auth/self/identities", middleware.Protected(routes.HandleListIdentities)).Methods(http.MethodGet)
		r.HandleFunc("/auth/self/identities/{slug}", middleware.Protected(routes.HandleLinkIdentity)).Methods(http.MethodPost)
		r.HandleFunc("/auth/self/identities/{slug}", middleware.Protected(routes.HandleUnlinkIdentity)).Methods(http.MethodDelete)

		// Pluggable SSO subsystem (Phase 3A): /auth/sso/config, /auth/sso/{slug}/login,
		// /auth/sso/{slug}/callback, and the /admin/sso-providers CRUD.
		routes.RegisterSSORoutes(r)

		// ---- Tenancy registry WRITES ----------------------------------------
		//
		// ⚠️ CONTROL PLANE ONLY, AND THAT IS THE POINT OF THE GATE. The registry
		// is the map that says which box a zone's data lives on; a zone process
		// that could mint or edit rows could point a row at itself, or at
		// somebody else, and every read made through it would return the wrong
		// tenant's data under a name that still looks right. Registration is what
		// enforces it — a zone answers 404 here, which is the honest answer to
		// "you asked the wrong plane" and is routable by a proxy in front of a
		// split deployment.
		//
		// Admin-only on top of that, exactly as the SSO provider CRUD above is:
		// its own /admin subrouter with SessionMiddleware, then RequireAdmin per
		// route. Two subrouters share the /admin prefix and mux is happy with
		// that — a prefix match whose children do not match falls through to the
		// next route — so the registry surface stays here in router.go where the
		// role gate is visible, instead of hiding inside a Register* call.
		//
		// The READS are deliberately elsewhere: GET /v1/zones and
		// GET /v1/zones/{zone}/projects are open to any authenticated session
		// because the project switcher needs them. Looking and writing are
		// different privileges.
		//
		// NOTE THE ABSENCE OF ANY DELETE. Retirement is a POST to .../retire,
		// which soft-deletes and keeps the row forever so the UNIQUE key on slug
		// makes reuse impossible; a DELETE route would be one careless refactor
		// away from a real one, and a recycled slug silently reattaches the
		// previous owner's surviving events and its permanent daily rollup to the
		// new one.
		//
		// Named registryAdmin, not `registry`: package main imports the registry
		// package (the zone/project cache) and a local variable shadowing it here
		// would compile fine and read as the cache to anyone skimming.
		registryAdmin := r.PathPrefix("/admin").Subrouter()
		registryAdmin.Use(middleware.SessionMiddleware)
		registryAdmin.HandleFunc("/zones", middleware.RequireAdmin(routes.HandleCreateZone)).Methods(http.MethodPost)
		registryAdmin.HandleFunc("/zones/{id}", middleware.RequireAdmin(routes.HandleUpdateZone)).Methods(http.MethodPut)
		registryAdmin.HandleFunc("/zones/{id}/retire", middleware.RequireAdmin(routes.HandleRetireZone)).Methods(http.MethodPost)
		// Probe-now: makes an outbound request to the zone's query_url, compares
		// what answers against what this row claims, and persists the verdict.
		// POST rather than GET because it has an effect and must never be
		// prefetched — see routes.HandleProbeZone.
		registryAdmin.HandleFunc("/zones/{id}/probe", middleware.RequireAdmin(routes.HandleProbeZone)).Methods(http.MethodPost)
		// Create hangs off the zone because a project slug is unique only within
		// one; update and retire take the project's own id, which is global.
		registryAdmin.HandleFunc("/zones/{id}/projects", middleware.RequireAdmin(routes.HandleCreateProject)).Methods(http.MethodPost)
		registryAdmin.HandleFunc("/projects/{id}", middleware.RequireAdmin(routes.HandleUpdateProject)).Methods(http.MethodPut)
		registryAdmin.HandleFunc("/projects/{id}/retire", middleware.RequireAdmin(routes.HandleRetireProject)).Methods(http.MethodPost)
	}

	// ---- Data plane, root router (MON_ROLE=zone or both) ---------------------
	if role.RunsDataPlane() {
		// Event ingestion — authenticated by X-Api-Key (used by go-monitor).
		// Needs routes.Queue and routes.EventHub, neither of which is created in
		// an app process.
		r.HandleFunc("/v1/events", middleware.IngestAuthMiddleware(routes.IngestEventsHandler)).Methods(http.MethodPost)

		// GitHub webhook — deliberately on the ROOT router, not the v1 subrouter, so
		// QueryAuthMiddleware does not run on it: GitHub cannot present an API key or
		// a session. Its authentication is the HMAC-SHA256 signature over the body
		// (github.VerifySignature), and it is CSRF-exempt for the same reason.
		//
		// Data plane despite touching only MariaDB: every write it makes lands on
		// the issue tracker (links, timeline), which the zone owns. Pointed at an
		// app process it would write PR state into a database that holds no issues
		// and report success — which is why it is gated on the plane that owns the
		// rows rather than on the store it happens to use.
		r.HandleFunc("/webhooks/github", routes.HandleGitHubWebhook).Methods(http.MethodPost)
	}

	// V1 API routes — protected by API key or a Monitor session.
	//
	// The subrouter itself exists in every role: both planes serve something
	// under /v1, and QueryAuthMiddleware works on both (the API-key branches read
	// MariaDB, the session branch verifies a JWT the control plane issued).
	v1 := r.PathPrefix("/v1").Subrouter()
	v1.Use(middleware.QueryAuthMiddleware)

	// Tenancy registry reads. These are what the project switcher in monitor-web
	// populates from, and they are READS ONLY — the registry is seeded by
	// bootstrap.EnsureZoneAndProject and managed out of band in this phase.
	//
	// Clients must not send the ?project selector to either: both run through
	// QueryAuthMiddleware, so a stale selection would refuse the exact request
	// needed to discover a valid one.
	v1.HandleFunc("/zones", routes.HandleListZones).Methods(http.MethodGet)
	v1.HandleFunc("/zones/{zone}/projects", routes.HandleListProjects).Methods(http.MethodGet)

	// Service → source-repository mapping. Monitor watches services across more
	// than one GitHub org, and several service versions share one repo, so this
	// is explicit configuration rather than anything derived from the name.
	v1.HandleFunc("/service-repos", routes.HandleListServiceRepos).Methods(http.MethodGet)
	v1.HandleFunc("/service-repos/{service}", routes.HandleGetServiceRepo).Methods(http.MethodGet)
	v1.HandleFunc("/service-repos/{service}", routes.HandleUpsertServiceRepo).Methods(http.MethodPut)
	v1.HandleFunc("/service-repos/{service}", routes.HandleDeleteServiceRepo).Methods(http.MethodDelete)

	// The registry and service-repo surfaces above, and the API-key surface
	// below, are the three that stay registered in BOTH roles, and that is a
	// deferral rather than a verdict. All three are pure MariaDB, so both planes
	// can serve them; deciding which plane OWNS them is the config-pull question
	// that Phase 2 deliberately does not answer. Leaving them on both is the
	// choice that changes nothing today — the alternative would invent an
	// ownership rule with no second zone to test it against.
	if role.RunsDataPlane() {
		v1.HandleFunc("/events", routes.QueryEventsHandler).Methods(http.MethodGet)
		v1.HandleFunc("/events/stream", routes.StreamEventsHandler).Methods(http.MethodGet)
		v1.HandleFunc("/labels/{label}/values", routes.GetLabelValuesHandler).Methods(http.MethodGet)
		v1.HandleFunc("/data/keys", routes.GetDataKeysHandler).Methods(http.MethodGet)
		v1.HandleFunc("/data/values", routes.GetDataValuesHandler).Methods(http.MethodGet)

		// Analytics routes (Grafana-compatible)
		v1.HandleFunc("/analytics", routes.AnalyticsHandler).Methods(http.MethodPost)
		v1.HandleFunc("/analytics", routes.AnalyticsQueryHandler).Methods(http.MethodGet)
		v1.HandleFunc("/timeseries", routes.TimeSeriesHandler).Methods(http.MethodPost)
		v1.HandleFunc("/timeseries", routes.TimeSeriesQueryHandler).Methods(http.MethodGet)
		v1.HandleFunc("/topn", routes.TopNHandler).Methods(http.MethodPost)
		v1.HandleFunc("/gauge", routes.GaugeHandler).Methods(http.MethodPost)
		v1.HandleFunc("/compare", routes.CompareHandler).Methods(http.MethodPost)
	}

	// API key management — protected by the session/API-key middleware (admin UI)
	v1.HandleFunc("/api-keys", routes.HandleListAPIKeys).Methods(http.MethodGet)
	v1.HandleFunc("/api-keys", routes.HandleCreateAPIKey).Methods(http.MethodPost)
	v1.HandleFunc("/api-keys/{id}", routes.HandleDeleteAPIKey).Methods(http.MethodDelete)

	if role.RunsDataPlane() {
		// Dashboard persistence
		v1.HandleFunc("/dashboards", routes.HandleListDashboards).Methods(http.MethodGet)
		v1.HandleFunc("/dashboards", routes.HandleCreateDashboard).Methods(http.MethodPost)
		v1.HandleFunc("/dashboards/{id}", routes.HandleGetDashboard).Methods(http.MethodGet)
		v1.HandleFunc("/dashboards/{id}", routes.HandleUpdateDashboard).Methods(http.MethodPut)
		v1.HandleFunc("/dashboards/{id}", routes.HandleDeleteDashboard).Methods(http.MethodDelete)

		// Saved views
		v1.HandleFunc("/views", routes.HandleListViews).Methods(http.MethodGet)
		v1.HandleFunc("/views", routes.HandleCreateView).Methods(http.MethodPost)
		v1.HandleFunc("/views/{id}", routes.HandleDeleteView).Methods(http.MethodDelete)

		// Alert rules
		v1.HandleFunc("/alert-rules", routes.HandleListAlertRules).Methods(http.MethodGet)
		v1.HandleFunc("/alert-rules", routes.HandleCreateAlertRule).Methods(http.MethodPost)
		v1.HandleFunc("/alert-rules/{id}", routes.HandleGetAlertRule).Methods(http.MethodGet)
		v1.HandleFunc("/alert-rules/{id}", routes.HandleUpdateAlertRule).Methods(http.MethodPut)
		v1.HandleFunc("/alert-rules/{id}", routes.HandleDeleteAlertRule).Methods(http.MethodDelete)
		v1.HandleFunc("/alert-rules/{id}/test", routes.HandleTestAlertRule).Methods(http.MethodPost)

		// Alert history
		v1.HandleFunc("/alert-history", routes.HandleListAlertHistory).Methods(http.MethodGet)

		// Notification channels
		v1.HandleFunc("/notification-channels", routes.HandleListNotificationChannels).Methods(http.MethodGet)
		v1.HandleFunc("/notification-channels", routes.HandleCreateNotificationChannel).Methods(http.MethodPost)
		v1.HandleFunc("/notification-channels/{id}", routes.HandleDeleteNotificationChannel).Methods(http.MethodDelete)
		v1.HandleFunc("/notification-channels/{id}/test", routes.HandleTestNotificationChannel).Methods(http.MethodPost)

		// Service groups
		v1.HandleFunc("/service-groups", routes.HandleListServiceGroups).Methods(http.MethodGet)
		v1.HandleFunc("/service-groups", routes.HandleCreateServiceGroup).Methods(http.MethodPost)
		v1.HandleFunc("/service-groups/{id}", routes.HandleUpdateServiceGroup).Methods(http.MethodPut)
		v1.HandleFunc("/service-groups/{id}", routes.HandleDeleteServiceGroup).Methods(http.MethodDelete)

		// Notification policies (routing rules)
		v1.HandleFunc("/notification-policies", routes.HandleListPolicies).Methods(http.MethodGet)
		v1.HandleFunc("/notification-policies", routes.HandleCreatePolicy).Methods(http.MethodPost)
		v1.HandleFunc("/notification-policies/reorder", routes.HandleReorderPolicies).Methods(http.MethodPut)
		v1.HandleFunc("/notification-policies/{id}", routes.HandleGetPolicy).Methods(http.MethodGet)
		v1.HandleFunc("/notification-policies/{id}", routes.HandleUpdatePolicy).Methods(http.MethodPut)
		v1.HandleFunc("/notification-policies/{id}", routes.HandleDeletePolicy).Methods(http.MethodDelete)

		// Alert notification stream (SSE for web/desktop notifications)
		v1.HandleFunc("/alerts/stream", routes.HandleStreamAlerts).Methods(http.MethodGet)

		// Issue tracking.
		//
		// The whole surface is data plane, including the routes whose own reads
		// are MariaDB-only. Two reasons, and the second is the load-bearing one:
		// GET /v1/issues/{id} and GET /v1/issues (with ?history=true) decorate
		// their MariaDB rows from the ClickHouse occurrence rollup, so they would
		// panic on a nil db.Conn; and issues are zone-owned by decision, so an app
		// process has no issue rows to serve even for the routes that would run.
		v1.HandleFunc("/issues", routes.HandleListIssues).Methods(http.MethodGet)
		v1.HandleFunc("/issues/{id}", routes.HandleGetIssue).Methods(http.MethodGet)
		v1.HandleFunc("/issues/{id}", routes.HandleUpdateIssue).Methods(http.MethodPut)
		v1.HandleFunc("/issues/{id}/events", routes.HandleGetIssueEvents).Methods(http.MethodGet)
		// Timeline, comments and links. The comment path is how agents leave notes as
		// they work; every write here records its actor.
		v1.HandleFunc("/issues/{id}/timeline", routes.HandleGetIssueTimeline).Methods(http.MethodGet)
		v1.HandleFunc("/issues/{id}/history", routes.HandleGetIssueHistory).Methods(http.MethodGet)
		v1.HandleFunc("/issues/{id}/comments", routes.HandleAddIssueComment).Methods(http.MethodPost)
		v1.HandleFunc("/issues/{id}/comments/{commentID}", routes.HandleEditIssueComment).Methods(http.MethodPatch)
		v1.HandleFunc("/issues/{id}/comments/{commentID}", routes.HandleDeleteIssueComment).Methods(http.MethodDelete)
		v1.HandleFunc("/issues/{id}/links", routes.HandleListIssueLinks).Methods(http.MethodGet)
		v1.HandleFunc("/issues/{id}/links", routes.HandleCreateIssueLink).Methods(http.MethodPost)
		v1.HandleFunc("/issues/{id}/links/{linkID}", routes.HandleDeleteIssueLink).Methods(http.MethodDelete)
	}

	return r
}
