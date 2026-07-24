package routes

import (
	"net/http"

	"github.com/aidenappl/monitor-core/middleware"
	"github.com/gorilla/mux"
)

// RegisterSSORoutes mounts the Phase 3A SSO subsystem onto r. It is ADDITIVE and
// touches none of the existing Forta or /v1 routes. Phase 3B calls this from
// main.go (and also calls sso.Install() to wire the revocation checkpoint); this
// phase only exposes it so the wiring stays a single, reviewable one-liner.
//
// Public (browser-facing, no session required):
//
//	GET  /auth/sso/config              provider discovery for the login page
//	GET  /auth/sso/{slug}/login        begin an SSO login (302 to the IdP)
//	GET  /auth/sso/callback            IdP redirect target (provider from state); mints a Monitor session
//
// Admin (SessionMiddleware + RequireAdmin):
//
//	GET    /admin/sso-providers          list providers (secrets never returned)
//	POST   /admin/sso-providers          create a provider
//	PUT    /admin/sso-providers/{slug}   update a provider
//	DELETE /admin/sso-providers/{slug}   delete a provider
func RegisterSSORoutes(r *mux.Router) {
	// Public SSO endpoints — all GET, so inherently CSRF-safe.
	r.HandleFunc("/auth/sso/config", HandleSSOConfig).Methods(http.MethodGet)
	r.HandleFunc("/auth/sso/{slug}/login", HandleSSOLogin).Methods(http.MethodGet)
	r.HandleFunc("/auth/sso/callback", HandleSSOCallback).Methods(http.MethodGet)

	// Admin SSO provider CRUD — authenticated as an active user, then gated to
	// role=admin. RequireAdmin reads the user SessionMiddleware puts in context.
	admin := r.PathPrefix("/admin").Subrouter()
	admin.Use(middleware.SessionMiddleware)
	admin.HandleFunc("/sso-providers", middleware.RequireAdmin(HandleListSSOProviders)).Methods(http.MethodGet)
	admin.HandleFunc("/sso-providers", middleware.RequireAdmin(HandleCreateSSOProvider)).Methods(http.MethodPost)
	admin.HandleFunc("/sso-providers/{slug}", middleware.RequireAdmin(HandleUpdateSSOProvider)).Methods(http.MethodPut)
	admin.HandleFunc("/sso-providers/{slug}", middleware.RequireAdmin(HandleDeleteSSOProvider)).Methods(http.MethodDelete)
}
