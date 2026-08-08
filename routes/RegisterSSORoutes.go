package routes

import (
	"net/http"

	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/sso"
	"github.com/gorilla/mux"
)

// RegisterSSORoutes mounts the SSO subsystem onto r. main.go calls this (and
// also calls sso.Install() to wire the revocation checkpoint); keeping the
// mounting here leaves the wiring a single, reviewable one-liner.
//
// Public (browser-facing, no session required):
//
//	GET  /auth/sso/config              provider discovery for the login page
//	GET  /auth/sso/icon/{slug}         cached provider icon, served from this origin
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
	// The cached provider icon. Public by necessity — the login page is
	// unauthenticated — and safe because it returns only bytes this server
	// re-encoded itself, for an enabled provider. Registered BEFORE the callback so
	// the {slug} pattern above cannot swallow "icon".
	r.HandleFunc("/auth/sso/icon/{slug}", HandleSSOIcon).Methods(http.MethodGet)
	r.HandleFunc("/auth/sso/callback", HandleSSOCallback).Methods(http.MethodGet)

	// OIDC Back-Channel Logout 1.0 §2.5 — a form POST from the identity provider,
	// not from a browser.
	//
	// ⚠️ PUBLIC AND UNAUTHENTICATED IN THE ORDINARY SENSE: no cookie, no bearer
	// token. Its authentication IS the signature on the logout token, checked
	// against the provider's JWKS and against this client_id. It must NOT be moved
	// under SessionMiddleware — the caller is Forta, which holds no Monitor
	// session, so requiring one would reject every genuine notification.
	//
	// Registered per slug rather than with a {slug} variable: the handler is built
	// around one provider's verifier, and a path variable would invite pointing a
	// notification at a provider that did not send it.
	r.Handle("/auth/sso/forta/backchannel-logout",
		sso.BackchannelLogoutHandler("forta")).Methods(http.MethodPost)

	// Admin SSO provider CRUD — authenticated as an active user, then gated to
	// role=admin. RequireAdmin reads the user SessionMiddleware puts in context.
	admin := r.PathPrefix("/admin").Subrouter()
	admin.Use(middleware.SessionMiddleware)
	admin.HandleFunc("/sso-providers", middleware.RequireAdmin(HandleListSSOProviders)).Methods(http.MethodGet)
	admin.HandleFunc("/sso-providers", middleware.RequireAdmin(HandleCreateSSOProvider)).Methods(http.MethodPost)
	admin.HandleFunc("/sso-providers/{slug}", middleware.RequireAdmin(HandleUpdateSSOProvider)).Methods(http.MethodPut)
	admin.HandleFunc("/sso-providers/{slug}", middleware.RequireAdmin(HandleDeleteSSOProvider)).Methods(http.MethodDelete)
}
