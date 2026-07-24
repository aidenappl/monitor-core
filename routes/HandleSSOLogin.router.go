package routes

import (
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/sso"
	"github.com/gorilla/mux"
)

// HandleSSOLogin begins an SSO login (GET /auth/sso/{slug}/login). It loads the
// provider, generates a single-use {state, nonce, PKCE verifier} record (stored
// server-side, 10-minute TTL) carrying the sanitized post-login return_url, and
// 302-redirects the browser to the provider's authorize URL. The browser holds
// only the opaque state; the nonce and verifier never leave the server.
func HandleSSOLogin(w http.ResponseWriter, r *http.Request) {
	slug := mux.Vars(r)["slug"]
	if slug == "" {
		responder.Error(w, http.StatusBadRequest, "provider slug is required")
		return
	}

	provider, err := sso.LoadProvider(db.SQL, slug)
	if err != nil {
		responder.Error(w, http.StatusNotFound, "sso provider not found")
		return
	}
	if !provider.Enabled {
		responder.Error(w, http.StatusNotFound, "sso provider is disabled")
		return
	}

	adapter, err := sso.NewAdapter(provider)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "sso provider misconfigured", err)
		return
	}

	returnURL := safeReturnURL(r.URL.Query().Get("return_url"))

	state, nonce, verifier, err := sso.GenerateState(db.SQL, slug, returnURL)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to start sso login", err)
		return
	}

	authURL, err := adapter.AuthCodeURL(state, nonce, verifier)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to build authorize url", err)
		return
	}

	http.Redirect(w, r, authURL, http.StatusFound)
}

// safeReturnURL sanitizes a caller-supplied return path to a single-slash-prefixed
// relative path, defeating open-redirect abuse. Anything that is not a lone
// relative path (e.g. "//evil.com", "https://evil.com", a scheme-relative URL) is
// dropped in favor of "/". The result is stored in the state and later turned into
// an absolute web-app URL by webAppURL at redirect time.
func safeReturnURL(raw string) string {
	if raw == "" || !strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, "//") {
		return "/"
	}
	return raw
}

// webAppURL turns a sanitized relative path into an absolute URL on the monitor-web
// origin (env.WebBaseURL). SSO runs on the API host, so post-login/error redirects
// MUST be absolute to the web app — a relative path would resolve to the API host.
func webAppURL(relPath string) string {
	base := strings.TrimRight(env.WebBaseURL, "/")
	if relPath == "" || !strings.HasPrefix(relPath, "/") || strings.HasPrefix(relPath, "//") {
		return base + "/"
	}
	return base + relPath
}
