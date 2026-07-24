package routes

import (
	"log"
	"net/http"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/sso"
	"github.com/gorilla/mux"
)

// linkReturnURL is where the SSO link callback sends the browser back to.
const linkReturnURL = "/settings/security"

// HandleListIdentities returns the current user's linked sign-in methods
// (GET /auth/self/identities). Secrets/hashes are stripped by structs.Identity.
func HandleListIdentities(w http.ResponseWriter, r *http.Request) {
	user, ok := middleware.GetUserFromContext(r.Context())
	if !ok || user == nil {
		responder.Error(w, http.StatusUnauthorized, "authentication required")
		return
	}

	identities, err := query.ListIdentitiesByUser(db.SQL, user.ID)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to load identities", err)
		return
	}
	responder.New(w, identities, "linked identities")
}

// linkIdentityResponse carries the provider authorize URL the SPA should send the
// browser to in order to complete the link.
type linkIdentityResponse struct {
	AuthorizeURL string `json:"authorize_url"`
}

// HandleLinkIdentity begins an authenticated account-LINK (POST
// /auth/self/identities/{slug}). The active session is the step-up proof, so we
// mint a link-flow SSO state carrying the current user id and hand the SPA the
// provider authorize URL. The callback attaches the returned identity to this
// user (see linkSSOIdentity), refusing to steal one owned by another account.
func HandleLinkIdentity(w http.ResponseWriter, r *http.Request) {
	user, ok := middleware.GetUserFromContext(r.Context())
	if !ok || user == nil {
		responder.Error(w, http.StatusUnauthorized, "authentication required")
		return
	}

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

	state, nonce, verifier, err := sso.GenerateLinkState(db.SQL, slug, linkReturnURL, user.ID)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to start link", err)
		return
	}

	authURL, err := adapter.AuthCodeURL(state, nonce, verifier)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to build authorize url", err)
		return
	}

	log.Printf("sso link: user %d beginning link to %q", user.ID, slug)
	responder.New(w, linkIdentityResponse{AuthorizeURL: authURL}, "link started")
}

// HandleUnlinkIdentity removes a linked provider from the current user
// (DELETE /auth/self/identities/{slug}). It refuses to remove the user's LAST
// identity — an account must always retain at least one way to sign in.
func HandleUnlinkIdentity(w http.ResponseWriter, r *http.Request) {
	user, ok := middleware.GetUserFromContext(r.Context())
	if !ok || user == nil {
		responder.Error(w, http.StatusUnauthorized, "authentication required")
		return
	}

	slug := mux.Vars(r)["slug"]
	if slug == "" {
		responder.Error(w, http.StatusBadRequest, "provider slug is required")
		return
	}

	identity, err := query.GetIdentityByUserAndProvider(db.SQL, user.ID, slug)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to load identity", err)
		return
	}
	if identity == nil {
		responder.Error(w, http.StatusNotFound, "no linked identity for that provider")
		return
	}

	count, err := query.CountIdentitiesByUser(db.SQL, user.ID)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to count identities", err)
		return
	}
	if count <= 1 {
		// Last-identity guard: unlinking would lock the user out of their own
		// account. Refuse (409) and tell them to add another method first.
		responder.Error(w, http.StatusConflict, "cannot unlink your only sign-in method — add another first")
		return
	}

	if err := query.DeleteIdentity(db.SQL, identity.ID); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to unlink identity", err)
		return
	}

	// If this SSO identity backed a revocation checkpoint session, drop it too.
	if slug != "password" {
		if delErr := query.DeleteSSOSession(db.SQL, user.ID); delErr != nil {
			log.Printf("sso unlink: failed to clear sso_session for user %d: %v", user.ID, delErr)
		}
	}

	log.Printf("sso unlink: user %d unlinked provider %q (identity %d)", user.ID, slug, identity.ID)
	responder.New(w, nil, "identity unlinked")
}
