package routes

import (
	"net/http"

	forta "github.com/aidenappl/go-forta"
	"github.com/aidenappl/monitor-core/responder"
)

// HandleGetSelf returns the currently authenticated Forta user.
// The route must be wrapped with forta.Protected so the user is
// guaranteed to be present in context when this runs.
func HandleGetSelf(w http.ResponseWriter, r *http.Request) {
	user, ok := forta.GetUserFromContext(r.Context())
	if !ok {
		// FetchUserOnProtect may be false; call the API explicitly.
		fetched, err := forta.FetchCurrentUser(r)
		if err != nil {
			responder.Error(w, http.StatusInternalServerError, "failed to fetch user profile")
			return
		}
		user = fetched
	}
	responder.New(w, user, "successfully retrieved current user")
}
