package routes

import (
	"encoding/json"
	"net/http"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/tools"
)

// selfResponse is the GET /auth/self payload: the neutral user record plus its
// linked sign-in methods. structs.Identity hides PasswordHash (json:"-"), so no
// secrets or hashes leak here.
type selfResponse struct {
	*structs.User
	Identities []structs.Identity `json:"identities"`
}

// HandleGetSelf returns the currently authenticated user and its identities.
// Runs behind SessionMiddleware, so the user is guaranteed to be in context.
func HandleGetSelf(w http.ResponseWriter, r *http.Request) {
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

	responder.New(w, selfResponse{User: user, Identities: identities}, "successfully retrieved current user")
}

// UpdateSelfRequest updates the current user's display name and/or password.
// CurrentPassword is required when CHANGING an existing password (re-auth); it is
// not needed when an SSO-provisioned account sets a password for the first time.
type UpdateSelfRequest struct {
	Name            *string `json:"name"`
	Password        *string `json:"password"`
	CurrentPassword *string `json:"current_password"`
}

// HandleUpdateSelf updates the current user's profile (PUT /auth/self). Setting
// a password creates the user's "password" identity if they don't have one yet
// (e.g. an SSO-provisioned account adding a local password), or updates the
// existing hash. Runs behind SessionMiddleware.
func HandleUpdateSelf(w http.ResponseWriter, r *http.Request) {
	user, ok := middleware.GetUserFromContext(r.Context())
	if !ok || user == nil {
		responder.Error(w, http.StatusUnauthorized, "authentication required")
		return
	}

	var body UpdateSelfRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if body.Password != nil {
		if err := tools.ValidatePassword(*body.Password); err != nil {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		hash, err := tools.HashPassword(*body.Password)
		if err != nil {
			responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to hash password", err)
			return
		}
		existing, err := query.GetIdentityByUserAndProvider(db.SQL, user.ID, "password")
		if err != nil {
			responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to load password identity", err)
			return
		}
		if existing != nil {
			// Changing an existing password requires re-authenticating with the
			// current one (defense against session hijack / drive-by change).
			if body.CurrentPassword == nil || !tools.CheckPassword(existing.PasswordHash, *body.CurrentPassword) {
				responder.Error(w, http.StatusUnauthorized, "current password is incorrect")
				return
			}
			if err := query.UpdateIdentityPassword(db.SQL, existing.ID, hash); err != nil {
				responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to update password", err)
				return
			}
		} else {
			if _, err := query.CreateIdentity(db.SQL, query.CreateIdentityRequest{
				UserID:         user.ID,
				Provider:       "password",
				ProviderUserID: user.Email,
				ProviderEmail:  &user.Email,
				EmailVerified:  user.EmailVerified,
				PasswordHash:   hash,
			}); err != nil {
				responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to set password", err)
				return
			}
		}
	}

	if body.Name != nil {
		if _, err := query.UpdateUser(db.SQL, user.ID, query.UpdateUserRequest{Name: body.Name}); err != nil {
			responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to update profile", err)
			return
		}
	}

	updated, err := query.GetUserByID(db.SQL, user.ID)
	if err != nil || updated == nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to reload profile", err)
		return
	}
	responder.New(w, updated, "profile updated")
}
