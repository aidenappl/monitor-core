package routes

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/tools"
)

// RegisterRequest is the native self-registration body (POST /auth/register).
type RegisterRequest struct {
	Email    string  `json:"email"`
	Password string  `json:"password"`
	Name     *string `json:"name"`
}

// HandleRegister self-provisions a native account. Monitor is an internal tool,
// so self-registration is disabled by default (env.AllowRegistration, from
// MON_ALLOW_REGISTRATION) — an admin bootstraps the first user and grants roles.
// When enabled, new accounts are created with role="pending" and an unverified
// email, awaiting admin approval before they can reach protected routes.
func HandleRegister(w http.ResponseWriter, r *http.Request) {
	if !env.AllowRegistration {
		responder.Error(w, http.StatusForbidden, "self-registration is disabled")
		return
	}

	var body RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	email := strings.TrimSpace(strings.ToLower(body.Email))
	if email == "" || body.Password == "" {
		responder.Error(w, http.StatusBadRequest, "email and password are required")
		return
	}
	if err := tools.ValidatePassword(body.Password); err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	existing, err := query.GetUserByEmail(db.SQL, email)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to check for existing account", err)
		return
	}
	if existing != nil {
		responder.Error(w, http.StatusConflict, "an account with that email already exists")
		return
	}

	hash, err := tools.HashPassword(body.Password)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to hash password", err)
		return
	}

	// Create the user and its password identity atomically. Without the tx a
	// failed identity insert would leave an orphaned UNIQUE(email) user row that
	// can neither log in (no identity) nor be re-registered (email taken).
	tx, err := db.SQL.Begin()
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to begin registration", err)
		return
	}

	user, err := query.CreateUser(tx, query.CreateUserRequest{
		Email:         email,
		EmailVerified: false,
		Name:          body.Name,
		Role:          "pending",
	})
	if err != nil {
		_ = tx.Rollback()
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to create account", err)
		return
	}

	if _, err := query.CreateIdentity(tx, query.CreateIdentityRequest{
		UserID:         user.ID,
		Provider:       "password",
		ProviderUserID: email,
		ProviderEmail:  &email,
		EmailVerified:  false,
		PasswordHash:   hash,
	}); err != nil {
		_ = tx.Rollback()
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to create identity", err)
		return
	}

	if err := tx.Commit(); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to commit registration", err)
		return
	}

	responder.New(w, user, "registration successful — your account is pending admin approval")
}
