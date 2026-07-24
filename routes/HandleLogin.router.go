package routes

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/tools"
)

// LoginRequest is the native email/password login body (POST /auth/login).
type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// HandleLogin authenticates a native (password) account and issues a Monitor
// session. Identities are keyed on (provider, provider_user_id); the password
// identity stores provider_user_id=email and the bcrypt hash. All failure modes
// return the same neutral 401 so the endpoint cannot be used to enumerate emails.
func HandleLogin(w http.ResponseWriter, r *http.Request) {
	var body LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	email := strings.TrimSpace(strings.ToLower(body.Email))
	if email == "" || body.Password == "" {
		responder.Error(w, http.StatusBadRequest, "email and password are required")
		return
	}

	identity, err := query.GetIdentityByProviderSubject(db.SQL, "password", email)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to look up account", err)
		return
	}
	if identity == nil || len(identity.PasswordHash) == 0 {
		// Equalize timing with the real bcrypt path so a fast response can't be
		// used to enumerate which emails have accounts.
		tools.DummyPasswordCheck(body.Password)
		responder.Error(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	if !tools.CheckPassword(identity.PasswordHash, body.Password) {
		responder.Error(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	user, err := query.GetUserByID(db.SQL, identity.UserID)
	if err != nil || user == nil {
		responder.Error(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	if !user.Active {
		responder.Error(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	if err := issueSession(w, user.ID); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to start session", err)
		return
	}

	responder.New(w, user, "login successful")
}
