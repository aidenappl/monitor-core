package routes

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/telemetry"
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
		loginFailed(w, r, "unknown_account", 0, nil)
		responder.Error(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	if !tools.CheckPassword(identity.PasswordHash, body.Password) {
		loginFailed(w, r, "bad_password", identity.UserID, nil)
		responder.Error(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	user, err := query.GetUserByID(db.SQL, identity.UserID)
	if err != nil || user == nil {
		// Still the neutral 401 for the caller; the event says whether the user
		// row is missing or MariaDB failed to return it.
		reason := "user_missing"
		if err != nil {
			reason = "user_lookup_failed"
		}
		loginFailed(w, r, reason, identity.UserID, err)
		responder.Error(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	if !user.Active {
		loginFailed(w, r, "account_inactive", user.ID, nil)
		responder.Error(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	if err := issueSession(w, user.ID, user.Role); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to start session", err)
		return
	}

	middleware.SetUser(w, user.ID)
	telemetry.Info(r.Context(), "auth.login.succeeded", map[string]any{
		"user_id": user.ID,
		"method":  "password",
	})
	responder.New(w, user, "login successful")
}

// loginFailed reports a refused login with a reason category — never the
// email, which is what an attacker would be enumerating. The caller still gets
// the one neutral 401.
//
// Coalesced per client IP: a password spray from one address is one event a
// minute whose `suppressed` count is the attempt rate, rather than an event per
// guess. The request's own event drops to info — this is the warning.
func loginFailed(w http.ResponseWriter, r *http.Request, reason string, userID int64, err error) {
	middleware.ExpectedClientError(w)
	clientIP := middleware.GetClientIPFromContext(r.Context())
	data := map[string]any{
		"method":    "password",
		"reason":    reason,
		"client_ip": clientIP,
		"outcome":   "returned 401",
	}
	if userID != 0 {
		data["user_id"] = userID
	}
	telemetry.WarnCoalesced(r.Context(), "auth.login.failed:"+clientIP, "auth.login.failed", err, data)
}
