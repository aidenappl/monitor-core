package routes

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/telemetry"
	"github.com/gorilla/mux"
)

// adminUserView is a user as the user admin lists it: the account plus the
// sign-in methods linked to it, so an approver can see where it came from.
type adminUserView struct {
	structs.User
	Providers []string `json:"providers"`
}

// HandleAdminListUsers lists accounts, pending first (GET /admin/users,
// admin only). Filters: role, q (email / name substring), limit, offset.
func HandleAdminListUsers(w http.ResponseWriter, r *http.Request) {
	req := query.ListUsersRequest{}
	if v := r.URL.Query().Get("role"); v != "" {
		if !query.ValidUserRoles[v] {
			responder.Error(w, http.StatusBadRequest, "role must be admin, editor, viewer or pending")
			return
		}
		req.Role = &v
	}
	if v := r.URL.Query().Get("q"); v != "" {
		req.Search = &v
	}
	for name, dst := range map[string]*int{"limit": &req.Limit, "offset": &req.Offset} {
		if v := r.URL.Query().Get(name); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				responder.Error(w, http.StatusBadRequest, name+" must be a non-negative integer")
				return
			}
			*dst = n
		}
	}

	users, total, err := query.ListUsers(db.SQL, req)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list users", err)
		return
	}
	ids := make([]int64, len(users))
	for i, u := range users {
		ids[i] = u.ID
	}
	providers, err := query.ListProvidersByUsers(db.SQL, ids)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list sign-in methods", err)
		return
	}
	out := make([]adminUserView, len(users))
	for i, u := range users {
		p := providers[u.ID]
		if p == nil {
			p = []string{}
		}
		out[i] = adminUserView{User: u, Providers: p}
	}
	responder.NewWithCount(w, out, total, "", "")
}

// HandleAdminUpdateUser changes a user's role or active flag (PUT
// /admin/users/{id}, admin only). Approving a pending account is a role change.
//
// Two refusals keep an admin from locking everyone out: you cannot change your
// own role or active flag, and no change may leave Monitor with zero active
// admins. The control plane re-reads the user on every request, so a demotion or
// disable is immediate there; a zone trusts the access token for up to 15 min
// (see middleware.validateSessionTokenCtx).
func HandleAdminUpdateUser(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(mux.Vars(r)["id"], 10, 64)
	if err != nil || id <= 0 {
		responder.Error(w, http.StatusBadRequest, "invalid user id")
		return
	}
	var body struct {
		Role   *string `json:"role"`
		Active *bool   `json:"active"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Role == nil && body.Active == nil {
		responder.Error(w, http.StatusBadRequest, "nothing to change: send role and/or active")
		return
	}
	if body.Role != nil && !query.ValidUserRoles[*body.Role] {
		responder.Error(w, http.StatusBadRequest, "role must be admin, editor, viewer or pending")
		return
	}
	if me, ok := middleware.GetUserID(r.Context()); ok && me == id {
		responder.Error(w, http.StatusBadRequest, "you cannot change your own role or active flag")
		return
	}

	existing, err := query.GetUserByID(db.SQL, id)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to load user", err)
		return
	}
	if existing == nil {
		responder.Error(w, http.StatusNotFound, "user not found")
		return
	}

	losesAdmin := existing.Role == "admin" && existing.Active &&
		((body.Role != nil && *body.Role != "admin") || (body.Active != nil && !*body.Active))
	if losesAdmin {
		n, err := query.CountActiveAdmins(db.SQL)
		if err != nil {
			responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to count admins", err)
			return
		}
		if n <= 1 {
			responder.Error(w, http.StatusConflict, "this is the last active admin")
			return
		}
	}

	user, err := query.UpdateUser(db.SQL, id, query.UpdateUserRequest{Role: body.Role, Active: body.Active})
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to update user", err)
		return
	}

	data := map[string]any{"user_id": id, "old_role": existing.Role, "role": user.Role, "old_active": existing.Active, "active": user.Active}
	if actor, ok := middleware.GetUserID(r.Context()); ok {
		data["by_user_id"] = actor
	}
	telemetry.Info(r.Context(), "admin.user.updated", data)
	responder.New(w, user)
}
