package routes

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/structs"
)

var userRowCols = []string{"id", "email", "email_verified", "name", "display_name", "role", "active", "updated_at", "inserted_at"}

func userRow(id int64, role string, active bool) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows(userRowCols).AddRow(id, "u@example.test", true, nil, nil, role, active, now, now)
}

func asAdmin(r *http.Request, id int64) *http.Request {
	u := &structs.User{ID: id, Role: "admin", Active: true}
	return r.WithContext(context.WithValue(r.Context(), middleware.UserContextKey, u))
}

func TestAdminUpdateUser(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		body     string
		setup    func(sqlmock.Sqlmock)
		wantCode int
	}{
		{
			name: "approve a pending user", target: "7", body: `{"role":"viewer"}`,
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM users WHERE id = ?").WillReturnRows(userRow(7, "pending", true))
				m.ExpectExec("UPDATE users SET role = ?").WithArgs("viewer", 7).WillReturnResult(sqlmock.NewResult(0, 1))
				m.ExpectQuery("FROM users WHERE id = ?").WillReturnRows(userRow(7, "viewer", true))
			},
			wantCode: http.StatusOK,
		},
		{
			name: "demoting the last active admin is refused", target: "7", body: `{"role":"viewer"}`,
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM users WHERE id = ?").WillReturnRows(userRow(7, "admin", true))
				m.ExpectQuery("role = 'admin' AND active = 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			},
			wantCode: http.StatusConflict,
		},
		{name: "changing yourself is refused", target: "1", body: `{"active":false}`, wantCode: http.StatusBadRequest},
		{name: "unknown role is refused", target: "7", body: `{"role":"owner"}`, wantCode: http.StatusBadRequest},
		{name: "empty body is refused", target: "7", body: `{}`, wantCode: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := withMockDB(t)
			if tt.setup != nil {
				tt.setup(mock)
			}
			w, r := adminRequest(t, http.MethodPut, "/admin/users/"+tt.target, tt.body, tt.target)
			HandleAdminUpdateUser(w, asAdmin(r, 1))
			if w.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d: %s", w.Code, tt.wantCode, w.Body)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestAdminListUsersIncludesProviders(t *testing.T) {
	mock := withMockDB(t)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM users").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
	mock.ExpectQuery("FROM users ORDER BY role = 'pending' DESC").WillReturnRows(userRow(4, "pending", true))
	mock.ExpectQuery("FROM identities").WillReturnRows(sqlmock.NewRows([]string{"user_id", "provider"}).AddRow(4, "forta"))

	w, r := adminRequest(t, http.MethodGet, "/admin/users", "", "")
	HandleAdminListUsers(w, asAdmin(r, 1))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"providers":["forta"]`) || !strings.Contains(w.Body.String(), `"role":"pending"`) {
		t.Fatalf("status = %d body = %s", w.Code, w.Body)
	}
}
