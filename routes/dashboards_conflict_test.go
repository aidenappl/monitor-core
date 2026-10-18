package routes

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/gorilla/mux"
)

// TestUpdateDashboardConflictReturnsTheCurrentDashboard pins the 409 contract
// (DASHBOARD-11): a stale version answers 409 with the current dashboard —
// including its version — in data, so the client can reload or overwrite.
func TestUpdateDashboardConflictReturnsTheCurrentDashboard(t *testing.T) {
	mock := withMockDB(t)
	now := time.Now()
	mock.ExpectQuery("FROM monitor.dashboards").WillReturnRows(sqlmock.NewRows([]string{
		"id", "project", "name", "description", "config", "version", "created_at", "updated_at",
	}).AddRow("d-1", "atlas", "Overview", "", `{"widgets":[]}`, 5, now, now))

	req := httptest.NewRequest(http.MethodPut, "/v1/dashboards/d-1", strings.NewReader(`{"config":"{}","version":4}`))
	req = mux.SetURLVars(req, map[string]string{"id": "d-1"})
	req = req.WithContext(scope.WithProject(req.Context(), "atlas"))
	rec := httptest.NewRecorder()
	HandleUpdateDashboard(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body %s)", rec.Code, rec.Body)
	}
	var body struct {
		Success   bool   `json:"success"`
		ErrorCode int    `json:"error_code"`
		Message   string `json:"message"`
		Data      struct {
			ID      string `json:"id"`
			Version int    `json:"version"`
			Config  string `json:"config"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Success || body.ErrorCode != 409 || body.Data.ID != "d-1" || body.Data.Version != 5 || body.Data.Config != `{"widgets":[]}` {
		t.Errorf("body = %+v", body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a stale write reached the database: %v", err)
	}
}
