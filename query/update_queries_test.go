package query

import (
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func strPtr(s string) *string { return &s }

// TestUpdateSavedViewScopesTheUpdate pins the project into the UPDATE's own
// WHERE, so an id from another tenant changes nothing.
func TestUpdateSavedViewScopesTheUpdate(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`UPDATE monitor.saved_views SET name = \? WHERE id = \? AND project = \?`).
		WithArgs("renamed", "v-1", "atlas").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT .* FROM monitor.saved_views WHERE monitor.saved_views.id = \? AND monitor.saved_views.project = \?`).
		WithArgs("v-1", "atlas").
		WillReturnRows(savedViewRows())

	v, err := UpdateSavedView(mockDB, "atlas", "v-1", UpdateSavedViewRequest{Name: strPtr("renamed")})
	if err != nil {
		t.Fatalf("UpdateSavedView: %v", err)
	}
	if v == nil || v.ID != "v-1" {
		t.Fatalf("view = %+v, want v-1", v)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestUpdateSavedViewRejects(t *testing.T) {
	tests := []struct {
		name    string
		project string
		req     UpdateSavedViewRequest
	}{
		{name: "no project", project: "", req: UpdateSavedViewRequest{Name: strPtr("x")}},
		{name: "nothing to update", project: "atlas", req: UpdateSavedViewRequest{}},
		{name: "empty name", project: "atlas", req: UpdateSavedViewRequest{Name: strPtr("")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			if _, err := UpdateSavedView(mockDB, tt.project, "v-1", tt.req); err == nil {
				t.Fatal("expected an error")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("a statement reached the database anyway: %v", err)
			}
		})
	}
}

// TestRenameAPIKeyScopesTheUpdate pins the zone/project subquery into the
// UPDATE, the same predicate DeleteAPIKey carries.
func TestRenameAPIKeyScopesTheUpdate(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`UPDATE api_keys SET name = \? WHERE id = \? AND project_id = \(SELECT p.id FROM projects p JOIN zones z ON z.id = p.zone_id WHERE z.slug = \? AND p.slug = \?\)`).
		WithArgs("new name", "k-1", "appleby", "atlas").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := RenameAPIKey(mockDB, "appleby", "atlas", "k-1", "new name"); err != nil {
		t.Fatalf("RenameAPIKey: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestUpdateNotificationChannelRejectsBeforeSQL(t *testing.T) {
	tests := []struct {
		name    string
		project string
		req     UpdateNotificationChannelRequest
	}{
		{name: "no project", project: "", req: UpdateNotificationChannelRequest{Name: strPtr("x")}},
		{name: "nothing to update", project: "atlas", req: UpdateNotificationChannelRequest{}},
		{name: "empty name", project: "atlas", req: UpdateNotificationChannelRequest{Name: strPtr("")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			if _, err := UpdateNotificationChannel(mockDB, tt.project, "c-1", tt.req); err == nil {
				t.Fatal("expected an error")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("a statement reached the database anyway: %v", err)
			}
		})
	}
}
