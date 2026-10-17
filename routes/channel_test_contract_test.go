package routes

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/gorilla/mux"
)

// TestChannelTestContract pins POST /v1/notification-channels/{id}/test
// (NOTIFICATIONS-04), which monitor-web codes against: an invalid config is a
// 400 naming the field, a failed delivery is 200 {ok:false, error}, success is
// 200 {ok:true} — and every outcome is recorded on the channel.
func TestChannelTestContract(t *testing.T) {
	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer okServer.Close()
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer failServer.Close()

	tests := []struct {
		name       string
		config     string
		wantStatus int
		wantOK     *bool
		wantMsg    string
		recordOK   bool
	}{
		{"invalid config", `{"url":""}`, http.StatusBadRequest, nil, "url is required", false},
		{"delivery fails", `{"url":"` + failServer.URL + `/x"}`, http.StatusOK, ptr(false), "the webhook destination answered http 404", false},
		{"delivered", `{"url":"` + okServer.URL + `/x"}`, http.StatusOK, ptr(true), "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := withMockDB(t)
			now := time.Now()
			mock.ExpectQuery("SELECT .* FROM monitor.notification_channels").
				WillReturnRows(sqlmock.NewRows([]string{"id", "project", "name", "type", "config", "config_enc", "created_at",
					"last_test_at", "last_test_ok", "last_test_error"}).
					AddRow("c-1", "atlas", "ops", "webhook", tt.config, nil, now, nil, nil, nil))
			mock.ExpectExec("UPDATE monitor.notification_channels SET last_test_at").
				WithArgs(sqlmock.AnyArg(), tt.recordOK, sqlmock.AnyArg(), "c-1", "atlas").
				WillReturnResult(sqlmock.NewResult(0, 1))

			req := httptest.NewRequest(http.MethodPost, "/v1/notification-channels/c-1/test", nil)
			req = mux.SetURLVars(req, map[string]string{"id": "c-1"})
			req = req.WithContext(scope.WithProject(req.Context(), "atlas"))
			rec := httptest.NewRecorder()
			HandleTestNotificationChannel(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantStatus, rec.Body)
			}
			var body struct {
				Message string `json:"message"`
				Data    *struct {
					OK    bool   `json:"ok"`
					Error string `json:"error"`
				} `json:"data"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if tt.wantOK == nil {
				if body.Message != tt.wantMsg {
					t.Errorf("message = %q, want %q", body.Message, tt.wantMsg)
				}
			} else {
				if body.Data == nil || body.Data.OK != *tt.wantOK {
					t.Fatalf("data = %+v, want ok=%v", body.Data, *tt.wantOK)
				}
				if body.Data.Error != "" && body.Data.Error != "the webhook destination answered HTTP 404" {
					t.Errorf("error = %q", body.Data.Error)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("the outcome was not recorded: %v", err)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }
