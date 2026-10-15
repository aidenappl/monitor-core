package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aidenappl/monitor-core/structs"
)

func TestIsWriteRequest(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   bool
	}{
		{http.MethodGet, "/v1/alert-rules", false},
		{http.MethodHead, "/v1/events", false},
		{http.MethodOptions, "/v1/api-keys", false},
		{http.MethodPost, "/v1/analytics", false},
		{http.MethodPost, "/v1/timeseries", false},
		{http.MethodPost, "/v1/topn", false},
		{http.MethodPost, "/v1/gauge", false},
		{http.MethodPost, "/v1/compare", false},
		{http.MethodPost, "/v1/api-keys", true},
		{http.MethodPost, "/v1/alert-rules/abc/test", false},
		{http.MethodPost, "/v1/notification-channels/abc/test", true},
		{http.MethodPost, "/v1/issues/bulk", true},
		{http.MethodPut, "/v1/alert-rules/abc", true},
		{http.MethodPatch, "/v1/issues/abc/comments/1", true},
		{http.MethodDelete, "/v1/api-keys/abc", true},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.path, nil)
			if got := isWriteRequest(r); got != tt.want {
				t.Errorf("isWriteRequest(%s %s) = %v, want %v", tt.method, tt.path, got, tt.want)
			}
		})
	}
}

func TestRequireSessionRole(t *testing.T) {
	tests := []struct {
		name      string
		role      string
		noUser    bool
		method    string
		path      string
		wantCode  int
		wantErrNo int
	}{
		{name: "viewer reads", role: "viewer", method: http.MethodGet, path: "/v1/events", wantCode: http.StatusOK},
		{name: "viewer reads analytics by POST", role: "viewer", method: http.MethodPost, path: "/v1/timeseries", wantCode: http.StatusOK},
		{name: "viewer cannot mint a key", role: "viewer", method: http.MethodPost, path: "/v1/api-keys", wantCode: http.StatusForbidden, wantErrNo: 4003},
		{name: "viewer cannot delete a rule", role: "viewer", method: http.MethodDelete, path: "/v1/alert-rules/x", wantCode: http.StatusForbidden, wantErrNo: 4003},
		{name: "roleless token fails closed on write", role: "", method: http.MethodPut, path: "/v1/dashboards/x", wantCode: http.StatusForbidden, wantErrNo: 4003},
		{name: "roleless token still reads", role: "", method: http.MethodGet, path: "/v1/dashboards", wantCode: http.StatusOK},
		{name: "editor writes", role: "editor", method: http.MethodPost, path: "/v1/alert-rules", wantCode: http.StatusOK},
		{name: "admin writes", role: "admin", method: http.MethodDelete, path: "/v1/api-keys/x", wantCode: http.StatusOK},
		{name: "pending cannot read", role: "pending", method: http.MethodGet, path: "/v1/events", wantCode: http.StatusForbidden, wantErrNo: 4004},
		{name: "pending cannot write", role: "pending", method: http.MethodPost, path: "/v1/api-keys", wantCode: http.StatusForbidden, wantErrNo: 4004},
		{name: "no user is unauthenticated", noUser: true, method: http.MethodGet, path: "/v1/events", wantCode: http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			})

			r := httptest.NewRequest(tt.method, tt.path, nil)
			if !tt.noUser {
				r = r.WithContext(context.WithValue(r.Context(), UserContextKey, &structs.User{ID: 7, Role: tt.role}))
			}
			rec := httptest.NewRecorder()
			requireSessionRole(next).ServeHTTP(rec, r)

			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantCode, rec.Body.String())
			}
			if called != (tt.wantCode == http.StatusOK) {
				t.Errorf("next called = %v, want %v", called, tt.wantCode == http.StatusOK)
			}
			if tt.wantErrNo != 0 {
				var body struct {
					ErrorCode int `json:"error_code"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("body is not JSON: %v", err)
				}
				if body.ErrorCode != tt.wantErrNo {
					t.Errorf("error_code = %d, want %d", body.ErrorCode, tt.wantErrNo)
				}
			}
		})
	}
}
