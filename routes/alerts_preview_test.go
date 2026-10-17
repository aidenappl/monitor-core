package routes

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aidenappl/monitor-core/scope"
)

// TestPreviewRoutingRefusesWhatCreateRefuses: the preview decodes and validates
// a draft with create's own code, so a draft create would 400 is a 400 here
// with the same field-named message — answered before any policy is read
// (db.SQL is nil in this test binary, so reaching the read would panic).
func TestPreviewRoutingRefusesWhatCreateRefuses(t *testing.T) {
	tests := []struct {
		name, body, wantMsg string
	}{
		{"cooldown overflow", `{"name":"x","type":"threshold","condition":"gt","threshold":1,"cooldown_seconds":99999999999}`, "cooldown_seconds must be a whole number of seconds between 0 and 604800"},
		{"missing threshold", `{"name":"x","type":"threshold","condition":"gt"}`, "threshold is required for threshold and rate_change rules"},
		{"missing name", `{"type":"threshold","condition":"gt","threshold":1}`, "name is required"},
		{"bad type", `{"name":"x","type":"sometimes","condition":"gt"}`, "invalid type: sometimes (must be threshold, absence, or rate_change)"},
		{"bad channel ids", `{"name":"x","type":"absence","condition":"gt","notification_channel_ids":"[oops"}`, ""},
		{"not json", `{`, "invalid request body"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/alert-rules/preview-routing", strings.NewReader(tt.body))
			req = req.WithContext(scope.WithProject(req.Context(), "atlas"))
			rec := httptest.NewRecorder()
			HandlePreviewAlertRuleRouting(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
			}
			var resp struct {
				ErrorMessage string `json:"error_message"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if tt.wantMsg != "" && resp.ErrorMessage != tt.wantMsg {
				t.Errorf("error_message = %q, want %q", resp.ErrorMessage, tt.wantMsg)
			}
			if !strings.HasPrefix(resp.ErrorMessage, "notification_channel_ids") && tt.wantMsg == "" {
				t.Errorf("error_message = %q, want it to name notification_channel_ids", resp.ErrorMessage)
			}
		})
	}
}
