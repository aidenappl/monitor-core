package routes

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aidenappl/monitor-core/query"
)

// TestDecodeAlertRuleBodyNamesTheField (ALERTS-08): a decode failure on a
// known field is a message starting with that field, never the bare "invalid
// request body" that left the form with nothing to highlight.
func TestDecodeAlertRuleBodyNamesTheField(t *testing.T) {
	tests := []struct {
		name, body, wantMsg string
	}{
		{"cooldown overflow", `{"cooldown_seconds": 99999999999}`, "cooldown_seconds must be a whole number of seconds between 0 and 604800"},
		{"negative interval", `{"evaluation_interval_seconds": -5}`, "evaluation_interval_seconds must be a whole number of seconds between 0 and 86400"},
		{"fractional for", `{"for_seconds": 1.5}`, "for_seconds must be a whole number of seconds between 0 and 86400"},
		{"threshold as text", `{"threshold": "ten"}`, "threshold must be a number"},
		{"enabled as text", `{"enabled": "yes"}`, "enabled must be true or false"},
		{"not json", `{`, "invalid request body"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/v1/alert-rules/r-1", strings.NewReader(tt.body))
			var dst query.UpdateAlertRuleRequest
			_, err := decodeAlertRuleBody(req, &dst)
			if err == nil || err.Error() != tt.wantMsg {
				t.Fatalf("err = %v, want %q", err, tt.wantMsg)
			}
		})
	}
}

// TestDecodeAlertRuleBodyReportsAClearedThreshold: null and "" are a cleared
// input, reported as such and decoded as nil; an omitted threshold is not
// cleared; a number decodes normally.
func TestDecodeAlertRuleBodyReportsAClearedThreshold(t *testing.T) {
	tests := []struct {
		body        string
		wantCleared bool
		wantNil     bool
	}{
		{`{"threshold": null, "name": "x"}`, true, true},
		{`{"threshold": ""}`, true, true},
		{`{"name": "x"}`, false, true},
		{`{"threshold": 0}`, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.body, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/v1/alert-rules/r-1", strings.NewReader(tt.body))
			var dst query.UpdateAlertRuleRequest
			cleared, err := decodeAlertRuleBody(req, &dst)
			if err != nil {
				t.Fatalf("decodeAlertRuleBody: %v", err)
			}
			if cleared != tt.wantCleared || (dst.Threshold == nil) != tt.wantNil {
				t.Errorf("cleared = %v, threshold nil = %v; want %v, %v", cleared, dst.Threshold == nil, tt.wantCleared, tt.wantNil)
			}
		})
	}
}
