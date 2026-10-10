package routes

import (
	"strings"
	"testing"
)

func TestBulkFilter(t *testing.T) {
	tests := []struct {
		name    string
		in      map[string]any
		wantErr string
	}{
		{"empty object refused", map[string]any{}, "at least one field"},
		{"blank values refused", map[string]any{"q": "  "}, "at least one field"},
		{"unknown key refused", map[string]any{"service": "x", "limit": 5.0}, "unsupported filter field(s): limit"},
		{"bad status refused", map[string]any{"status": "done"}, "invalid status"},
		{"nested value refused", map[string]any{"service": []any{"a"}}, "must be a string"},
		{"service accepted", map[string]any{"service": "scraper-service", "q": "429"}, ""},
		{"bool has_pr accepted", map[string]any{"has_pr": true}, ""},
		{"numeric assignee accepted", map[string]any{"assignee": 12.0}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := bulkFilter(tt.in)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestBulkFilterCarriesValues(t *testing.T) {
	req, err := bulkFilter(map[string]any{"service": "scraper-service", "q": "429", "assignee": 12.0, "has_pr": false})
	if err != nil {
		t.Fatalf("bulkFilter: %v", err)
	}
	if req.Service == nil || *req.Service != "scraper-service" {
		t.Errorf("service = %v", req.Service)
	}
	if req.Search == nil || *req.Search != "429" {
		t.Errorf("q = %v", req.Search)
	}
	if req.AssigneeUserID == nil || *req.AssigneeUserID != 12 {
		t.Errorf("assignee = %v", req.AssigneeUserID)
	}
	if req.HasPR == nil || *req.HasPR {
		t.Errorf("has_pr = %v", req.HasPR)
	}
}
