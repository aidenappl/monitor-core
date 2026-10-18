package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORSPolicies(t *testing.T) {
	h := corsHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	tests := []struct {
		origin      string
		allowOrigin string
		credentials string
	}{
		{"https://monitor.appleby.cloud", "https://monitor.appleby.cloud", "true"},
		{"http://localhost:3020", "http://localhost:3020", "true"},
		// Any other subdomain (an appleby.cloud Site) may call, but never with cookies.
		{"https://demo.appleby.cloud", "https://demo.appleby.cloud", ""},
		{"https://app.trailblaze.to", "https://app.trailblaze.to", ""},
		{"https://evil.example.com", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.origin, func(t *testing.T) {
			for _, method := range []string{http.MethodGet, http.MethodOptions} {
				r := httptest.NewRequest(method, "/v1/events", nil)
				r.Header.Set("Origin", tt.origin)
				if method == http.MethodOptions {
					r.Header.Set("Access-Control-Request-Method", "POST")
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if got := w.Header().Get("Access-Control-Allow-Origin"); got != tt.allowOrigin {
					t.Fatalf("%s: Allow-Origin = %q, want %q", method, got, tt.allowOrigin)
				}
				if got := w.Header().Get("Access-Control-Allow-Credentials"); got != tt.credentials {
					t.Fatalf("%s: Allow-Credentials = %q, want %q", method, got, tt.credentials)
				}
			}
		})
	}
}
