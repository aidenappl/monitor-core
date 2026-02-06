package middleware

import (
	"net/http"

	"github.com/aidenappl/monitor-core/env"
)

// AuthMiddleware checks the X-Api-Key header
func AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// If no API key is configured, allow all requests (for development)
		if env.APIKey == "" {
			next.ServeHTTP(w, r)
			return
		}

		if r.Header.Get("X-Api-Key") != env.APIKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
