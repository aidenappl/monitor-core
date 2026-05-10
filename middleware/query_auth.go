package middleware

import (
	"net/http"

	forta "github.com/aidenappl/go-forta"
	"github.com/aidenappl/monitor-core/apikeys"
	"github.com/aidenappl/monitor-core/env"
)

// QueryAuthMiddleware authenticates query/analytics requests.
// Accepts a valid X-Api-Key header (env-based or DB-stored) OR a valid Forta JWT.
func QueryAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Api-Key")
		if key != "" {
			// Check env-based key
			if env.IngestKey != "" && key == env.IngestKey {
				next.ServeHTTP(w, r)
				return
			}
			// Check DB-stored keys
			if apikeys.Validate(key) {
				next.ServeHTTP(w, r)
				return
			}
		}
		forta.Protected(next.ServeHTTP)(w, r)
	})
}
