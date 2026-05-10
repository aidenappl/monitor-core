package middleware

import (
	"net/http"

	forta "github.com/aidenappl/go-forta"
	"github.com/aidenappl/monitor-core/apikeys"
	"github.com/aidenappl/monitor-core/env"
)

// QueryAuthMiddleware authenticates query/analytics requests.
// Accepts: env-based master key, DB-stored admin-scoped keys, or a valid Forta JWT.
// Rejects: ingest-scoped keys (they are write-only).
func QueryAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Api-Key")
		if key != "" {
			// Env-based master key always has full access
			if env.IngestKey != "" && key == env.IngestKey {
				next.ServeHTTP(w, r)
				return
			}
			// DB-stored keys — only admin scope can query
			scope := apikeys.ValidateWithScope(key)
			if scope == apikeys.ScopeAdmin {
				next.ServeHTTP(w, r)
				return
			}
			if scope == apikeys.ScopeIngest {
				http.Error(w, "Forbidden: ingest keys cannot access query endpoints", http.StatusForbidden)
				return
			}
		}
		forta.Protected(next.ServeHTTP)(w, r)
	})
}
