package middleware

import (
	"net/http"

	"github.com/aidenappl/monitor-core/apikeys"
	"github.com/aidenappl/monitor-core/env"
)

// IngestAuthMiddleware authenticates event ingestion requests.
// Accepts: the env-based MONITOR_API_KEY, or any DB-stored key (admin or ingest scope).
func IngestAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Api-Key")

		// Env-based master key (used by go-monitor backend services)
		if env.IngestKey != "" && key == env.IngestKey {
			next(w, r)
			return
		}

		// DB-stored keys — both admin and ingest scopes can write events
		if key != "" && apikeys.Validate(key) {
			next(w, r)
			return
		}

		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	}
}
