package middleware

import (
	"net/http"

	"github.com/aidenappl/monitor-core/env"
)

// IngestAuthMiddleware authenticates event ingestion requests via the
// X-Ingest-Key header. Used only on the POST /v1/events route.
// If no INGEST_KEY is configured, all requests are allowed (development mode).
func IngestAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if env.IngestKey != "" && r.Header.Get("X-Api-Key") != env.IngestKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
