package middleware

import (
	"net/http"

	"github.com/aidenappl/monitor-core/apikeys"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/structs"
)

// QueryAuthMiddleware authenticates query/analytics requests.
// Accepts: env-based master key, DB-stored admin-scoped keys, or a valid Monitor
// session (mon-access-token cookie or Bearer access JWT — validated exactly as
// SessionMiddleware does, including the SSO revocation checkpoint).
// Rejects: ingest-scoped keys (they are write-only).
func QueryAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Api-Key")
		if key != "" {
			// Env-based master key always has full access
			if env.IngestKey != "" && key == env.IngestKey {
				next.ServeHTTP(w, r.WithContext(WithActor(r.Context(), structs.SystemActor(EnvMasterKeyLabel))))
				return
			}
			// DB-stored keys — only admin scope can query
			identity, ok := apikeys.ValidateWithIdentity(key)
			if ok && identity.Scope == apikeys.ScopeAdmin {
				actor := structs.APIKeyActor(identity.ID, identity.Name)
				next.ServeHTTP(w, r.WithContext(WithActor(r.Context(), actor)))
				return
			}
			if ok && identity.Scope == apikeys.ScopeIngest {
				http.Error(w, "Forbidden: ingest keys cannot access query endpoints", http.StatusForbidden)
				return
			}
		}

		// Fall back to a Monitor-owned session. SessionMiddleware validates the
		// access JWT (cookie or Bearer), loads the active user, runs the SSO
		// checkpoint, injects the user into context, and 401s on failure.
		SessionMiddleware(next).ServeHTTP(w, r)
	})
}
