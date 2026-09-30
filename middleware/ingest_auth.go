package middleware

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/aidenappl/monitor-core/apikeys"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/telemetry"
)

// WithProject and GetProject move the resolved project slug through a request.
//
// They are thin pass-throughs to scope, which now OWNS the context key. They
// used to own it here, next to WithActor/GetActor, and that was right while
// ingest was the only consumer. It stopped being right when reads had to be
// scoped too: the event and analytics builders in services/ and the issue-event
// lookups in routes/ all need the same value, and none of them can import an
// HTTP middleware package to get at a WHERE clause.
//
// They are kept as wrappers rather than deleted because this is the vocabulary
// the ingest path already reads in, and because delegation cannot drift — there
// is exactly one key and exactly one place it is stored. Prefer scope directly
// in new code outside this package.
func WithProject(ctx context.Context, project string) context.Context {
	return scope.WithProject(ctx, project)
}

// GetProject returns the project slug resolved by the authenticating middleware.
//
// The SLUG travels rather than the numeric project id because the slug is what
// ClickHouse stores in the event's project column — on the write side the ingest
// handler stamps it, on the read side every builder compares against it — so
// carrying the id would mean a lookup per request to turn it back into the thing
// we already had.
func GetProject(ctx context.Context) (string, bool) {
	return scope.GetProject(ctx)
}

// IngestAuthMiddleware authenticates event ingestion requests.
// Accepts: the env-based MONITOR_API_KEY, or a DB-stored key whose scope is
// ingest. On success it injects the project the credential files under, which
// the ingest handler stamps onto every event in the body.
//
// Admin-scoped keys are refused here. They are query credentials — they live in
// dashboards, MCP configs and developer shells, which is a far wider blast
// radius than a service's ingest key, and nothing about reading events implies
// permission to forge them. The scope column already existed and this path
// simply ignored it, accepting any key of any scope via apikeys.Validate.
func IngestAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Api-Key")

		// Env-based master key (used by go-monitor backend services).
		//
		// It still grants ingest here AND full query in QueryAuthMiddleware, and
		// that breadth is deliberate for now: this key is presented by every
		// deployed service in the fleet, we cannot enumerate them from here, and
		// narrowing it blind would silently stop ingestion everywhere at once.
		// Follow-up, once API keys carry a project binding: issue the fleet real
		// ingest-scoped keys and demote this to a break-glass credential.
		//
		// It has no api_keys row and therefore no project binding of its own —
		// but "no binding" must not become "no tenant", so it stamps the default
		// project instead. That follows the Mimir "anonymous" and Loki "fake"
		// precedent: those systems assign a placeholder tenant rather than leave
		// the dimension null, because a null tenant is unqueryable by every
		// tenant-scoped filter — the rows are invisible right up until a second
		// project exists, at which point they quietly become someone else's data
		// with nothing left in them to say otherwise.
		if matchesEnvMasterKey(key) {
			next(w, r.WithContext(WithProject(r.Context(), env.DefaultProjectSlug)))
			return
		}

		// DB-stored keys — ingest scope only.
		//
		// Resolved as a full Identity rather than a bare scope because the key's
		// row is also where the tenant comes from: the single lookup that
		// authorises the write is the one that decides whose data it is, so the
		// answer to "may you write?" and the answer to "as whom?" cannot drift
		// apart or be resolved against different rows.
		rejection := map[string]any{"reason": "missing_key"}
		if key != "" {
			identity, ok := apikeys.ValidateWithIdentity(key)
			if ok && identity.Scope == apikeys.ScopeIngest {
				next(w, r.WithContext(WithProject(r.Context(), identity.ProjectSlug)))
				return
			}
			if ok {
				rejection = map[string]any{
					"reason":  "wrong_scope",
					"key_id":  identity.ID,
					"scope":   string(identity.Scope),
					"project": identity.ProjectSlug,
				}
			} else {
				rejection = map[string]any{"reason": "unknown_key", "key_prefix": presentedKeyPrefix(key)}
			}
		}

		// Coalesced per reason: a producer with a revoked key retries every
		// batch, and appleby-core's own telemetry is one of those producers when
		// its key is wrong — uncoalesced, each refusal would be an event that is
		// itself refused. The successful path emits nothing (see
		// LoggingMiddleware), so this is the only record of a refusal.
		ingestAuthRejected.Add(1)
		rejection["client_ip"] = GetClientIPFromContext(r.Context())
		rejection["outcome"] = "returned 401; the batch was not ingested"
		telemetry.WarnCoalesced(r.Context(), "ingest.auth.rejected:"+rejection["reason"].(string), "ingest.auth.rejected", nil, rejection)

		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	}
}

// ingestAuthRejected counts refused ingest credentials, for the periodic ingest
// summary.
var ingestAuthRejected atomic.Int64

// IngestAuthRejected is the number of ingest requests refused for their
// credential since boot.
func IngestAuthRejected() int64 {
	return ingestAuthRejected.Load()
}

// presentedKeyPrefix identifies an unrecognised key by the same 12-character
// prefix the admin UI shows for every stored key (api_keys.key_prefix), so a
// revoked key can be matched to its row. Anything too short to be a real key
// yields nothing: a 12-character "prefix" of a short value would be the whole
// value.
func presentedKeyPrefix(key string) string {
	if len(key) < 24 {
		return ""
	}
	return key[:12]
}
