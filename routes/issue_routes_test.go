package routes

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

// issueRouter mirrors the issue routes as main.go mounts them, so path templates
// and method matching can be tested without a database.
//
// It deliberately reuses the REAL handler names: a renamed or unmounted handler
// fails to compile here, which is the point.
func issueRouter() *mux.Router {
	r := mux.NewRouter()
	v1 := r.PathPrefix("/v1").Subrouter()

	v1.HandleFunc("/issues", HandleListIssues).Methods(http.MethodGet)
	v1.HandleFunc("/issues/{id}", HandleGetIssue).Methods(http.MethodGet)
	v1.HandleFunc("/issues/{id}", HandleUpdateIssue).Methods(http.MethodPut)
	v1.HandleFunc("/issues/{id}/events", HandleGetIssueEvents).Methods(http.MethodGet)
	v1.HandleFunc("/issues/{id}/timeline", HandleGetIssueTimeline).Methods(http.MethodGet)
	v1.HandleFunc("/issues/{id}/history", HandleGetIssueHistory).Methods(http.MethodGet)
	v1.HandleFunc("/issues/{id}/comments", HandleAddIssueComment).Methods(http.MethodPost)
	v1.HandleFunc("/issues/{id}/comments/{commentID}", HandleEditIssueComment).Methods(http.MethodPatch)
	v1.HandleFunc("/issues/{id}/comments/{commentID}", HandleDeleteIssueComment).Methods(http.MethodDelete)
	v1.HandleFunc("/issues/{id}/links", HandleListIssueLinks).Methods(http.MethodGet)
	v1.HandleFunc("/issues/{id}/links", HandleCreateIssueLink).Methods(http.MethodPost)
	v1.HandleFunc("/issues/{id}/links/{linkID}", HandleDeleteIssueLink).Methods(http.MethodDelete)
	v1.HandleFunc("/service-repos", HandleListServiceRepos).Methods(http.MethodGet)
	v1.HandleFunc("/service-repos/{service}", HandleGetServiceRepo).Methods(http.MethodGet)
	v1.HandleFunc("/service-repos/{service}", HandleUpsertServiceRepo).Methods(http.MethodPut)
	v1.HandleFunc("/service-repos/{service}", HandleDeleteServiceRepo).Methods(http.MethodDelete)
	return r
}

// TestIssueRoutesAreMounted asserts every path resolves to a handler.
//
// A 404 here means an unmounted route, and a 405 means the method template is
// wrong — both are silent in production until someone calls the endpoint and
// gets an error that looks like a client bug.
func TestIssueRoutesAreMounted(t *testing.T) {
	routes := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/issues"},
		{http.MethodGet, "/v1/issues/abc"},
		{http.MethodPut, "/v1/issues/abc"},
		{http.MethodGet, "/v1/issues/abc/events"},
		{http.MethodGet, "/v1/issues/abc/timeline"},
		{http.MethodGet, "/v1/issues/abc/history"},
		{http.MethodPost, "/v1/issues/abc/comments"},
		{http.MethodPatch, "/v1/issues/abc/comments/1"},
		{http.MethodDelete, "/v1/issues/abc/comments/1"},
		{http.MethodGet, "/v1/issues/abc/links"},
		{http.MethodPost, "/v1/issues/abc/links"},
		{http.MethodDelete, "/v1/issues/abc/links/1"},
		{http.MethodGet, "/v1/service-repos"},
		{http.MethodGet, "/v1/service-repos/scraper-service"},
		{http.MethodPut, "/v1/service-repos/scraper-service"},
		{http.MethodDelete, "/v1/service-repos/scraper-service"},
	}

	router := issueRouter()
	for _, rt := range routes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			var match mux.RouteMatch
			req := httptest.NewRequest(rt.method, rt.path, nil)
			if !router.Match(req, &match) {
				t.Fatalf("no route matched %s %s", rt.method, rt.path)
			}
			if match.MatchErr == mux.ErrMethodMismatch {
				t.Fatalf("%s is mounted but not for %s", rt.path, rt.method)
			}
			if match.Handler == nil {
				t.Fatalf("%s %s matched with no handler", rt.method, rt.path)
			}
		})
	}
}

// TestIssueSubPathsDoNotCollideWithGetIssue guards the ordering hazard: an
// over-broad /issues/{id} template would swallow /issues/{id}/timeline and every
// sibling, so each would silently return an issue instead of its own resource.
func TestIssueSubPathsDoNotCollideWithGetIssue(t *testing.T) {
	router := issueRouter()

	subPaths := []string{"timeline", "history", "links", "events"}
	for _, sub := range subPaths {
		t.Run(sub, func(t *testing.T) {
			var match mux.RouteMatch
			req := httptest.NewRequest(http.MethodGet, "/v1/issues/abc/"+sub, nil)
			if !router.Match(req, &match) {
				t.Fatalf("no route matched /v1/issues/abc/%s", sub)
			}
			vars := match.Vars
			if vars["id"] != "abc" {
				t.Errorf(`id = %q, want "abc"`, vars["id"])
			}
		})
	}
}

// TestIssueRouteVarsAreExtracted pins the path-variable names the handlers read.
// A template using {comment_id} while the handler reads {commentID} compiles,
// mounts, and fails only at runtime with "invalid comment id".
func TestIssueRouteVarsAreExtracted(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		want   map[string]string
	}{
		{
			name: "comment id", method: http.MethodPatch, path: "/v1/issues/iss-1/comments/42",
			want: map[string]string{"id": "iss-1", "commentID": "42"},
		},
		{
			name: "link id", method: http.MethodDelete, path: "/v1/issues/iss-1/links/7",
			want: map[string]string{"id": "iss-1", "linkID": "7"},
		},
		{
			name: "service", method: http.MethodPut, path: "/v1/service-repos/auth-service-v2",
			want: map[string]string{"service": "auth-service-v2"},
		},
	}

	router := issueRouter()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var match mux.RouteMatch
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if !router.Match(req, &match) {
				t.Fatalf("no route matched %s %s", tt.method, tt.path)
			}
			for key, want := range tt.want {
				if got := match.Vars[key]; got != want {
					t.Errorf("var %q = %q, want %q", key, got, want)
				}
			}
		})
	}
}

// TestParseTimeParam covers the from/to filter parsing shared by the list and
// history endpoints.
func TestParseTimeParam(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{name: "rfc3339", in: "2026-08-01T00:00:00Z"},
		{name: "rfc3339 with offset", in: "2026-08-01T00:00:00+01:00"},
		{name: "unix seconds", in: "1756684800"},
		{name: "empty", in: "", wantErr: true},
		{name: "prose", in: "yesterday", wantErr: true},
		{name: "date only", in: "2026-08-01", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTimeParam(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseTimeParam(%q) = %v, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseTimeParam(%q) failed: %v", tt.in, err)
			}
			if got.IsZero() {
				t.Error("parsed to the zero time")
			}
		})
	}
}

// TestParseRepository covers the PUT /service-repos body, which accepts either a
// pasted URL or the shorthand.
func TestParseRepository(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		wantOwner string
		wantRepo  string
		wantErr   bool
	}{
		{name: "shorthand", in: "TeamTrailblaze/auth-service", wantOwner: "TeamTrailblaze", wantRepo: "auth-service"},
		{name: "https url", in: "https://github.com/TeamTrailblaze/auth-service", wantOwner: "TeamTrailblaze", wantRepo: "auth-service"},
		{name: "url with trailing slash", in: "https://github.com/TeamTrailblaze/auth-service/", wantOwner: "TeamTrailblaze", wantRepo: "auth-service"},
		{name: "git suffix", in: "https://github.com/TeamTrailblaze/auth-service.git", wantOwner: "TeamTrailblaze", wantRepo: "auth-service"},
		{name: "deep url resolves to its repo", in: "https://github.com/TeamTrailblaze/auth-service/pull/42", wantOwner: "TeamTrailblaze", wantRepo: "auth-service"},
		{name: "www", in: "https://www.github.com/TeamTrailblaze/auth-service", wantOwner: "TeamTrailblaze", wantRepo: "auth-service"},

		{name: "owner only", in: "TeamTrailblaze", wantErr: true},
		{name: "empty", in: "", wantErr: true},
		{name: "trailing slash only", in: "TeamTrailblaze/", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, repo, err := parseRepository(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseRepository(%q) = %s/%s, want an error", tt.in, owner, repo)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRepository(%q) failed: %v", tt.in, err)
			}
			if owner != tt.wantOwner || repo != tt.wantRepo {
				t.Errorf("= %s/%s, want %s/%s", owner, repo, tt.wantOwner, tt.wantRepo)
			}
		})
	}
}

// TestIssueQueryParamsRejectsBadInput asserts filters are validated before any
// query is built — sort in particular reaches SQL as an identifier rather than a
// bound parameter, so it is an injection boundary.
func TestIssueQueryParamsRejectsBadInput(t *testing.T) {
	// Values are URL-encoded, as a real client would send them — a raw space in a
	// request target is not a query string, it is a malformed request line.
	bad := []string{
		"?sort=created_at",
		"?sort=" + url.QueryEscape("last_seen;DROP TABLE monitor.issues--"),
		"?sort=" + url.QueryEscape("1 UNION SELECT secret_value FROM secrets"),
		"?status=backlog",
		"?status=nonsense",
		"?has_pr=maybe",
		"?assignee=abc",
		"?from=yesterday",
		"?to=soon",
		"?limit=lots",
		"?offset=some",
	}

	for _, q := range bad {
		t.Run(q, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/issues"+q, nil)
			if _, err := issueQueryParams(req); err == nil {
				t.Errorf("issueQueryParams accepted %q", q)
			}
		})
	}
}

func TestIssueQueryParamsAcceptsValidInput(t *testing.T) {
	good := []string{
		"",
		"?status=in_progress",
		"?status=unresolved&service=scraper-service",
		"?sort=occurrences&order=asc",
		"?sort=first_seen",
		"?has_pr=true",
		"?assignee=none",
		"?assignee=42",
		"?q=timeout",
		"?from=2026-08-01T00:00:00Z&to=2026-08-25T00:00:00Z",
		"?limit=100&offset=200",
	}

	for _, q := range good {
		t.Run("/v1/issues"+q, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/issues"+q, nil)
			if _, err := issueQueryParams(req); err != nil {
				t.Errorf("issueQueryParams rejected %q: %v", q, err)
			}
		})
	}
}

// TestIssueQueryParamsDefaults pins that a bare request sorts newest-first,
// which is what the errors page has always shown.
func TestIssueQueryParamsDefaults(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/issues", nil)
	got, err := issueQueryParams(req)
	if err != nil {
		t.Fatalf("issueQueryParams failed: %v", err)
	}
	if !got.Descending {
		t.Error("default order is ascending; the list should default to newest first")
	}
	if got.Status != nil || got.Service != nil || got.Search != nil || got.HasPR != nil {
		t.Error("a bare request set a filter it was not given")
	}
	if !strings.EqualFold(string(got.Sort), "") && !got.Sort.IsValid() {
		t.Errorf("default sort %q is not valid", got.Sort)
	}
}
