package routes

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aidenappl/monitor-core/scope"
)

// TestUpsertServiceRepoRejectsInvalidRepository pins that a mapping GitHub could
// never host is refused with a 400 naming the field before anything is written
// (ZONE-SETTINGS-02 / -13). db.SQL is nil here, so reaching the query would
// panic: a 400 proves the refusal happened first.
func TestUpsertServiceRepoRejectsInvalidRepository(t *testing.T) {
	tests := []struct {
		name, body, wantMsg string
	}{
		{"dot-dot repository", `{"repository":"../.."}`, "owner"},
		{"dot repo", `{"repository":"aidenappl/."}`, "repo"},
		{"foreign host", `{"repository":"https://gitlab.com/a/b"}`, "owner"},
		{"explicit fields", `{"owner":"..","repo":"x"}`, "owner"},
		{"bad branch", `{"repository":"aidenappl/monitor-core","default_branch":"main; <script>x</script> 🌿"}`, "default_branch"},
		{"branch with ..", `{"repository":"aidenappl/monitor-core","default_branch":"a..b"}`, "default_branch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/v1/service-repos/api", strings.NewReader(tt.body))
			req = req.WithContext(scope.WithProject(req.Context(), "default"))
			rec := httptest.NewRecorder()
			issueRouter().ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body)
			}
			if !strings.Contains(rec.Body.String(), tt.wantMsg) {
				t.Errorf("body %s does not name %q", rec.Body, tt.wantMsg)
			}
		})
	}
}
