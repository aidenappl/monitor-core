package routes

import (
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/structs"
)

// TestEnrichIssuesResolvesRepositoriesInTheCallersProject pins that the
// repository attached to each issue comes from the page's own project.
//
// A service name is unique within one project and nowhere else. The unscoped
// lookup this replaced returned a service mapped ONLY in another project, so a
// tenant whose `api` was unmapped got the other tenant's owner/repo on its issue
// and a working link to it behind "view source" — a leak that looks like a
// feature. The bulk read must bind the caller's project, and whatever it does
// not return must stay unmapped rather than be filled from elsewhere.
func TestEnrichIssuesResolvesRepositoriesInTheCallersProject(t *testing.T) {
	tests := []struct {
		name    string
		project string
		// mapped is what the scoped read returns for this project: service → repo.
		mapped map[string]string
		// want is the repository each issue ends up with ("" = none).
		want map[string]string
	}{
		{
			name:    "mapped service resolves, unmapped stays empty",
			project: "atlas",
			mapped:  map[string]string{"api": "api-server"},
			want:    map[string]string{"iss-api": "api-server", "iss-web": ""},
		},
		{
			// The same services under another tenant bind THAT tenant, which is
			// what makes "api" here and "api" there two different mappings.
			name:    "another project binds its own name",
			project: "johnnies",
			mapped:  map[string]string{"web": "johnnies-web"},
			want:    map[string]string{"iss-api": "", "iss-web": "johnnies-web"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := withMockDB(t)

			list := []structs.Issue{
				{ID: "iss-api", Project: tt.project, Service: "api"},
				{ID: "iss-web", Project: tt.project, Service: "web"},
			}

			mock.ExpectQuery(`FROM monitor.issue_links WHERE monitor.issue_links.issue_id IN \(\?,\?\)`).
				WithArgs("iss-api", "iss-web").
				WillReturnRows(sqlmock.NewRows([]string{"id"}))

			repos := sqlmock.NewRows([]string{
				"project", "service", "provider", "owner", "repo",
				"default_branch", "inserted_at", "updated_at",
			})
			now := time.Now()
			for service, repo := range tt.mapped {
				repos.AddRow(tt.project, service, "github", "acme", repo, "main", now, now)
			}
			mock.ExpectQuery(`FROM monitor.service_repos WHERE monitor.service_repos.service IN \(\?,\?\) AND monitor.service_repos.project = \?`).
				WithArgs("api", "web", tt.project).
				WillReturnRows(repos)

			if err := enrichIssues(tt.project, list); err != nil {
				t.Fatalf("enrichIssues: %v", err)
			}

			for _, issue := range list {
				want := tt.want[issue.ID]
				switch {
				case want == "" && issue.Repository != nil:
					t.Errorf("%s got repository %+v, want none", issue.ID, *issue.Repository)
				case want != "" && (issue.Repository == nil || issue.Repository.Repo != want):
					t.Errorf("%s repository = %+v, want %s", issue.ID, issue.Repository, want)
				case want != "" && issue.Repository.Project != tt.project:
					t.Errorf("%s repository belongs to project %q, want %q", issue.ID, issue.Repository.Project, tt.project)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("the repository lookup is not bound to the caller's project: %v", err)
			}
		})
	}
}
