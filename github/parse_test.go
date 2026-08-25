package github

import (
	"testing"

	"github.com/aidenappl/monitor-core/structs"
)

func TestParseURL(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantKind   structs.IssueLinkKind
		wantOwner  string
		wantRepo   string
		wantNumber int
		wantSHA    string
		wantErr    bool
	}{
		{
			name:     "pull request",
			raw:      "https://github.com/aidenappl/monitor-core/pull/42",
			wantKind: structs.IssueLinkPullRequest, wantOwner: "aidenappl", wantRepo: "monitor-core", wantNumber: 42,
		},
		{
			name:     "pull request with trailing view segment",
			raw:      "https://github.com/aidenappl/monitor-core/pull/42/files",
			wantKind: structs.IssueLinkPullRequest, wantOwner: "aidenappl", wantRepo: "monitor-core", wantNumber: 42,
		},
		{
			name:     "pull request diff",
			raw:      "https://github.com/aidenappl/monitor-core/pull/42.diff",
			wantKind: structs.IssueLinkPullRequest, wantOwner: "aidenappl", wantRepo: "monitor-core", wantNumber: 42,
		},
		{
			name:     "pull request with query and fragment",
			raw:      "https://github.com/aidenappl/monitor-core/pull/42?w=1#discussion_r1",
			wantKind: structs.IssueLinkPullRequest, wantOwner: "aidenappl", wantRepo: "monitor-core", wantNumber: 42,
		},
		{
			name:     "issue",
			raw:      "https://github.com/TeamTrailblaze/scraper-service/issues/7",
			wantKind: structs.IssueLinkIssue, wantOwner: "TeamTrailblaze", wantRepo: "scraper-service", wantNumber: 7,
		},
		{
			name:     "commit",
			raw:      "https://github.com/aidenappl/monitor-core/commit/a7a6524ff1e2d3c4b5a697889900112233445566",
			wantKind: structs.IssueLinkCommit, wantOwner: "aidenappl", wantRepo: "monitor-core",
			wantSHA: "a7a6524ff1e2d3c4b5a697889900112233445566",
		},
		{
			name:     "abbreviated commit sha",
			raw:      "https://github.com/aidenappl/monitor-core/commit/a7a6524",
			wantKind: structs.IssueLinkCommit, wantOwner: "aidenappl", wantRepo: "monitor-core", wantSHA: "a7a6524",
		},
		{
			name:     "uppercase sha is normalised",
			raw:      "https://github.com/aidenappl/monitor-core/commit/A7A6524",
			wantKind: structs.IssueLinkCommit, wantOwner: "aidenappl", wantRepo: "monitor-core", wantSHA: "a7a6524",
		},
		{
			name:     "www host",
			raw:      "https://www.github.com/aidenappl/monitor-core/pull/1",
			wantKind: structs.IssueLinkPullRequest, wantOwner: "aidenappl", wantRepo: "monitor-core", wantNumber: 1,
		},

		{name: "empty", raw: "", wantErr: true},
		{name: "not a url", raw: "://nope", wantErr: true},
		{name: "not github", raw: "https://gitlab.com/a/b/pull/1", wantErr: true},
		{name: "lookalike host", raw: "https://github.com.evil.example/a/b/pull/1", wantErr: true},
		{name: "wrong scheme", raw: "ftp://github.com/a/b/pull/1", wantErr: true},
		{name: "repo root", raw: "https://github.com/aidenappl/monitor-core", wantErr: true},
		{name: "unsupported type", raw: "https://github.com/aidenappl/monitor-core/releases/tag/v1", wantErr: true},
		{name: "non-numeric pr", raw: "https://github.com/a/b/pull/abc", wantErr: true},
		{name: "zero pr number", raw: "https://github.com/a/b/pull/0", wantErr: true},
		{name: "negative pr number", raw: "https://github.com/a/b/pull/-1", wantErr: true},
		{name: "non-hex sha", raw: "https://github.com/a/b/commit/zzzzzzz", wantErr: true},
		{name: "too-short sha", raw: "https://github.com/a/b/commit/abc", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := ParseURL(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseURL(%q) = %+v, want an error", tt.raw, ref)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseURL(%q) returned an unexpected error: %v", tt.raw, err)
			}
			if ref.Kind != tt.wantKind {
				t.Errorf("Kind = %q, want %q", ref.Kind, tt.wantKind)
			}
			if ref.Owner != tt.wantOwner {
				t.Errorf("Owner = %q, want %q", ref.Owner, tt.wantOwner)
			}
			if ref.Repo != tt.wantRepo {
				t.Errorf("Repo = %q, want %q", ref.Repo, tt.wantRepo)
			}
			if ref.Number != tt.wantNumber {
				t.Errorf("Number = %d, want %d", ref.Number, tt.wantNumber)
			}
			if ref.SHA != tt.wantSHA {
				t.Errorf("SHA = %q, want %q", ref.SHA, tt.wantSHA)
			}
		})
	}
}

// TestParseURLCanonicalises pins the normalisation that makes the
// (issue_id, url) unique key meaningful: several spellings of one PR must
// collapse to a single stored row rather than appearing as separate links.
func TestParseURLCanonicalises(t *testing.T) {
	variants := []string{
		"https://github.com/aidenappl/monitor-core/pull/42",
		"https://github.com/aidenappl/monitor-core/pull/42/files",
		"https://github.com/aidenappl/monitor-core/pull/42.diff",
		"https://github.com/aidenappl/monitor-core/pull/42?w=1",
		"https://www.github.com/aidenappl/monitor-core/pull/42",
		"http://github.com/aidenappl/monitor-core/pull/42",
	}

	const want = "https://github.com/aidenappl/monitor-core/pull/42"
	for _, v := range variants {
		t.Run(v, func(t *testing.T) {
			ref, err := ParseURL(v)
			if err != nil {
				t.Fatalf("ParseURL(%q) failed: %v", v, err)
			}
			if got := ref.URL(); got != want {
				t.Errorf("canonical URL = %q, want %q", got, want)
			}
		})
	}
}
