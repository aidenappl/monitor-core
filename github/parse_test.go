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

// TestParseRefShorthand covers the resolver that makes the service→repository
// mapping pay off: a bare "#42" on an error in auth-service-v2 lands on that
// service's repo without anyone restating it.
func TestParseRefShorthand(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		fallbackOwner string
		fallbackRepo  string
		wantOwner     string
		wantRepo      string
		wantNumber    int
		wantKind      structs.IssueLinkKind
		wantErr       bool
	}{
		{
			name:  "hash number uses the fallback repo",
			input: "#42", fallbackOwner: "TeamTrailblaze", fallbackRepo: "auth-service",
			wantOwner: "TeamTrailblaze", wantRepo: "auth-service", wantNumber: 42,
			wantKind: structs.IssueLinkPullRequest,
		},
		{
			name:  "bare number uses the fallback repo",
			input: "42", fallbackOwner: "TeamTrailblaze", fallbackRepo: "auth-service",
			wantOwner: "TeamTrailblaze", wantRepo: "auth-service", wantNumber: 42,
			wantKind: structs.IssueLinkPullRequest,
		},
		{
			name:  "explicit repo overrides the fallback",
			input: "aidenappl/monitor-core#7", fallbackOwner: "TeamTrailblaze", fallbackRepo: "auth-service",
			wantOwner: "aidenappl", wantRepo: "monitor-core", wantNumber: 7,
			wantKind: structs.IssueLinkPullRequest,
		},
		{
			name:          "full url ignores the fallback entirely",
			input:         "https://github.com/aidenappl/monitor-core/issues/9",
			fallbackOwner: "TeamTrailblaze", fallbackRepo: "auth-service",
			wantOwner: "aidenappl", wantRepo: "monitor-core", wantNumber: 9,
			wantKind: structs.IssueLinkIssue,
		},
		{
			name:      "host-relative url still parses",
			input:     "github.com/aidenappl/monitor-core/pull/3",
			wantOwner: "aidenappl", wantRepo: "monitor-core", wantNumber: 3,
			wantKind: structs.IssueLinkPullRequest,
		},

		// A shorthand with no mapping must REFUSE rather than guess — attaching a
		// link to the wrong repository is worse than declining to attach one.
		{name: "shorthand with no fallback is refused", input: "#42", wantErr: true},
		{name: "bare number with no fallback is refused", input: "42", wantErr: true},
		{name: "partial fallback is refused", input: "#42", fallbackOwner: "TeamTrailblaze", wantErr: true},

		{name: "empty", input: "", wantErr: true},
		{name: "not a number", input: "#abc", fallbackOwner: "o", fallbackRepo: "r", wantErr: true},
		{name: "zero", input: "#0", fallbackOwner: "o", fallbackRepo: "r", wantErr: true},
		{name: "prose", input: "see the PR", fallbackOwner: "o", fallbackRepo: "r", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := ParseRef(tt.input, tt.fallbackOwner, tt.fallbackRepo)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseRef(%q) = %+v, want an error", tt.input, ref)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseRef(%q) failed: %v", tt.input, err)
			}
			if ref.Owner != tt.wantOwner || ref.Repo != tt.wantRepo {
				t.Errorf("repo = %s/%s, want %s/%s", ref.Owner, ref.Repo, tt.wantOwner, tt.wantRepo)
			}
			if ref.Number != tt.wantNumber {
				t.Errorf("Number = %d, want %d", ref.Number, tt.wantNumber)
			}
			if ref.Kind != tt.wantKind {
				t.Errorf("Kind = %q, want %q", ref.Kind, tt.wantKind)
			}
		})
	}
}
