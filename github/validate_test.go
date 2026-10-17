package github

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateOwner(t *testing.T) {
	for _, ok := range []string{"aidenappl", "TeamTrailblaze", "a", "my-org", strings.Repeat("a", 39)} {
		if err := ValidateOwner(ok); err != nil {
			t.Errorf("ValidateOwner(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"", ".", "..", "-org", "gitlab.com", "a/b", "my_org", "a b", strings.Repeat("a", 40), "https:"} {
		if err := ValidateOwner(bad); !errors.Is(err, ErrInvalidRepository) {
			t.Errorf("ValidateOwner(%q) = %v, want ErrInvalidRepository", bad, err)
		}
	}
}

func TestValidateRepoName(t *testing.T) {
	for _, ok := range []string{"monitor-core", "go.mod", "a_b", ".github", "x", strings.Repeat("a", 100)} {
		if err := ValidateRepoName(ok); err != nil {
			t.Errorf("ValidateRepoName(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"", ".", "..", "a/b", "r?tab=x", "a b", "<x>", strings.Repeat("a", 101)} {
		if err := ValidateRepoName(bad); !errors.Is(err, ErrInvalidRepository) {
			t.Errorf("ValidateRepoName(%q) = %v, want ErrInvalidRepository", bad, err)
		}
	}
}

// TestValidateBranch pins git check-ref-format's rules (ZONE-SETTINGS-13).
func TestValidateBranch(t *testing.T) {
	for _, ok := range []string{"main", "release/1.2", "feature/foo-bar_baz", "v1.0", "a@b", "dependabot/npm_and_yarn/x-1.2.3"} {
		if err := ValidateBranch(ok); err != nil {
			t.Errorf("ValidateBranch(%q) = %v, want nil", ok, err)
		}
	}
	tests := map[string]string{
		"main; <script>x</script> 🌿": "spaces",
		"":                           "empty",
		"-main":                      "start with '-'",
		"feature/":                   "start or end with '/'",
		"/main":                      "start or end with '/'",
		"a..b":                       "'..'",
		"a@{1}":                      "'@{'",
		"@":                          `"@"`,
		"main.lock":                  ".lock",
		"x/y.lock/z":                 ".lock",
		"main.":                      "end with '.'",
		".hidden":                    "starting with '.'",
		"a//b":                       "'//'",
		"a~1":                        `'~'`,
		"a^":                         `'^'`,
		"a:b":                        `':'`,
		"a?":                         `'?'`,
		"a*":                         `'*'`,
		"a[b":                        `'['`,
		`a\b`:                        `'\\'`,
		"a\tb":                       "control",
		"a\x7fb":                     "control",
		strings.Repeat("a", 256):     "at most 255",
	}
	for in, wantSubstr := range tests {
		err := ValidateBranch(in)
		if !errors.Is(err, ErrInvalidRepository) {
			t.Errorf("ValidateBranch(%q) = %v, want ErrInvalidRepository", in, err)
			continue
		}
		if !strings.Contains(err.Error(), wantSubstr) || !strings.Contains(err.Error(), "default_branch") {
			t.Errorf("ValidateBranch(%q) = %q, want a default_branch message mentioning %q", in, err, wantSubstr)
		}
	}
}
