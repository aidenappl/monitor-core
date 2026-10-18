package github

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// ErrInvalidRepository marks an owner, repository name or branch that GitHub
// itself would never accept. A mapping holding one is not harmless: monitor-web
// builds links from it (`../..` resolved to https://github.com/), and every
// shorthand `#123` on that service's issues resolves against it.
var ErrInvalidRepository = errors.New("invalid repository")

// ownerRe is GitHub's login/org rule: alphanumerics and hyphens, starting with
// an alphanumeric, at most 39 characters. Dots are never legal in an owner, so
// a pasted gitlab.com/... URL fails here rather than mapping owner "gitlab.com".
var ownerRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,38}$`)

// repoNameRe is GitHub's repository-name charset. "." and ".." match it and are
// refused separately — they are path segments, not names.
var repoNameRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}$`)

// MAX_BRANCH_LENGTH is service_repos.default_branch's VARCHAR(255).
const MAX_BRANCH_LENGTH = 255

// ValidateOwner checks a GitHub owner (user or organisation) name.
func ValidateOwner(owner string) error {
	if !ownerRe.MatchString(owner) {
		return fmt.Errorf("%w: owner %q must be a GitHub user or organisation name (letters, digits and hyphens, at most 39 characters)", ErrInvalidRepository, owner)
	}
	return nil
}

// ValidateRepoName checks a GitHub repository name.
func ValidateRepoName(repo string) error {
	if repo == "." || repo == ".." || !repoNameRe.MatchString(repo) {
		return fmt.Errorf("%w: repo %q must be a GitHub repository name (letters, digits, '.', '_' and '-', at most 100 characters)", ErrInvalidRepository, repo)
	}
	return nil
}

// ValidateBranch applies git check-ref-format's rules to a branch name, plus
// the column's length. Anything that builds a blob or tree URL from the branch
// depends on it being a real ref name.
func ValidateBranch(branch string) error {
	reason := branchProblem(branch)
	if reason == "" {
		return nil
	}
	return fmt.Errorf("%w: default_branch %s", ErrInvalidRepository, reason)
}

func branchProblem(b string) string {
	switch {
	case b == "":
		return "must not be empty"
	case !utf8.ValidString(b):
		return "must be valid UTF-8"
	case utf8.RuneCountInString(b) > MAX_BRANCH_LENGTH:
		return fmt.Sprintf("must be at most %d characters", MAX_BRANCH_LENGTH)
	case b == "@":
		return `must not be "@"`
	case strings.HasPrefix(b, "-"):
		return "must not start with '-'"
	case strings.HasPrefix(b, "/") || strings.HasSuffix(b, "/"):
		return "must not start or end with '/'"
	case strings.HasSuffix(b, "."):
		return "must not end with '.'"
	case strings.Contains(b, ".."):
		return "must not contain '..'"
	case strings.Contains(b, "@{"):
		return "must not contain '@{'"
	case strings.Contains(b, "//"):
		return "must not contain '//'"
	}
	for _, r := range b {
		if r < 0x20 || r == 0x7f {
			return "must not contain control characters"
		}
		if r == ' ' {
			return "must not contain spaces"
		}
		if strings.ContainsRune(`~^:?*[\`, r) {
			return fmt.Sprintf("must not contain %q", r)
		}
	}
	for _, part := range strings.Split(b, "/") {
		if strings.HasPrefix(part, ".") {
			return "must not have a path component starting with '.'"
		}
		if strings.HasSuffix(part, ".lock") {
			return "must not have a path component ending in '.lock'"
		}
	}
	return ""
}
