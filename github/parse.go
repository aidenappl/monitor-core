// Package github links Monitor issues to GitHub pull requests, issues and
// commits.
//
// GitHub is a FIELD ON THE ISSUE here, never the system of record. Status,
// comments and triage state live in MariaDB; this package only resolves a URL
// into structured coordinates and keeps a cached view of what GitHub says about
// it. Every observability platform surveyed (Sentry, GlitchTip, Highlight.io,
// Bugsink) converged on that split, and the failure mode of the alternative is
// severe: an outage or a rate limit at GitHub would take out issue triage.
//
// Accordingly every call in this package is best-effort. A failure degrades a
// link to a bare URL; it never fails the write that created it.
package github

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/aidenappl/monitor-core/structs"
)

// Ref is a parsed GitHub URL.
type Ref struct {
	Kind   structs.IssueLinkKind
	Owner  string
	Repo   string
	Number int    // pull requests and issues
	SHA    string // commits
}

// URL renders the canonical https URL for the ref. Used to normalise what gets
// stored, so the same PR linked as a .diff URL, with a query string, or with a
// #discussion fragment collapses to one row under the (issue_id, url) unique key
// rather than appearing as several distinct links.
func (r Ref) URL() string {
	base := "https://github.com/" + r.Owner + "/" + r.Repo
	switch r.Kind {
	case structs.IssueLinkPullRequest:
		return fmt.Sprintf("%s/pull/%d", base, r.Number)
	case structs.IssueLinkIssue:
		return fmt.Sprintf("%s/issues/%d", base, r.Number)
	case structs.IssueLinkCommit:
		return base + "/commit/" + r.SHA
	}
	return base
}

// ParseURL resolves a GitHub URL into structured coordinates.
//
// Deliberately strict: only github.com, only https/http, and only the three
// shapes below. An unrecognised URL is an error rather than a half-populated Ref,
// because a link stored without owner/repo/number can never be matched to a
// webhook delivery and would sit in the UI as a permanently stale chip.
//
//	https://github.com/{owner}/{repo}/pull/{n}
//	https://github.com/{owner}/{repo}/issues/{n}
//	https://github.com/{owner}/{repo}/commit/{sha}
//
// Trailing segments (/files, /commits, .diff) and any query or fragment are
// discarded — they address a view of the resource, not a different resource.
func ParseURL(raw string) (Ref, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Ref{}, fmt.Errorf("url is required")
	}

	u, err := url.Parse(raw)
	if err != nil {
		return Ref{}, fmt.Errorf("not a valid url: %w", err)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return Ref{}, fmt.Errorf("unsupported url scheme %q", u.Scheme)
	}

	host := strings.ToLower(u.Hostname())
	if host != "github.com" && host != "www.github.com" {
		return Ref{}, fmt.Errorf("not a github.com url: %s", host)
	}

	parts := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
	if len(parts) < 4 {
		return Ref{}, fmt.Errorf("url does not reference a pull request, issue or commit")
	}

	owner, repo, kind, ident := parts[0], parts[1], parts[2], parts[3]
	if owner == "" || repo == "" {
		return Ref{}, fmt.Errorf("url is missing an owner or repository")
	}
	// A .diff/.patch suffix addresses the same PR in another format.
	ident = strings.TrimSuffix(strings.TrimSuffix(ident, ".diff"), ".patch")

	switch kind {
	case "pull", "pulls":
		n, err := strconv.Atoi(ident)
		if err != nil || n <= 0 {
			return Ref{}, fmt.Errorf("invalid pull request number %q", ident)
		}
		return Ref{Kind: structs.IssueLinkPullRequest, Owner: owner, Repo: repo, Number: n}, nil

	case "issues":
		n, err := strconv.Atoi(ident)
		if err != nil || n <= 0 {
			return Ref{}, fmt.Errorf("invalid issue number %q", ident)
		}
		return Ref{Kind: structs.IssueLinkIssue, Owner: owner, Repo: repo, Number: n}, nil

	case "commit", "commits":
		if !isHexSHA(ident) {
			return Ref{}, fmt.Errorf("invalid commit sha %q", ident)
		}
		return Ref{Kind: structs.IssueLinkCommit, Owner: owner, Repo: repo, SHA: strings.ToLower(ident)}, nil
	}

	return Ref{}, fmt.Errorf("unsupported github url type %q", kind)
}

// isHexSHA reports whether s looks like a git object id — 7 to 40 hex digits,
// covering both abbreviated and full SHAs.
func isHexSHA(s string) bool {
	if len(s) < 7 || len(s) > 40 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}
