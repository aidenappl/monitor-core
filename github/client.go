package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/structs"
)

// apiBase is GitHub's REST root. A var only so tests can point at a stub
// server — there is no configuration path to change it, since this integration
// targets github.com and a GHES deployment would need a different auth story.
var apiBase = "https://api.github.com"

// requestTimeout bounds a single API call. Short on purpose — this runs inline
// on a user's link-creation request, and a slow GitHub must not hold that open.
// A timeout degrades the link to a bare URL, which is a fine outcome.
const requestTimeout = 8 * time.Second

// maxResponseBytes caps how much of a response body is read. GitHub's payloads
// are small; anything larger is a proxy error page or a compromised endpoint,
// and reading it unbounded would be a memory DoS.
const maxResponseBytes = 2 << 20 // 2 MiB

var httpClient = &http.Client{Timeout: requestTimeout}

// Enabled reports whether API calls are configured. Callers should check this
// before fetching so an unconfigured install stays silent rather than logging a
// failure for every link.
func Enabled() bool { return env.GitHubToken != "" }

// Resource is the live state of a linked GitHub object.
type Resource struct {
	Title  string
	State  string // "open" | "closed" (and "merged" is State=closed + Merged=true)
	Merged bool
	Author string
}

// Fetch retrieves the current state of whatever a ref points at.
//
// Returns (nil, nil) when the integration is not configured — "nothing to say"
// rather than an error, so callers need not special-case it.
func Fetch(ctx context.Context, ref Ref) (*Resource, error) {
	if !Enabled() {
		return nil, nil
	}

	switch ref.Kind {
	case structs.IssueLinkPullRequest:
		return fetchPullRequest(ctx, ref)
	case structs.IssueLinkIssue:
		return fetchIssue(ctx, ref)
	case structs.IssueLinkCommit:
		return fetchCommit(ctx, ref)
	}
	return nil, fmt.Errorf("unsupported link kind %q", ref.Kind)
}

type ghUser struct {
	Login string `json:"login"`
}

type ghPullRequest struct {
	Title  string `json:"title"`
	State  string `json:"state"`
	Merged bool   `json:"merged"`
	User   ghUser `json:"user"`
}

type ghIssue struct {
	Title string `json:"title"`
	State string `json:"state"`
	User  ghUser `json:"user"`
}

type ghCommit struct {
	Commit struct {
		Message string `json:"message"`
		Author  struct {
			Name string `json:"name"`
		} `json:"author"`
	} `json:"commit"`
	Author ghUser `json:"author"`
}

func fetchPullRequest(ctx context.Context, ref Ref) (*Resource, error) {
	var pr ghPullRequest
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d", ref.Owner, ref.Repo, ref.Number)
	if err := get(ctx, path, &pr); err != nil {
		return nil, err
	}
	return &Resource{Title: pr.Title, State: pr.State, Merged: pr.Merged, Author: pr.User.Login}, nil
}

func fetchIssue(ctx context.Context, ref Ref) (*Resource, error) {
	var issue ghIssue
	path := fmt.Sprintf("/repos/%s/%s/issues/%d", ref.Owner, ref.Repo, ref.Number)
	if err := get(ctx, path, &issue); err != nil {
		return nil, err
	}
	return &Resource{Title: issue.Title, State: issue.State, Author: issue.User.Login}, nil
}

func fetchCommit(ctx context.Context, ref Ref) (*Resource, error) {
	var commit ghCommit
	path := fmt.Sprintf("/repos/%s/%s/commits/%s", ref.Owner, ref.Repo, ref.SHA)
	if err := get(ctx, path, &commit); err != nil {
		return nil, err
	}
	// A commit has no state, and its "title" is the first line of the message.
	author := commit.Author.Login
	if author == "" {
		author = commit.Commit.Author.Name
	}
	return &Resource{Title: firstLine(commit.Commit.Message), Author: author}, nil
}

// get performs an authenticated GET and decodes the JSON body.
func get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+path, nil)
	if err != nil {
		return fmt.Errorf("failed to build github request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+env.GitHubToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "monitor-core")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("github request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Surface rate limiting distinctly — it is the one failure that means
		// "back off", not "this link is wrong". GitHub's REST limit is 5,000/hr
		// authenticated; this integration is webhook-driven and should never
		// approach it, so hitting this means something is polling in a loop.
		if resp.StatusCode == http.StatusForbidden && resp.Header.Get("X-RateLimit-Remaining") == "0" {
			reset := resp.Header.Get("X-RateLimit-Reset")
			return fmt.Errorf("github rate limit exhausted (resets at %s)", reset)
		}
		// Never echo the response body: it can contain the request URL and
		// headers, and this error is logged.
		return fmt.Errorf("github returned %d for %s", resp.StatusCode, path)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("failed to read github response: %w", err)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("failed to decode github response: %w", err)
	}
	return nil
}

func firstLine(s string) string {
	for i, c := range s {
		if c == '\n' || c == '\r' {
			return s[:i]
		}
	}
	return s
}
