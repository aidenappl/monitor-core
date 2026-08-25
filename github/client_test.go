package github

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/structs"
)

func withToken(t *testing.T, token string) {
	t.Helper()
	previous := env.GitHubToken
	env.GitHubToken = token
	t.Cleanup(func() { env.GitHubToken = previous })
}

// withStubAPI points the client at a test server for the duration of a test.
func withStubAPI(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	srv := httptest.NewServer(handler)

	prevClient, prevBase := httpClient, apiBase
	httpClient = srv.Client()
	apiBase = srv.URL

	t.Cleanup(func() {
		httpClient, apiBase = prevClient, prevBase
		srv.Close()
	})
}

// TestFetchIsInertWhenUnconfigured pins that an install with no token stays
// silent rather than erroring on every link — "nothing to say", not a failure.
func TestFetchIsInertWhenUnconfigured(t *testing.T) {
	withToken(t, "")

	if Enabled() {
		t.Error("Enabled() = true with no token")
	}
	res, err := Fetch(context.Background(), Ref{Kind: structs.IssueLinkPullRequest, Owner: "a", Repo: "b", Number: 1})
	if err != nil {
		t.Errorf("Fetch returned an error when unconfigured: %v", err)
	}
	if res != nil {
		t.Errorf("Fetch returned %+v when unconfigured, want nil", res)
	}
}

func TestFetchPullRequestSendsCorrectRequest(t *testing.T) {
	withToken(t, "gh-token")

	var gotPath, gotAuth, gotAccept, gotVersion string
	withStubAPI(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotAccept = r.Header.Get("Accept")
		gotVersion = r.Header.Get("X-GitHub-Api-Version")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"title":"Fix the thing","state":"closed","merged":true,"user":{"login":"aidenappl"}}`))
	})

	res, err := Fetch(context.Background(), Ref{
		Kind: structs.IssueLinkPullRequest, Owner: "aidenappl", Repo: "monitor-core", Number: 42,
	})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	if gotPath != "/repos/aidenappl/monitor-core/pulls/42" {
		t.Errorf("path = %q", gotPath)
	}
	if gotAuth != "Bearer gh-token" {
		t.Errorf("Authorization header = %q", gotAuth)
	}
	if gotAccept != "application/vnd.github+json" {
		t.Errorf("Accept header = %q", gotAccept)
	}
	if gotVersion != "2022-11-28" {
		t.Errorf("X-GitHub-Api-Version = %q", gotVersion)
	}

	if res.Title != "Fix the thing" || res.State != "closed" || !res.Merged || res.Author != "aidenappl" {
		t.Errorf("resource = %+v", res)
	}
}

// TestFetchRateLimitIsDistinguishable asserts an exhausted quota reports as
// such. It means "back off", not "this link is wrong", and the two need
// different responses from an operator reading the logs.
func TestFetchRateLimitIsDistinguishable(t *testing.T) {
	withToken(t, "gh-token")

	withStubAPI(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", "1700000000")
		w.WriteHeader(http.StatusForbidden)
	})

	_, err := Fetch(context.Background(), Ref{
		Kind: structs.IssueLinkPullRequest, Owner: "a", Repo: "b", Number: 1,
	})
	if err == nil {
		t.Fatal("expected an error on a rate-limited response")
	}
	if !strings.Contains(err.Error(), "rate limit") {
		t.Errorf("error = %q, want it to mention the rate limit", err)
	}
}

// TestFetchErrorsDoNotEchoBody guards against leaking a response body into logs:
// GitHub error payloads can carry the request URL, and this error is logged.
func TestFetchErrorsDoNotEchoBody(t *testing.T) {
	withToken(t, "gh-token")

	const secretish = "ghp_thislookslikeatoken"
	withStubAPI(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Not Found","documentation_url":"` + secretish + `"}`))
	})

	_, err := Fetch(context.Background(), Ref{
		Kind: structs.IssueLinkPullRequest, Owner: "a", Repo: "b", Number: 1,
	})
	if err == nil {
		t.Fatal("expected an error on 404")
	}
	if strings.Contains(err.Error(), secretish) {
		t.Errorf("error echoed the response body: %q", err)
	}
}

func TestFirstLine(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "single line", in: "fix the thing", want: "fix the thing"},
		{name: "multi line", in: "fix the thing\n\nwith detail", want: "fix the thing"},
		{name: "crlf", in: "fix the thing\r\nmore", want: "fix the thing"},
		{name: "empty", in: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := firstLine(tt.in); got != tt.want {
				t.Errorf("firstLine(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
