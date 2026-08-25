package github

import (
	"testing"

	"github.com/aidenappl/monitor-core/env"
)

func TestTokenEnvKey(t *testing.T) {
	tests := []struct {
		name  string
		owner string
		want  string
	}{
		{name: "simple", owner: "aidenappl", want: "MON_GITHUB_TOKEN_AIDENAPPL"},
		{name: "mixed case", owner: "TeamTrailblaze", want: "MON_GITHUB_TOKEN_TEAMTRAILBLAZE"},
		{name: "hyphen dropped", owner: "Team-Trailblaze", want: "MON_GITHUB_TOKEN_TEAMTRAILBLAZE"},
		{name: "digits kept", owner: "org2", want: "MON_GITHUB_TOKEN_ORG2"},
		{name: "empty", owner: "", want: ""},
		{name: "punctuation only", owner: "---", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tokenEnvKey(tt.owner); got != tt.want {
				t.Errorf("tokenEnvKey(%q) = %q, want %q", tt.owner, got, tt.want)
			}
		})
	}
}

// TestTokenForPrefersOwnerSpecific pins the resolution order. A fine-grained PAT
// is scoped to one org, so an owner with its own token must never be served the
// default — that request would 404 and look like a broken link.
func TestTokenForPrefersOwnerSpecific(t *testing.T) {
	previous := env.GitHubToken
	env.GitHubToken = "default-token"
	t.Cleanup(func() { env.GitHubToken = previous })

	t.Setenv("MON_GITHUB_TOKEN_AIDENAPPL", "appleby-token")

	if got := TokenFor("aidenappl"); got != "appleby-token" {
		t.Errorf("TokenFor(aidenappl) = %q, want the owner-specific token", got)
	}
	if got := TokenFor("TeamTrailblaze"); got != "default-token" {
		t.Errorf("TokenFor(TeamTrailblaze) = %q, want the default token", got)
	}
}

// TestTokenForFallsBackToDefault is what keeps a single-org install working
// with no per-owner configuration at all.
func TestTokenForFallsBackToDefault(t *testing.T) {
	previous := env.GitHubToken
	env.GitHubToken = "default-token"
	t.Cleanup(func() { env.GitHubToken = previous })

	if got := TokenFor("SomeOtherOrg"); got != "default-token" {
		t.Errorf("TokenFor = %q, want the default token", got)
	}
	if !EnabledFor("SomeOtherOrg") {
		t.Error("EnabledFor = false despite a default token being set")
	}
}

func TestTokenForUnconfigured(t *testing.T) {
	previous := env.GitHubToken
	env.GitHubToken = ""
	t.Cleanup(func() { env.GitHubToken = previous })

	if got := TokenFor("aidenappl"); got != "" {
		t.Errorf("TokenFor = %q, want empty when nothing is configured", got)
	}
	if EnabledFor("aidenappl") {
		t.Error("EnabledFor = true with no token configured")
	}
}
