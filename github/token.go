package github

import (
	"os"
	"strings"

	"github.com/aidenappl/monitor-core/env"
)

// tokenEnvPrefix is the convention for per-owner tokens.
//
// Monitor watches services across more than one GitHub org, and a fine-grained
// PAT is scoped to a single one. A token that works for TeamTrailblaze cannot
// read aidenappl, so a single global token silently degrades every link outside
// its scope to a bare URL.
//
// Rather than a mapping table, the env var name is DERIVED from the owner:
//
//	TeamTrailblaze  ->  MON_GITHUB_TOKEN_TEAMTRAILBLAZE
//	aidenappl       ->  MON_GITHUB_TOKEN_AIDENAPPL
//
// so adding an org is one Keyring secret with a name you can work out, and no
// code change.
const tokenEnvPrefix = "MON_GITHUB_TOKEN_"

// TokenFor returns the API token to use for an owner.
//
// Resolution order:
//  1. MON_GITHUB_TOKEN_<OWNER> — the owner-specific token, name derived as above
//  2. env.GitHubToken — the configured default (MON_GITHUB_TOKEN_TRAILBLAZE)
//
// The fallback is what keeps a single-org install working with no extra
// configuration, and it is why the existing Trailblaze token keeps serving every
// repo until a more specific one is added. Returns "" when neither is set, which
// callers treat as "not configured" rather than as an error.
func TokenFor(owner string) string {
	if key := tokenEnvKey(owner); key != "" {
		if token := os.Getenv(key); token != "" {
			return token
		}
	}
	return env.GitHubToken
}

// tokenEnvKey derives the env var name for an owner, or "" if the owner has no
// usable characters.
//
// Only ASCII letters and digits survive, upper-cased. GitHub logins may contain
// hyphens (`Team-Trailblaze`), which are not valid in an env var name, so they
// are dropped rather than substituted — one owner must map to exactly one key,
// and a substitution scheme invites two owners colliding on it.
func tokenEnvKey(owner string) string {
	var b strings.Builder
	for _, c := range owner {
		switch {
		case c >= 'a' && c <= 'z':
			b.WriteRune(c - 32)
		case c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			b.WriteRune(c)
		}
	}
	if b.Len() == 0 {
		return ""
	}
	return tokenEnvPrefix + b.String()
}

// EnabledFor reports whether an owner has any usable token.
func EnabledFor(owner string) bool { return TokenFor(owner) != "" }
