package tools

import (
	"fmt"
	"regexp"
)

// Slug validation for the tenancy registry (zones and projects).
//
// This lives in tools/ rather than next to structs.Zone/structs.Project for two
// reasons. The rule is identical for both entities and is a property of neither,
// so hanging it off one struct would make the other import it sideways. And the
// reserved list below encodes knowledge of the FRONTEND's route table, not of
// the data model — the same shape of cross-cutting, security-adjacent input
// check that ValidateExternalURL in this package already is.
//
// The format half of the rule is enforced a second time in the database, by the
// ck_zones_slug_format / ck_projects_slug_format CHECK constraints in
// db/migrations/116_create_registry.sql, so a row written outside the query
// layer still cannot violate it. The two encodings must be kept in step; that
// file carries the matching note. The RESERVED half is deliberately NOT in the
// DDL: it changes whenever the frontend adds a route, and a schema migration is
// the wrong place to track that.

// Slug length bounds. 3 is long enough to be typed and read; 30 keeps a slug
// inside a DNS label (63) even once it is combined with a prefix or suffix, and
// matches the VARCHAR(30) the columns are declared as.
const (
	SLUG_MIN_LENGTH = 3
	SLUG_MAX_LENGTH = 30
)

// SlugPattern is the GCP project-id rule in spirit: lowercase letters, digits
// and hyphens, starting with a letter and not ending with a hyphen.
//
// Length is checked separately rather than folded into the pattern as a repeat
// count, so the bounds have exactly one definition (the constants above) and a
// too-short slug reports its actual length instead of "malformed". The DDL has
// to fold them together — SQL cannot compose the two checks — which is why the
// counts appear there and not here.
//
// Each restriction pays for itself downstream: no uppercase, so a slug survives
// a case-insensitive DNS label and a case-sensitive path segment identically; no
// underscore or dot, so it needs no quoting in a hostname; no leading digit, so
// it can never be mistaken for an id by a route that accepts both; no trailing
// hyphen, so `zone-` never composes into `zone--project` and back-parses wrong.
var SlugPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*[a-z0-9]$`)

// reservedSlugs are names a zone or project may not take.
//
// The frontend resolves STATIC route segments before dynamic ones. A project
// slugged `settings` would therefore be shadowed by the app's own /settings page
// and be permanently unreachable — with no error raised anywhere: the row exists,
// the API serves it, and the only symptom is a link that lands on the wrong
// screen. The failure is silent by construction, so it has to be prevented at
// creation time, which is the only moment the name is still negotiable (slugs
// are immutable and never reusable).
//
// Three groups, kept together because they are one namespace from the URL's
// point of view:
//
//	frontend  — static segments of the monitor-web route table
//	api       — path segments rooted on the API host alongside a tenant segment
//	sentinels — registry nouns, and the strings a frontend bug stringifies into
//	            a URL when a value is missing (`undefined` in a path is a bug
//	            report, and must never resolve to a real project)
//
// A few entries (`_next`) are already unreachable through SlugPattern, which
// rejects a leading underscore. They are listed anyway so the set reads as the
// namespace being defended rather than as the subset the regex happens to admit.
var reservedSlugs = map[string]bool{
	// frontend
	"monitor":       true,
	"auth":          true,
	"admin":         true,
	"api":           true,
	"login":         true,
	"logout":        true,
	"register":      true,
	"pending":       true,
	"unauthorized":  true,
	"settings":      true,
	"errors":        true,
	"live":          true,
	"analytics":     true,
	"dashboard":     true,
	"dashboards":    true,
	"alerts":        true,
	"notifications": true,
	"performance":   true,
	"static":        true,
	"_next":         true,
	"favicon":       true,
	"assets":        true,
	"public":        true,

	// api
	"health":   true,
	"ready":    true,
	"v1":       true,
	"webhooks": true,
	"events":   true,
	"issues":   true,
	"services": true,
	"users":    true,

	// sentinels
	"zones":     true,
	"projects":  true,
	"new":       true,
	"me":        true,
	"null":      true,
	"undefined": true,
}

// IsReservedSlug reports whether a name is claimed by a static route.
func IsReservedSlug(slug string) bool {
	return reservedSlugs[slug]
}

// ValidateSlug checks a zone or project slug against the format rule and the
// reserved list. It does NOT normalize: a slug is immutable and never reusable,
// so quietly rewriting the operator's chosen identifier into something else is
// worse than refusing it — the corrected name would then be permanent and
// unexplained. Callers trim surrounding whitespace (env vars collect it) and
// otherwise pass the value through as given.
func ValidateSlug(slug string) error {
	if slug == "" {
		return fmt.Errorf("slug is required")
	}
	if len(slug) < SLUG_MIN_LENGTH || len(slug) > SLUG_MAX_LENGTH {
		return fmt.Errorf("slug must be between %d and %d characters, got %d", SLUG_MIN_LENGTH, SLUG_MAX_LENGTH, len(slug))
	}
	if !SlugPattern.MatchString(slug) {
		return fmt.Errorf("slug %q must contain only lowercase letters, digits and hyphens, start with a letter and not end with a hyphen", slug)
	}
	if IsReservedSlug(slug) {
		return fmt.Errorf("slug %q is reserved and would be shadowed by an existing route", slug)
	}
	return nil
}
