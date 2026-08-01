package routes

import (
	"testing"
	"time"

	"github.com/aidenappl/monitor-core/structs"
)

func strp(s string) *string { return &s }

// TestSafeColor is the CSS-injection guard.
//
// ⚠️ These values end up inside CSS on an UNAUTHENTICATED page. The admin API
// validates them on write, and this re-validates on render — because validating
// only on write trusts that every row ever written passed through that check,
// which is false for a row inserted before the validation existed, by a
// migration, or by hand.
//
// Anything not exactly #rrggbb is dropped to null rather than escaped. Refusing
// is checkable; escaping CSS correctly is not something to reinvent on a login
// page.
func TestSafeColor(t *testing.T) {
	tests := []struct {
		name  string
		in    *string
		allow bool
		why   string
	}{
		{"nil_is_nil", nil, false, ""},
		{"lowercase_hex", strp("#1a2b3c"), true, ""},
		{"uppercase_hex", strp("#AABBCC"), true, ""},
		{"mixed_case", strp("#aAbBcC"), true, ""},

		{"shorthand_is_refused", strp("#abc"), false, "three-digit shorthand is valid CSS but is not the shape validated on write; accepting a second shape widens what has to be reasoned about"},
		{"missing_hash", strp("1a2b3c"), false, ""},
		{"named_colour", strp("red"), false, "a named colour is not a hex triple and opens the door to arbitrary identifiers"},
		{"too_long", strp("#1a2b3c4"), false, ""},
		{"too_short", strp("#1a2b3"), false, ""},
		{"non_hex_digits", strp("#zzzzzz"), false, ""},
		{"empty", strp(""), false, ""},
		{"whitespace_padded", strp(" #1a2b3c "), false, "the regexp is anchored; a padded value would otherwise smuggle in leading content"},

		{
			name:  "css_injection_closing_the_declaration",
			in:    strp("#000; background: url(https://evil.example/x)"),
			allow: false,
			why:   "THE ATTACK. If this reached a stylesheet it would issue a request from every visitor to the login page.",
		},
		{
			name:  "css_injection_with_expression",
			in:    strp("red;}body{display:none"),
			allow: false,
			why:   "closing the rule and opening another rewrites the whole page",
		},
		{
			name:  "newline_injection",
			in:    strp("#000\n}\nbody{display:none}"),
			allow: false,
			why:   "the regexp must be anchored such that a newline cannot terminate the match early",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := safeColor(tt.in)
			if tt.allow {
				if got == nil {
					t.Fatalf("safeColor(%q) = nil, want the value passed through", *tt.in)
				}
				return
			}
			if got != nil {
				msg := "safeColor accepted %q, which must be dropped to null."
				if tt.why != "" {
					msg += "\n\n" + tt.why
				}
				t.Fatalf(msg, *tt.in)
			}
		})
	}
}

// TestIconURLFor pins the resolution order and, more importantly, pins that an
// administrator's third-party URL NEVER becomes the rendered icon.
func TestIconURLFor(t *testing.T) {
	now := time.Now()

	t.Run("cached_asset_points_at_this_server", func(t *testing.T) {
		got := iconURLFor(structs.SSOProvider{
			Slug:          "forta",
			IconURL:       strp("https://cdn.evil.example/logo.png"),
			IconCacheType: strp("image/png"),
			IconCachedAt:  &now,
		})
		if got == nil {
			t.Fatal("a provider with a cached icon returned no icon URL")
		}
		if *got != "/auth/sso/icon/forta" {
			t.Fatalf("icon URL = %q, want /auth/sso/icon/forta", *got)
		}
	})

	t.Run("the_admins_url_is_never_returned", func(t *testing.T) {
		// ⚠️ THE POINT OF THE WHOLE ICON PIPELINE. Returning icon_url here would
		// hot-link a third party from an unauthenticated page, leaking every
		// visitor's IP, User-Agent and Referer, and letting that party swap the
		// image after review.
		const adminURL = "https://cdn.evil.example/logo.png"
		for _, p := range []structs.SSOProvider{
			{Slug: "a", IconURL: strp(adminURL), IconCacheType: strp("image/png")},
			{Slug: "b", IconURL: strp(adminURL)},
			{Slug: "c", IconURL: strp(adminURL), DisplayIcon: strp("google")},
		} {
			got := iconURLFor(p)
			if got != nil && *got == adminURL {
				t.Fatalf("iconURLFor returned the administrator's third-party URL for slug %q. The login page would hot-link it.", p.Slug)
			}
		}
	})

	t.Run("bundled_slug_is_opaque", func(t *testing.T) {
		got := iconURLFor(structs.SSOProvider{Slug: "x", DisplayIcon: strp("google")})
		if got == nil || *got != "bundled:google" {
			t.Fatalf("icon = %v, want bundled:google — a short identifier the frontend maps, not a path an administrator controls", got)
		}
	})

	t.Run("no_icon_is_null_and_that_is_contractual", func(t *testing.T) {
		if got := iconURLFor(structs.SSOProvider{Slug: "x"}); got != nil {
			t.Fatalf("icon = %v, want nil. Null is the contractual 'render a text button' state, not an error — it is what a provider looks like before an icon is configured and after a fetch fails.", *got)
		}
	})

	t.Run("cached_asset_beats_bundled_slug", func(t *testing.T) {
		got := iconURLFor(structs.SSOProvider{
			Slug: "forta", DisplayIcon: strp("google"), IconCacheType: strp("image/png"),
		})
		if got == nil || *got != "/auth/sso/icon/forta" {
			t.Fatalf("icon = %v; an administrator who supplied a URL meant it to override the bundled default", got)
		}
	})
}

// TestDisplayLabel covers the button text fallback.
func TestDisplayLabel(t *testing.T) {
	if got := displayLabel(structs.SSOProvider{DisplayName: "Forta"}); got != "Forta" {
		t.Fatalf("label = %q, want the display name when no button label is set", got)
	}
	if got := displayLabel(structs.SSOProvider{DisplayName: "Forta", ButtonLabel: strp("Sign in with Forta")}); got != "Sign in with Forta" {
		t.Fatalf("label = %q, want the explicit button label", got)
	}
	if got := displayLabel(structs.SSOProvider{DisplayName: "Forta", ButtonLabel: strp("")}); got != "Forta" {
		t.Fatalf("label = %q; an empty button label must fall back rather than render an empty button", got)
	}
}

// TestBundledIcons pins the allowlist.
//
// ⚠️ display_icon is sent to an UNAUTHENTICATED login page which turns it into
// something it renders. Accepting free text would let an administrator put a path
// or a URL there. The allowlist means the only thing they choose is which asset
// the frontend already ships.
func TestBundledIcons(t *testing.T) {
	for _, slug := range []string{"google", "github", "microsoft", "forta", "okta", "gitlab", "apple"} {
		if !bundledIcons[slug] {
			t.Errorf("%q is missing from the bundled icon allowlist", slug)
		}
	}

	rejected := []struct{ value, why string }{
		{"../../etc/passwd", "a traversal path must not be accepted as an icon identifier"},
		{"https://evil.example/logo.png", "a URL belongs in icon_url, where it is fetched and validated — not here, where it would be rendered directly"},
		{"/static/anything.svg", "an absolute path would let an administrator point the login page at any asset"},
		{"data:image/svg+xml;base64,PHN2Zz4=", "a data URI is a way to smuggle an inline SVG past the fetch pipeline entirely"},
		{"GOOGLE", "matching is exact; a case variant is not an asset the frontend ships"},
		{"", "empty is handled by the caller as 'not set', never as a lookup"},
	}
	for _, tt := range rejected {
		t.Run("rejects_"+tt.value, func(t *testing.T) {
			if bundledIcons[tt.value] {
				t.Fatalf("%q is in the bundled icon allowlist. %s", tt.value, tt.why)
			}
		})
	}
}
