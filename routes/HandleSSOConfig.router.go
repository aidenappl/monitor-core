package routes

import (
	"net/http"
	"regexp"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE PUBLIC SSO CONFIG CONTRACT
//
// One shape, shared by monitor-core, lattice-api and openbucket-api, so a login
// page written once renders correctly against any of them. Modelled on Zulip's
// server_settings, which solves the same problem: an unauthenticated page that
// must know which identity providers exist before anyone has logged in.
//
//	{ "providers": [ { "name": "forta", "display_name": "Forta",
//	    "display_icon": "/auth/sso/icon/forta", "button_color": null,
//	    "button_text_color": null, "login_url": "/auth/sso/forta/login",
//	    "sort_order": 0 } ] }
//
// ⚠️ THIS ENDPOINT IS UNAUTHENTICATED. Everything it returns is visible to anyone
// who can reach the login page, so it carries display data ONLY — never an
// issuer URL, a client_id, a scope list or anything else that describes the
// integration. Adding a field here is publishing it.
// ─────────────────────────────────────────────────────────────────────────────

// ssoConfigResponse is the whole contract.
//
// An OBJECT with a `providers` array, not a bare array. A bare array cannot grow
// a sibling field without breaking every client, and this response will
// eventually want one (whether native password login is enabled, for instance).
type ssoConfigResponse struct {
	Providers []ssoProviderEntry `json:"providers"`
}

// ssoProviderEntry is one login button.
type ssoProviderEntry struct {
	// Name is the provider slug. Named `name` rather than `slug` to match Zulip's
	// vocabulary, since the whole shape is borrowed from it.
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`

	// DisplayIcon is a URL ON THIS SERVER, or null.
	//
	// ⚠️ NULL IS CONTRACTUAL, NOT AN ERROR STATE. A provider with no icon renders
	// a plain text button. Every client must handle it, because it is the state a
	// provider is in before an icon is configured and the state it returns to when
	// a fetch fails.
	//
	// ⚠️ It is NEVER the administrator's third-party URL. Hot-linking would leak
	// every unauthenticated visitor's IP, User-Agent and Referer to that third
	// party and make this page depend on their uptime. The bytes were fetched once
	// at save time and are served from this origin — see
	// HandleSSOIcon and go-forta/sso's FetchIcon.
	DisplayIcon *string `json:"display_icon"`

	// ButtonColor and ButtonTextColor are #rrggbb, or null for the default.
	//
	// ⚠️ RE-VALIDATED HERE, on the way out, even though the admin API validates on
	// the way in. These end up inside CSS on an unauthenticated page. Validating
	// only on write trusts that every row ever written passed through that
	// validation — which is false for a row inserted before the check existed, or
	// by a migration, or by hand. The cost of re-checking is a regexp; the cost of
	// not re-checking is CSS injection on the login page.
	ButtonColor     *string `json:"button_color"`
	ButtonTextColor *string `json:"button_text_color"`

	// LoginURL is COMPUTED FROM THE SLUG, never stored.
	//
	// ⚠️ THIS IS A SECURITY PROPERTY, NOT A CONVENIENCE. A stored login URL is an
	// administrator-controlled value that an unauthenticated page turns into a
	// link the user is expected to click — an open-redirect primitive with your
	// domain on it, and a phishing lure that survives review because the page
	// itself is genuine. Deriving it from the slug means the only thing an
	// administrator controls is which of THIS server's routes is linked.
	LoginURL string `json:"login_url"`

	SortOrder int `json:"sort_order"`
}

// hexColor matches an #rrggbb colour exactly.
//
// Anchored at both ends, six digits, no shorthand and no named colours: the value
// is interpolated into a CSS custom property, and anything that is not exactly
// this shape is refused rather than escaped. Refusing is checkable; escaping CSS
// correctly is not something to reinvent on a login page.
var hexColor = regexp.MustCompile(`^#[0-9a-fA-F]{6}$`)

// safeColor returns the colour if it is a well-formed #rrggbb, else nil.
func safeColor(c *string) *string {
	if c == nil || !hexColor.MatchString(*c) {
		return nil
	}
	return c
}

// HandleSSOConfig is the PUBLIC provider discovery endpoint (GET
// /auth/sso/config).
func HandleSSOConfig(w http.ResponseWriter, r *http.Request) {
	providers, err := query.ListEnabledProviders(db.SQL)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to load sso providers", err)
		return
	}

	entries := make([]ssoProviderEntry, 0, len(providers))
	for _, p := range providers {
		entries = append(entries, ssoProviderEntry{
			Name:            p.Slug,
			DisplayName:     displayLabel(p),
			DisplayIcon:     iconURLFor(p),
			ButtonColor:     safeColor(p.ButtonColor),
			ButtonTextColor: safeColor(p.ButtonTextColor),
			LoginURL:        "/auth/sso/" + p.Slug + "/login",
			SortOrder:       p.SortOrder,
		})
	}

	responder.New(w, ssoConfigResponse{Providers: entries}, "sso providers")
}

// displayLabel is what the button says: the explicit button_label if set, else
// the provider's display name.
func displayLabel(p structs.SSOProvider) string {
	if p.ButtonLabel != nil && *p.ButtonLabel != "" {
		return *p.ButtonLabel
	}
	return p.DisplayName
}

// iconURLFor resolves which icon the login page should render, in order:
//
//  1. a cached third-party asset  → this server's own icon route
//  2. a bundled slug              → "bundled:<slug>", which the frontend maps to
//     an asset it ships
//  3. neither                     → nil, meaning render a text button
//
// The cached asset wins because an administrator who went to the trouble of
// supplying a URL meant it to override the bundled default.
func iconURLFor(p structs.SSOProvider) *string {
	if p.IconCacheType != nil && *p.IconCacheType != "" {
		u := "/auth/sso/icon/" + p.Slug
		return &u
	}
	if p.DisplayIcon != nil && *p.DisplayIcon != "" {
		// A short, opaque identifier rather than a path. The frontend owns the
		// mapping to an asset it ships, so an administrator cannot make this string
		// resolve to an arbitrary URL.
		u := "bundled:" + *p.DisplayIcon
		return &u
	}
	return nil
}
