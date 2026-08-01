package routes

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	sso "github.com/aidenappl/go-forta/sso"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/tools"
	"github.com/gorilla/mux"
)

// adminProviderView is the admin-facing projection of an sso_providers row. It
// deliberately omits client_secret_ref / client_secret_enc and exposes only a
// has_secret boolean — the secret itself is never returned by the API.
type adminProviderView struct {
	ID                 int64   `json:"id"`
	Slug               string  `json:"slug"`
	DisplayName        string  `json:"display_name"`
	Kind               string  `json:"kind"`
	IssuerURL          *string `json:"issuer_url,omitempty"`
	AuthorizeURL       *string `json:"authorize_url,omitempty"`
	TokenURL           *string `json:"token_url,omitempty"`
	UserInfoURL        *string `json:"userinfo_url,omitempty"`
	JWKSURL            *string `json:"jwks_url,omitempty"`
	IntrospectURL      *string `json:"introspect_url,omitempty"`
	ClientID           *string `json:"client_id,omitempty"`
	Scopes             string  `json:"scopes"`
	EmailClaim         string  `json:"email_claim"`
	EmailVerifiedClaim string  `json:"email_verified_claim"`
	TrustEmailVerified bool    `json:"trust_email_verified"`
	SubjectClaim       string  `json:"subject_claim"`
	ButtonLabel        *string `json:"button_label,omitempty"`
	AllowAutoLink      bool    `json:"allow_auto_link"`
	AutoProvision      bool    `json:"auto_provision"`
	Enabled            bool    `json:"enabled"`
	HasSecret          bool    `json:"has_secret"`

	DisplayIcon     *string `json:"display_icon,omitempty"`
	IconURL         *string `json:"icon_url,omitempty"`
	ButtonColor     *string `json:"button_color,omitempty"`
	ButtonTextColor *string `json:"button_text_color,omitempty"`
	SortOrder       int     `json:"sort_order"`

	// HasIcon and IconError let the admin UI show whether the last fetch worked
	// and, when it did not, why — without shipping the image bytes in a listing.
	HasIcon   bool    `json:"has_icon"`
	IconError *string `json:"icon_error,omitempty"`
}

func toAdminView(p *structs.SSOProvider) adminProviderView {
	return adminProviderView{
		ID:                 p.ID,
		Slug:               p.Slug,
		DisplayName:        p.DisplayName,
		Kind:               p.Kind,
		IssuerURL:          p.IssuerURL,
		AuthorizeURL:       p.AuthorizeURL,
		TokenURL:           p.TokenURL,
		UserInfoURL:        p.UserInfoURL,
		JWKSURL:            p.JWKSURL,
		IntrospectURL:      p.IntrospectURL,
		ClientID:           p.ClientID,
		Scopes:             p.Scopes,
		EmailClaim:         p.EmailClaim,
		EmailVerifiedClaim: p.EmailVerifiedClaim,
		TrustEmailVerified: p.TrustEmailVerified,
		SubjectClaim:       p.SubjectClaim,
		ButtonLabel:        p.ButtonLabel,
		DisplayIcon:        p.DisplayIcon,
		IconURL:            p.IconURL,
		ButtonColor:        p.ButtonColor,
		ButtonTextColor:    p.ButtonTextColor,
		SortOrder:          p.SortOrder,
		HasIcon:            p.IconCacheType != nil && *p.IconCacheType != "",
		IconError:          p.IconError,
		AllowAutoLink:      p.AllowAutoLink,
		AutoProvision:      p.AutoProvision,
		Enabled:            p.Enabled,
		HasSecret:          p.HasSecret(),
	}
}

// HandleListSSOProviders returns all providers (admin only, GET
// /admin/sso-providers).
func HandleListSSOProviders(w http.ResponseWriter, r *http.Request) {
	providers, err := query.ListProviders(db.SQL)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list sso providers", err)
		return
	}
	views := make([]adminProviderView, 0, len(providers))
	for i := range providers {
		views = append(views, toAdminView(&providers[i]))
	}
	responder.New(w, views, "sso providers")
}

// ssoProviderBody is the create/update payload. ClientSecret is plaintext and,
// when present, is AES-256-GCM encrypted into client_secret_enc — it is never
// stored or echoed in the clear. Provide either ClientSecret (encrypted at rest)
// or ClientSecretRef (a Keyring secret name), not both.
type ssoProviderBody struct {
	Slug               *string `json:"slug"`
	DisplayName        *string `json:"display_name"`
	Kind               *string `json:"kind"`
	IssuerURL          *string `json:"issuer_url"`
	AuthorizeURL       *string `json:"authorize_url"`
	TokenURL           *string `json:"token_url"`
	UserInfoURL        *string `json:"userinfo_url"`
	JWKSURL            *string `json:"jwks_url"`
	IntrospectURL      *string `json:"introspect_url"`
	ClientID           *string `json:"client_id"`
	ClientSecret       *string `json:"client_secret"`
	ClientSecretRef    *string `json:"client_secret_ref"`
	Scopes             *string `json:"scopes"`
	EmailClaim         *string `json:"email_claim"`
	EmailVerifiedClaim *string `json:"email_verified_claim"`
	TrustEmailVerified *bool   `json:"trust_email_verified"`
	SubjectClaim       *string `json:"subject_claim"`
	ButtonLabel        *string `json:"button_label"`
	AllowAutoLink      *bool   `json:"allow_auto_link"`
	AutoProvision      *bool   `json:"auto_provision"`
	Enabled            *bool   `json:"enabled"`

	// ── Branding ────────────────────────────────────────────────────────────
	DisplayIcon     *string `json:"display_icon"`
	IconURL         *string `json:"icon_url"`
	ButtonColor     *string `json:"button_color"`
	ButtonTextColor *string `json:"button_text_color"`
	SortOrder       *int    `json:"sort_order"`
}

// bundledIcons is the set of icon slugs the frontends ship an asset for.
//
// ⚠️ AN ALLOWLIST, NOT FREE TEXT, and that is the point. display_icon is sent to
// an unauthenticated login page which turns it into something it renders; if any
// string were accepted, an administrator could put a path or a URL there. Adding
// an icon here means shipping the asset in the frontends in the same change.
var bundledIcons = map[string]bool{
	"google": true, "github": true, "microsoft": true,
	"forta": true, "okta": true, "gitlab": true, "apple": true,
}

// validateBranding checks the branding fields. Returns a client-facing message
// and false on the first failure.
//
// ⚠️ COLOURS ARE VALIDATED HERE **AND AGAIN ON RENDER** (see safeColor in
// HandleSSOConfig). Belt and braces on purpose: these values reach CSS on an
// unauthenticated page, and validating only on write trusts that every row ever
// stored came through this function — untrue for anything inserted by a
// migration or by hand.
func validateBranding(body *ssoProviderBody) (string, bool) {
	if body.DisplayIcon != nil && *body.DisplayIcon != "" && !bundledIcons[*body.DisplayIcon] {
		return "display_icon: not a bundled icon slug", false
	}
	for _, c := range []struct {
		name string
		val  *string
	}{
		{"button_color", body.ButtonColor},
		{"button_text_color", body.ButtonTextColor},
	} {
		if c.val != nil && *c.val != "" && !hexColor.MatchString(*c.val) {
			return c.name + ": must be #rrggbb", false
		}
	}
	// icon_url goes through the same SSRF guard as every other provider URL. It is
	// checked again, more thoroughly, inside sso.FetchIcon — this is the early,
	// cheap refusal so an obviously-bad value never reaches the fetch.
	if body.IconURL != nil && *body.IconURL != "" {
		if err := tools.ValidateExternalURL(*body.IconURL); err != nil {
			return "icon_url: " + err.Error(), false
		}
	}
	return "", true
}

// refreshProviderIcon fetches, validates and caches a provider's icon.
//
// ─────────────────────────────────────────────────────────────────────────────
// ⚠️ A FAILED FETCH MUST NEVER FAIL THE SAVE.
//
// An administrator correcting an issuer URL must not be blocked because the logo
// they set last week now 404s. The provider is already written by the time this
// runs; the outcome here only decides whether an icon is cached or an error is
// recorded, and the login page falls back to a text button either way.
//
// It runs SYNCHRONOUSLY rather than in a goroutine, deliberately: the admin who
// pressed Save is the person who can fix a bad URL, and they should be told now
// rather than discovering it on the login page later. sso.FetchIcon is bounded at
// five seconds, which is an acceptable cost on an infrequent admin action.
// ─────────────────────────────────────────────────────────────────────────────
func refreshProviderIcon(ctx context.Context, slug string, iconURL *string) {
	if iconURL == nil || *iconURL == "" {
		// Cleared, or never set. Drop any cached bytes so removing the URL actually
		// removes the icon rather than leaving a stale one served forever.
		if err := query.SetProviderIcon(db.SQL, slug, "", nil, ""); err != nil {
			log.Printf("sso: failed to clear icon for %q: %v", slug, err)
		}
		return
	}

	icon, err := sso.FetchIcon(ctx, *iconURL)
	if err != nil {
		log.Printf("sso: icon fetch failed for %q: %v", slug, err)
		if setErr := query.SetProviderIcon(db.SQL, slug, "", nil, err.Error()); setErr != nil {
			log.Printf("sso: failed to record icon error for %q: %v", slug, setErr)
		}
		return
	}

	if err := query.SetProviderIcon(db.SQL, slug, icon.ContentType, icon.Data, ""); err != nil {
		log.Printf("sso: failed to cache icon for %q: %v", slug, err)
	}
}

// validateProviderURLs runs the SSRF guard over every provider URL present in
// the body. Returns a client-facing error message and false on the first failure.
func validateProviderURLs(body *ssoProviderBody) (string, bool) {
	checks := []struct {
		name string
		val  *string
	}{
		{"issuer_url", body.IssuerURL},
		{"authorize_url", body.AuthorizeURL},
		{"token_url", body.TokenURL},
		{"userinfo_url", body.UserInfoURL},
		{"jwks_url", body.JWKSURL},
		{"introspect_url", body.IntrospectURL},
	}
	for _, c := range checks {
		if c.val != nil && *c.val != "" {
			if err := tools.ValidateExternalURL(*c.val); err != nil {
				return c.name + ": " + err.Error(), false
			}
		}
	}
	return "", true
}

// HandleCreateSSOProvider creates a provider (admin only, POST
// /admin/sso-providers).
func HandleCreateSSOProvider(w http.ResponseWriter, r *http.Request) {
	var body ssoProviderBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Slug == nil || *body.Slug == "" || body.DisplayName == nil || *body.DisplayName == "" {
		responder.Error(w, http.StatusBadRequest, "slug and display_name are required")
		return
	}
	if msg, ok := validateProviderURLs(&body); !ok {
		responder.Error(w, http.StatusBadRequest, msg)
		return
	}
	if msg, ok := validateBranding(&body); !ok {
		responder.Error(w, http.StatusBadRequest, msg)
		return
	}

	req := query.CreateProviderRequest{
		Slug:               *body.Slug,
		DisplayName:        *body.DisplayName,
		IssuerURL:          body.IssuerURL,
		AuthorizeURL:       body.AuthorizeURL,
		TokenURL:           body.TokenURL,
		UserInfoURL:        body.UserInfoURL,
		JWKSURL:            body.JWKSURL,
		IntrospectURL:      body.IntrospectURL,
		ClientID:           body.ClientID,
		ClientSecretRef:    body.ClientSecretRef,
		Scopes:             body.Scopes,
		EmailClaim:         body.EmailClaim,
		EmailVerifiedClaim: body.EmailVerifiedClaim,
		TrustEmailVerified: body.TrustEmailVerified,
		SubjectClaim:       body.SubjectClaim,
		ButtonLabel:        body.ButtonLabel,
		AllowAutoLink:      body.AllowAutoLink,
		AutoProvision:      body.AutoProvision,
		Enabled:            body.Enabled,

		DisplayIcon:     body.DisplayIcon,
		IconURL:         body.IconURL,
		ButtonColor:     body.ButtonColor,
		ButtonTextColor: body.ButtonTextColor,
		SortOrder:       body.SortOrder,
	}
	if body.Kind != nil {
		req.Kind = *body.Kind
	}
	if body.ClientSecret != nil && *body.ClientSecret != "" {
		enc, err := tools.Encrypt(*body.ClientSecret)
		if err != nil {
			responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to encrypt client secret", err)
			return
		}
		req.ClientSecretEnc = &enc
	}

	provider, err := query.CreateProvider(db.SQL, req)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to create sso provider", err)
		return
	}

	// After the write, never before: the icon is decoration and its failure must
	// not cost the administrator the provider they just configured.
	refreshProviderIcon(r.Context(), provider.Slug, body.IconURL)

	// Re-read so the response carries the icon state this request just produced,
	// rather than the pre-fetch row.
	if fresh, ferr := query.GetProviderBySlug(db.SQL, provider.Slug); ferr == nil && fresh != nil {
		provider = fresh
	}
	responder.New(w, toAdminView(provider), "sso provider created")
}

// HandleUpdateSSOProvider updates a provider by slug (admin only, PUT
// /admin/sso-providers/{slug}).
func HandleUpdateSSOProvider(w http.ResponseWriter, r *http.Request) {
	slug := mux.Vars(r)["slug"]
	if slug == "" {
		responder.Error(w, http.StatusBadRequest, "slug is required")
		return
	}

	var body ssoProviderBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if msg, ok := validateProviderURLs(&body); !ok {
		responder.Error(w, http.StatusBadRequest, msg)
		return
	}
	if msg, ok := validateBranding(&body); !ok {
		responder.Error(w, http.StatusBadRequest, msg)
		return
	}

	req := query.UpdateProviderRequest{
		DisplayName:        body.DisplayName,
		Kind:               body.Kind,
		IssuerURL:          body.IssuerURL,
		AuthorizeURL:       body.AuthorizeURL,
		TokenURL:           body.TokenURL,
		UserInfoURL:        body.UserInfoURL,
		JWKSURL:            body.JWKSURL,
		IntrospectURL:      body.IntrospectURL,
		ClientID:           body.ClientID,
		ClientSecretRef:    body.ClientSecretRef,
		Scopes:             body.Scopes,
		EmailClaim:         body.EmailClaim,
		EmailVerifiedClaim: body.EmailVerifiedClaim,
		TrustEmailVerified: body.TrustEmailVerified,
		SubjectClaim:       body.SubjectClaim,
		ButtonLabel:        body.ButtonLabel,
		AllowAutoLink:      body.AllowAutoLink,
		AutoProvision:      body.AutoProvision,
		Enabled:            body.Enabled,

		DisplayIcon:     body.DisplayIcon,
		IconURL:         body.IconURL,
		ButtonColor:     body.ButtonColor,
		ButtonTextColor: body.ButtonTextColor,
		SortOrder:       body.SortOrder,
	}
	if body.ClientSecret != nil && *body.ClientSecret != "" {
		enc, err := tools.Encrypt(*body.ClientSecret)
		if err != nil {
			responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to encrypt client secret", err)
			return
		}
		req.ClientSecretEnc = &enc
	}

	existing, err := query.GetProviderBySlug(db.SQL, slug)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to load sso provider", err)
		return
	}
	if existing == nil {
		responder.Error(w, http.StatusNotFound, "sso provider not found")
		return
	}

	provider, err := query.UpdateProvider(db.SQL, slug, req)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to update sso provider", err)
		return
	}

	// Re-fetch the icon only when icon_url was actually part of this request.
	//
	// ⚠️ The nil check matters. This is a PATCH-shaped endpoint — an absent field
	// means "leave it alone". Refetching unconditionally would hit the third party
	// on every unrelated save (toggling `enabled`, fixing a scope), and clearing on
	// a nil would delete a working icon the moment anyone edited anything else.
	if body.IconURL != nil {
		refreshProviderIcon(r.Context(), slug, body.IconURL)
		if fresh, ferr := query.GetProviderBySlug(db.SQL, slug); ferr == nil && fresh != nil {
			provider = fresh
		}
	}
	responder.New(w, toAdminView(provider), "sso provider updated")
}

// HandleDeleteSSOProvider removes a provider by slug (admin only, DELETE
// /admin/sso-providers/{slug}).
func HandleDeleteSSOProvider(w http.ResponseWriter, r *http.Request) {
	slug := mux.Vars(r)["slug"]
	if slug == "" {
		responder.Error(w, http.StatusBadRequest, "slug is required")
		return
	}
	if err := query.DeleteProvider(db.SQL, slug); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete sso provider", err)
		return
	}
	responder.New(w, nil, "sso provider deleted")
}
