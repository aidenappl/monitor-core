package routes

import (
	"encoding/json"
	"net/http"

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
