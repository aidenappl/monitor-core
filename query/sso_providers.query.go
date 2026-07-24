package query

import (
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

var ssoProviderColumns = []string{
	"id", "slug", "display_name", "kind", "issuer_url", "authorize_url",
	"token_url", "userinfo_url", "jwks_url", "introspect_url", "client_id",
	"client_secret_ref", "client_secret_enc", "scopes", "email_claim",
	"email_verified_claim", "trust_email_verified", "subject_claim", "button_label",
	"allow_auto_link", "auto_provision", "enabled", "inserted_at",
}

type ssoProviderScanner interface {
	Scan(dest ...interface{}) error
}

func scanSSOProvider(row ssoProviderScanner) (*structs.SSOProvider, error) {
	var p structs.SSOProvider
	err := row.Scan(
		&p.ID, &p.Slug, &p.DisplayName, &p.Kind, &p.IssuerURL, &p.AuthorizeURL,
		&p.TokenURL, &p.UserInfoURL, &p.JWKSURL, &p.IntrospectURL, &p.ClientID,
		&p.ClientSecretRef, &p.ClientSecretEnc, &p.Scopes, &p.EmailClaim,
		&p.EmailVerifiedClaim, &p.TrustEmailVerified, &p.SubjectClaim, &p.ButtonLabel,
		&p.AllowAutoLink, &p.AutoProvision, &p.Enabled, &p.InsertedAt,
	)
	return &p, err
}

// ListEnabledProviders returns every enabled provider, ordered by slug. This is
// what the public /auth/sso/config endpoint renders as login buttons.
func ListEnabledProviders(engine db.Queryable) ([]structs.SSOProvider, error) {
	return listProviders(engine, sq.Eq{"enabled": true})
}

// ListProviders returns every provider (enabled or not) for the admin surface.
func ListProviders(engine db.Queryable) ([]structs.SSOProvider, error) {
	return listProviders(engine, nil)
}

func listProviders(engine db.Queryable, where sq.Sqlizer) ([]structs.SSOProvider, error) {
	q := sq.Select(ssoProviderColumns...).From("sso_providers")
	if where != nil {
		q = q.Where(where)
	}
	query, args, err := q.OrderBy("slug ASC").ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := engine.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("list sso providers: %w", err)
	}
	defer rows.Close()

	providers := []structs.SSOProvider{}
	for rows.Next() {
		p, err := scanSSOProvider(rows)
		if err != nil {
			return nil, fmt.Errorf("scan sso provider: %w", err)
		}
		providers = append(providers, *p)
	}
	return providers, rows.Err()
}

// GetProviderBySlug resolves a single provider by its unique slug. Returns
// (nil, nil) when absent.
func GetProviderBySlug(engine db.Queryable, slug string) (*structs.SSOProvider, error) {
	query, args, err := sq.Select(ssoProviderColumns...).
		From("sso_providers").
		Where(sq.Eq{"slug": slug}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	p, err := scanSSOProvider(engine.QueryRow(query, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get sso provider: %w", err)
	}
	return p, nil
}

// CreateProviderRequest carries a new provider row. Exactly one of
// ClientSecretRef / ClientSecretEnc should be set (or neither for public
// clients). The handler is responsible for encrypting any plaintext secret into
// ClientSecretEnc before calling — the raw secret never reaches this layer.
type CreateProviderRequest struct {
	Slug               string
	DisplayName        string
	Kind               string
	IssuerURL          *string
	AuthorizeURL       *string
	TokenURL           *string
	UserInfoURL        *string
	JWKSURL            *string
	IntrospectURL      *string
	ClientID           *string
	ClientSecretRef    *string
	ClientSecretEnc    *string
	Scopes             *string
	EmailClaim         *string
	EmailVerifiedClaim *string
	TrustEmailVerified *bool
	SubjectClaim       *string
	ButtonLabel        *string
	AllowAutoLink      *bool
	AutoProvision      *bool
	Enabled            *bool
}

// CreateProvider inserts a new provider and returns the hydrated row.
func CreateProvider(engine db.Queryable, req CreateProviderRequest) (*structs.SSOProvider, error) {
	if req.Kind == "" {
		req.Kind = "oidc"
	}

	cols := map[string]interface{}{
		"slug":              req.Slug,
		"display_name":      req.DisplayName,
		"kind":              req.Kind,
		"issuer_url":        req.IssuerURL,
		"authorize_url":     req.AuthorizeURL,
		"token_url":         req.TokenURL,
		"userinfo_url":      req.UserInfoURL,
		"jwks_url":          req.JWKSURL,
		"introspect_url":    req.IntrospectURL,
		"client_id":         req.ClientID,
		"client_secret_ref": req.ClientSecretRef,
		"client_secret_enc": req.ClientSecretEnc,
		"button_label":      req.ButtonLabel,
	}
	if req.Scopes != nil {
		cols["scopes"] = *req.Scopes
	}
	if req.EmailClaim != nil {
		cols["email_claim"] = *req.EmailClaim
	}
	if req.EmailVerifiedClaim != nil {
		cols["email_verified_claim"] = *req.EmailVerifiedClaim
	}
	if req.TrustEmailVerified != nil {
		cols["trust_email_verified"] = *req.TrustEmailVerified
	}
	if req.SubjectClaim != nil {
		cols["subject_claim"] = *req.SubjectClaim
	}
	if req.AllowAutoLink != nil {
		cols["allow_auto_link"] = *req.AllowAutoLink
	}
	if req.AutoProvision != nil {
		cols["auto_provision"] = *req.AutoProvision
	}
	if req.Enabled != nil {
		cols["enabled"] = *req.Enabled
	}

	columns := make([]string, 0, len(cols))
	values := make([]interface{}, 0, len(cols))
	for c, v := range cols {
		columns = append(columns, c)
		values = append(values, v)
	}

	query, args, err := sq.Insert("sso_providers").Columns(columns...).Values(values...).ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}
	if _, err := engine.Exec(query, args...); err != nil {
		return nil, fmt.Errorf("create sso provider: %w", err)
	}
	return GetProviderBySlug(engine, req.Slug)
}

// UpdateProviderRequest is a partial update keyed by slug. A nil field is left
// unchanged. Secret handling mirrors CreateProviderRequest.
type UpdateProviderRequest struct {
	DisplayName        *string
	Kind               *string
	IssuerURL          *string
	AuthorizeURL       *string
	TokenURL           *string
	UserInfoURL        *string
	JWKSURL            *string
	IntrospectURL      *string
	ClientID           *string
	ClientSecretRef    *string
	ClientSecretEnc    *string
	Scopes             *string
	EmailClaim         *string
	EmailVerifiedClaim *string
	TrustEmailVerified *bool
	SubjectClaim       *string
	ButtonLabel        *string
	AllowAutoLink      *bool
	AutoProvision      *bool
	Enabled            *bool
}

// UpdateProvider applies a partial update and returns the refreshed row.
func UpdateProvider(engine db.Queryable, slug string, req UpdateProviderRequest) (*structs.SSOProvider, error) {
	q := sq.Update("sso_providers").Where(sq.Eq{"slug": slug})

	set := func(col string, v interface{}) { q = q.Set(col, v) }
	hasUpdate := false
	apply := func(col string, v interface{}, ok bool) {
		if ok {
			set(col, v)
			hasUpdate = true
		}
	}

	apply("display_name", derefStr(req.DisplayName), req.DisplayName != nil)
	apply("kind", derefStr(req.Kind), req.Kind != nil)
	apply("issuer_url", req.IssuerURL, req.IssuerURL != nil)
	apply("authorize_url", req.AuthorizeURL, req.AuthorizeURL != nil)
	apply("token_url", req.TokenURL, req.TokenURL != nil)
	apply("userinfo_url", req.UserInfoURL, req.UserInfoURL != nil)
	apply("jwks_url", req.JWKSURL, req.JWKSURL != nil)
	apply("introspect_url", req.IntrospectURL, req.IntrospectURL != nil)
	apply("client_id", req.ClientID, req.ClientID != nil)
	apply("client_secret_ref", req.ClientSecretRef, req.ClientSecretRef != nil)
	apply("client_secret_enc", req.ClientSecretEnc, req.ClientSecretEnc != nil)
	apply("scopes", derefStr(req.Scopes), req.Scopes != nil)
	apply("email_claim", derefStr(req.EmailClaim), req.EmailClaim != nil)
	apply("email_verified_claim", derefStr(req.EmailVerifiedClaim), req.EmailVerifiedClaim != nil)
	apply("trust_email_verified", derefBool(req.TrustEmailVerified), req.TrustEmailVerified != nil)
	apply("subject_claim", derefStr(req.SubjectClaim), req.SubjectClaim != nil)
	apply("button_label", req.ButtonLabel, req.ButtonLabel != nil)
	apply("allow_auto_link", derefBool(req.AllowAutoLink), req.AllowAutoLink != nil)
	apply("auto_provision", derefBool(req.AutoProvision), req.AutoProvision != nil)
	apply("enabled", derefBool(req.Enabled), req.Enabled != nil)

	if !hasUpdate {
		return GetProviderBySlug(engine, slug)
	}

	query, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}
	if _, err := engine.Exec(query, args...); err != nil {
		return nil, fmt.Errorf("update sso provider: %w", err)
	}
	return GetProviderBySlug(engine, slug)
}

// DeleteProvider removes a provider by slug.
func DeleteProvider(engine db.Queryable, slug string) error {
	query, args, err := sq.Delete("sso_providers").Where(sq.Eq{"slug": slug}).ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}
	if _, err := engine.Exec(query, args...); err != nil {
		return fmt.Errorf("delete sso provider: %w", err)
	}
	return nil
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func derefBool(p *bool) bool {
	return p != nil && *p
}
