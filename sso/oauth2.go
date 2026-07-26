package sso

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// oauth2Adapter implements Adapter for kind="oauth2" providers — non-OIDC OAuth2
// servers that expose explicit authorize/token/userinfo URLs and return user
// data from a userinfo endpoint rather than a verifiable id_token. It is
// deliberately tolerant: a plain RFC 6749 provider works, and so does one that
// wraps its replies in a {success,data{...}} envelope. Because there is no
// id_token, there is no nonce to verify here; state (single-use, server-side)
// carries the CSRF/replay protection for this kind.
type oauth2Adapter struct {
	provider *Provider
}

func newOAuth2Adapter(p *Provider) (Adapter, error) {
	if p.AuthorizeURL == nil || *p.AuthorizeURL == "" {
		return nil, fmt.Errorf("sso: oauth2 provider %q missing authorize_url", p.Slug)
	}
	if p.TokenURL == nil || *p.TokenURL == "" {
		return nil, fmt.Errorf("sso: oauth2 provider %q missing token_url", p.Slug)
	}
	if p.ClientID == nil || *p.ClientID == "" {
		return nil, fmt.Errorf("sso: oauth2 provider %q missing client_id", p.Slug)
	}
	return &oauth2Adapter{provider: p}, nil
}

func (a *oauth2Adapter) AuthCodeURL(state, nonce, verifier string) (string, error) {
	p := a.provider
	params := url.Values{
		"client_id":     {*p.ClientID},
		"redirect_uri":  {p.RedirectURL()},
		"response_type": {"code"},
		"scope":         {p.Scopes},
		"state":         {state},
	}
	return *p.AuthorizeURL + "?" + params.Encode(), nil
}

func (a *oauth2Adapter) Exchange(ctx context.Context, code, verifier, nonce string) (*NormalizedIdentity, *TokenSet, error) {
	tok, err := a.exchangeCode(ctx, code)
	if err != nil {
		return nil, nil, err
	}

	userInfo, err := a.fetchUserInfo(ctx, tok.AccessToken)
	if err != nil {
		return nil, nil, err
	}

	subject := getSubject(userInfo)
	if subject == "" {
		return nil, nil, fmt.Errorf("sso: oauth2 identity missing subject")
	}
	email := getUserEmail(userInfo, a.provider.EmailClaim)

	// Trust the provider's email_verified claim when present; otherwise fall back
	// to the per-provider TrustEmailVerified flag. That flag is explicit config,
	// never a hardcoded slug — it exists for providers that verify emails upstream
	// but return no email_verified claim.
	emailVerified := a.provider.TrustEmailVerified ||
		claimBool(userInfo, a.provider.EmailVerifiedClaim, "email_verified")

	raw, _ := json.Marshal(userInfo)
	ni := &NormalizedIdentity{
		Provider:      a.provider.Slug,
		Subject:       subject,
		Email:         email,
		EmailVerified: emailVerified,
		Name:          namePtr(getUserName(userInfo)),
		Picture:       namePtr(getUserPicture(userInfo)),
		RawClaims:     raw,
	}
	return ni, tok, nil
}

// exchangeCode swaps an authorization code for tokens, trying three request
// shapes for provider compatibility (JSON body → HTTP Basic → body creds),
// then unwrapping either a standard OAuth2 response or a wrapped envelope.
func (a *oauth2Adapter) exchangeCode(ctx context.Context, code string) (*TokenSet, error) {
	p := a.provider

	form := url.Values{
		"grant_type":   {"authorization_code"},
		"code":         {code},
		"redirect_uri": {p.RedirectURL()},
	}

	// 1. JSON body (some providers require this).
	jsonBody, _ := json.Marshal(map[string]string{
		"client_id":     *p.ClientID,
		"client_secret": p.ClientSecret,
		"code":          code,
		"redirect_uri":  p.RedirectURL(),
	})
	if req, err := http.NewRequestWithContext(ctx, http.MethodPost, *p.TokenURL, bytes.NewReader(jsonBody)); err == nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		if tok, err := doTokenRequest(req); err == nil {
			return tok, nil
		}
	}

	// 2. HTTP Basic auth (RFC 6749 preferred).
	if req, err := http.NewRequestWithContext(ctx, http.MethodPost, *p.TokenURL, strings.NewReader(form.Encode())); err == nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")
		req.SetBasicAuth(*p.ClientID, p.ClientSecret)
		if tok, err := doTokenRequest(req); err == nil {
			return tok, nil
		}
	}

	// 3. Credentials in the POST body.
	body := url.Values{}
	for k, v := range form {
		body[k] = v
	}
	body.Set("client_id", *p.ClientID)
	body.Set("client_secret", p.ClientSecret)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, *p.TokenURL, strings.NewReader(body.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	return doTokenRequest(req)
}

// tokenResponse is the wire shape of an OAuth2 token endpoint reply.
type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token"`
}

func doTokenRequest(req *http.Request) (*TokenSet, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sso: token exchange: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("sso: token endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	body, _ := io.ReadAll(resp.Body)

	// Standard OAuth2 response.
	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err == nil && tr.AccessToken != "" {
		return &TokenSet{AccessToken: tr.AccessToken, RefreshToken: tr.RefreshToken}, nil
	}

	// Wrapped envelope: {"success":true,"data":{"authorization":{"access_token":...}}}.
	var env1 struct {
		Success bool `json:"success"`
		Data    struct {
			Authorization tokenResponse `json:"authorization"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &env1); err == nil && env1.Success && env1.Data.Authorization.AccessToken != "" {
		return &TokenSet{AccessToken: env1.Data.Authorization.AccessToken, RefreshToken: env1.Data.Authorization.RefreshToken}, nil
	}

	// Wrapped envelope with the token at data level.
	var env2 struct {
		Success bool          `json:"success"`
		Data    tokenResponse `json:"data"`
	}
	if err := json.Unmarshal(body, &env2); err == nil && env2.Success && env2.Data.AccessToken != "" {
		return &TokenSet{AccessToken: env2.Data.AccessToken, RefreshToken: env2.Data.RefreshToken}, nil
	}

	trunc := body
	if len(trunc) > 200 {
		trunc = trunc[:200]
	}
	return nil, fmt.Errorf("sso: unrecognized token response: %s", string(trunc))
}

// fetchUserInfo calls the userinfo endpoint and unwraps a wrapped envelope.
func (a *oauth2Adapter) fetchUserInfo(ctx context.Context, accessToken string) (map[string]any, error) {
	if a.provider.UserInfoURL == nil || *a.provider.UserInfoURL == "" {
		return nil, fmt.Errorf("sso: oauth2 provider %q missing userinfo_url", a.provider.Slug)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, *a.provider.UserInfoURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sso: userinfo request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("sso: userinfo returned %d: %s", resp.StatusCode, string(body))
	}

	var userInfo map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&userInfo); err != nil {
		return nil, fmt.Errorf("sso: decode userinfo: %w", err)
	}

	// Unwrap a wrapped envelope: {"success":...,"data":{...}}.
	if _, hasSuccess := userInfo["success"]; hasSuccess {
		if data, ok := userInfo["data"].(map[string]any); ok {
			return data, nil
		}
	}
	return userInfo, nil
}

// --- userinfo field mappers (copied from OpenBucket, claim name configurable) ---

func getSubject(userInfo map[string]any) string {
	for _, field := range []string{"sub", "id", "user_id"} {
		if v, ok := userInfo[field]; ok {
			if s := fmt.Sprint(v); s != "" && s != "<nil>" {
				return s
			}
		}
	}
	return ""
}

func getUserEmail(userInfo map[string]any, emailClaim string) string {
	fields := []string{}
	if emailClaim != "" {
		fields = append(fields, emailClaim)
	}
	fields = append(fields, "email", "preferred_username", "upn", "mail")
	for _, field := range fields {
		if v, ok := userInfo[field]; ok {
			if s := fmt.Sprint(v); s != "" && s != "<nil>" && strings.Contains(s, "@") {
				return s
			}
		}
	}
	return ""
}

func getUserName(userInfo map[string]any) string {
	if v, ok := userInfo["name"]; ok {
		if s := fmt.Sprint(v); s != "" && s != "<nil>" {
			return s
		}
	}
	if given, ok := userInfo["given_name"]; ok {
		gs := fmt.Sprint(given)
		if gs != "" && gs != "<nil>" {
			name := gs
			if family, ok := userInfo["family_name"]; ok {
				fs := fmt.Sprint(family)
				if fs != "" && fs != "<nil>" {
					name += " " + fs
				}
			}
			return strings.TrimSpace(name)
		}
	}
	return ""
}

func getUserPicture(userInfo map[string]any) string {
	for _, field := range []string{"picture", "avatar_url", "photo", "profile_image_url", "image"} {
		if v, ok := userInfo[field]; ok {
			s := fmt.Sprint(v)
			if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
				return s
			}
		}
	}
	return ""
}

func namePtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
