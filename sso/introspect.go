package sso

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// IntrospectResponse is the subset of RFC 7662 §2.2 fields the checkpoint reads.
type IntrospectResponse struct {
	Active   bool   `json:"active"`
	Scope    string `json:"scope,omitempty"`
	ClientID string `json:"client_id,omitempty"`
	Username string `json:"username,omitempty"`
	Sub      string `json:"sub,omitempty"`
	Exp      int64  `json:"exp,omitempty"`
}

// Introspect calls a provider's RFC 7662 introspection endpoint with the
// provider's client credentials and the user's token. active=false means the
// token is expired, revoked, or the underlying grant is gone. hint is optional
// ("access_token"/"refresh_token"); pass "" to let the server decide.
func Introspect(p *Provider, token, hint string) (*IntrospectResponse, error) {
	if p.IntrospectURL == nil || *p.IntrospectURL == "" {
		return nil, fmt.Errorf("sso: provider %q has no introspect_url", p.Slug)
	}
	if p.ClientID == nil || *p.ClientID == "" || p.ClientSecret == "" {
		return nil, fmt.Errorf("sso: provider %q missing client credentials for introspection", p.Slug)
	}

	form := url.Values{}
	form.Set("token", token)
	if hint != "" {
		form.Set("token_type_hint", hint)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, *p.IntrospectURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("sso: introspect request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(*p.ClientID, p.ClientSecret)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sso: introspect: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("sso: introspect status %d: %s", resp.StatusCode, string(body))
	}

	var out IntrospectResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("sso: introspect decode: %w", err)
	}
	return &out, nil
}
