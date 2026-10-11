package sso

import (
	"context"
	"fmt"
	"strings"
	"sync"

	oidcsession "github.com/aidenappl/go-oidc-session"
	"github.com/aidenappl/go-oidc-session/sqlstore"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/telemetry"
	"github.com/aidenappl/monitor-core/tools"
)

// SessionCookie is the browser's SSO session cookie: opaque and HttpOnly. It
// stands for an oidc_sessions row holding the provider's tokens (encrypted with
// MON_CRYPTO_KEY). It is set on MON_COOKIE_DOMAIN like the mon-* cookies,
// because the browser reaches /auth/refresh through monitor-web's proxy on the
// web host, which forwards the Cookie header here.
//
// Monitor still mints its own short access JWT, because zones verify that
// statelessly. What changed is the refresh: POST /auth/refresh runs
// Manager().Authenticate against this cookie, which refreshes the provider's
// tokens when they are due and ends the session when the provider refuses —
// so a Monitor refresh family only lives as long as the provider session it
// came from. See routes/HandleRefresh.router.go.
const SessionCookie = "mon_sso_session"

var (
	managerOnce sync.Once
	manager     *oidcsession.Manager
)

// Manager returns the SSO session manager. Providers are re-resolved from
// sso_providers on every use, so admin changes apply without a restart.
func Manager() *oidcsession.Manager {
	managerOnce.Do(func() {
		web := strings.TrimRight(env.WebBaseURL, "/")
		manager = &oidcsession.Manager{
			Providers:        loadLibProvider,
			States:           NewStateStore(db.SQL),
			Sessions:         Store(),
			Users:            NewResolver(db.SQL),
			CookieName:       SessionCookie,
			CookieDomain:     env.CookieDomain,
			Insecure:         env.CookieInsecure,
			DefaultReturnURL: web + "/",
			LoginPageURL:     web + "/login",
			// signed_out=1 stops the login page from signing straight back in
			// silently. Register this exact URL as a logout redirect URI with
			// the provider.
			PostLogoutURL: web + "/login?signed_out=1",
			OnEvent: func(ctx context.Context, e oidcsession.Event) {
				name := "auth.sso." + e.Name
				if e.Warn {
					telemetry.Warn(ctx, name, e.Fields)
					return
				}
				telemetry.Info(ctx, name, e.Fields)
			},
			Logf: libraryLogfCtx,
		}
	})
	return manager
}

// Store is the oidc_sessions store (migration 135).
func Store() *sqlstore.Store {
	return &sqlstore.Store{
		DB:     db.SQL,
		Sealer: cryptoSealer{},
		Logf:   libraryLogf,
	}
}

// loadLibProvider resolves a provider slug from sso_providers.
func loadLibProvider(_ context.Context, slug string) (*oidcsession.Provider, error) {
	p, err := LoadProvider(db.SQL, slug)
	if err != nil {
		return nil, err
	}
	if !p.Enabled() {
		return nil, fmt.Errorf("sso: provider %q is disabled", slug)
	}
	return p.Provider, nil
}

// cryptoSealer encrypts session tokens with MON_CRYPTO_KEY (tools.Encrypt).
type cryptoSealer struct{}

func (cryptoSealer) Seal(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	return tools.Encrypt(plaintext)
}

func (cryptoSealer) Open(sealed string) (string, error) {
	if sealed == "" {
		return "", nil
	}
	return tools.Decrypt(sealed)
}
