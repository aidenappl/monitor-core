package routes

import (
	"net/http"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
)

// ssoConfigEntry is one login button rendered by the frontend login page.
type ssoConfigEntry struct {
	Slug        string `json:"slug"`
	ButtonLabel string `json:"button_label"`
	LoginURL    string `json:"login_url"`
}

// HandleSSOConfig is the PUBLIC provider discovery endpoint (GET
// /auth/sso/config). It returns only the enabled providers as {slug,
// button_label, login_url} — never any secret or URL detail — so an
// unauthenticated login page can render one button per provider.
func HandleSSOConfig(w http.ResponseWriter, r *http.Request) {
	providers, err := query.ListEnabledProviders(db.SQL)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to load sso providers", err)
		return
	}

	entries := make([]ssoConfigEntry, 0, len(providers))
	for _, p := range providers {
		label := p.DisplayName
		if p.ButtonLabel != nil && *p.ButtonLabel != "" {
			label = *p.ButtonLabel
		}
		entries = append(entries, ssoConfigEntry{
			Slug:        p.Slug,
			ButtonLabel: label,
			LoginURL:    "/auth/sso/" + p.Slug + "/login",
		})
	}

	responder.New(w, entries, "sso providers")
}
