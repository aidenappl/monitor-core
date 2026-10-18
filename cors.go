package main

import (
	"net/http"

	"github.com/rs/cors"
)

// Two CORS policies, picked by the request's Origin.
//
// Credentialed: only the Monitor dashboards. The mon-* session cookies are
// host-only on the API host, but a fetch from any *.appleby.cloud page is
// same-site, so the browser attaches them. With *.appleby.cloud credentialed,
// any page on any subdomain (every appleby.cloud Site included) could read
// Monitor data as whoever was viewing it.
//
// Public: everything else that may call in from a browser (monitor-js ingest
// from appleby.cloud and Trailblaze apps). It authenticates with X-Api-Key and
// never sends cookies, so it needs no credentials.
var (
	dashboardOrigins = []string{
		"https://monitor.local.appleby.cloud:3020",
		"https://monitor.appleby.cloud",
		"http://localhost:*",
	}
	publicOrigins = []string{
		"https://trailblaze.to",
		"https://*.trailblaze.to",
		"https://*.appleby.cloud",
	}
)

var corsHeaders = []string{"X-Requested-With", "Content-Type", "Origin", "Authorization", "Accept", "Referer", "Dnt", "User-Agent", "X-Api-Key", "X-CSRF-Token"}

func corsHandler(h http.Handler) http.Handler {
	options := func(origins []string, credentials bool) cors.Options {
		return cors.Options{
			AllowedOrigins:   origins,
			AllowCredentials: credentials,
			AllowedHeaders:   corsHeaders,
			AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
			// Let browsers cache a preflight for 2h (Chromium's cap) instead of
			// the 5s default. Without it monitor-js pays a preflight before nearly
			// every ingest flush. The one cost: a change to AllowedHeaders or
			// AllowedMethods can take up to 2h to reach an already-open browser.
			MaxAge: 7200,
		}
	}
	dashboard := cors.New(options(dashboardOrigins, true))
	withDashboard := dashboard.Handler(h)
	withPublic := cors.New(options(publicOrigins, false)).Handler(h)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if dashboard.OriginAllowed(r) {
			withDashboard.ServeHTTP(w, r)
			return
		}
		withPublic.ServeHTTP(w, r)
	})
}
