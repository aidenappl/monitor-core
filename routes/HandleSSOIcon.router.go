package routes

import (
	"net/http"
	"strconv"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/gorilla/mux"
)

// HandleSSOIcon serves a provider's cached icon (GET /auth/sso/icon/{slug}).
//
// ─────────────────────────────────────────────────────────────────────────────
// THIS EXISTS SO THE LOGIN PAGE NEVER HOT-LINKS A THIRD PARTY.
//
// The bytes were fetched once at save time by go-forta/sso's FetchIcon, which is
// where the SSRF, SVG and size defences live, re-encoded to PNG, and stored. This
// route hands them back from this application's own origin — so an unauthenticated
// visitor's IP, User-Agent and Referer never reach whoever hosts the original,
// and the login page does not break when that host does.
// ─────────────────────────────────────────────────────────────────────────────
//
// It is PUBLIC and unauthenticated, necessarily — the login page is where it is
// used. That is safe because it can only ever return bytes this server produced
// with its own PNG encoder, for a slug that exists and is enabled.
func HandleSSOIcon(w http.ResponseWriter, r *http.Request) {
	slug := mux.Vars(r)["slug"]
	if slug == "" {
		http.NotFound(w, r)
		return
	}

	contentType, data, err := query.GetProviderIcon(db.SQL, slug)
	if err != nil {
		// 404 rather than 500: this is on the login page, and a database blip must
		// not render an error where a missing image would do. The login page's
		// fallback for a broken icon is the text button, which is the same fallback
		// as for a provider that has no icon at all.
		http.NotFound(w, r)
		return
	}
	if len(data) == 0 {
		// No icon configured, or the fetch failed. Contractually the same thing:
		// /auth/sso/config would not have pointed here in that case, so this is a
		// stale or hand-built URL.
		http.NotFound(w, r)
		return
	}

	// ⚠️ EVERY ONE OF THESE HEADERS IS LOAD-BEARING.
	//
	// nosniff: the bytes are always PNG produced by our own encoder, but a browser
	// that sniffs could still be talked into treating some future stored value as
	// something else. Turning sniffing off is the difference between "this is an
	// image" and "this is whatever the browser decides".
	//
	// Content-Disposition: inline with no filename — the asset is rendered, never
	// downloaded, and there is no user-supplied filename to reflect.
	//
	// The Content-Type comes from the SNIFFED type recorded at fetch time, not
	// from anything the administrator or the remote server said.
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Disposition", "inline")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))

	// Long cache: the bytes only change when an administrator re-saves the
	// provider, and the login page loads this on every visit. `immutable` is
	// deliberately NOT set — the URL has no content hash, so the bytes at this URL
	// genuinely can change and a client must be able to revalidate.
	w.Header().Set("Cache-Control", "public, max-age=86400")

	_, _ = w.Write(data)
}
