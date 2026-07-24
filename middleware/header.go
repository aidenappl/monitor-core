package middleware

import "net/http"

func MuxHeaderMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// CORS is owned entirely by rs/cors (configured in main.go). Do NOT set
		// Access-Control-* headers here — they would clobber rs/cors and break
		// credentialed CORS (a wildcard origin is invalid with credentials).
		w.Header().Set("Server", "Go")
		next.ServeHTTP(w, r)
	})
}
