package sso

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	ssolib "github.com/aidenappl/go-forta/sso"
	"github.com/aidenappl/monitor-core/middleware"
)

// fakeSessionStore holds one due-for-check SSO session per user, so every Check
// reaches introspection. No database is touched.
type fakeSessionStore struct {
	mu      sync.Mutex
	deleted map[int64]bool
}

func (f *fakeSessionStore) SaveSession(context.Context, int64, ssolib.Session) error { return nil }

func (f *fakeSessionStore) LoadSession(_ context.Context, userID int64) (*ssolib.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleted[userID] {
		return nil, nil
	}
	// LastCheckedAt zero = never checked, which forces introspection.
	return &ssolib.Session{
		Provider: "forta",
		Subject:  "sub-1",
		Tokens:   ssolib.TokenSet{RefreshToken: "rt-opaque"},
	}, nil
}

func (f *fakeSessionStore) TouchSession(context.Context, int64) error { return nil }

func (f *fakeSessionStore) DeleteSession(_ context.Context, userID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleted == nil {
		f.deleted = map[int64]bool{}
	}
	f.deleted[userID] = true
	return nil
}

// introspectionServer is a stand-in for forta-api's introspection endpoint that
// records the X-Request-ID each call arrives with.
func introspectionServer(t *testing.T, active bool) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("X-Request-ID"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if active {
			_, _ = w.Write([]byte(`{"active":true}`))
			return
		}
		_, _ = w.Write([]byte(`{"active":false}`))
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

// The request id monitor-core's RequestIDMiddleware puts on the context must
// arrive at the IdP as X-Request-ID, through the ctx-aware hook, on both an
// allow and a deny. Without a request id nothing is sent — and never a
// placeholder like "unknown".
func TestCheckpointForwardsRequestIDToIntrospection(t *testing.T) {
	const rid = "3f2b8c1e-7a4d-4e0b-9c2a-1d5e6f7a8b9c"

	tests := []struct {
		name      string
		active    bool
		ctxRID    string
		wantAllow bool
		wantRID   string
	}{
		{name: "active grant, request id forwarded", active: true, ctxRID: rid, wantAllow: true, wantRID: rid},
		{name: "revoked grant, request id forwarded", active: false, ctxRID: rid, wantAllow: false, wantRID: rid},
		{name: "no request id on context, none sent", active: true, wantAllow: true, wantRID: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, seen := introspectionServer(t, tc.active)
			providers := func(context.Context, string) (*ssolib.Provider, error) {
				return &ssolib.Provider{
					Slug:          "forta",
					IntrospectURL: srv.URL,
					ClientID:      "monitor",
					ClientSecret:  "secret",
				}, nil
			}
			decide := checkpointDecision(newCheckpointer(&fakeSessionStore{}, providers))

			ctx := context.Background()
			if tc.ctxRID != "" {
				ctx = context.WithValue(ctx, middleware.RequestIDKey, tc.ctxRID)
			}

			if got := decide(ctx, 42); got != tc.wantAllow {
				t.Errorf("decision = %v, want %v", got, tc.wantAllow)
			}
			calls := seen()
			if len(calls) != 1 {
				t.Fatalf("introspection calls = %d, want 1", len(calls))
			}
			if calls[0] != tc.wantRID {
				t.Errorf("X-Request-ID at IdP = %q, want %q", calls[0], tc.wantRID)
			}
		})
	}
}

func TestRequestCorrelationReadsMonitorRequestID(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{name: "request id present", ctx: context.WithValue(context.Background(), middleware.RequestIDKey, "abc12345"), want: "abc12345"},
		{name: "no request id", ctx: context.Background(), want: ""},
		{name: "wrong type ignored", ctx: context.WithValue(context.Background(), middleware.RequestIDKey, 12345), want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rid, tid := requestCorrelation(tc.ctx)
			if rid != tc.want {
				t.Errorf("request id = %q, want %q", rid, tc.want)
			}
			if tid != "" {
				t.Errorf("trace id = %q, want empty (monitor-core has none)", tid)
			}
		})
	}
}
