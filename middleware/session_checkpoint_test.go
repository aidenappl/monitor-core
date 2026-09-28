package middleware

import (
	"context"
	"testing"
)

// withCheckpointHooks swaps both process-wide checkpoint hooks for one test and
// restores them. sso.Install assigns them at boot; nothing else mutates them.
func withCheckpointHooks(t *testing.T, legacy func(int64) bool, withCtx func(context.Context, int64) bool) {
	t.Helper()
	prevLegacy, prevCtx := SSOCheckpoint, SSOCheckpointCtx
	SSOCheckpoint, SSOCheckpointCtx = legacy, withCtx
	t.Cleanup(func() { SSOCheckpoint, SSOCheckpointCtx = prevLegacy, prevCtx })
}

// The context-aware hook must win whenever it is installed, the legacy bool
// hook must still decide on its own when it is the only one, and no hook at all
// must pass — SSO unconfigured is a legitimate state.
func TestPassesSSOCheckpointHookPrecedence(t *testing.T) {
	tests := []struct {
		name        string
		legacy      *bool // nil = hook not installed
		withCtx     *bool // nil = hook not installed
		want        bool
		wantLegacy  bool
		wantCtxCall bool
	}{
		{name: "no hooks passes", want: true},
		{name: "legacy only allows", legacy: ptr(true), want: true, wantLegacy: true},
		{name: "legacy only denies", legacy: ptr(false), want: false, wantLegacy: true},
		{name: "ctx only allows", withCtx: ptr(true), want: true, wantCtxCall: true},
		{name: "ctx only denies", withCtx: ptr(false), want: false, wantCtxCall: true},
		{name: "ctx preferred: ctx denies, legacy would allow", legacy: ptr(true), withCtx: ptr(false), want: false, wantCtxCall: true},
		{name: "ctx preferred: ctx allows, legacy would deny", legacy: ptr(false), withCtx: ptr(true), want: true, wantCtxCall: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var legacyCalled, ctxCalled bool
			var legacy func(int64) bool
			var withCtx func(context.Context, int64) bool
			if tc.legacy != nil {
				v := *tc.legacy
				legacy = func(int64) bool { legacyCalled = true; return v }
			}
			if tc.withCtx != nil {
				v := *tc.withCtx
				withCtx = func(context.Context, int64) bool { ctxCalled = true; return v }
			}
			withCheckpointHooks(t, legacy, withCtx)

			if got := passesSSOCheckpoint(context.Background(), 42); got != tc.want {
				t.Errorf("passesSSOCheckpoint = %v, want %v", got, tc.want)
			}
			if legacyCalled != tc.wantLegacy {
				t.Errorf("legacy hook called = %v, want %v", legacyCalled, tc.wantLegacy)
			}
			if ctxCalled != tc.wantCtxCall {
				t.Errorf("ctx hook called = %v, want %v", ctxCalled, tc.wantCtxCall)
			}
		})
	}
}

// The request's context — and so its request id — must reach the ctx hook
// unchanged, along with the user id. That is the whole point of the hook: the
// id rides on to the IdP introspection.
func TestPassesSSOCheckpointForwardsRequestContext(t *testing.T) {
	const rid = "3f2b8c1e-7a4d-4e0b-9c2a-1d5e6f7a8b9c"

	var gotRID string
	var gotUser int64
	withCheckpointHooks(t, nil, func(ctx context.Context, userID int64) bool {
		gotRID, _ = ctx.Value(RequestIDKey).(string)
		gotUser = userID
		return true
	})

	ctx := context.WithValue(context.Background(), RequestIDKey, rid)
	if !passesSSOCheckpoint(ctx, 7) {
		t.Fatal("checkpoint denied")
	}
	if gotRID != rid {
		t.Errorf("request id at hook = %q, want %q", gotRID, rid)
	}
	if gotUser != 7 {
		t.Errorf("user id at hook = %d, want 7", gotUser)
	}
}

func ptr(b bool) *bool { return &b }
