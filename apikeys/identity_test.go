package apikeys

import "testing"

// seedCache replaces the validation cache for the duration of a test, restoring
// it afterwards. It bypasses the DB so key resolution can be tested in isolation.
func seedCache(t *testing.T, entries map[string]cachedKey) {
	t.Helper()

	cacheMu.Lock()
	previous := cache
	cache = entries
	cacheMu.Unlock()

	t.Cleanup(func() {
		cacheMu.Lock()
		cache = previous
		cacheMu.Unlock()
	})
}

func TestValidateWithIdentity(t *testing.T) {
	const (
		adminRaw   = "raw-admin-key"
		ingestRaw  = "raw-ingest-key"
		unnamedRaw = "raw-unnamed-key"
	)

	seedCache(t, map[string]cachedKey{
		hashKey(adminRaw):   {ID: "k-admin", Name: "monitor-mcp", Scope: ScopeAdmin},
		hashKey(ingestRaw):  {ID: "k-ingest", Name: "go-monitor", Scope: ScopeIngest},
		hashKey(unnamedRaw): {ID: "k-unnamed", Name: "", Scope: ScopeAdmin},
	})

	tests := []struct {
		name      string
		raw       string
		wantOK    bool
		wantID    string
		wantName  string
		wantScope Scope
	}{
		{name: "admin key resolves with name", raw: adminRaw, wantOK: true, wantID: "k-admin", wantName: "monitor-mcp", wantScope: ScopeAdmin},
		{name: "ingest key resolves with its scope", raw: ingestRaw, wantOK: true, wantID: "k-ingest", wantName: "go-monitor", wantScope: ScopeIngest},
		{name: "unnamed key still resolves", raw: unnamedRaw, wantOK: true, wantID: "k-unnamed", wantName: "", wantScope: ScopeAdmin},
		{name: "unknown key does not resolve", raw: "nope", wantOK: false},
		{name: "empty key does not resolve", raw: "", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity, ok := ValidateWithIdentity(tt.raw)
			if ok != tt.wantOK {
				t.Fatalf("ValidateWithIdentity(%q) ok = %v, want %v", tt.raw, ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if identity.ID != tt.wantID {
				t.Errorf("ID = %q, want %q", identity.ID, tt.wantID)
			}
			if identity.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", identity.Name, tt.wantName)
			}
			if identity.Scope != tt.wantScope {
				t.Errorf("Scope = %q, want %q", identity.Scope, tt.wantScope)
			}
		})
	}
}

// TestValidateWithScopeStillWorks guards the pre-existing contract, since
// ValidateWithScope was reimplemented on top of ValidateWithIdentity.
func TestValidateWithScopeStillWorks(t *testing.T) {
	const adminRaw = "raw-admin-key"

	seedCache(t, map[string]cachedKey{
		hashKey(adminRaw): {ID: "k-admin", Name: "monitor-mcp", Scope: ScopeAdmin},
	})

	if got := ValidateWithScope(adminRaw); got != ScopeAdmin {
		t.Errorf("ValidateWithScope(valid) = %q, want %q", got, ScopeAdmin)
	}
	if got := ValidateWithScope("nope"); got != "" {
		t.Errorf("ValidateWithScope(invalid) = %q, want empty string", got)
	}
}
