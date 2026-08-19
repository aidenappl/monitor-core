package structs

import "testing"

func strPtr(s string) *string { return &s }

func TestActorKindIsValid(t *testing.T) {
	tests := []struct {
		name string
		kind ActorKind
		want bool
	}{
		{name: "user", kind: ActorKindUser, want: true},
		{name: "api_key", kind: ActorKindAPIKey, want: true},
		{name: "system", kind: ActorKindSystem, want: true},
		{name: "empty", kind: ActorKind(""), want: false},
		{name: "unknown", kind: ActorKind("robot"), want: false},
		{name: "wrong case", kind: ActorKind("User"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.kind.IsValid(); got != tt.want {
				t.Errorf("ActorKind(%q).IsValid() = %v, want %v", tt.kind, got, tt.want)
			}
		})
	}
}

// TestUserActorLabelPrecedence pins the label fallback chain. The label is
// denormalised onto audit rows, so an empty one would produce history that
// cannot name who acted.
func TestUserActorLabelPrecedence(t *testing.T) {
	tests := []struct {
		name string
		user *User
		want string
	}{
		{
			name: "display name wins",
			user: &User{ID: 1, Email: "a@b.c", Name: strPtr("Aiden"), DisplayName: strPtr("aiden.dev")},
			want: "aiden.dev",
		},
		{
			name: "falls back to name",
			user: &User{ID: 1, Email: "a@b.c", Name: strPtr("Aiden")},
			want: "Aiden",
		},
		{
			name: "falls back to email",
			user: &User{ID: 1, Email: "a@b.c"},
			want: "a@b.c",
		},
		{
			name: "empty pointers are skipped, not used",
			user: &User{ID: 1, Email: "a@b.c", Name: strPtr(""), DisplayName: strPtr("")},
			want: "a@b.c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actor := UserActor(tt.user)
			if actor == nil {
				t.Fatal("UserActor returned nil for a non-nil user")
			}
			if actor.Kind != ActorKindUser {
				t.Errorf("Kind = %q, want %q", actor.Kind, ActorKindUser)
			}
			if actor.Label != tt.want {
				t.Errorf("Label = %q, want %q", actor.Label, tt.want)
			}
			if actor.UserID == nil || *actor.UserID != tt.user.ID {
				t.Errorf("UserID = %v, want %d", actor.UserID, tt.user.ID)
			}
			if actor.APIKeyID != nil {
				t.Errorf("APIKeyID = %v, want nil for a user actor", actor.APIKeyID)
			}
		})
	}
}

func TestUserActorNilUser(t *testing.T) {
	if actor := UserActor(nil); actor != nil {
		t.Errorf("UserActor(nil) = %+v, want nil", actor)
	}
}

func TestAPIKeyActor(t *testing.T) {
	tests := []struct {
		name      string
		id        string
		keyName   string
		wantLabel string
	}{
		{name: "named key", id: "k1", keyName: "monitor-mcp", wantLabel: "monitor-mcp"},
		{name: "unnamed key falls back to id", id: "k2", keyName: "", wantLabel: "api key k2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actor := APIKeyActor(tt.id, tt.keyName)
			if actor.Kind != ActorKindAPIKey {
				t.Errorf("Kind = %q, want %q", actor.Kind, ActorKindAPIKey)
			}
			if actor.Label != tt.wantLabel {
				t.Errorf("Label = %q, want %q", actor.Label, tt.wantLabel)
			}
			if actor.APIKeyID == nil || *actor.APIKeyID != tt.id {
				t.Errorf("APIKeyID = %v, want %q", actor.APIKeyID, tt.id)
			}
			if actor.UserID != nil {
				t.Errorf("UserID = %v, want nil for an api key actor", actor.UserID)
			}
		})
	}
}

func TestSystemActor(t *testing.T) {
	actor := SystemActor("env-master-key")
	if actor.Kind != ActorKindSystem {
		t.Errorf("Kind = %q, want %q", actor.Kind, ActorKindSystem)
	}
	if actor.Label != "env-master-key" {
		t.Errorf("Label = %q, want %q", actor.Label, "env-master-key")
	}
	if actor.UserID != nil || actor.APIKeyID != nil {
		t.Errorf("system actor must carry neither UserID nor APIKeyID, got %v / %v", actor.UserID, actor.APIKeyID)
	}
}
