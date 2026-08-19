package structs

// ActorKind identifies what sort of principal made a request.
type ActorKind string

const (
	// ActorKindUser is a human authenticated by a Monitor session JWT.
	ActorKindUser ActorKind = "user"
	// ActorKindAPIKey is a DB-stored api_keys credential — how agents (monitor-mcp)
	// and CI authenticate.
	ActorKindAPIKey ActorKind = "api_key"
	// ActorKindSystem is Monitor itself: the env master key, and automated
	// transitions with no external caller (e.g. an issue regressing on recurrence).
	ActorKindSystem ActorKind = "system"
)

// IsValid reports whether the kind is one of the known values.
func (k ActorKind) IsValid() bool {
	switch k {
	case ActorKindUser, ActorKindAPIKey, ActorKindSystem:
		return true
	}
	return false
}

// Actor is the resolved principal behind a request, injected into the request
// context by QueryAuthMiddleware/SessionMiddleware and read back with
// middleware.GetActor.
//
// It exists because authorship is not recoverable after the fact: an API key can
// be deleted, a user can be deactivated, and the audit row still has to say who
// did the thing. Anything that records an actor should persist Label alongside
// the id, denormalised, so history survives the principal it refers to.
//
// Exactly one of UserID / APIKeyID is set, matching Kind. Both are nil for
// ActorKindSystem.
type Actor struct {
	Kind     ActorKind `json:"kind"`
	UserID   *int64    `json:"user_id,omitempty"`
	APIKeyID *string   `json:"api_key_id,omitempty"`
	// Label is a human-recognisable name for the actor — a user's display name or
	// email, an API key's name, or a fixed identifier for system actions. Never
	// empty.
	Label string `json:"label"`
}

// SystemActor returns the actor used for automated, uncalled transitions.
func SystemActor(label string) *Actor {
	return &Actor{Kind: ActorKindSystem, Label: label}
}

// UserActor builds an actor from an authenticated user, preferring the display
// name over the email as a label but never returning an empty one.
func UserActor(u *User) *Actor {
	if u == nil {
		return nil
	}
	label := u.Email
	if u.Name != nil && *u.Name != "" {
		label = *u.Name
	}
	if u.DisplayName != nil && *u.DisplayName != "" {
		label = *u.DisplayName
	}
	id := u.ID
	return &Actor{Kind: ActorKindUser, UserID: &id, Label: label}
}

// APIKeyActor builds an actor from a validated API key.
func APIKeyActor(id, name string) *Actor {
	label := name
	if label == "" {
		label = "api key " + id
	}
	keyID := id
	return &Actor{Kind: ActorKindAPIKey, APIKeyID: &keyID, Label: label}
}
