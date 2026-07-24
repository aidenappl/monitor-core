package env

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Dev-only fallback secrets. These let `go build`/`go test`/local runs work
// without Keyring, but a production process (CookieInsecure=false) MUST NOT boot
// on them — RequireProductionSecrets enforces that. Keep these in sync with the
// getEnv fallbacks below.
const (
	devJWTSigningKey = "dev-monitor-jwt-signing-key-change-me"
	devCryptoKey     = "dev-monitor-crypto-key-32-byte!!"
)

var (
	Port               string
	ClickHouseAddr     string
	ClickHouseDatabase string
	ClickHouseUsername string
	ClickHousePassword string
	IngestKey          string
	BatchSize          int

	// MariaDB (relational auth data layer — users, identities, tokens, api_keys)
	MonDBDSN      string
	FlushInterval time.Duration
	QueueSize     int

	// Monitor-owned session auth (Phase 2).
	// JWTSigningKey signs Monitor's own HS512 access/refresh JWTs.
	// CryptoKey (exactly 32 bytes) is the AES-256-GCM key used to encrypt
	// SSO secrets/tokens at rest (needed by Phase 3).
	// CookieDomain/CookieInsecure control the mon-* auth cookie flags.
	JWTSigningKey  string
	CryptoKey      string
	CookieDomain   string
	CookieInsecure bool

	// PublicBaseURL is Monitor's externally-reachable origin (scheme+host, no
	// trailing slash). SSO builds the per-provider redirect_uri from it as
	// {PublicBaseURL}/auth/sso/{slug}/callback — this MUST match byte-for-byte
	// the redirect registered with each IdP (OAuth redirect_uri exact-match).
	PublicBaseURL string

	// SSE
	MaxSSESubscribers int

	// Native auth bootstrap & policy.
	// AdminEmail/AdminPassword seed the first admin on a fresh DB (see
	// bootstrap.EnsureAdminUser). AllowRegistration gates public self-service
	// /auth/register — off by default because Monitor is an internal tool.
	AdminEmail        string
	AdminPassword     string
	AllowRegistration bool

	// Forta SSO provider seed (migration path). When these are set, bootstrap
	// seeds a "forta" row into sso_providers on startup so existing Forta users
	// can keep clicking "Continue with Forta" (see bootstrap.EnsureFortaProvider).
	// All empty means "no Forta button" — seeding is skipped and startup is
	// unaffected. The client secret is NOT read here: the seed stores a Keyring
	// reference (client_secret_ref = "MON_SSO_FORTA_CLIENT_SECRET"), resolved at
	// login time by sso.resolveSecret.
	FortaAuthorizeURL  string
	FortaTokenURL      string
	FortaUserInfoURL   string
	FortaIntrospectURL string
	FortaClientID      string
	FortaScopes        string
)

// Load reads all configuration from environment variables.
// Call this after injecting secrets (e.g. via go-keyring) so that
// any Keyring-provided values are present in os.Getenv before this runs.
func Load() {
	Port = getEnv("HTTP_PORT", "8080")
	ClickHouseAddr = getEnv("CLICKHOUSE_ADDR", "localhost:9000")
	ClickHouseDatabase = getEnv("CLICKHOUSE_DATABASE", "monitor")
	ClickHouseUsername = getEnv("CLICKHOUSE_USERNAME", "default")
	ClickHousePassword = getEnv("CLICKHOUSE_PASSWORD", "")
	IngestKey = getEnv("MONITOR_API_KEY", "")

	// MariaDB DSN — sourced from Keyring (MON_DB_DSN) in production, injected
	// into the environment before Load() runs. The fallback targets the local
	// docker-compose MariaDB (host port 3336) so build/tests don't require Keyring.
	MonDBDSN = getEnv("MON_DB_DSN", "monitor:monitor@tcp(127.0.0.1:3336)/monitor_auth")

	// Monitor-owned session auth (Phase 2). The dev fallbacks let build/tests run
	// without Keyring; production sources these from Keyring (MON_JWT_SIGNING_KEY,
	// MON_CRYPTO_KEY) injected into the environment before Load() runs.
	JWTSigningKey = getEnv("MON_JWT_SIGNING_KEY", devJWTSigningKey)
	CryptoKey = getEnv("MON_CRYPTO_KEY", devCryptoKey) // exactly 32 bytes
	CookieDomain = getEnv("MON_COOKIE_DOMAIN", "")
	CookieInsecure = getEnv("MON_COOKIE_INSECURE", "false") == "true"
	PublicBaseURL = getEnv("MON_PUBLIC_URL", "https://monitor.appleby.cloud")

	BatchSize = getEnvInt("BATCH_SIZE", 1000)
	FlushInterval = getEnvDuration("FLUSH_INTERVAL", 5*time.Second)
	QueueSize = getEnvInt("QUEUE_SIZE", 100000)

	MaxSSESubscribers = getEnvInt("MAX_SSE_SUBSCRIBERS", 100)

	// Native auth bootstrap & policy. AdminEmail/AdminPassword come from Keyring
	// (MON_ADMIN_EMAIL / MON_ADMIN_PASSWORD) in production; empty means "don't
	// seed an admin". Registration is disabled unless MON_ALLOW_REGISTRATION=true.
	AdminEmail = getEnv("MON_ADMIN_EMAIL", "")
	AdminPassword = getEnv("MON_ADMIN_PASSWORD", "")
	AllowRegistration = getEnv("MON_ALLOW_REGISTRATION", "false") == "true"

	// Forta SSO provider seed. Empty defaults mean "don't seed the Forta row"
	// (no Forta button); a missing Forta config never blocks startup.
	FortaAuthorizeURL = getEnv("MON_SSO_FORTA_AUTHORIZE_URL", "")
	FortaTokenURL = getEnv("MON_SSO_FORTA_TOKEN_URL", "")
	FortaUserInfoURL = getEnv("MON_SSO_FORTA_USERINFO_URL", "")
	FortaIntrospectURL = getEnv("MON_SSO_FORTA_INTROSPECT_URL", "")
	FortaClientID = getEnv("MON_SSO_FORTA_CLIENT_ID", "")
	FortaScopes = getEnv("MON_SSO_FORTA_SCOPES", "openid email profile")
}

// RequireProductionSecrets returns an error if the process is running in a
// non-dev profile (CookieInsecure=false) while still using the committed dev
// fallback secrets, or a CryptoKey of the wrong length. A production process
// booting on the well-known default JWT key would let anyone forge an admin
// session token, and the default AES key would make every "encrypted" SSO
// secret trivially decryptable — so main() must fail fast on this.
// Call after Load(). In local dev, set MON_COOKIE_INSECURE=true to allow the
// defaults.
func RequireProductionSecrets() error {
	if CookieInsecure {
		return nil
	}
	if JWTSigningKey == devJWTSigningKey || JWTSigningKey == "" {
		return fmt.Errorf("MON_JWT_SIGNING_KEY is unset or the dev default; refusing to start in production (set MON_COOKIE_INSECURE=true for local dev)")
	}
	if CryptoKey == devCryptoKey || CryptoKey == "" {
		return fmt.Errorf("MON_CRYPTO_KEY is unset or the dev default; refusing to start in production (set MON_COOKIE_INSECURE=true for local dev)")
	}
	if len(CryptoKey) != 32 {
		return fmt.Errorf("MON_CRYPTO_KEY must be exactly 32 bytes for AES-256-GCM, got %d", len(CryptoKey))
	}
	return nil
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}
