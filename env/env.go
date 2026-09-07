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

	// ClickHouseMaxMemoryUsage is the per-query memory ceiling in BYTES, passed
	// to ClickHouse as the max_memory_usage setting. Exposed as config because
	// the right number is a property of the host, not of this code — see the
	// comment on the Settings map in db/clickhouse.go for why the default is
	// what it is.
	ClickHouseMaxMemoryUsage int

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

	// WebBaseURL is the monitor-web app origin (scheme+host, no trailing slash).
	// SSO login/callback run on monitor-core (PublicBaseURL / the API host), but
	// after completing they must send the browser back to the WEB app — so the
	// post-login and error redirects are built against this, not a relative path
	// (which would resolve to the API host). Defaults to the dashboard origin.
	WebBaseURL string

	// SSE
	MaxSSESubscribers int

	// Native auth bootstrap & policy.
	// AdminEmail/AdminPassword seed the first admin on a fresh DB (see
	// bootstrap.EnsureAdminUser). AllowRegistration gates public self-service
	// /auth/register — off by default because Monitor is an internal tool.
	AdminEmail        string
	AdminPassword     string
	AllowRegistration bool

	// Tenancy registry seed (see bootstrap.EnsureZoneAndProject). Both name the
	// single zone and single project a fresh install is seeded with. Slugs are
	// immutable, so changing either after the rows exist renames NOTHING — the
	// next boot seeds an additional zone or project beside the old one.
	//
	// NEITHER IS BOOT-ONLY, and DefaultProjectSlug least of all. An earlier
	// version of this comment claimed both were read only by the seeder; that was
	// true when it was written and stopped being true when reads became
	// project-scoped. Changing MON_DEFAULT_PROJECT on a running install is a live
	// behaviour change with a data-visibility blast radius, because it is read:
	//   * per request, by IngestAuthMiddleware and QueryAuthMiddleware — it is the
	//     project the env master key writes AND reads, and the one a dashboard
	//     session reads;
	//   * per query, by scope.ProjectPredicate — and ONLY the reader whose project
	//     equals it also matches the pre-006 rows that carry an empty project, so
	//     pointing it somewhere else makes every unbackfilled event invisible;
	//   * per event, by scope.Matches on the SSE path, under the same rule;
	//   * per error, by issues.fingerprintProject, which folds an empty project
	//     onto it — so it is a component of the issue fingerprint, and moving it
	//     re-keys every issue derived from an unstamped row.
	// The api_keys rows are NOT re-pointed by changing it: they hold a project id
	// (migration 117), so a credential keeps reading the project it was bound to
	// while the master key and every session move to the new one. ZoneSlug is
	// narrower but still not boot-only — apikeys.resolveProject reads it on
	// POST /v1/api-keys to decide which zone a new key's project is looked up in.
	//
	// ZoneSlug defaults to "trailblaze" because that is what the existing data
	// actually is: all 15 services currently reporting to this instance are
	// Trailblaze services, so any other default would be a fiction the first
	// operator has to correct.
	//
	// DefaultProjectSlug defaults to "default" because every api_keys row binds
	// to it, and because the env master key — which has no api_keys row and
	// therefore no binding of its own — stamps its events with it too. That
	// follows the Mimir "anonymous" and Loki "fake" precedent: the tenant
	// dimension is never left null, because a null tenant is a row that every
	// later filter silently drops.
	ZoneSlug           string
	DefaultProjectSlug string

	// GitHub integration for issue links. Both are OPTIONAL — with neither set,
	// links are still stored and rendered, they just carry no live state and no
	// webhook updates. GitHub must never be required for issue triage to work.
	//
	// The token is scoped to one org (Trailblaze, hence the suffix). A link to a
	// repo outside that scope degrades to a bare URL rather than failing; adding
	// another org means adding another token and a lookup by owner.
	GitHubToken         string
	GitHubWebhookSecret string
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

	// 2 GiB. Sized against the deployed host (worker-6, ~8 GB shared with other
	// containers), not against ClickHouse's own 10 GiB default — which is larger
	// than the whole machine and therefore no limit at all here.
	ClickHouseMaxMemoryUsage = getEnvInt("CLICKHOUSE_MAX_MEMORY_USAGE", 2147483648)

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
	PublicBaseURL = getEnv("MON_PUBLIC_URL", "https://api.monitor.appleby.cloud")
	WebBaseURL = getEnv("MON_WEB_URL", "https://monitor.appleby.cloud")

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

	// Tenancy registry seed. Both are read once, by bootstrap.EnsureZoneAndProject,
	// and both are validated as slugs there — a typo fails the boot rather than
	// minting a permanently misnamed row, since slugs are immutable.
	ZoneSlug = getEnv("MON_ZONE_SLUG", "trailblaze")
	DefaultProjectSlug = getEnv("MON_DEFAULT_PROJECT", "default")

	// Sourced from Keyring in production. No dev fallback and no panic: absent
	// means the GitHub features are simply inactive.
	GitHubToken = getEnv("MON_GITHUB_TOKEN_TRAILBLAZE", "")
	GitHubWebhookSecret = getEnv("MON_GITHUB_WEBHOOK_SECRET_TRAILBLAZE", "")
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
