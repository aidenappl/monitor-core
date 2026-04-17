package env

import (
	"os"
	"strconv"
	"time"
)

var (
	Port               string
	ClickHouseAddr     string
	ClickHouseDatabase string
	ClickHouseUsername string
	ClickHousePassword string
	IngestKey          string
	BatchSize          int
	FlushInterval      time.Duration
	QueueSize          int

	// Forta OAuth2 authentication
	FortaAppDomain          string
	FortaAPIDomain          string
	FortaLoginDomain        string
	FortaClientID           string
	FortaClientSecret       string
	FortaCallbackURL        string
	FortaJWTSigningKey      string
	FortaPostLoginRedirect  string
	FortaPostLogoutRedirect string
	FortaCookieDomain       string
	FortaCookieInsecure     bool
	FortaFetchUserOnProtect bool
	FortaDisableAutoRefresh bool
	FortaEnforceGrants      bool
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
	BatchSize = getEnvInt("BATCH_SIZE", 1000)
	FlushInterval = getEnvDuration("FLUSH_INTERVAL", 5*time.Second)
	QueueSize = getEnvInt("QUEUE_SIZE", 100000)

	FortaAppDomain = getEnv("MON_FORTA_APP_DOMAIN", "")
	FortaAPIDomain = getEnv("FORTA_API_DOMAIN", "")
	FortaLoginDomain = getEnv("FORTA_LOGIN_DOMAIN", "")
	FortaClientID = getEnv("MON_FORTA_CLIENT_ID", "")
	FortaClientSecret = getEnv("MON_FORTA_CLIENT_SECRET", "")
	FortaCallbackURL = getEnv("FORTA_CALLBACK_URL", "")
	FortaJWTSigningKey = getEnv("FORTA_JWT_SIGNING_KEY", "")
	FortaPostLoginRedirect = getEnv("FORTA_POST_LOGIN_REDIRECT", "/")
	FortaPostLogoutRedirect = getEnv("FORTA_POST_LOGOUT_REDIRECT", "/")
	FortaCookieDomain = getEnv("FORTA_COOKIE_DOMAIN", "")
	FortaCookieInsecure = getEnv("FORTA_COOKIE_INSECURE", "false") == "true"
	FortaFetchUserOnProtect = getEnv("FORTA_FETCH_USER_ON_PROTECT", "true") == "true"
	FortaDisableAutoRefresh = getEnv("FORTA_DISABLE_AUTO_REFRESH", "false") == "true"
	FortaEnforceGrants = getEnv("FORTA_ENFORCE_GRANTS", "false") == "true"
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
