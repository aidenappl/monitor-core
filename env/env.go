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
	APIKey             string
	BatchSize          int
	FlushInterval      time.Duration
	QueueSize          int
)

// Load reads all configuration from environment variables.
// Call this after injecting secrets (e.g. via go-keyring) so that
// any Keyring-provided values are present in os.Getenv before this runs.
func Load() {
	Port               = getEnv("HTTP_PORT", "8080")
	ClickHouseAddr     = getEnv("CLICKHOUSE_ADDR", "localhost:9000")
	ClickHouseDatabase = getEnv("CLICKHOUSE_DATABASE", "monitor")
	ClickHouseUsername = getEnv("CLICKHOUSE_USERNAME", "default")
	ClickHousePassword = getEnv("CLICKHOUSE_PASSWORD", "")
	APIKey             = getEnv("API_KEY", "")
	BatchSize          = getEnvInt("BATCH_SIZE", 1000)
	FlushInterval      = getEnvDuration("FLUSH_INTERVAL", 5*time.Second)
	QueueSize          = getEnvInt("QUEUE_SIZE", 100000)
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
