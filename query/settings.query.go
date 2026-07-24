package query

import (
	"github.com/aidenappl/monitor-core/db"
)

// GetSetting reads a single KV setting. Used for oauth state (sso_state:*) and
// misc configuration. Returns sql.ErrNoRows when the key is absent.
func GetSetting(engine db.Queryable, key string) (string, error) {
	var value string
	err := engine.QueryRow("SELECT value FROM settings WHERE `key` = ?", key).Scan(&value)
	return value, err
}

// SetSetting upserts a KV setting.
func SetSetting(engine db.Queryable, key, value string) error {
	_, err := engine.Exec(
		"INSERT INTO settings (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)",
		key, value,
	)
	return err
}

// DeleteSetting removes a KV setting (e.g. consuming a single-use oauth state).
func DeleteSetting(engine db.Queryable, key string) error {
	_, err := engine.Exec("DELETE FROM settings WHERE `key` = ?", key)
	return err
}

// DeleteSettingExisted deletes a KV setting and reports whether a row was
// actually removed. Because DELETE takes a row lock, exactly one of N concurrent
// callers sees deleted=true — this makes single-use consumption (e.g. an OAuth
// state) atomic: only the caller that observes true may trust the value it read.
func DeleteSettingExisted(engine db.Queryable, key string) (bool, error) {
	res, err := engine.Exec("DELETE FROM settings WHERE `key` = ?", key)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

// GetSettingsByPrefix returns all settings whose key starts with prefix.
func GetSettingsByPrefix(engine db.Queryable, prefix string) (map[string]string, error) {
	rows, err := engine.Query("SELECT `key`, value FROM settings WHERE `key` LIKE ?", prefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		result[k] = v
	}
	return result, rows.Err()
}
