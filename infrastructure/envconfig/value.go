package envconfig

import (
	"fmt"
	"os"
	"strings"
)

// Value reads KEY_FILE when configured, otherwise KEY. Secret-file values are
// trimmed because Docker secrets conventionally end with one newline.
func Value(key string) (string, error) {
	file := strings.TrimSpace(os.Getenv(key + "_FILE"))
	if file == "" {
		return os.Getenv(key), nil
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("read %s_FILE %s: %w", key, file, err)
	}
	return strings.TrimSpace(string(data)), nil
}
