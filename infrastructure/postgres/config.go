package postgres

import "github.com/aliighahremanii/buildingblocks/infrastructure/envconfig"

const defaultDatabaseURL = "postgres://app:app@localhost:5432/app?sslmode=disable"

// Config holds the Postgres connection details used by hosts and migrators.
type Config struct {
	URL string
	Err error
}

func ConfigFromEnv() Config {
	url, err := envconfig.Value("POSTGRES_DSN")
	if err != nil {
		return Config{Err: err}
	}
	if url == "" {
		url = defaultDatabaseURL
	}

	return Config{URL: url}
}
