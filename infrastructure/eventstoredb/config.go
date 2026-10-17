package eventstoredb

import "github.com/aliighahremanii/buildingblocks/infrastructure/envconfig"

const (
	urlEnv     = "KURRENTDB_URL"
	defaultURL = "kurrentdb://localhost:2113?tls=false"
)

type Config struct {
	URL string
	Err error
}

func ConfigFromEnv() Config {
	url, err := envconfig.Value(urlEnv)
	if err != nil {
		return Config{Err: err}
	}
	if url == "" {
		url = defaultURL
	}
	return Config{URL: url}
}

func (c Config) Enabled() bool {
	return c.Err != nil || c.URL != ""
}
