package eventstoredb

import (
	"context"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// OpenClient creates a new EventStoreDB (KurrentDB) client from the given config.
// The client is safe for concurrent use across the API and worker processes.
func OpenClient(_ context.Context, cfg Config) (*kurrentdb.Client, error) {
	if cfg.Err != nil {
		return nil, cfg.Err
	}
	conf, err := kurrentdb.ParseConnectionString(cfg.URL)
	if err != nil {
		return nil, err
	}
	return kurrentdb.NewClient(conf)
}
