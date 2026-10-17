package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func OpenPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	if cfg.Err != nil {
		return nil, cfg.Err
	}
	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}
