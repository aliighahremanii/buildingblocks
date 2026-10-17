// Package reads provides a generic Postgres pagination scaffold for list read
// models. SearchPaged centralizes the LIMIT/OFFSET page query plus the separate
// COUNT(*) total query that every module's postgres list read used to hand-roll
// as boilerplate.
//
// The helper is generic over the read-model item T and takes a RowScanner
// callback that decodes one pgx.Rows row into T. It depends only on pgx and the
// building-blocks pagination abstractions, so it does not pull in any
// module-specific helpers.
package reads

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	bbabstractions "github.com/aliighahremanii/buildingblocks/application/abstractions"
)

// RowScanner decodes the current row of rows into a read-model item T. It is
// called once per page row after rows.Next reports true; it must call
// rows.Scan itself.
type RowScanner[T any] func(rows pgx.Rows) (T, error)

// SearchPaged runs a Postgres paginated search. The page query uses
// LIMIT $1 OFFSET $2; the total is read from a separate SELECT COUNT(*) FROM
// fromClause query. scan is called once per page row to produce the
// denormalized read model.
//
// pagination is normalized against sortAllowList, which maps the public sort
// key to the physical column. An unmatched or empty SortBy falls back to
// sortAllowList[defaultSortBy].
//
// selectColumns is the comma-separated column list; fromClause is the full FROM
// clause (e.g. "sample1_items" or a joined expression), and is reused verbatim
// for the COUNT(*) query.
func SearchPaged[T any](
	ctx context.Context,
	pool *pgxpool.Pool,
	pagination bbabstractions.Pagination,
	sortAllowList map[string]string,
	defaultSortBy string,
	selectColumns string,
	fromClause string,
	scan RowScanner[T],
) (bbabstractions.PaginationResult[T], error) {
	normalized := pagination.Normalize(sortAllowList, defaultSortBy)

	query := fmt.Sprintf(`
SELECT %s
FROM %s
ORDER BY %s %s
LIMIT $1 OFFSET $2;`, selectColumns, fromClause, normalized.SortBy, normalized.SortOrder)

	rows, err := pool.Query(ctx, query, normalized.PageSize, normalized.Offset())
	if err != nil {
		return bbabstractions.PaginationResult[T]{}, fmt.Errorf("search query: %w", err)
	}
	defer rows.Close()

	items := make([]T, 0, normalized.PageSize)
	for rows.Next() {
		item, err := scan(rows)
		if err != nil {
			return bbabstractions.PaginationResult[T]{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return bbabstractions.PaginationResult[T]{}, fmt.Errorf("search iterate: %w", err)
	}

	var total int64
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s;`, fromClause)
	if err := pool.QueryRow(ctx, countQuery).Scan(&total); err != nil {
		return bbabstractions.PaginationResult[T]{}, fmt.Errorf("search count: %w", err)
	}

	return bbabstractions.PaginationResult[T]{
		Items:    items,
		Total:    total,
		Page:     normalized.Page,
		PageSize: normalized.PageSize,
	}, nil
}
