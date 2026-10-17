package reads

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	bbabstractions "github.com/aliighahremanii/buildingblocks/application/abstractions"
)

// SearchPagedWithFilters runs a Postgres paginated search with an optional WHERE
// clause. It is the filtered counterpart to [SearchPaged]: the page query uses
// `WHERE <whereClause> ORDER BY ... LIMIT $n OFFSET $n+1` and the total uses
// `SELECT COUNT(*) FROM <fromClause> WHERE <whereClause>` so filtered grids and
// dropdowns get correct totals.
//
// whereClause may be empty (in which case the function behaves like SearchPaged).
// whereArgs are the bind parameters for the WHERE placeholders ($1, $2, ...);
// the pagination LIMIT/OFFSET parameters are appended after them automatically.
//
// selectColumns, fromClause, sortAllowList, defaultSortBy, and scan have the
// same meaning as in SearchPaged.
func SearchPagedWithFilters[T any](
	ctx context.Context,
	pool *pgxpool.Pool,
	pagination bbabstractions.Pagination,
	sortAllowList map[string]string,
	defaultSortBy string,
	selectColumns string,
	fromClause string,
	whereClause string,
	whereArgs []any,
	scan RowScanner[T],
) (bbabstractions.PaginationResult[T], error) {
	normalized := pagination.Normalize(sortAllowList, defaultSortBy)

	whereFragment := ""
	args := whereArgs
	if whereClause != "" {
		whereFragment = "WHERE " + whereClause
	}

	// Build the page query. WHERE placeholders are $1..$k; LIMIT and OFFSET are
	// $k+1 and $k+2.
	limitIdx := len(args) + 1
	offsetIdx := len(args) + 2
	query := fmt.Sprintf(`
SELECT %s
FROM %s
%s
ORDER BY %s %s
LIMIT $%d OFFSET $%d;`, selectColumns, fromClause, whereFragment, normalized.SortBy, normalized.SortOrder, limitIdx, offsetIdx)

	pageArgs := append(append([]any{}, args...), normalized.PageSize, normalized.Offset())
	rows, err := pool.Query(ctx, query, pageArgs...)
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

	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s %s;`, fromClause, whereFragment)
	var total int64
	if err := pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return bbabstractions.PaginationResult[T]{}, fmt.Errorf("search count: %w", err)
	}

	return bbabstractions.PaginationResult[T]{
		Items:    items,
		Total:    total,
		Page:     normalized.Page,
		PageSize: normalized.PageSize,
	}, nil
}

// Compile-time guard that SearchPagedWithFilters shares the RowScanner shape.
var _ RowScanner[any] = func(rows pgx.Rows) (any, error) { return nil, nil }
