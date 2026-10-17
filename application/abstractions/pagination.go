package abstractions

import "strings"

type Pagination struct {
	Page      int
	PageSize  int
	SortBy    string
	SortOrder string
}

func (p Pagination) Normalize(sortByAllowList map[string]string, defaultSortBy string) Pagination {
	page := p.Page
	if page < 1 {
		page = 1
	}
	pageSize := p.PageSize
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}

	sortBy := strings.ToLower(strings.TrimSpace(p.SortBy))
	column, ok := sortByAllowList[sortBy]
	if !ok {
		column = sortByAllowList[defaultSortBy]
	}

	sortOrder := strings.ToUpper(strings.TrimSpace(p.SortOrder))
	if sortOrder != "ASC" && sortOrder != "DESC" {
		sortOrder = "DESC"
	}

	return Pagination{
		Page:      page,
		PageSize:  pageSize,
		SortBy:    column,
		SortOrder: sortOrder,
	}
}

func (p Pagination) Offset() int { return (p.Page - 1) * p.PageSize }

type PaginationResult[T any] struct {
	Items    []T
	Total    int64
	Page     int
	PageSize int
}

func (r PaginationResult[T]) TotalPages() int {
	if r.PageSize <= 0 {
		return 0
	}
	return int((r.Total + int64(r.PageSize) - 1) / int64(r.PageSize))
}
