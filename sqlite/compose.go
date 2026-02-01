package sqlite

// I'm making this package the starting point of composing sqlite queries from banquet requests
// I'm making it a separate package so that I can add more sql dialects later
// without forcing import of sqlite specific dialect if other developers want to use with other sql dialects.
// package sqliter

import (
	"strings"

	"github.com/darianmavgo/banquet"
)

// Compose builds a SQL query string from a Banquet struct.
// This implementation uses double-quoting for identifiers to prevent basic SQL injection
// and handle reserved words/spaces in names.
func Compose(bq *banquet.Banquet) string {
	var parts []string

	// SELECT
	selectClause := "*"
	if len(bq.Select) > 0 && bq.Select[0] != "*" {
		quotedCols := make([]string, len(bq.Select))
		for i, col := range bq.Select {
			quotedCols[i] = QuoteIdentifier(col)
		}
		selectClause = strings.Join(quotedCols, ", ")
	}
	parts = append(parts, "SELECT "+selectClause)

	// FROM
	table := bq.Table
	if table == "" {
		table = InferTable(bq)
	}
	parts = append(parts, "FROM "+QuoteIdentifier(table))

	// WHERE
	if bq.Where != "" {
		parts = append(parts, "WHERE "+bq.Where)
	}

	// GROUP BY
	if bq.GroupBy != "" {
		parts = append(parts, "GROUP BY "+QuoteIdentifier(bq.GroupBy))
	}

	// HAVING
	if bq.Having != "" {
		parts = append(parts, "HAVING "+bq.Having)
	}

	// ORDER BY
	if bq.OrderBy != "" {
		orderBy := QuoteIdentifier(bq.OrderBy)
		if bq.SortDirection != "" {
			orderBy += " " + bq.SortDirection
		}
		parts = append(parts, "ORDER BY "+orderBy)
	}

	// LIMIT
	if bq.Limit != "" {
		parts = append(parts, "LIMIT "+bq.Limit)
	}

	// OFFSET
	if bq.Offset != "" {
		parts = append(parts, "OFFSET "+bq.Offset)
	}

	return strings.Join(parts, " ")
}

// QuoteIdentifier wraps a string in double quotes and escapes existing double quotes.
func QuoteIdentifier(s string) string {
	if s == "" || s == "*" {
		return s
	}
	return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\""
}

// InferTable attempts to deduce the table name when one is not explicitly provided.
// It checks the DataSetPath extension and whether columns were requested.
func InferTable(bq *banquet.Banquet) string {
	if bq.Table != "" {
		return bq.Table
	}

	lower := strings.ToLower(bq.DataSetPath)
	if strings.HasSuffix(lower, ".sqlite") || strings.HasSuffix(lower, ".db") {
		return "sqlite_master"
	}

	// Default fallback for flat files or if columns are specified but table is implicit
	return "tb0"
}
