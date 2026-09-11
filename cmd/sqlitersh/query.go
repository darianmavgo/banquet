package main

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/darianmavgo/banquet"
	"github.com/darianmavgo/banquet/sqlite"
	_ "github.com/mattn/go-sqlite3"
	"github.com/olekukonko/tablewriter"
)

func formatNumber(n interface{}) string {
	str := fmt.Sprintf("%v", n)
	if !strings.ContainsAny(str, "0123456789") {
		return str
	}
	parts := strings.Split(str, ".")
	intPart := parts[0]
	isNeg := false
	if strings.HasPrefix(intPart, "-") {
		isNeg = true
		intPart = intPart[1:]
	}
	var res []byte
	for i := 0; i < len(intPart); i++ {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			res = append(res, ',')
		}
		res = append(res, intPart[i])
	}
	finalInt := string(res)
	if isNeg {
		finalInt = "-" + finalInt
	}
	if len(parts) > 1 {
		return finalInt + "." + parts[1]
	}
	return finalInt
}

func truncate(s string, max int) string {
	if len(s) > max {
		return s[:max-3] + "..."
	}
	return s
}

func handleQuery(bq *banquet.Banquet) (string, error) {
	db, err := sql.Open("sqlite3", bq.DataSetPath)
	if err != nil {
		return "", err
	}
	defer db.Close()

	query := sqlite.Compose(bq)
	return printQueryGrid(db, bq.Table, query)
}

func printQueryGrid(db *sql.DB, tableName string, query string) (string, error) {
	rows, err := db.Query(query)
	if err != nil {
		return "", fmt.Errorf("query error: %w\nQuery: %s", err, query)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return "", err
	}

	pkCol := ""
	if tableName != "" {
		infoRows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%q)", tableName))
		if err == nil {
			defer infoRows.Close()
			for infoRows.Next() {
				var cid, notnull, pk int
				var name, ctype string
				var dfltValue interface{}
				if err := infoRows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err == nil {
					if pk > 0 {
						pkCol = name
						break
					}
				}
			}
		}
	}

	var allRows [][]interface{}
	colHasData := make(map[string]bool)

	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range cols {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return "", err
		}

		row := make([]interface{}, len(cols))
		for i, val := range values {
			if val != nil {
				if b, ok := val.([]byte); ok && len(b) > 0 {
					colHasData[cols[i]] = true
				} else if s, ok := val.(string); ok && s != "" {
					colHasData[cols[i]] = true
				} else if !ok {
					colHasData[cols[i]] = true
				}

				switch v := val.(type) {
				default:
					row[i] = v
				}
			} else {
				row[i] = nil
			}
		}
		allRows = append(allRows, row)
	}

	if len(allRows) == 0 {
		return "No results found\n", nil
	}

	sysCols := map[string]bool{"created_at": true, "updated_at": true, "deleted_at": true, "version": true, "tenant_id": true, "uuid": true}
	var visibleCols []string

	if pkCol != "" {
		for _, c := range cols {
			if strings.EqualFold(c, pkCol) {
				visibleCols = append(visibleCols, c)
				break
			}
		}
	}

	for _, c := range cols {
		if strings.EqualFold(c, pkCol) {
			continue
		}
		lower := strings.ToLower(c)
		if strings.Contains(lower, "name") || strings.Contains(lower, "label") || strings.Contains(lower, "description") {
			if colHasData[c] && !sysCols[lower] {
				visibleCols = append(visibleCols, c)
			}
		}
	}

	for _, c := range cols {
		if strings.EqualFold(c, pkCol) {
			continue
		}
		lower := strings.ToLower(c)
		if strings.Contains(lower, "name") || strings.Contains(lower, "label") || strings.Contains(lower, "description") {
			continue
		}
		if colHasData[c] && !sysCols[lower] {
			visibleCols = append(visibleCols, c)
		}
	}

	var b strings.Builder
	table := tablewriter.NewWriter(&b)
	table.SetHeader(visibleCols)
	table.SetBorder(false)
	table.SetAutoWrapText(false)

	colIdx := make(map[string]int)
	for i, c := range cols {
		colIdx[c] = i
	}

	for _, r := range allRows {
		var rowStr []string
		for _, colName := range visibleCols {
			idx := colIdx[colName]
			val := r[idx]
			ctype := colTypes[idx].DatabaseTypeName()

			if val == nil {
				rowStr = append(rowStr, "NULL")
			} else {
				if strings.Contains(strings.ToUpper(ctype), "BOOL") {
					if fmt.Sprintf("%v", val) == "1" || fmt.Sprintf("%v", val) == "true" {
						rowStr = append(rowStr, "Yes")
					} else {
						rowStr = append(rowStr, "No")
					}
				} else {
					switch v := val.(type) {
					case []byte:
						rowStr = append(rowStr, fmt.Sprintf("<BLOB: %d bytes>", len(v)))
					case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
						rowStr = append(rowStr, formatNumber(v))
					case string:
						clean := strings.ReplaceAll(v, "\n", " ")
						clean = strings.ReplaceAll(clean, "\r", "")
						rowStr = append(rowStr, truncate(clean, 120))
					default:
						strVal := fmt.Sprintf("%v", v)
						clean := strings.ReplaceAll(strVal, "\n", " ")
						clean = strings.ReplaceAll(clean, "\r", "")
						rowStr = append(rowStr, truncate(clean, 120))
					}
				}
			}
		}
		table.Append(rowStr)
	}

	table.Render()
	return b.String(), nil
}
