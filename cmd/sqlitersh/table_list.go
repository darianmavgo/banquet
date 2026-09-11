package main

import (
	"database/sql"
	"fmt"
	"strings"
	"sort"

	"github.com/darianmavgo/banquet"
	_ "github.com/mattn/go-sqlite3"
)

func handleTableList(bq *banquet.Banquet) (string, error) {
	db, err := sql.Open("sqlite3", bq.DataSetPath)
	if err != nil {
		return "", err
	}
	defer db.Close()

	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}

	type TableInfo struct {
		Name     string
		RowCount int
		ColCount int
		Category int // 0 = Tiny, 1 = Large, 2 = Empty
	}
	var b strings.Builder
	var tableInfos []TableInfo

	for _, tName := range tables {
		var count int
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %q", tName)).Scan(&count)
		if err != nil {
			continue
		}
		var colCount int
		err = db.QueryRow(fmt.Sprintf("SELECT count(*) FROM pragma_table_info(%q)", tName)).Scan(&colCount)
		if err != nil {
			colCount = 0
		}

		cat := 1
		if count == 0 {
			cat = 2
		} else if count < 20 {
			cat = 0
		}
		
		tableInfos = append(tableInfos, TableInfo{
			Name:     tName,
			RowCount: count,
			ColCount: colCount,
			Category: cat,
		})
	}

	sort.Slice(tableInfos, func(i, j int) bool {
		if tableInfos[i].Category != tableInfos[j].Category {
			return tableInfos[i].Category < tableInfos[j].Category
		}
		return tableInfos[i].RowCount > tableInfos[j].RowCount
	})

	for i, info := range tableInfos {
		if i > 0 {
			fmt.Fprintln(&b)
		}
		fmt.Fprintf(&b, "=== Table: %s (%d rows, %d columns) ===\n", info.Name, info.RowCount, info.ColCount)
		
		if info.RowCount == 0 {
			fmt.Fprintln(&b, "(Empty)")
			continue
		}

		query := fmt.Sprintf("SELECT * FROM %q LIMIT 20", info.Name)
		out, err := printQueryGrid(db, info.Name, query)
		if err != nil {
			fmt.Fprintf(&b, "Error rendering table: %v\n", err)
		} else {
			fmt.Fprint(&b, out)
		}
	}

	return b.String(), nil
}
