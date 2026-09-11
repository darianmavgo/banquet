package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/darianmavgo/banquet"
	_ "github.com/mattn/go-sqlite3"
	"github.com/olekukonko/tablewriter"
)

func handleCollection(bq *banquet.Banquet) (string, error) {
	basePath := bq.DataSetPath
	if basePath == "" {
		basePath = "."
	}

	type DbEntry struct {
		Name       string
		Location   string
		Key        string
		TableCount int
		Size       string
		SizeBytes  int64
		Modified   string
		Status     string
	}

	var entries []DbEntry

	err := filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			rel, err := filepath.Rel(basePath, path)
			if err != nil {
				return nil
			}
			depth := strings.Count(rel, string(os.PathSeparator))
			if depth >= 5 {
				return filepath.SkipDir
			}
			return nil
		}

		lower := strings.ToLower(info.Name())
		if strings.HasSuffix(lower, ".db") || strings.HasSuffix(lower, ".sqlite") {
			rel, _ := filepath.Rel(basePath, path)
			
			status := "ok"
			tableCount := 0
			db, openErr := sql.Open("sqlite3", path)
			if openErr != nil {
				status = "error"
			} else {
				err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table';").Scan(&tableCount)
				if err != nil {
					status = "error"
				}
				db.Close()
			}
			
			sizeStr := fmt.Sprintf("%d B", info.Size())
			if info.Size() > 1024*1024 {
				sizeStr = fmt.Sprintf("%.1f MB", float64(info.Size())/(1024*1024))
			} else if info.Size() > 1024 {
				sizeStr = fmt.Sprintf("%.1f KB", float64(info.Size())/1024)
			}

			entries = append(entries, DbEntry{
				Name:       info.Name(),
				Location:   rel,
				Key:        rel,
				TableCount: tableCount,
				Size:       sizeStr,
				SizeBytes:  info.Size(),
				Modified:   info.ModTime().Format("2006-01-02 15:04:05"),
				Status:     status,
			})
		}
		return nil
	})

	if err != nil {
		return "", fmt.Errorf("failed to scan collection: %w", err)
	}

	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if bq.OrderBy != "" {
			col := strings.ToLower(bq.OrderBy)
			desc := bq.SortDirection == "DESC"
			switch col {
			case "size_bytes", "size":
				if desc {
					return a.SizeBytes > b.SizeBytes
				}
				return a.SizeBytes < b.SizeBytes
			case "modified":
				if desc {
					return a.Modified > b.Modified
				}
				return a.Modified < b.Modified
			}
		}
		return a.Name < b.Name
	})

	var b strings.Builder
	table := tablewriter.NewWriter(&b)
	table.SetHeader([]string{"name", "location", "key", "table_count", "size", "size_bytes", "modified", "status", "open"})
	table.SetBorder(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)

	for _, e := range entries {
		openPath := filepath.Join(basePath, e.Location)
		table.Append([]string{
			e.Name,
			e.Location,
			e.Key,
			fmt.Sprintf("%d", e.TableCount),
			e.Size,
			fmt.Sprintf("%d", e.SizeBytes),
			e.Modified,
			e.Status,
			openPath,
		})
	}
	
	table.Render()
	return b.String(), nil
}
