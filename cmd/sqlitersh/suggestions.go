package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/darianmavgo/banquet"
	_ "github.com/mattn/go-sqlite3"
)

type SuggestionType int

const (
	SuggestTable SuggestionType = iota
	SuggestColumn
	SuggestHistory
	SuggestDatabase
	SuggestCommand
)

func (t SuggestionType) Icon() string {
	switch t {
	case SuggestTable:
		return "📊"
	case SuggestColumn:
		return "🔢"
	case SuggestHistory:
		return "🕒"
	case SuggestDatabase:
		return "📁"
	case SuggestCommand:
		return "⚡"
	default:
		return "💡"
	}
}

func (t SuggestionType) Category() string {
	switch t {
	case SuggestTable:
		return "Table"
	case SuggestColumn:
		return "Column"
	case SuggestHistory:
		return "History"
	case SuggestDatabase:
		return "Database"
	case SuggestCommand:
		return "Command"
	default:
		return "Item"
	}
}

type Suggestion struct {
	Type        SuggestionType
	Text        string // Display name / snippet
	Description string // Category / context description
	Replacement string // Full string to put in the Banquet Bar
}

// extractDBPath returns the absolute filesystem path to a .db or .sqlite file if present in the URL or context.
func extractDBPath(urlStr string) string {
	if urlStr == "" {
		return ""
	}
	bq, err := banquet.ParseBanquet(urlStr)
	if err == nil && !bq.IsCollection && bq.DataSetPath != "" {
		if strings.HasSuffix(bq.DataSetPath, ".db") || strings.HasSuffix(bq.DataSetPath, ".sqlite") || strings.HasSuffix(bq.DataSetPath, ".sqlite3") {
			return bq.DataSetPath
		}
	}

	clean := strings.TrimPrefix(urlStr, "file://")
	parts := strings.Split(clean, "/")
	var accum []string
	for _, p := range parts {
		accum = append(accum, p)
		if strings.HasSuffix(p, ".db") || strings.HasSuffix(p, ".sqlite") || strings.HasSuffix(p, ".sqlite3") {
			return "/" + filepath.Join(accum...)
		}
	}
	return ""
}

// fetchTables queries all non-internal user tables from the given SQLite database.
func fetchTables(dbPath string) ([]string, error) {
	if dbPath == "" {
		return nil, nil
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}
	return tables, nil
}

// fetchColumns queries column names for a specific table in an SQLite database.
func fetchColumns(dbPath, tableName string) ([]string, error) {
	if dbPath == "" || tableName == "" {
		return nil, nil
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT name FROM pragma_table_info(%q) ORDER BY cid;", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			cols = append(cols, name)
		}
	}
	return cols, nil
}

// findLocalDatabases returns all .db/.sqlite files in the given directory.
func findLocalDatabases(dir string) []string {
	if dir == "" {
		var err error
		dir, err = os.Getwd()
		if err != nil {
			return nil
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var dbs []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".db") || strings.HasSuffix(name, ".sqlite") || strings.HasSuffix(name, ".sqlite3") {
			dbs = append(dbs, name)
		}
	}
	return dbs
}

// generateSuggestions builds ranked suggestions based on input, context, history, and schema.
func generateSuggestions(
	rawInput string,
	currentCtx string,
	history []string,
	tables []string,
	cols []string,
	activeTable string,
	workingDir string,
) []Suggestion {
	input := strings.TrimSpace(rawInput)
	lowerInput := strings.ToLower(input)

	var suggestions []Suggestion
	seen := make(map[string]bool)

	addSuggestion := func(s Suggestion) {
		if seen[s.Replacement] {
			return
		}
		seen[s.Replacement] = true
		suggestions = append(suggestions, s)
	}

	dbPath := extractDBPath(currentCtx)
	if dbPath == "" {
		dbPath = extractDBPath(rawInput)
	}
	dbName := filepath.Base(dbPath)

	// Prefix in rawInput before the last segment/table being typed
	basePrefix := ""
	queryPart := lowerInput
	if strings.Contains(input, "/") {
		lastSlash := strings.LastIndex(input, "/")
		basePrefix = input[:lastSlash+1]
		queryPart = strings.ToLower(input[lastSlash+1:])
	}

	// 1. Matching Tables from Open Database
	if len(tables) > 0 {
		for _, t := range tables {
			lowerT := strings.ToLower(t)
			matches := false
			if queryPart == "" || strings.HasPrefix(lowerT, queryPart) || strings.Contains(lowerT, queryPart) {
				matches = true
			}

			if matches {
				replacement := basePrefix + t
				if basePrefix == "" && currentCtx != "" && !strings.Contains(input, "://") {
					replacement = strings.TrimRight(currentCtx, "/") + "/" + t
				} else if basePrefix == "" && currentCtx == "" {
					replacement = t
				}
				addSuggestion(Suggestion{
					Type:        SuggestTable,
					Text:        t,
					Description: fmt.Sprintf("table in %s", dbName),
					Replacement: replacement,
				})
			}
		}
	}

	// 2. Matching Columns from Active Table
	if activeTable != "" && len(cols) > 0 && (strings.HasSuffix(input, "/") || strings.Contains(input, activeTable)) {
		for _, col := range cols {
			lowerCol := strings.ToLower(col)
			if queryPart == "" || strings.HasPrefix(lowerCol, queryPart) || strings.Contains(lowerCol, queryPart) {
				replacement := basePrefix + col
				addSuggestion(Suggestion{
					Type:        SuggestColumn,
					Text:        col,
					Description: fmt.Sprintf("column in %s", activeTable),
					Replacement: replacement,
				})
			}
		}
	}

	// 3. Built-in Commands
	commands := []struct {
		cmd  string
		desc string
	}{
		{"open", "open database file picker"},
		{"help", "show help documentation"},
		{"clear", "clear viewport output"},
		{"exit", "exit shell"},
	}
	for _, c := range commands {
		if input != "" && strings.HasPrefix(c.cmd, lowerInput) && c.cmd != lowerInput {
			addSuggestion(Suggestion{
				Type:        SuggestCommand,
				Text:        c.cmd,
				Description: c.desc,
				Replacement: c.cmd,
			})
		}
	}

	// 4. Recent History (matching query, most recent first)
	for i := len(history) - 1; i >= 0; i-- {
		h := history[i]
		if h == input {
			continue
		}
		if input == "" || strings.Contains(strings.ToLower(h), lowerInput) {
			addSuggestion(Suggestion{
				Type:        SuggestHistory,
				Text:        h,
				Description: "recent history",
				Replacement: h,
			})
		}
	}

	// 5. Local Database Files
	localDBs := findLocalDatabases(workingDir)
	for _, ldb := range localDBs {
		lowerLDB := strings.ToLower(ldb)
		if input == "" || strings.HasPrefix(lowerLDB, lowerInput) || strings.Contains(lowerLDB, lowerInput) {
			dirPath := workingDir
			if dirPath == "" {
				dirPath, _ = os.Getwd()
			}
			replacement := "file://" + filepath.Join(dirPath, ldb)
			addSuggestion(Suggestion{
				Type:        SuggestDatabase,
				Text:        ldb,
				Description: "local database file",
				Replacement: replacement,
			})
		}
	}

	// Cap suggestions to top 8 items
	if len(suggestions) > 8 {
		suggestions = suggestions[:8]
	}

	return suggestions
}
