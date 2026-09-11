package main

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	_ "github.com/mattn/go-sqlite3"
)

func TestStandards(t *testing.T) {
	dbDir, _ := filepath.Abs("../../tests")
	dbFile := filepath.Join(dbDir, "tests.db")

	t.Run("CollectionStyle", func(t *testing.T) {
		output, err := executeCommand("file://" + dbDir)
		if err != nil {
			t.Fatalf("failed to run collection mode: %v", err)
		}
		
		expectedCols := []string{"name", "location", "key", "table count", "size", "size bytes", "modified", "status", "open"}
		for _, col := range expectedCols {
			if !strings.Contains(strings.ToLower(output), col) {
				t.Errorf("expected column %s in collection output", col)
			}
		}
		if !strings.Contains(output, "tests.db") {
			t.Errorf("expected to find tests.db in collection list")
		}
	})

	t.Run("TableListStyle", func(t *testing.T) {
		output, err := executeCommand("file://" + dbFile)
		if err != nil {
			t.Fatalf("failed to run table list mode: %v", err)
		}

		if strings.Contains(output, "PREVIEW") {
			t.Errorf("expected independent grids, but found legacy PREVIEW column")
		}
		
		idxTiny := strings.Index(output, "=== Table: trajectory_meta")
		idxLarge := strings.Index(output, "=== Table: steps")
		idxEmpty := strings.Index(output, "=== Table: battle_mode_infos")
		
		if idxTiny == -1 || idxLarge == -1 || idxEmpty == -1 {
			t.Fatalf("expected all tables to be printed with distinct headers")
		}
		
		if !(idxTiny < idxLarge && idxLarge < idxEmpty) {
			t.Errorf("expected order: Tiny -> Large -> Empty, but got incorrect order. Indices: Tiny=%d, Large=%d, Empty=%d", idxTiny, idxLarge, idxEmpty)
		}
		
		if !strings.Contains(output[idxEmpty:], "(Empty)") {
			t.Errorf("expected (Empty) placeholder after battle_mode_infos")
		}
	})

	t.Run("QueryStyle", func(t *testing.T) {
		output, err := executeCommand("file://" + dbFile + "/steps")
		if err != nil {
			t.Fatalf("failed to run query mode: %v", err)
		}
		
		if !strings.Contains(output, "IDX") {
			t.Errorf("expected primary key column 'IDX'")
		}
	})

	t.Run("SortExpressionStyle", func(t *testing.T) {
		output, err := executeCommand("file://" + dbFile + "/steps/-idx[0:3]")
		if err != nil {
			t.Fatalf("failed to run query mode: %v", err)
		}

		// Since highest idx is 3151, and we format numbers with commas:
		if !strings.Contains(output, "3,151") {
			t.Errorf("expected highest idx 3,151, got:\n%s", output)
		}
	})

	t.Run("SliceNotationStyle", func(t *testing.T) {
		output, err := executeCommand("file://" + dbFile + "/steps/idx[0:3]")
		if err != nil {
			t.Fatalf("failed to run query mode: %v", err)
		}

		lines := strings.Split(strings.TrimSpace(output), "\n")
		// Header + Separator + 3 data rows = 5 lines
		if len(lines) != 5 {
			t.Errorf("expected 5 lines for a limit 3 query, got %d:\n%s", len(lines), output)
		}
	})

	t.Run("GridFormattingStyle", func(t *testing.T) {
		output, err := executeCommand("file://" + dbFile + "/executor_metadata[0:3]")
		if err != nil {
			t.Fatalf("failed to run query mode: %v", err)
		}
		
		// 1. Check BLOB placeholder
		if !strings.Contains(output, "<BLOB:") {
			t.Errorf("expected BLOB placeholder, got:\n%s", output)
		}
		
		// 2. Check strict single-line enforcement (line count)
		lines := strings.Split(strings.TrimSpace(output), "\n")
		// Header (1) + Separator (1) + Data Rows (3) = 5 lines total
		if len(lines) != 5 {
			t.Errorf("expected strict single-line rows resulting in 5 lines, got %d lines:\n%s", len(lines), output)
		}
	})

	t.Run("GridBoundaryRules", func(t *testing.T) {
		// 1. Zero-row query (asserting "No results found")
		zeroOutput, err := executeCommand("file://" + dbFile + "/steps?where=1=0")
		if err != nil {
			t.Fatalf("failed zero-row query: %v", err)
		}
		if !strings.Contains(zeroOutput, "No results found") {
			t.Errorf("expected 'No results found' for 0-row query, got:\n%s", zeroOutput)
		}

		// 2. Long string cell truncation (> 120 chars with ...)
		tmpDir := t.TempDir()
		tmpDbFile := filepath.Join(tmpDir, "trunc_test.db")
		db, err := sql.Open("sqlite3", tmpDbFile)
		if err != nil {
			t.Fatalf("failed to open temp db: %v", err)
		}
		defer db.Close()

		_, err = db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, note TEXT);`)
		if err != nil {
			t.Fatalf("failed to create items table: %v", err)
		}

		longStr := strings.Repeat("X", 150)
		_, err = db.Exec(`INSERT INTO items (id, note) VALUES (1, ?);`, longStr)
		if err != nil {
			t.Fatalf("failed to insert long string: %v", err)
		}

		truncOutput, err := executeCommand("file://" + tmpDbFile + "/items")
		if err != nil {
			t.Fatalf("failed to query table with long string: %v", err)
		}

		if !strings.Contains(truncOutput, "...") {
			t.Errorf("expected ellipsis '...' for >120 char string, got:\n%s", truncOutput)
		}
		if strings.Contains(truncOutput, longStr) {
			t.Errorf("expected long string to be truncated, but found full 150 chars in output")
		}
	})

	t.Run("ContextNavigation", func(t *testing.T) {
		// 1. Traversing directories with ..
		if got := resolveContext("file:///dir/db.sqlite/steps", ".."); got != "file:///dir/db.sqlite" {
			t.Errorf("resolveContext .. from table failed: expected 'file:///dir/db.sqlite', got %q", got)
		}
		if got := resolveContext("file:///dir/subdir/db.sqlite", ".."); got != "file:///dir/subdir" {
			t.Errorf("resolveContext .. from db failed: expected 'file:///dir/subdir', got %q", got)
		}
		if got := resolveContext("file:///dir/subdir", ".."); got != "file:///dir" {
			t.Errorf("resolveContext .. from dir failed: expected 'file:///dir', got %q", got)
		}

		// 2. Appending table names and relative paths
		if got := resolveContext("file:///dir/db.sqlite", "steps"); got != "file:///dir/db.sqlite/steps" {
			t.Errorf("resolveContext relative table failed: expected 'file:///dir/db.sqlite/steps', got %q", got)
		}
		if got := resolveContext("file:///dir/db.sqlite/", "steps"); got != "file:///dir/db.sqlite/steps" {
			t.Errorf("resolveContext relative table with slash failed: expected 'file:///dir/db.sqlite/steps', got %q", got)
		}
		if got := resolveContext("file:///dir/db.sqlite", "."); got != "file:///dir/db.sqlite" {
			t.Errorf("resolveContext . failed: expected 'file:///dir/db.sqlite', got %q", got)
		}
		if got := resolveContext("file:///dir/db.sqlite/steps", "[-idx]"); got != "file:///dir/db.sqlite/steps[-idx]" {
			t.Errorf("resolveContext clause append failed: expected 'file:///dir/db.sqlite/steps[-idx]', got %q", got)
		}

		// 3. Switching database contexts
		if got := resolveContext("file:///dir/db1.sqlite", "file:///other/db2.sqlite"); got != "file:///other/db2.sqlite" {
			t.Errorf("resolveContext full URL switch failed: expected 'file:///other/db2.sqlite', got %q", got)
		}
		if got := resolveContext("file:///dir/db1.sqlite", "/other/db2.sqlite"); got != "file:///other/db2.sqlite" {
			t.Errorf("resolveContext root path switch failed: expected 'file:///other/db2.sqlite', got %q", got)
		}
	})

	t.Run("BubbleTeaModel", func(t *testing.T) {
		startURL := "file://" + dbFile
		m := initialModel(startURL)

		// 1. Initial State: textinput focused by default, value matches current context URL
		if !m.textInput.Focused() {
			t.Errorf("expected Banquet Bar (textinput) to be focused by default")
		}
		if m.textInput.Value() != startURL {
			t.Errorf("expected Banquet Bar value to be %q, got %q", startURL, m.textInput.Value())
		}

		// 2. Send tea.KeyEnter on the initial URL
		updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)

		// Verify Banquet Bar updates/maintains its value and retains focus
		if !m.textInput.Focused() {
			t.Errorf("expected Banquet Bar to retain focus after Enter")
		}
		if m.textInput.Value() != startURL {
			t.Errorf("expected Banquet Bar value %q after Enter, got %q", startURL, m.textInput.Value())
		}
		// Verify viewport updated with table list
		if !strings.Contains(m.viewport.View(), "=== Table: steps") {
			t.Errorf("expected viewport to contain table list, got:\n%s", m.viewport.View())
		}

		// 3. Send tea.KeyRunes to append relative path "/steps"
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/steps")})
		m = updatedModel.(model)

		expectedTypedURL := startURL + "/steps"
		if m.textInput.Value() != expectedTypedURL {
			t.Errorf("expected Banquet Bar value %q after typing runes, got %q", expectedTypedURL, m.textInput.Value())
		}

		// 4. Send tea.KeyEnter on the updated query
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)

		if !m.textInput.Focused() {
			t.Errorf("expected Banquet Bar to retain focus after query evaluation")
		}
		if m.textInput.Value() != expectedTypedURL {
			t.Errorf("expected Banquet Bar value %q, got %q", expectedTypedURL, m.textInput.Value())
		}
		if !strings.Contains(m.viewport.View(), "IDX") {
			t.Errorf("expected viewport to contain query results with 'IDX', got:\n%s", m.viewport.View())
		}

		// 5. Verify View() layout includes Banquet Bar
		renderedView := m.View()
		if !strings.Contains(renderedView, "Banquet") {
			t.Errorf("expected rendered View to include Banquet Bar prompt")
		}
	})
}
