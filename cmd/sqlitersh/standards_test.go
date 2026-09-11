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

	t.Run("HistoryNavigation", func(t *testing.T) {
		startURL := "file://" + dbFile
		m := initialModel(startURL)

		// Submit command 1: /steps
		m.textInput.SetValue(startURL + "/steps")
		updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)

		// Submit command 2: /trajectory_meta
		m.textInput.SetValue(startURL + "/trajectory_meta")
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)

		// Set draft input
		m.textInput.SetValue("my draft query")

		// Press Up arrow: recalls last command (/trajectory_meta)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyUp})
		m = updatedModel.(model)
		if m.textInput.Value() != startURL+"/trajectory_meta" {
			t.Errorf("expected Up arrow to recall %q, got %q", startURL+"/trajectory_meta", m.textInput.Value())
		}

		// Press Up arrow again: recalls previous command (/steps)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyUp})
		m = updatedModel.(model)
		if m.textInput.Value() != startURL+"/steps" {
			t.Errorf("expected Up arrow to recall %q, got %q", startURL+"/steps", m.textInput.Value())
		}

		// Press Down arrow: returns to (/trajectory_meta)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
		m = updatedModel.(model)
		if m.textInput.Value() != startURL+"/trajectory_meta" {
			t.Errorf("expected Down arrow to recall %q, got %q", startURL+"/trajectory_meta", m.textInput.Value())
		}

		// Press Down arrow again: returns to draft input
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
		m = updatedModel.(model)
		if m.textInput.Value() != "my draft query" {
			t.Errorf("expected Down arrow to restore draft %q, got %q", "my draft query", m.textInput.Value())
		}
	})

	t.Run("AutocompleteAndSuggestions", func(t *testing.T) {
		startURL := "file://" + dbFile
		m := initialModel(startURL)

		// Type "ste" into the text input
		m.textInput.SetValue(startURL + "/ste")
		m.updateSuggestions()

		foundSteps := false
		for _, s := range m.suggestions {
			if s.Text == "steps" && s.Type == SuggestTable {
				foundSteps = true
				break
			}
		}
		if !foundSteps {
			t.Errorf("expected 'steps' table in suggestions for prefix 'ste', got: %+v", m.suggestions)
		}

		// Press Tab to autocomplete
		m.showDropdown = true
		m.suggestionIdx = 0
		updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyTab})
		m = updatedModel.(model)

		expectedCompleted := startURL + "/steps"
		if m.textInput.Value() != expectedCompleted {
			t.Errorf("expected Tab autocomplete to set %q, got %q", expectedCompleted, m.textInput.Value())
		}
	})

	t.Run("DropdownPanel", func(t *testing.T) {
		startURL := "file://" + dbFile
		m := initialModel(startURL)

		// Type partial "tra"
		m.textInput.SetValue(startURL + "/tra")
		m.updateSuggestions()
		m.showDropdown = true

		if len(m.suggestions) == 0 {
			t.Fatalf("expected suggestions for 'tra', got none")
		}

		// Verify View() renders dropdown overlay
		renderedView := m.View()
		if !strings.Contains(renderedView, "Suggestions") {
			t.Errorf("expected View() to contain 'Suggestions' header, got:\n%s", renderedView)
		}
		if !strings.Contains(renderedView, "trajectory_meta") {
			t.Errorf("expected View() to contain suggestion 'trajectory_meta', got:\n%s", renderedView)
		}

		// Test Down arrow navigates dropdown
		updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyDown})
		m = updatedModel.(model)
		if m.suggestionIdx != 0 {
			t.Errorf("expected suggestionIdx 0 after Down arrow, got %d", m.suggestionIdx)
		}

		// Test Esc dismisses dropdown
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
		m = updatedModel.(model)
		if m.showDropdown {
			t.Errorf("expected Esc to close dropdown")
		}

		// Test Enter on highlighted suggestion evaluates it
		m.textInput.SetValue(startURL + "/tra")
		m.updateSuggestions()
		m.showDropdown = true
		m.suggestionIdx = 0 // points to trajectory_meta

		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)

		if m.showDropdown {
			t.Errorf("expected dropdown to close after Enter")
		}
		if m.textInput.Value() != startURL+"/trajectory_meta" {
			t.Errorf("expected textInput to be evaluated to trajectory_meta, got %q", m.textInput.Value())
		}
		if !strings.Contains(m.viewport.View(), "trajectory") && !strings.Contains(m.viewport.View(), "TRAJECTORY") {
			t.Errorf("expected viewport to contain results from trajectory_meta, got:\n%s", m.viewport.View())
		}
	})

	t.Run("BackForwardNavigation", func(t *testing.T) {
		startURL := "file://" + dbFile
		m := initialModel(startURL)

		// 1. Initial URL: tests.db (table list)
		updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)

		// 2. Query steps
		urlSteps := startURL + "/steps"
		m.textInput.SetValue(urlSteps)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)
		if !strings.Contains(m.viewport.View(), "IDX") {
			t.Fatalf("expected viewport for steps to contain 'IDX'")
		}

		// 3. Query trajectory_meta
		urlTraj := startURL + "/trajectory_meta"
		m.textInput.SetValue(urlTraj)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)

		// 4. Back (Alt+Left) -> should return to urlSteps
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyLeft, Alt: true})
		m = updatedModel.(model)
		if m.textInput.Value() != urlSteps {
			t.Errorf("expected Back (Alt+Left) to set bar to %q, got %q", urlSteps, m.textInput.Value())
		}
		if !strings.Contains(m.viewport.View(), "IDX") {
			t.Errorf("expected viewport after Back to display steps results with 'IDX', got:\n%s", m.viewport.View())
		}

		// 5. Back again (Alt+Left) -> should return to startURL (table list)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyLeft, Alt: true})
		m = updatedModel.(model)
		if m.textInput.Value() != startURL {
			t.Errorf("expected Back (Alt+Left) to set bar to %q, got %q", startURL, m.textInput.Value())
		}
		if !strings.Contains(m.viewport.View(), "=== Table: steps") {
			t.Errorf("expected viewport after Back to display table list, got:\n%s", m.viewport.View())
		}

		// 6. Forward (Alt+Right) -> should advance back to urlSteps
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyRight, Alt: true})
		m = updatedModel.(model)
		if m.textInput.Value() != urlSteps {
			t.Errorf("expected Forward (Alt+Right) to set bar to %q, got %q", urlSteps, m.textInput.Value())
		}
		if !strings.Contains(m.viewport.View(), "IDX") {
			t.Errorf("expected viewport after Forward to display steps results with 'IDX', got:\n%s", m.viewport.View())
		}

		// 7. Test Ctrl+[ (Back) and Ctrl+] (Forward)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyCtrlOpenBracket})
		m = updatedModel.(model)
		if m.textInput.Value() != startURL {
			t.Errorf("expected Ctrl+[ to navigate Back to %q, got %q", startURL, m.textInput.Value())
		}

		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyCtrlCloseBracket})
		m = updatedModel.(model)
		if m.textInput.Value() != urlSteps {
			t.Errorf("expected Ctrl+] to navigate Forward to %q, got %q", urlSteps, m.textInput.Value())
		}

		// 8. Test boundary conditions
		// Forward at end of history
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyRight, Alt: true}) // advance to urlTraj
		m = updatedModel.(model)
		if m.textInput.Value() != urlTraj {
			t.Errorf("expected Forward to advance to %q, got %q", urlTraj, m.textInput.Value())
		}
		// Another Forward should be a no-op (no panic or change)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyRight, Alt: true})
		m = updatedModel.(model)
		if m.textInput.Value() != urlTraj {
			t.Errorf("expected boundary Forward to stay at %q, got %q", urlTraj, m.textInput.Value())
		}

		// 9. History truncation on branch
		// Go back to urlSteps
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyLeft, Alt: true})
		m = updatedModel.(model)
		if m.textInput.Value() != urlSteps {
			t.Errorf("expected Back to set %q, got %q", urlSteps, m.textInput.Value())
		}

		// Enter a new query from the past: /battle_mode_infos
		urlBattle := startURL + "/battle_mode_infos"
		m.textInput.SetValue(urlBattle)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)

		// Forward should now be inactive because we branched at urlBattle
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyRight, Alt: true})
		m = updatedModel.(model)
		if m.textInput.Value() != urlBattle {
			t.Errorf("expected Forward to do nothing after branch, stayed at %q, got %q", urlBattle, m.textInput.Value())
		}
	})

	t.Run("InteractiveTypingAndDropdownWrap", func(t *testing.T) {
		startURL := "file://" + dbFile
		m := initialModel(startURL)

		// Type "/st" via KeyRunes
		updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/st")})
		m = updatedModel.(model)

		if !m.showDropdown {
			t.Errorf("expected showDropdown to be true after typing '/st'")
		}
		if len(m.suggestions) == 0 {
			t.Fatalf("expected suggestions after typing '/st', got none")
		}

		// Test Up arrow wrap-around in dropdown (from -1 to last item)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyUp})
		m = updatedModel.(model)
		if m.suggestionIdx != len(m.suggestions)-1 {
			t.Errorf("expected suggestionIdx to wrap to %d, got %d", len(m.suggestions)-1, m.suggestionIdx)
		}

		// Test Down arrow wrap-around in dropdown (from last item to 0)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
		m = updatedModel.(model)
		if m.suggestionIdx != 0 {
			t.Errorf("expected suggestionIdx to wrap to 0, got %d", m.suggestionIdx)
		}
	})

	t.Run("OpenDBAndTableKeystrokes", func(t *testing.T) {
		// Start in collection directory
		m := initialModel("file://" + dbDir)

		// 1. Initial view must show the collection table
		if !strings.Contains(m.viewport.View(), "tests.db") {
			t.Fatalf("expected collection view to contain tests.db, got:\n%s", m.viewport.View())
		}

		// 2. Open DB by typing "file://tests.db" or relative "tests.db"
		// The exact bug from screenshot: typing file://<dbname> was failing into collection mode
		m.textInput.SetValue("file://" + dbFile)
		updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)

		// MUST open the DB table list, NOT remain in collection mode
		if strings.Contains(m.viewport.View(), "TABLE COUNT") {
			t.Errorf("failed to open database: viewport still showing collection list with 'TABLE COUNT'")
		}
		if !strings.Contains(m.viewport.View(), "=== Table: steps") {
			t.Errorf("expected table list with '=== Table: steps', got:\n%s", m.viewport.View())
		}

		// 3. Open table by typing table name "steps"
		m.textInput.SetValue("steps")
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)

		// MUST display query grid for steps
		if !strings.Contains(m.viewport.View(), "IDX") {
			t.Errorf("expected table steps query results with 'IDX', got:\n%s", m.viewport.View())
		}
		if m.textInput.Value() != "file://"+dbFile+"/steps" {
			t.Errorf("expected Banquet Bar to reflect %q, got %q", "file://"+dbFile+"/steps", m.textInput.Value())
		}

		// 4. Switch tables directly by typing another table name "trajectory_meta"
		m.textInput.SetValue("trajectory_meta")
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)

		// MUST switch to trajectory_meta, NOT append to steps
		expectedURL := "file://" + dbFile + "/trajectory_meta"
		if m.textInput.Value() != expectedURL {
			t.Errorf("expected Banquet Bar to switch to %q, got %q", expectedURL, m.textInput.Value())
		}
		if !strings.Contains(m.viewport.View(), "TRAJECTORY") && !strings.Contains(m.viewport.View(), "trajectory") {
			t.Errorf("expected viewport for trajectory_meta, got:\n%s", m.viewport.View())
		}

		// 5. Open via "open <dbpath>" command
		m.textInput.SetValue("open file://" + dbFile)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
		m = updatedModel.(model)
		if !strings.Contains(m.viewport.View(), "=== Table: steps") {
			t.Errorf("expected 'open file://...' to open database table list, got:\n%s", m.viewport.View())
		}
	})

	t.Run("OpenDBAndTableMouseClicks", func(t *testing.T) {
		// Start in collection directory
		m := initialModel("file://" + dbDir)

		if !strings.Contains(m.viewport.View(), "tests.db") {
			t.Fatalf("expected collection view to contain tests.db")
		}

		// 1. Mouse Click on the database row in collection view
		// Viewport content start is at Y=3. Line 0=Header, Line 1=Separator, Line 2=tests.db
		// Therefore Y = 3 + 2 = 5
		clickMsg := tea.MouseMsg{
			Action: tea.MouseActionPress,
			Button: tea.MouseButtonLeft,
			X:      10,
			Y:      5,
		}
		updatedModel, _ := m.Update(clickMsg)
		m = updatedModel.(model)

		// MUST open the clicked database (tests.db)
		if strings.Contains(m.viewport.View(), "TABLE COUNT") {
			t.Errorf("mouse click on database row failed: still showing collection view")
		}
		if !strings.Contains(m.viewport.View(), "=== Table:") {
			t.Errorf("expected table list after mouse clicking database row, got:\n%s", m.viewport.View())
		}
		if !strings.Contains(m.textInput.Value(), "tests.db") {
			t.Errorf("expected Banquet Bar to reflect opened database URL, got %q", m.textInput.Value())
		}

		// 2. Mouse Click on a table header in the table list view
		// Find line number of "=== Table: steps" in viewport content
		lines := strings.Split(m.viewportContent, "\n")
		stepsLineIdx := -1
		for i, l := range lines {
			if strings.Contains(l, "=== Table: steps") {
				stepsLineIdx = i
				break
			}
		}
		if stepsLineIdx == -1 {
			t.Fatalf("could not find '=== Table: steps' in table list output: %s", m.viewportContent)
		}

		// Click on the steps table block
		clickTableMsg := tea.MouseMsg{
			Action: tea.MouseActionPress,
			Button: tea.MouseButtonLeft,
			X:      5,
			Y:      3 + stepsLineIdx, // viewportTop is 3
		}
		updatedModel, _ = m.Update(clickTableMsg)
		m = updatedModel.(model)

		// MUST open the steps table query
		if !strings.Contains(m.viewport.View(), "IDX") {
			t.Errorf("mouse click on table failed to open query: got:\n%s", m.viewport.View())
		}
		if !strings.HasSuffix(m.textInput.Value(), "/steps") {
			t.Errorf("expected Banquet Bar to end in '/steps', got %q", m.textInput.Value())
		}

		// 3. Mouse Wheel Scroll
		wheelDown := tea.MouseMsg{
			Action: tea.MouseActionPress,
			Button: tea.MouseButtonWheelDown,
		}
		updatedModel, _ = m.Update(wheelDown)
		m = updatedModel.(model)
		if m.viewport.YOffset == 0 && len(lines) > 20 {
			t.Errorf("expected viewport YOffset to increase on wheel down")
		}
	})
}
