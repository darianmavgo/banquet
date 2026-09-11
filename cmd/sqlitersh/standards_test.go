package main

import (
	"path/filepath"
	"strings"
	"testing"
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
}
