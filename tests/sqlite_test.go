package tests

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/darianmavgo/banquet"

	_ "github.com/mattn/go-sqlite3"
)

func TestNotNull(t *testing.T) {
	// 1. Parse URL
	rawURL := "http://localhost:8081/History.xlsx.db/raw_content/academic_resume_cv!=Undergraduate%20Studies"
	b, err := banquet.ParseBanquet(rawURL)
	if err != nil {
		t.Fatalf("Failed to parse URL: %v", err)
	}

	// 2. Verify basic fields
	if b.Table != "raw_content" {
		t.Errorf("Expected Table 'raw_content', got '%s'", b.Table)
	}

	// 3. Verify Where clause is constructed correctly
	// The user expects "translate != to whatever the correct syntax is for sqlite"
	// And since the value is a string, it should be quoted.
	expectedWhere := "academic_resume_cv != 'Undergraduate Studies'"
	if !strings.Contains(b.Where, expectedWhere) {
		t.Errorf("Expected Where clause to contain %q, got %q", expectedWhere, b.Where)
	}

	// 4. End-to-end test with SQLite
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite db: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`CREATE TABLE raw_content (id INTEGER PRIMARY KEY, academic_resume_cv TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec(`INSERT INTO raw_content (academic_resume_cv) VALUES ('Undergraduate Studies')`)
	if err != nil {
		t.Fatalf("Failed to insert row 1: %v", err)
	}
	_, err = db.Exec(`INSERT INTO raw_content (academic_resume_cv) VALUES ('PhD')`)
	if err != nil {
		t.Fatalf("Failed to insert row 2: %v", err)
	}

	// Construct Query
	// Note: ParseBanquet puts "raw_content" in b.Table
	query := fmt.Sprintf("SELECT academic_resume_cv FROM %s WHERE %s", b.Table, b.Where)
	t.Logf("Executing query: %s", query)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, val)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d: %v", len(results), results)
	} else if results[0] != "PhD" {
		t.Errorf("Expected result 'PhD', got '%s'", results[0])
	}
}
