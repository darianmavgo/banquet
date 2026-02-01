package sqlite

import (
	"testing"

	"github.com/darianmavgo/banquet"
)

func TestConstructSQL(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		// --- 1. Basic Table Inference ---
		{
			// CSV path with implicit table (no table tier)
			url:      "users.csv",
			expected: "SELECT * FROM \"tb0\"",
		},
		{
			// SQLite path with implicit table (no table tier).
			// If no columns are requested and it's a sqlite file, InferTable returns "sqlite_master" per implementation.
			url:      "data.sqlite",
			expected: "SELECT * FROM \"sqlite_master\"",
		},
		{
			// Explicit table "users"
			url:      "data.sqlite;users",
			expected: "SELECT * FROM \"users\"",
		},

		// --- 2. Column Selection ---
		{
			// Single column
			url:      "data.sqlite;users;id",
			expected: "SELECT \"id\" FROM \"users\"",
		},
		{
			// Multiple columns
			url:      "data.sqlite;users;id,name,email",
			expected: "SELECT \"id\", \"name\", \"email\" FROM \"users\"",
		},
		{
			// Explicit table tier with no column tier -> SELECT *
			url:      "data.sqlite;users",
			expected: "SELECT * FROM \"users\"",
		},
		{
			// Explicit table tier with * column
			url:      "data.sqlite;users;*",
			expected: "SELECT * FROM \"users\"",
		},

		// --- 3. Sorting ---
		{
			// Ascending sort, implicitly selects * because sort col is excluded from select
			url:      "data.sqlite;users;+name",
			expected: "SELECT * FROM \"users\" ORDER BY \"name\" ASC",
		},
		{
			// Descending sort
			url:      "data.sqlite;users;-created_at",
			expected: "SELECT * FROM \"users\" ORDER BY \"created_at\" DESC",
		},
		{
			// Mixed Select and Sort: Select 'id', Sort by 'name' ASC
			url:      "data.sqlite;users;id,+name",
			expected: "SELECT \"id\" FROM \"users\" ORDER BY \"name\" ASC",
		},
		{
			// Sort col in middle of select list
			url:      "data.sqlite;users;id,-age,email",
			expected: "SELECT \"id\", \"email\" FROM \"users\" ORDER BY \"age\" DESC",
		},

		// --- 4. Slice Notation (Limit/Offset) ---
		{
			// Simple slice at end
			url:      "data.sqlite;users[0:10]",
			expected: "SELECT * FROM \"users\" LIMIT 10 OFFSET 0",
		},
		{
			// Slice with offset
			url:      "data.sqlite;users[20:30]",
			expected: "SELECT * FROM \"users\" LIMIT 10 OFFSET 20",
		},
		{
			// Slice amidst columns - standard behavior
			url:      "data.sqlite;users;id[5:15],name",
			expected: "SELECT \"id\", \"name\" FROM \"users\" LIMIT 10 OFFSET 5",
		},
		{
			// Slice on last column
			url:      "data.sqlite;users;id,name[0:50]",
			expected: "SELECT \"id\", \"name\" FROM \"users\" LIMIT 50 OFFSET 0",
		},

		// --- 5. Filtering (WHERE) ---
		{
			// Query param where
			url:      "data.sqlite;users?where=age>18",
			expected: "SELECT * FROM \"users\" WHERE age>18",
		},
		{
			// Path condition (custom banquet syntax if supported) AND query param
			// Note: Current ParseBanquet implementation supports path conditions via parsePathConditions (x!=y)
			url:      "data.sqlite;users;status!=active?where=age>18",
			expected: "SELECT * FROM \"users\" WHERE age>18 AND status != 'active'",
		},
		{
			// Multiple path conditions
			url:      "data.sqlite;users;status!=active,role!=admin",
			expected: "SELECT * FROM \"users\" WHERE status != 'active' AND role != 'admin'",
		},

		// --- 6. Grouping and Having ---
		{
			// Group By via Query Param
			url:      "data.sqlite;users?groupby=country",
			expected: "SELECT * FROM \"users\" GROUP BY \"country\"",
		},
		{
			// Having clause
			url:      "data.sqlite;users?groupby=country&having=count(*)>5",
			expected: "SELECT * FROM \"users\" GROUP BY \"country\" HAVING count(*)>5",
		},

		// --- 7. Complex Combinations ---
		{
			// Select, Filter, Sort, Limit
			url:      "data.sqlite;users;id,name,-age?where=active=1&limit=5",
			expected: "SELECT \"id\", \"name\" FROM \"users\" WHERE active=1 ORDER BY \"age\" DESC LIMIT 5",
		},
		{
			// Slice with Sort and Select
			url:      "data.sqlite;users;id,email,+joined[10:20]",
			expected: "SELECT \"id\", \"email\" FROM \"users\" ORDER BY \"joined\" ASC LIMIT 10 OFFSET 10",
		},
		{
			// URL decoding in filters: "name!=O%27Reilly" decodes to "name!=O'Reilly"
			// Then quoted to 'O''Reilly'
			url:      "data.sqlite;users;name!=O%27Reilly",
			expected: "SELECT * FROM \"users\" WHERE name != 'O''Reilly'",
		},

		// --- 8. Heuristic Path Parsing (No Semicolons) ---
		{
			// Heuristic: file.csv/col1,col2 -> col1, col2 from tb0
			url:      "file.csv/col1,col2",
			expected: "SELECT \"col1\", \"col2\" FROM \"tb0\"",
		},
		{
			// Heuristic: db.sqlite/table/col1 -> table explicit
			url:      "db.sqlite/mytable/col1",
			expected: "SELECT \"col1\" FROM \"mytable\"",
		},
		{
			// Heuristic: db.sqlite/table -> select * from table
			url:      "db.sqlite/mytable",
			expected: "SELECT * FROM \"mytable\"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			bq, err := banquet.ParseBanquet(tt.url)
			if err != nil {
				t.Fatalf("ParseBanquet(%q) error: %v", tt.url, err)
			}
			got := Compose(bq)
			if got != tt.expected {
				t.Errorf("ConstructSQL() = %q, want %q", got, tt.expected)
			}
		})
	}
}
