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
		{
			url:      "users.csv",
			expected: "SELECT * FROM \"tb0\"",
		},
		{
			url:      "data.sqlite/users",
			expected: "SELECT * FROM \"users\"",
		},
		{
			url:      "data.sqlite/users/id,name",
			expected: "SELECT \"id\", \"name\" FROM \"users\"",
		},
		{
			url:      "data.sqlite/users/-id",
			expected: "SELECT * FROM \"users\" ORDER BY \"id\" DESC",
		},
		{
			url:      "data.sqlite/users/+name",
			expected: "SELECT * FROM \"users\" ORDER BY \"name\" ASC",
		},
		{
			url:      "data.sqlite/users[0:10]",
			expected: "SELECT * FROM \"users\" LIMIT 10 OFFSET 0",
		},
		{
			url:      "data.sqlite/users[10:30]",
			expected: "SELECT * FROM \"users\" LIMIT 20 OFFSET 10",
		},
		{
			url:      "data.sqlite/users/id,name[5:15]",
			expected: "SELECT \"id\", \"name\" FROM \"users\" LIMIT 10 OFFSET 5",
		},
		{
			url:      "data.sqlite/users?where=age > 18",
			expected: "SELECT * FROM \"users\" WHERE age > 18",
		},
		{
			url:      "data.sqlite/users/id,name?where=age > 18&limit=5",
			expected: "SELECT \"id\", \"name\" FROM \"users\" WHERE age > 18 LIMIT 5",
		},
		{
			url:      "data.sqlite/users/age!=20",
			expected: "SELECT * FROM \"users\" WHERE age != 20",
		},
		{
			url:      "data.sqlite/users/name!=John,age!=30",
			expected: "SELECT * FROM \"users\" WHERE name != 'John' AND age != 30",
		},
		{
			url:      "data.sqlite/users/id,name/-id[0:5]?where=active=1",
			expected: "SELECT \"id\", \"name\" FROM \"users\" WHERE active=1 ORDER BY \"id\" DESC LIMIT 5 OFFSET 0",
		},
		{
			url:      "data.sqlite/users?groupby=category",
			expected: "SELECT * FROM \"users\" GROUP BY \"category\"",
		},
		{
			url:      "data.sqlite/users/id,name?orderby=name&limit=10",
			expected: "SELECT \"id\", \"name\" FROM \"users\" ORDER BY \"name\" LIMIT 10",
		},
		{
			url:      "data.sqlite/users?having=count(*) > 1",
			expected: "SELECT * FROM \"users\" HAVING count(*) > 1",
		},
		{
			url:      "data.sqlite/users/id,name?where=id IN (1,2,3)",
			expected: "SELECT \"id\", \"name\" FROM \"users\" WHERE id IN (1,2,3)",
		},
		{
			url:      "data.sqlite/users/id,name/-name[0:10]?where=age > 20&groupby=dept",
			expected: "SELECT \"id\", \"name\" FROM \"users\" WHERE age > 20 GROUP BY \"dept\" ORDER BY \"name\" DESC LIMIT 10 OFFSET 0",
		},
		{
			url:      "data.sqlite/users/id,name/+id?offset=20&limit=10",
			expected: "SELECT \"id\", \"name\" FROM \"users\" ORDER BY \"id\" ASC LIMIT 10 OFFSET 20",
		},
		{
			url:      "data.sqlite/users/id,name?where=name LIKE 'A%'",
			expected: "SELECT \"id\", \"name\" FROM \"users\" WHERE name LIKE 'A%'",
		},
		{
			// Empty table for SQLite without columns -> sqlite_master
			url:      "data.sqlite",
			expected: "SELECT * FROM \"sqlite_master\"",
		},
		{
			// Empty table for CSV -> tb0
			url:      "data.csv",
			expected: "SELECT * FROM \"tb0\"",
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
