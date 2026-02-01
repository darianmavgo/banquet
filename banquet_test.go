package banquet

import (
	"fmt"
	"testing"
)

func TestLog(t *testing.T) {

	fmt.Printf("--- START: %s\n", t.Name())
}

func TestSuperUglyUrlParseNested(t *testing.T) {
	ugly := "http://localhost:8080/https://bucket.appspot.com:8080/v1/{banquet}/path:with;@+,$/[^]|\\< >~%25/column1,column2/+const?orderid=-1&tag=prime+val&filter={status:active}&search=~alt#fragment-top"
	b, err := ParseNested(ugly)
	if err != nil {
		t.Errorf("Failed to parse nested URL: %v", err)
		return
	}
	FmtPrintln(b)
	// wants
	wants := []string{"bucket.appspot.com:8080",
		"/v1/{banquet}/path:with;@+,$/[^]|\\< >~%/column1,column2/+const",
		"orderid=-1&tag=prime+val&filter={status:active}&search=~alt"}
	// gots
	gots := []string{b.Host, b.Path, b.RawQuery}
	for i, want := range wants {
		if want != gots[i] {
			t.Errorf("Expected: %s\nGot: %s", want, gots[i])
		}
	}
}
func TestCleanUrl(t *testing.T) {
	url := "/http:/darianhickman.com:8080/some/local/path/file.csv;col1,col2,col3"
	cleanUrl := CleanUrl(url)
	if cleanUrl != "http://darianhickman.com:8080/some/local/path/file.csv;col1,col2,col3" {
		t.Errorf("Expected: %s\nGot: %s", "http://darianhickman.com:8080/some/local/path/file.csv;col1,col2,col3", cleanUrl)
	}
}

func TestLocalParse(t *testing.T) {
	// Thorough checking of banquet fields for a local/nested URL
	// Using the URL format: "http://localhost:8080/some/local/path/file.csv;col1,col2,col3"
	b, err := ParseNested("http://localhost:8080/some/local/path/file.csv;col1,col2,col3")
	if err != nil {
		t.Fatalf("Failed to parse nested URL: %v", err)
	}

	// 1. Verify DataSetPath extraction
	if b.DataSetPath != "some/local/path/file.csv" {
		t.Errorf("DataSetPath mismatch: got %q, want %q", b.DataSetPath, "some/local/path/file.csv")
	}

	// 2. Verify Table identification (in 2-tier format, second part is Table)
	if b.Table != "col1,col2,col3" {
		t.Errorf("Table mismatch: got %q, want %q", b.Table, "col1,col2,col3")
	}

	// 3. Verify Selection (no column tier provided, so should default to *)
	if len(b.Select) != 1 || b.Select[0] != "*" {
		t.Errorf("Select mismatch: got %v, want %v", b.Select, []string{"*"})
	}

	// 4. Add a more complex case to be truly thorough
	complexURL := "http://localhost:8080/gs:/my-bucket/database.sqlite;customers;id,name,+age?where=age>18&limit=50"
	bc, err := ParseNested(complexURL)
	if err != nil {
		t.Fatalf("Failed to parse complex nested URL: %v", err)
	}

	if bc.Scheme != "gs" {
		t.Errorf("Complex Case Scheme: got %q, want %q", bc.Scheme, "gs")
	}
	if bc.Host != "my-bucket" {
		t.Errorf("Complex Case Host: got %q, want %q", bc.Host, "my-bucket")
	}
	if bc.DataSetPath != "/database.sqlite" {
		t.Errorf("Complex Case DataSetPath: got %q, want %q", bc.DataSetPath, "/database.sqlite")
	}
	if bc.Table != "customers" {
		t.Errorf("Complex Case Table: got %q, want %q", bc.Table, "customers")
	}
	if bc.Where != "age>18" {
		t.Errorf("Complex Case Where: got %q, want %q", bc.Where, "age>18")
	}
	if bc.OrderBy != "age" {
		t.Errorf("Complex Case OrderBy: got %q, want %q", bc.OrderBy, "age")
	}
	if bc.Limit != "50" {
		t.Errorf("Complex Case Limit: got %q, want %q", bc.Limit, "50")
	}

	// Verify Select columns (sort indicators should be excluded from selection per current implementation)
	expectedCols := []string{"id", "name"}
	if len(bc.Select) != len(expectedCols) {
		t.Errorf("Complex Case Select count: got %d, want %d: %v", len(bc.Select), len(expectedCols), bc.Select)
	} else {
		for i, col := range expectedCols {
			if bc.Select[i] != col {
				t.Errorf("Complex Case Select[%d]: got %q, want %q", i, bc.Select[i], col)
			}
		}
	}
}

func TestParseNested(t *testing.T) {
	reqURL := "https://localhost:8080/gs:/matrix@bucket.appspot.com:8080/some/file/path.csv/column1,column2/+column3?orderid=1"
	b, err := ParseNested(reqURL)
	if err != nil {
		t.Errorf("Failed to parse nested URL: %v", err)
		return
	}
	if b.Scheme != "gs" {
		t.Errorf("Scheme is incorrect. Got %s, expected %s", b.Scheme, "gs")
	}
	if b.User.Username() != "matrix" {
		t.Errorf("Username is incorrect. Got %s, expected %s", b.User.Username(), "matrix")
	}
	if b.Host != "bucket.appspot.com:8080" {
		t.Errorf("Host is incorrect. Got %s, expected %s", b.Host, "bucket.appspot.com:8080")
	}
	if b.Port() != "8080" {
		t.Errorf("Port is incorrect. Got %s, expected %s", b.Port(), "8080")
	}
	if b.Path != "/some/file/path.csv/column1,column2/+column3" {
		t.Errorf("Path is incorrect. Got %s, expected %s", b.Path, "/some/file/path.csv/column1,column2/+column3")
	}
	if b.RawQuery != "orderid=1" {
		t.Errorf("Query is incorrect. Got %s, expected %s", b.RawQuery, "orderid=1")
	}

}
func TestParseBanquet(t *testing.T) {
	// Complex URL to test all fields
	// Scheme: gs
	// Bucket: bucket.appspot.com:8080 (authority)
	// Path: /some/file/path.csv/column1,column2/^column3
	// Query: where=age>20&limit=10&offset=5&groupby=department&having=count>1&orderby=name
	testURL := "gs://bucket.appspot.com:8080/some/file/path.csv/column1,column2,+column3?where=age>20&limit=10&offset=5&groupby=department&having=count>1"

	b, err := ParseBanquet(testURL)
	if err != nil {
		t.Fatalf("ParseBanquet failed: %v", err)
	}

	// Verify basic URL fields
	if b.Scheme != "gs" {
		t.Errorf("Expected Scheme 'gs', got '%s'", b.Scheme)
	}
	if b.Host != "bucket.appspot.com:8080" {
		t.Errorf("Expected Host 'bucket.appspot.com:8080', got '%s'", b.Host)
	}

	// Verify Table
	// For CSV/flat files without explicit table tier, Table should be empty or derived from ColumnPath
	// In this heuristic path, b.Table remains empty because the ColumnPath contains sort/select indicators.
	if b.Table != "" {
		t.Errorf("Expected empty Table for heuristic path, got %q", b.Table)
	}

	// Verify Select
	// column1, column2, +column3 (sort indicator + causes exclusion from selection)
	expectedSelect := []string{"column1", "column2"}
	if len(b.Select) != 2 {
		t.Errorf("Expected 2 Select columns, got %d: %v", len(b.Select), b.Select)
	} else {
		for i, col := range b.Select {
			if col != expectedSelect[i] {
				t.Errorf("Expected Select[%d] = '%s', got '%s'", i, expectedSelect[i], col)
			}
		}
	}

	// Verify Sort
	// ^column3 in path
	if b.OrderBy != "column3" {
		t.Errorf("Expected OrderBy 'column3', got '%s'", b.OrderBy)
	}

	// Verify Query Params
	if b.Where != "age>20" {
		t.Errorf("Expected Where 'age>20', got '%s'", b.Where)
	}
	if b.Limit != "10" {
		t.Errorf("Expected Limit '10', got '%s'", b.Limit)
	}
	if b.Offset != "5" {
		t.Errorf("Expected Offset '5', got '%s'", b.Offset)
	}
	if b.GroupBy != "department" {
		t.Errorf("Expected GroupBy 'department', got '%s'", b.GroupBy)
	}
	if b.Having != "count>1" {
		t.Errorf("Expected Having 'count>1', got '%s'", b.Having)
	}

}

func TestParseNestedUrl(t *testing.T) {
	reqURL := "https://localhost:8080/gs:/matrix@bucket.appspot.com:8080/some/file/path.csv/column1,column2,+column3?orderid=1"

	banquet, err := ParseNested(reqURL)
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}

	// Check scheme
	if banquet.Scheme != "gs" {
		t.Errorf("Scheme is incorrect. Got %s, expected %s", banquet.Scheme, "gs")
	}

	// Check host
	if banquet.Host != "bucket.appspot.com:8080" {
		t.Errorf("Host is incorrect. Got %s, expected %s", banquet.Host, "bucket.appspot.com:8080")
	}

	// Check port
	if banquet.Port() != "8080" {
		t.Errorf("Port is incorrect. Got %s, expected %s", banquet.Port(), "8080")
	}

	// Check path
	expectedPath := "/some/file/path.csv/column1,column2,+column3"
	if banquet.Path != expectedPath {
		t.Errorf("Path is incorrect. Got %s, expected %s", banquet.Path, expectedPath)
	}

	// Check query parameters
	expectedQuery := "orderid=1"
	if banquet.RawQuery != expectedQuery {
		t.Errorf("Query is incorrect. Got %s, expected %s", banquet.RawQuery, expectedQuery)
	}
}

// TestUnescapetolerant tests the unescapetolerant function
// Note: UnescapeTolerant is in core/core.go now per Step 487. Banquet doesn't use it directly?
// Or maybe I should move it to banquet as well?
// core.UnescapeTolerant exists. banquet.go used Cleaning function but not UnescapeTolerant specifically?
// I'll skip this test if UnescapeTolerant is not in banquet package.
// Or implement it in banquet if needed.
// For now, I'll comment out UnescapeTolerant test if it refers to undefined function.

// TestParseSelect is calling internal parseSelect which I didn't export?
// parseSelect IS internal (lowercase). Valid in same package.

func TestParseSelect(t *testing.T) {
	afterTable := "column1,+column2,-column3"
	// Currently, columns with sort prefixes (+/-) are excluded from selection
	expected := []string{"column1"}

	result := ParseSelect(afterTable)

	if len(result) != len(expected) {
		t.Errorf("Expected length %d, got %d: %v", len(expected), len(result), result)
	} else {
		for i := range result {
			if result[i] != expected[i] {
				t.Errorf("Expected %v, but got %v", expected, result)
			}
		}
	}
}

func TestParseGroupBy(t *testing.T) {
	TestLog(t)
	afterPart := "some_column(group_column)"
	expected := "group_column"

	result := ParseGroupBy(afterPart, "") // Updated signature

	if result != expected {
		t.Errorf("Expected %v, but got %v", expected, result)
	}
}

func TestParseLegacyLiteral(t *testing.T) {
	afterTable := "^column1,!^column2"
	// Now ^ and !^ should be treated as literal parts of the column name
	expected := []string{"^column1", "!^column2"}

	result := ParseSelect(afterTable)

	if len(result) != len(expected) {
		t.Errorf("Expected length %d, got %d", len(expected), len(result))
	} else {
		for i := range result {
			if result[i] != expected[i] {
				t.Errorf("Expected Select[%d] = '%s', got '%s'", i, expected[i], result[i])
			}
		}
	}

	ob, dir := parseOrderBy(afterTable, "")
	if ob != "" || dir != "" {
		t.Errorf("Expected no OrderBy for legacy literals, got %s (%s)", ob, dir)
	}
}
