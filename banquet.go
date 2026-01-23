package banquet

import (
	"fmt"
	"log"
	"net/url"
	"path"
	"strconv"
	"strings"
)

var verbose bool

// SetVerbose enables or disables verbose logging for the package.
func SetVerbose(v bool) {
	verbose = v
}

// IsVerbose returns true if verbose logging is enabled.
func IsVerbose() bool {
	return verbose
}

// Banquet package and Banquet struct are designed to provide a URL standardization and parsing framework for tabular data.
// What will banquet have the url.URL doesn't?
// 1. Where clause
// 2. Select clause
// 3. Sort clause
// 4. Limit clause
// 5. Offset clause
// 6. Group by clause
// 7. Having clause
// 8. Order by clause
// 9. Slice Notation [2:30] inspired by python and golang to signal subset of rows.
// 10. Special characters to signal sorting direction, ascending or descending.
// 11. Repurpose userinfo to signal authentication.
// 12. Repurpose path to signal file path or db path or object key or zip file path.
// 13. Repurpose query to signal query parameters.
// 14. Lots of tolerance for unescaped characters.
// ColumnPath is the path to the column.
type Banquet struct {
	*url.URL
	Where         string
	Table         string   // table name to go in FROM clause parsed from request url
	Select        []string // columns to select.  empty or * means all columns
	SortDirection string   // refactor this to mean ASC or DESC. We have OrderBy for previous Sort meaning.
	Limit         string
	Offset        string
	GroupBy       string
	Having        string
	OrderBy       string
	DataSetPath   string // Server needs this to respond with the downloadable, convedata set. Excel, CSV, eventually BigQuery dataset.

	ColumnPath string // Formatted table/column1, column2. Empty means select * from dataset that only has one table..
	// fields below are for internal use
	rawurl string
	path   string
}

const (
	// Sort direction tokens
	ASC  = "+" // token to signal the following column is sorted ascending
	DESC = "-" // token to signal the following column is sorted descending

)

/*
// General form of a URL:
//
//	[scheme:][//[userinfo@]host][/]path[?query][#fragment]
*/
/* General form of a Banquet:
//
Familiar form:
//	[scheme:][//[userinfo@]host][/]path/to/dataset/table/column1,column2,column3...?[where][select][sort][limit][offset][groupby][having][orderby][slice][#fragment]
Since I can't find a way to signal a priori that path part is table vs column name vs file path, we will use a convention
Canonical form:
[scheme:][//[userinfo@]host][/]path/to/dataset;table/column1,column2,column3...?[where][select][sort][limit][offset][groupby][having][orderby][slice][#fragment]
//
// List of prefixes to support
//  + and - prefixed to column name signals sorting on that column.
//  [number:number] signals slice notation. It gets translated to limit and offset.
//  TBD signals group by.
//  TBD signals having.
//  TBD signals order by.
// List of suffixes to support
//  Suffixes such as file ending are so important to parsing that I will probably avoid more suffixes.
*/

/*Example Banquet Unparseable	reqURL := "https://localhost:8080/gs:/matrix@bucket.appspot.com:8080/some/file/path.csv/column1,column2/^column3?orderid=1"
Files found in tests:
reqURL := "/gs://wc2022-356423.appspot.com/Expenses.csv/%5EDescription"
reqURL := "gs:///wc2022-356423.appspot.com/Expenses.csv/^Description"
testURL := "https://raw.githubusercontent.com/uiuc-cse/data-fa14/gh-pages/data/iris.csv"
testURL := "https://raw.githubusercontent.com/holtzy/data_to_viz/master/Example_dataset/1_OneNum.csv"
testURL := "https://raw.githubusercontent.com/uiuc-cse/data-fa14/gh-pages/data/iris.csv"
reqURL := "https://localhost:8080/gs:/matrix@bucket.appspot.com:8080/some/file/path.csv/column1,column2/^column3?orderid=1"
reqURL := "https://localhost:8080/gs:/some/file/path.csv/column1,column2/^column3?where=1=1"
reqURL := "gs:/maverick@buckt1.appspot.com/some/file/path.csv/column1,column2/^column3?where=1=1"
reqURL := "http://localhost:8080/sample%20data/demo%20mavgo%20flight/Expenses.csv"
reqURL := "http://localhost:8080/data.csv/^column1"
*/

// A big UI incompatibility is that I hate url escape sequences.

// So unescape tolerant parsing need to be here. ! PathUnescape already exists. !
// Trims leading slash, fixes scheme format, but expecting more cleanings as we go.
func CleanUrl(rawurl string) string {
	// housekeeping before url.Parse
	// 1. Trim leading slash if left over from http.Request
	rawurl = strings.TrimPrefix(rawurl, "/")

	// Ensure standard scheme format (e.g., gs:/ -> gs://) for proper authority parsing
	if idx := strings.Index(rawurl, ":/"); idx != -1 {
		if !strings.HasPrefix(rawurl[idx:], "://") {
			rawurl = strings.Replace(rawurl, ":/", "://", 1)
		}
	}

	return rawurl
}

// go DOES NOT support override of URL.Parse so instead we will use a factory function.
func ParseBanquet(rawurl string) (*Banquet, error) {
	if verbose {
		log.Printf("[BANQUET] Parsing URL: %s", rawurl)
	}
	// Standardize/Clean the URL (trim leading slash, fix scheme)
	rawurl = CleanUrl(rawurl)

	if verbose {
		log.Printf("[BANQUET] Cleaned URL: %s", rawurl)
	}

	u, err := url.Parse(rawurl)
	if err != nil {
		if verbose {
			log.Printf("[BANQUET] URL parse error: %v", err)
		}
		return nil, err
	}

	b := &Banquet{
		URL:    u,
		rawurl: rawurl,
	}

	b.DataSetPath, b.ColumnPath = parseDataSetColumnPath(b.Path)
	if verbose {
		log.Printf("[BANQUET] DataSetPath: %s, ColumnPath: %s", b.DataSetPath, b.ColumnPath)
	}

	// Populate fields using private parsers
	b.Select = ParseSelect(b.ColumnPath)
	if verbose {
		log.Printf("[BANQUET] Selected columns: %v", b.Select)
	}

	// Combine query params 'where' and path conditions
	queryWhere := parseWhere(b.RawQuery)
	pathWhere := parsePathConditions(b.ColumnPath)

	if pathWhere != "" {
		if queryWhere != "" {
			b.Where = queryWhere + " AND " + pathWhere
		} else {
			b.Where = pathWhere
		}
	} else {
		b.Where = queryWhere
	}

	if verbose && b.Where != "" {
		log.Printf("[BANQUET] effective WHERE: %s", b.Where)
	}

	b.GroupBy = ParseGroupBy(b.Path, b.RawQuery)
	// Table parsing logic
	b.Table = parseTable(b.Path)
	if verbose {
		log.Printf("[BANQUET] Table identified: %s", b.Table)
	}

	b.Limit = parseLimit(b.RawQuery, b.Path)
	b.Offset = parseOffset(b.RawQuery, b.Path)
	b.Having = parseHaving(b.RawQuery)
	if ob, dir := parseOrderBy(b.ColumnPath, b.RawQuery); ob != "" {
		b.OrderBy = ob
		if dir != "" {
			b.SortDirection = dir
		}
	}

	return b, nil
}

func FmtPrintln(b *Banquet) {
	fmt.Printf(`rawurl: %s
Scheme: %s
  Host:   %s
  Path:   %s
  RawQuery: %s
  Where:      %q
  Table:      %q
  Limit:      %q
`, b.rawurl, b.Scheme, b.Host, b.Path, b.RawQuery, b.Where, b.Table, b.Limit)
}

// Don't create func Parse(rawurl string) (*Banquet, error) we basically have that in url.Parse

// ParseNested is a factory function that creates a new Banquet from Banquet URL nested in an outer URL.
// Expecting this a lot from http.Request.
func ParseNested(rawURL string) (*Banquet, error) {
	// 1. Parse the outer envelope
	// If rawURL is http://localhost..., url.Parse works.
	// If rawURL is just /http..., we need to trim prefix.
	if strings.HasPrefix(rawURL, "/") {
		rawURL = strings.TrimPrefix(rawURL, "/")
	}

	outer, err := url.Parse(rawURL)
	if err != nil {
		// If outer parse fails, we might just try to treat the whole thing as an inner url?
		// But usually this means it's really malformed.
		return nil, err
	}

	// Key Fix: Use EscapedPath() to get the path segment exactly as it was on the wire (checking for %25 etc)
	// If we use outer.Path, %25 is decoded to %, which might form invalid escapes later.
	inner := outer.EscapedPath()

	// If strings.HasPrefix(inner, "/") -> trimming?
	// ParseBanquet trims leading slash.

	if outer.RawQuery != "" {
		inner += "?" + outer.RawQuery
	}

	b, err := ParseBanquet(inner)
	if err != nil {
		fmt.Printf("Error parsing inner URL '%s': %v. Continuing with raw URL.\n", inner, err)
		// Return partial Banquet with just the raw extraction
		return &Banquet{
			URL:    &url.URL{Path: inner}, // Best effort
			rawurl: inner,
		}, nil
	}
	return b, nil
}

// Internal parsing functions
func parseDataSetColumnPath(rawpath string) (datasetPath string, columnPath string) {
	// if rawpath contains ";" first part is dataset pathrawpath second part is columnpathrawpath
	if idx := strings.Index(rawpath, ";"); idx != -1 {
		return rawpath[:idx], rawpath[idx+1:]
	}

	// if there is no ";" then use existing file extension logic to split path into dataset path and column path
	parts := strings.Split(rawpath, "/")
	for i, part := range parts {
		if strings.HasSuffix(part, ".zip") ||
			strings.HasSuffix(part, ".csv") ||
			strings.HasSuffix(part, ".sqlite") ||
			strings.HasSuffix(part, ".db") ||
			strings.HasSuffix(part, ".xlsx") ||
			strings.HasSuffix(part, ".json") ||
			(strings.HasSuffix(part, ".html") && part != "test.html") ||
			strings.HasSuffix(part, ".txt") {

			datasetPath = strings.Join(parts[:i+1], "/")
			if i+1 < len(parts) {
				columnPath = strings.Join(parts[i+1:], "/")
			}
			return
		}
	}
	return rawpath, ""
}

// getSegments identifies the part of the path that contains columns or conditions
func getSegments(columnPath string) []string {
	parts := strings.Split(columnPath, "/")
	if len(parts) == 0 {
		return nil
	}
	startIndex := -1
	// Find where the file/table definition ends using generic extension check
	// We iterate from left to right to find the *first* part that looks like a file?
	// Or last? Usually path/to/file.ext/col1.
	// Current logic was first match. Let's stick to first match.
	for i, part := range parts {
		if path.Ext(part) != "" {
			startIndex = i + 1
			break
		}
	}

	// If no file extension found, fall back to checking strict last part
	if startIndex == -1 {
		lastPart := parts[len(parts)-1]
		// If last part looks like a file, we definitely don't have columns after it.
		if path.Ext(lastPart) != "" {
			return nil
		}
		// Otherwise, assume the last part contains columns if it doesn't look like a file/resource?
		// Heuristic: If it has commas or sort prefix, it's columns.
		// Added != for conditions
		if strings.Contains(lastPart, ",") || strings.HasPrefix(lastPart, ASC) || strings.HasPrefix(lastPart, DESC) || strings.Contains(lastPart, "!=") {
			startIndex = len(parts) - 1
		} else {
			// If ambiguous (no indicators), assume it's part of the path (no selection)
			startIndex = len(parts) - 1
		}
	}

	if startIndex >= len(parts) {
		return nil
	}

	return parts[startIndex:]
}

func ParseSelect(columnPath string) []string {
	segments := getSegments(columnPath)
	if len(segments) == 0 {
		return []string{"*"}
	}

	var collected []string
	for _, segment := range segments {
		// Ignore slice notation
		if strings.HasPrefix(segment, "[") && strings.HasSuffix(segment, "]") && strings.Contains(segment, ":") {
			continue
		}
		if segment == "" {
			continue
		}
		cols := strings.Split(segment, ",")
		for _, col := range cols {
			// Ignore conditions
			if strings.Contains(col, "!=") {
				continue
			}

			// Clean up sort indicators
			col = strings.TrimPrefix(col, ASC)
			col = strings.TrimPrefix(col, DESC)

			// Basic cleanup
			col = strings.TrimSpace(col)
			if col != "" {
				collected = append(collected, col)
			}
		}
	}

	if len(collected) == 0 {
		return []string{"*"}
	}

	return collected
}

func parsePathConditions(columnPath string) string {
	segments := getSegments(columnPath)
	if len(segments) == 0 {
		return ""
	}

	var conditions []string
	for _, segment := range segments {
		if segment == "" {
			continue
		}
		// Conditions can be comma separated? e.g. col1!=val1,col2!=val2
		// Assuming yes since ParseSelect splits by comma.
		parts := strings.Split(segment, ",")
		for _, part := range parts {
			if strings.Contains(part, "!=") {
				// Split
				kv := strings.SplitN(part, "!=", 2)
				if len(kv) == 2 {
					col := strings.TrimSpace(kv[0])
					val := strings.TrimSpace(kv[1])

					// URL Decode value
					decodedVal, err := url.QueryUnescape(val)
					if err == nil {
						val = decodedVal
					}

					// Quote if not number
					if _, err := strconv.ParseFloat(val, 64); err != nil {
						// Quote single quotes for SQL safety
						val = strings.ReplaceAll(val, "'", "''")
						val = "'" + val + "'"
					}

					conditions = append(conditions, fmt.Sprintf("%s != %s", col, val))
				}
			}
		}
	}

	if len(conditions) == 0 {
		return ""
	}
	return strings.Join(conditions, " AND ")
}

func parseWhere(query string) string {
	// Simple extraction of 'where' parameter
	v, err := url.ParseQuery(query)
	if err != nil {
		return ""
	}
	return v.Get("where")
}

func ParseGroupBy(path string, query string) string {
	// check query first
	v, _ := url.ParseQuery(query)
	if g := v.Get("groupby"); g != "" {
		return g
	}

	// check path for (expression)
	if strings.Contains(path, "(") && strings.Contains(path, ")") {
		start := strings.Index(path, "(")
		end := strings.Index(path, ")")
		if start < end {
			return path[start+1 : end]
		}
	}
	return ""
}

// parseSort function deleted as it is superseded by parseOrderBy and parseSortStr

// parseTable attempts to identify the table from the path.
// This is a simplified version and might need robust logic akin to core/parse.go eventually.
func parseTable(path string) string {
	parts := strings.Split(path, "/")
	// Iterate backwards.
	// The last part is usually select/sort or slice.
	// The part before that might be the table.
	// If the file extension is .csv, table is effectively the file (or "tb0" per core).
	// If .sqlite, next part is table.

	// For now, heuristic:
	// If part has extension .csv, return "tb0".
	// If part has extension .sqlite, look at next part.

	for i := 0; i < len(parts); i++ {
		part := parts[i]
		if strings.HasSuffix(part, ".csv") {
			return "tb0"
		}
		if strings.HasSuffix(part, ".sqlite") || strings.HasSuffix(part, ".db") {
			if i+1 < len(parts) {
				// The next part might be the table name, provided it's not a select string
				next := parts[i+1]
				if !strings.Contains(next, ",") && !strings.HasPrefix(next, ASC) && !strings.HasPrefix(next, DESC) && !strings.HasPrefix(next, "!") && !strings.Contains(next, "!=") {
					return next
				}
				// If next is empty or is select/sort, then default table "sqlite_master"
				return "sqlite_master"
			}
			return "sqlite_master"
		}
	}

	// Fallback/Generic behavior for non-file paths or unknown structure
	// If structure is table/selector
	if len(parts) >= 1 {
		// Check last part
		last := parts[len(parts)-1]
		// If last part does not look like a selector (comma, sort prefix)
		// Updated to include != check via ! prefix (though != starts with !) or containing !=
		if !strings.Contains(last, ",") && !strings.HasPrefix(last, ASC) && !strings.HasPrefix(last, DESC) && !strings.Contains(last, "!=") {
			// It is likely the table (or resource)
			// Verify it's not empty or .
			if last != "" && last != "." {
				return last
			}
		}

		// If last part IS a selector, then table is previous part
		if len(parts) >= 2 {
			p := parts[len(parts)-2]
			if p != "" && !strings.Contains(p, ".") {
				return p
			}
		}
	}

	return ""
}

func parseLimit(query string, path string) string {
	v, _ := url.ParseQuery(query)
	if l := v.Get("limit"); l != "" {
		return l
	}
	// Check path for slice notation [offset:limit]
	if limit, _ := parseSlice(path); limit != "" {
		return limit
	}
	return ""
}

func parseOffset(query string, path string) string {
	v, _ := url.ParseQuery(query)
	if o := v.Get("offset"); o != "" {
		return o
	}
	_, offset := parseSlice(path)
	return offset
}

func parseHaving(query string) string {
	v, _ := url.ParseQuery(query)
	return v.Get("having")
}

func parseOrderBy(columnPath string, query string) (string, string) {
	v, _ := url.ParseQuery(query)
	if ob := v.Get("orderby"); ob != "" {
		return ob, ""
	}

	// check path parts
	parts := strings.Split(columnPath, "/")
	for _, part := range parts {
		cols := strings.Split(part, ",")
		for _, col := range cols {
			col = strings.TrimSpace(col)
			if strings.HasPrefix(col, ASC) {
				return strings.TrimPrefix(col, ASC), "ASC"
			}
			if strings.HasPrefix(col, DESC) {
				return strings.TrimPrefix(col, DESC), "DESC"
			}
		}
	}
	return "", ""
}

func parseSlice(pathStr string) (string, string) {
	if !strings.HasSuffix(pathStr, "]") {
		return "", ""
	}
	idx := strings.LastIndex(pathStr, "[")
	if idx == -1 {
		return "", ""
	}
	content := pathStr[idx+1 : len(pathStr)-1]
	parts := strings.Split(content, ":")
	if len(parts) != 2 {
		return "", ""
	}

	startStr := strings.TrimSpace(parts[0])
	endStr := strings.TrimSpace(parts[1])

	start := 0
	end := 0
	hasLimit := false

	if startStr != "" {
		s, err := strconv.Atoi(startStr)
		if err != nil {
			return "", ""
		}
		start = s
	}

	if endStr != "" {
		e, err := strconv.Atoi(endStr)
		if err != nil {
			return "", ""
		}
		end = e
		hasLimit = true
	}

	offset := start
	limit := ""

	if hasLimit {
		l := end - start
		if l < 0 {
			l = 0
		}
		limit = strconv.Itoa(l)
	}

	return limit, strconv.Itoa(offset)
}
