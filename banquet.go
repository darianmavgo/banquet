// Package banquet provides a framework for standardizing and parsing URLs for tabular data access.
// It supports querying datasets (CSV, SQLite, etc.) via URL paths and query parameters, allowing typical SQL-like operations
// such as selection, filtering, sorting, limiting, and grouping directly from the URL.
//
// Banquet Classifications & URL Structure:
//
// 1. Flat (1-tier): path/to/dataset;;Column
// 2. Nested Table (2-tier): path/to/dataset;Table
// 3. Nested Column (2-tier): path/to/dataset;Table;Column
//
// Fallback Convention (semicolon-less):
// If no semicolons are present, the package attempts to heuristically determine the table or column based on file usage.
//
// Supported Prefixes/Suffixes:
// - Sort: +column (ASC), -column (DESC)
// - Slice: [start:end] (translated to LIMIT/OFFSET)
package banquet

import (
	"fmt"
	"log"
	"net/url"
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

// Banquet represents a parsed URL request for tabular data.
// It extends url.URL with SQL-like clauses derived from the path and query parameters.
type Banquet struct {
	*url.URL
	Where         string
	Table         string   // Table name derived from the URL path.
	Select        []string // Columns to select. Empty or ["*"] implies all columns.
	SortDirection string   // "ASC" or "DESC".
	Limit         string
	Offset        string
	GroupBy       string
	Having        string
	OrderBy       string
	DataSetPath   string // Path to the source dataset file (e.g., .csv, .sqlite).

	ColumnPath string // The remaining path segment containing columns, sort intructions, or conditions.
	// fields below are for internal use
	rawurl string
	path   string
}

const (
	// ASC is the prefix token to signal ascending sort order.
	ASC = "+"
	// DESC is the prefix token to signal descending sort order.
	DESC = "-"
)

// CleanUrl prepares a raw URL string for standard parsing.
// It trims leading slashes (unless it's the root path) and ensures standard scheme formatting.
func CleanUrl(rawurl string) string {
	// housekeeping before url.Parse
	if rawurl == "/" {
		return "."
	}
	rawurl = strings.TrimPrefix(rawurl, "/")

	// Ensure standard scheme format (e.g., gs:/ -> gs://) for proper authority parsing
	if idx := strings.Index(rawurl, ":/"); idx != -1 {
		if !strings.HasPrefix(rawurl[idx:], "://") {
			rawurl = strings.Replace(rawurl, ":/", "://", 1)
		}
	}

	return rawurl
}

// ParseBanquet parses a raw URL string into a functioning Banquet object.
// It handles cleaning, URL parsing, and decomposition into Dataset, Table, and Column path segments.
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

	b.DataSetPath, b.Table, b.ColumnPath = parseDataSetColumnPath(b.Path)
	if verbose {
		log.Printf("[BANQUET] DataSetPath: %s, Table: %q, ColumnPath: %s", b.DataSetPath, b.Table, b.ColumnPath)
	}

	// Table parsing logic - fallback to heuristic only if not explicitly set via semicolon
	if b.Table == "" {
		b.Table = parseTable(b.ColumnPath)
		if verbose {
			log.Printf("[BANQUET] Table identified via heuristic: %s", b.Table)
		}
	}

	// Populate fields using private parsers
	b.Select = ParseSelect(b.ColumnPath)
	if verbose {
		log.Printf("[BANQUET] Selected columns: %v", b.Select)
	}

	// Fix: if Select is just the Table name, change to *
	if len(b.Select) == 1 && b.Select[0] == b.Table {
		b.Select = []string{"*"}
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
  DataSetPath: %s
  ColumnPath: %s
  RawQuery: %s
  Table:      %q
`, b.rawurl, b.Scheme, b.Host, b.DataSetPath, b.ColumnPath, b.RawQuery, b.Table)
}

func FmtSprintf(b *Banquet) string {
	return fmt.Sprintf(`rawurl: %s\n
  S: %s H: %s DP: %sCP: %sRQ:%sTB:%q
`, b.rawurl, b.Scheme, b.Host, b.DataSetPath, b.ColumnPath, b.RawQuery, b.Table)
}

// ParseNested extracts and parses a Banquet URL that wraps an inner URL.
// This is common when a server receives a request like "http://localhost/gs://bucket/file...".
func ParseNested(rawURL string) (*Banquet, error) {
	// 1. Parse the outer envelope
	// If rawURL is http://localhost..., url.Parse works.
	// If rawURL is just /http..., we need to trim prefix, but not if it's just "/"
	if rawURL != "/" {
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

// parseDataSetColumnPath splits the raw path into dataset, table, and column segments.
// It supports explicit tiers separated by semicolons (dataset;table;column) or implicit tiers based on file extensions.
func parseDataSetColumnPath(rawpath string) (datasetPath string, table string, columnPath string) {
	// If rawpath contains semicolons, we use explicit tier parsing: dataset;table;columns
	if strings.Contains(rawpath, ";") {
		parts := strings.SplitN(rawpath, ";", 3)
		datasetPath = parts[0]
		if len(parts) > 1 {
			table = parts[1]
		}
		if len(parts) > 2 {
			columnPath = parts[2]
		}
		return
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
			return datasetPath, "", columnPath
		}
	}
	return rawpath, "", ""
}

// getSegments identifies the part of the path that contains columns or conditions
func getSegments(columnPath string) []string {
	parts := strings.Split(columnPath, "/")
	if len(parts) == 0 || (len(parts) == 1 && parts[0] == "") {
		return nil
	}

	// Find the first segment that contains clear column/sort/condition indicators
	firstClearSegment := -1
	for i, part := range parts {
		if strings.Contains(part, ",") ||
			strings.HasPrefix(part, ASC) ||
			strings.HasPrefix(part, DESC) ||
			strings.Contains(part, "!=") ||
			(strings.HasPrefix(part, "[") && strings.Contains(part, ":")) {
			firstClearSegment = i
			break
		}
	}

	if firstClearSegment != -1 {
		return parts[firstClearSegment:]
	}

	// If no indicators, just return the last segment (could be table or single column)
	return parts[len(parts)-1:]
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

			// If it has a sort prefix, it's for ordering, not necessarily for selection.
			// In banquet, table/+id implies SELECT * FROM table ORDER BY id ASC.
			if strings.HasPrefix(col, ASC) || strings.HasPrefix(col, DESC) {
				continue
			}

			// Clean up slice notation
			if idx := strings.Index(col, "["); idx != -1 {
				col = col[:idx]
			}

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
	if query == "" {
		return ""
	}
	// Simple extraction of 'where' parameter
	// url.ParseQuery is too strict for Banquet's "unescape tolerant" goal.
	params := strings.Split(query, "&")
	for _, p := range params {
		if strings.HasPrefix(p, "where=") {
			val := strings.TrimPrefix(p, "where=")
			// Try to unescape but if it fails, just return the raw value
			if decoded, err := url.QueryUnescape(val); err == nil {
				return decoded
			}
			return val
		}
	}
	return ""
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

// parseTable attempts to identify the table from the path.
// This is a simplified version and might need robust logic akin to core/parse.go eventually.
// parseTable parses /<table_name>/column, column
// However on sources like csv it's possible to have /column since csv 1 table.
func parseTable(columnPath string) string {
	if columnPath == "" {
		return ""
	}
	// Trim slashes and split
	parts := strings.Split(strings.Trim(columnPath, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return ""
	}

	first := parts[0]

	// Indicators that this segment is a selector (column list, sort, or filter) and NOT a table name.
	// Comparison operators, commas, and sort prefixes are clear structural clues.
	if strings.Contains(first, ",") ||
		strings.HasPrefix(first, ASC) ||
		strings.HasPrefix(first, DESC) ||
		strings.Contains(first, "!=") ||
		strings.Contains(first, "=") ||
		strings.Contains(first, ">") ||
		strings.Contains(first, "<") ||
		(strings.HasPrefix(first, "[") && strings.Contains(first, ":")) {
		return ""
	}

	// Heuristic: If it's not a selector, it's likely a table or resource name.
	// This works for SQLite (db.sqlite/users) and handles CSV (file.csv/col1,col2) correctly.
	// For ambiguous single segments like /column, it will tentatively return it as a table;
	// downstream logic in ParseBanquet handles the table/column overlap.
	return first
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
			// Strip slice notation
			if idx := strings.Index(col, "["); idx != -1 {
				col = col[:idx]
			}
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
