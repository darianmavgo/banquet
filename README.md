
Banquet is a superset of net/url for querying tabular data. 
I didn't want to make a contraction from tabular data url because the obvious word play is "turtle" but I definitely don't want to imply slowness of any kind. 

So banquet because is the moniker because banquets have tables everywhere with abundance.

### Overview

`banquet` provides standardized URL parsing for tabular data resources, encapsulating common query patterns like filtering, selecting, sorting, and pagination directly within the URL structure. It supports both path-based and query-parameter-based directives.

### Capabilities

#### Parsing
- **`ParseBanquet(rawurl string)`**: The core factory function that parses a raw URL string into a `Banquet` struct.
- **`ParseNested(rawurl string)`**: Handles nested URLs (e.g., specific protocols like `gs://` embedded within an HTTP request path).
- **`ParseNestedBanquet(r *http.Request)`**: Convenience wrapper for parsing URLs directly from an `http.Request`.

#### URL Standardization
- **Scheme Normalization**: Automatically corrects malformed schemes like `gs:/` to `gs://`.
- **Path Cleaning**: Trims leading slashes from nested paths.

#### Field Extraction
`Banquet` extracts the following standard fields from URLs:

*   **`Table`**: Heuristically identified from the path (e.g., file name like `data.csv` or specific path segments).
*   **`Select`**: Columns specified in the path (e.g., `/data.csv/col1,col2`) or via generic file extension detection.
*   **`Where`**: Extracted from the `where` query parameter.
*   **`Sort`**: Identified by `^` (Ascending) or `!^` (Descending) prefixes in the path (e.g., `/^col1`) or `sort` query parameter.
*   **`Limit`**: Extracted from the `limit` query parameter.
*   **`Offset`**: Extracted from the `offset` query parameter.
*   **`GroupBy`**: Extracted from the `groupby` query parameter or `(expression)` in the path.
*   **`Having`**: Extracted from the `having` query parameter.
*   **`OrderBy`**: Extracted from the `orderby` query parameter.

### Limitations

*   **Heuristic Table Parsing**: The `parseTable` logic relies on simplified heuristics (like file extensions `.csv`, `.sqlite`, `.db`) or path position. It currently defaults to "sqlite_master" or "tb0" in specific scenarios and may require robustification for complex schemas.
*   **Slice Notation**: While planned (e.g., `[2:30]`), slice notation parsing from the path is **not yet implemented**.
*   **Select Parsing**: Relies on `path.Ext` to detect where the "file" part ends and column selection begins. Paths without clear file extensions might be ambiguous.
*   **Error Handling**: Some internal parsers (like `parseWhere`) return empty strings on failure rather than specific errors.
