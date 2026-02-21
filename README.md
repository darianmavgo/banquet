# Banquet

Banquet is a URL parsing library designed to standardize the way tabular data is queried over HTTP. It provides a superset of `net/url` features, enabling SQL-like operations (selection, filtering, sorting, pagination) directly within the URL structure.

## Features Supported

*   **Standardized URL Parsing**: Converts raw URLs into structured `Banquet` objects containing dataset paths, tables, columns, and query clauses.
*   **Dual-Mode Parsing**: Supports both explicit, delimiter-based paths and heuristic, "familiar" paths.
*   **SQL Clause Extraction**: Automatically parses `SELECT`, `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `GROUP BY`, and `HAVING` from both path segments and query parameters.
*   **Nested URL Support**: Can parse URLs embedded within other URLs (common in proxying or gateway scenarios).
*   **Robust Normalization**: Handles scheme cleanup (e.g., fixing `gs:/` to `gs://`) and path sanitization.

## Area of Responsibility

Banquet's sole responsibility is **parsing and interpretation**. It defines the "grammar" of the data ecosystem. It takes a raw string (the URL) and produces a structured, semantic representation of the user's intent (the Query). It acts as the bridge between a user-facing string and the backend execution engine.

## Scope (What it explicitly doesn't do)

*   **No Execution**: Banquet does not fetch data, open files, or execute SQL queries. It only describes *what* should be fetched.
*   **No Storage**: It has no concept of where data lives or how it is stored, beyond path string manipulation.
*   **No Authentication**: It does not handle user identity or permissions.

## Banquet Notation

Banquet supports a flexible syntax that ranges from explicit to inferred, designed to be both machine-precise and human-readable.

### 1. Explicit Banquet Notation
For unambiguous parsing, Banquet uses semicolon (`;`) delimiters to strictly separate the setup components.
*   **Format**: `path/to/dataset;Table;Column`
*   **Example**: `data/sales.sqlite;orders;amount`
*   This explicitly tells the parser: "Dataset is `data/sales.sqlite`, Table is `orders`, Component is `amount`".

### 2. Familiar Syntax
For ease of use, Banquet supports a standard slash-delimited syntax that mimics file system paths or standard REST URLs.
*   **Format**: `path/to/dataset/table/column`
*   **Example**: `data/sales.csv/amount`
*   Banquet uses heuristics (checking for file extensions like `.csv`, `.sqlite`, `.db`) to guess where the dataset path ends and the query begins.

### 3. Inferred Defaults
Banquet strives to "do what you mean":
*   **Select All**: If the URL points to a table but specifies no columns (or effectively selects the table name itself), Banquet infers `SELECT *`.
*   **Table Guessing**: In simple one-table formats (like CSV), functionality allows omitting the table name, treating the file as the table.

### 4. Syntax Sugar for Slice Notation
Banquet supports Python-like slice notation in the path to handle pagination (`LIMIT` and `OFFSET`).
*   **Syntax**: `[start:end]`
*   **Behavior**:
    *   `start` becomes `OFFSET`.
    *   `end - start` becomes `LIMIT`.
*   **Example**: `/data/users[10:20]`
    *   Parses to: `OFFSET 10`, `LIMIT 10`.

### 5. Sort
Sort order can be defined directly in the path using prefix modifiers on column names.
*   **Ascending**: `+` prefix. Example: `/data/users/+lastname` (Sort by lastname ASC).
*   **Descending**: `-` prefix. Example: `/data/users/-age` (Sort by age DESC).
*   *Note: This can also be handled via the `orderby` query parameter.*

### 6. Equality & Filtering
Simple equality checks can be embedded directly in the path segments alongside columns.
*   **Syntax**: `Column!=Value`
*   **Example**: `/data/users/status!=active`
*   **Behavior**: This is parsed into the `WHERE` clause.
*   Complex filters are supported via the standard `where` query parameter (e.g., `?where=age>21`).

## Flutter Go Bridge Integration (Manual CGO)

We use a manual CGO approach to expose Banquet's parsing logic to Flutter via `dart:ffi`.

### 1. Bridge Implementation
- **Go Side**: `cmd/libbanquet/main.go` exports a C-compatible function `BanquetParse`.
  - It takes a C string (URL).
  - It returns a JSON string (BanquetDTO or error).
  - It manages memory with `FreeString`.

### 2. Building Shared Library
To build the shared library for macOS:
```bash
go build -buildmode=c-shared -o ../sqliter/macos/Frameworks/libbanquet.dylib ./cmd/libbanquet/main.go
```

### 3. Dart/Flutter Integration
- **Dart Side**: `sqliter/lib/bridge/banquet_bridge.dart` uses `dart:ffi` to load `libbanquet.dylib`.
- **API**: `BanquetBridge.parse(String url)` returns a `Future<Map<String, dynamic>>`.

