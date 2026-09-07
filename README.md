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

## Out of Scope (What it explicitly doesn't do)

*   **No Execution**: Banquet does not fetch data, open files, or execute SQL queries. It only describes *what* should be fetched.
*   **No Storage**: It has no concept of where data lives or how it is stored, beyond path string manipulation.
*   **No Authentication**: It does not handle user identity or permissions.

## Standards & Conventions

Banquet's URL structure and client rendering behaviors are defined by a set of formal standards located in the [`docs/`](docs/) directory:

*   **[Banquet URL Syntax](docs/banquet-url-syntax.md)**: The core grammar for encoding dataset paths, table names, queries, slices, sorting, and filtering into a single URL.
*   **[Collections & Database List Style](docs/banquet-db-list-style.md)**: The standard for mapping folders into queryable catalogs, and UI conventions for displaying the resulting lists of databases.
*   **[Table List Style](docs/banquet-table-list-style.md)**: Conventions for listing available tables within a dataset.
*   **[Query Style](docs/banquet-query-style.md)**: Recommended conventions for displaying query inputs and execution parameters.
*   **[Grid Style](docs/banquet-grid-style.md)**: Standards for rendering tabular data grids, including alignment and pagination.

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

