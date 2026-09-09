# Banquet Table List Style

**Status: recommended (host / renderer convention).**

This standard controls what displays when listing all tables within a specific database.

1. **Table Rendering**: The list of tables MUST be displayed as a table itself, reusing the standard [Banquet Grid Style](banquet-grid-style.md) already defined.
2. **Tiny Tables Definition**: A "tiny table" is defined as a table containing **under 20 rows**.
3. **Table List Rendering**: The list of tables MUST NOT be a single meta-grid (e.g. `TABLE NAME | PREVIEW`). Instead, it MUST render a sequence of independent, distinct data grids for each table in the database, stacked vertically like HTML tables.
4. **Row Expansion (Union All Behavior)**: Each table's grid MUST display the actual contents of the table, limited to the first 20 rows (e.g. `SELECT * FROM table LIMIT 20`).
5. **Sorting**: The sequence of tables MUST be ordered by category: Tiny (1-19 rows) first, then Large (20+ rows), then Empty (0 rows). Within categories, sort by row count descending.
6. **Empty Tables**: Empty tables MUST output exactly 1 row with a placeholder (e.g., `(Empty)`).
7. **Headers**: Each grid MUST be preceded by a distinct header indicating the table name, row count, and column count (e.g., `=== Table: steps (3152 rows, 11 columns) ===`).
