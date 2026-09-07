# Banquet Table List Style

**Status: recommended (host / renderer convention).**

This standard controls what displays when listing all tables within a specific database.

1. **Table Rendering**: The list of tables MUST be displayed as a table itself, reusing the standard [Banquet Grid Style](banquet-grid-style.md) already defined.
2. **Tiny Tables Definition**: A "tiny table" is defined as a table containing **under 20 rows**.
3. **Displaying Tiny Tables**: For all tiny tables, the UI should automatically display the *contents* of the table inline or expanded within the list, rather than just showing the table name.
4. **Displaying Normal Tables**: For tables that are not tiny (20 or more rows), the list should display the table name, along with metadata such as the table size (row count or bytes) and column count. It should not automatically expand to show the contents.
5. **Columns in List**: The table displaying the list of tables should include standard columns such as `table_name`, `row_count`, `size`, and optionally an inline preview area for the contents of tiny tables.
