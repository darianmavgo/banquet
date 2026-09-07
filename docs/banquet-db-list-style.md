# Banquet DB List Style

**Status: recommended (host / renderer convention).**

This standard controls the default display when a user requests a datastore or a folder that contains one or more `.sqlite` (or compatible database) files within its subfolders.

1. **Table Rendering**: The list of discovered databases MUST be displayed as a table, reusing the standard [Banquet Grid Style](banquet-grid-style.md) already defined.
2. **Path Representation**: The table should include the relative path and name of each discovered database file as columns (e.g., `path`, `filename`).
3. **Metadata Columns**: The table should display file metadata such as size, last modified date, and access status.
4. **Interactivity**: Each row in the database list table should be navigable, allowing the user to click into a database to view its Table List.
