# Banquet Grid Style

**Status: recommended (renderer convention).** How a result is *drawn* once
[Banquet Query Style](banquet-query-style.md) has chosen the columns.

These are guidance for the grid renderer, independent of the parser. The
parenthetical notes reflect the reference implementation (`sqlite.mavgo.com`).

1. **Max Cell Size & Wrapping**: For terminal/text renderers, rows MUST be constrained to exactly 1 line in height. Truncate free-form text longer than ~120 characters with an ellipsis (`...`). Strip or replace any literal newline (`\n`) or carriage return (`\r`) characters with spaces to prevent cells from spanning multiple lines and breaking vertical alignment.
2. **Binary Data (BLOBs)**: Raw binary fields MUST NOT be printed raw. They contain unprintable control characters that destroy terminal formatting. Instead, they MUST be replaced with a metadata placeholder (e.g., `<BLOB: 1059 bytes>`).
3. **Value Translation (Original vs Translated)**: Format values consistently for readability by default (e.g., epoch timestamps translated to readable dates, booleans translated to Yes/No, or foreign keys translated to a display name if joined). The original raw value should remain accessible, either via a hover tooltip or by toggling a "raw" view. Numbers should be thousands-separated.
3. When a grouping column is in play (Query Style rule 4), give the group-header
   cell light visual separation — a background tint and an accent left border.
   Blank each repeated dimension prefix so the parent value reads as a section
   header rather than a denormalised cell; a header-click sort turns the
   blanking and the tint off. *(implemented in the reference host)*
4. On zero rows, show a "No results found" message rather than a bare empty grid.

The SQL behind whatever the grid is showing SHOULD be visible to the user — the
reference host mirrors it into the toolbar, one line, click-to-copy.
