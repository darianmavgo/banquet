# Rendering Style

**Status: recommended (renderer convention).** How a result is *drawn* once
[Banquet Query Style](query-style.md) has chosen the columns.

These are guidance for the grid renderer, independent of the parser. The
parenthetical notes reflect the reference implementation (`sqlite.mavgo.com`).

1. Truncate or soft-wrap free-form text longer than ~120 characters so the grid
   stays scannable.
2. Format values consistently: dates readable, numbers thousands-separated,
   booleans as Yes / No.
3. When a grouping column is in play (Query Style rule 4), give the group-header
   cell light visual separation — a background tint and an accent left border.
   Blank each repeated dimension prefix so the parent value reads as a section
   header rather than a denormalised cell; a header-click sort turns the
   blanking and the tint off. *(implemented in the reference host)*
4. On zero rows, show a "No results found" message rather than a bare empty grid.

The SQL behind whatever the grid is showing SHOULD be visible to the user — the
reference host mirrors it into the toolbar, one line, click-to-copy.
