# Banquet Standards

Banquet's core library (`banquet.go`, `dart/lib/banquet.dart`) is **parse-only** —
it turns a URL into a structured query and nothing more. These documents cover
the wider conventions that make a Banquet-speaking data browser feel consistent.

They come in two tiers.

## Normative — the URL grammar

The parser implements these; a conforming implementation must agree.

- **[collections.md](collections.md)** — a path with no dataset extension names a
  *container*; how the parser marks it and the shape of the catalog a host
  returns for it.

## Conventions — host & renderer guidance

Guidance for the layer above the parser — the thing that actually opens datasets
and draws tables. Not implemented by the library; recommended for any host that
wants results to read the way Banquet users expect.

- **[banquet-query-style.md](banquet-query-style.md)** — which columns a result shows, and in
  what order ("trim" view).
- **[banquet-grid-style.md](banquet-grid-style.md)** — how the chosen columns and rows
  are drawn.
- **[banquet-db-list-style.md](banquet-db-list-style.md)** — how a datastore or folder containing sqlite files is displayed.
- **[banquet-table-list-style.md](banquet-table-list-style.md)** — how a list of tables within a database is displayed.

## Reference implementation

`sqlite.mavgo.com` (the `sqldoc` front end) implements all three. Where a rule
below cites code, it points there.
