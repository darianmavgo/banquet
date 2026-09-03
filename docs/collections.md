# Banquet Collections

**Status: normative.** The parser recognises a collection path and marks it
`IsCollection`; a host that speaks Banquet answers such a request with a
synthetic catalog of the datasets under the container. Reference implementation:
`sqlite.mavgo.com` (`sql-engine.js`, `banquet.js`).

## Grammar

Banquet's dataset invariant: **the dataset path ends at the segment carrying a
recognized file extension** (`.db` `.sqlite` `.csv` `.xlsx` `.json` `.zip` `.txt`
`.html`).

The **collection request** is the corollary:

> A Banquet path whose dataset portion carries **no recognized extension** names
> a *container*, not a single dataset. The host responds with a synthetic
> **catalog** describing the datasets reachable under it — recursively: a folder
> lists the databases in it and its subfolders.

`ParseBanquet` sets `IsCollection = true` and leaves the container path in
`DataSetPath`. The container runs up to the first segment that is a **reserved
catalog table** (`databases`, `tables`) or is **clause-like** (contains `,`,
starts with `+` / `-`, contains `!=`, or is a `[a:b]` slice); everything after is
parsed as the ordinary table + sort + slice + filter grammar, applied to the
catalog.

```
/                                              root container — every reachable dataset
/d1                                            every D1 database
/local/Documents/Income                        container (host opens it on `databases`)
/local/Documents/Income/                        trailing slash optional
/local/Documents/Income/-size_bytes            catalog sorted by size, descending
/local/Documents/Income/databases/-size_bytes  same, with the catalog table named
/local/Documents/Income/tables/+database        the `tables` catalog, sorted
/my-bucket/some/prefix                          objects under an object-store prefix
```

A semicolon-delimited path (`dataset;table;column`) is always an explicit
dataset, never a collection.

`?depth=N` bounds recursion (`1` = the container's immediate children; the
default and ceiling are host-defined — the reference host defaults to 5 and caps
at 12). Hosts SHOULD skip VCS / cache / package directories.

A host that reflects grid state back into the address bar SHOULD emit a trailing
slash for the bare container and omit the default `databases` table unless a sort
follows.

## Response contract — the catalog

**Status: recommended.** A host that lists a datastore SHOULD return a read-only
synthetic dataset shaped like this, so a Banquet client can treat it like any
other database:

| table | one row per | columns (minimum) |
|---|---|---|
| `databases` | dataset under the container | `name`, `location`, `key`, `table_count`, `size`, `size_bytes`, `modified` (ISO-8601), `status`, **`open`** |
| `tables` | (dataset, table) pair | `database`, `table`, **`open`** |
| `_style` | — | column-order hints (see [query-style.md](query-style.md)); `title` = the container's last segment |

`open` is a Banquet path that opens that dataset (or dataset + table). The
catalog opens in **trim** kind. An empty container yields an empty catalog
(0 rows), not an error.

Because the catalog is itself a real (in-memory) SQLite database, every Banquet
clause — sort, slice, `where`, `select` — applies to it unchanged. `/` is just
the empty-container case of the same rule.

## Host responsibilities

The library does not fetch anything. A host implementing collections must:

- Enumerate datasets under the container, recursively, honouring `?depth=`. The
  reference host scans the local filesystem under `~/<prefix>`, lists an object
  store with a prefix query, and enumerates D1 databases at the root.
- Keep dataset keys relative to the store root so `open` paths resolve.
- Reject a `prefix` that escapes the store root.
