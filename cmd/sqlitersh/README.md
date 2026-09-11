# Sqlitersh (Banquet Interactive Shell)

`sqlitersh` is an interactive command-line shell (REPL) built on top of the [Banquet](../../README.md) library. While `sqliter` functions as a one-shot `curl`-like tool for datasets, `sqlitersh` provides an interactive environment where you can retain context, navigate paths, and continuously query tabular data without re-typing long URLs.

## Installation

```bash
go build -o sqlitersh ./cmd/sqlitersh
```

## Usage

Start the shell with an optional starting context (defaults to `file://.`):

```bash
./sqlitersh [initial_context_url]
```

Example:

```bash
$ ./sqlitersh file:///home/user/data/sales.sqlite
Sqlitersh - The Banquet Interactive Shell
Type 'help' for more information.
sales.sqlite> 
```

### REPL Commands

- `help`: Show the help message.
- `exit` or `quit`: Exit the interactive shell.
- `clear`: Clear the screen.
- `cd <path>`: Change the current context. You can use absolute `file://` URLs, absolute file paths, or relative paths. Using `cd ..` navigates up one directory/context level.
- `pwd`: Show the current context URL.
- `.`: Evaluate the current context directly (e.g., list tables in a database, or list files in a collection).

### Querying

Any input that is not a built-in command is treated as a Banquet query relative to the current context. 

For example, if your context is `file:///home/user/data/sales.sqlite`, typing `users` will append `users` to the context and execute the query as `file:///home/user/data/sales.sqlite/users`. 

```bash
sales.sqlite> users
  ID |   NAME   
-----+----------
   1 | Alice    
   2 | Bob      
```

You can also use Banquet syntax features directly in the shell:
```bash
sales.sqlite> orders/status='shipped'[-amount][0:5]
```

### Navigation Example

```bash
.> cd testdb.sqlite
testdb.sqlite> .
  TABLE NAME | ROW COUNT |               PREVIEW                
-------------+-----------+--------------------------------------
  users      | 2         | [1, Alice] | [2, Bob]
testdb.sqlite> users
  ID | NAME  
-----+-------
   1 | Alice 
   2 | Bob   
testdb.sqlite> cd ..
.> 
```
