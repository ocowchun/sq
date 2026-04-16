# sq

`sq` is a SQL-first tool for exploring objects in Amazon S3.

The problem it is trying to solve is simple: S3 is easy to store data in, but awkward to
inspect at speed. When you need to answer questions like "what large files are under this
prefix?" or "which keys match this pattern?", `aws s3 ls` and shell pipelines get tedious
fast. `sq` aims to make that workflow feel more like querying a table.

## Why `sq`

The intended use case is day-to-day S3 exploration:

- inspect objects in a bucket without switching mental models
- filter by bucket, prefix, size, or simple string patterns
- reshape output with projections and aliases

## CLI Usage

Install the CLI:

```sh
go install github.com/ocowchun/sq/cmd/sq@latest
```

Execute a query:

```sh
sq -e 'select key, size from objects where bucket_name = "my-bucket" and key like "logs/%"'
```

Filter to a prefix:

```sh
sq -e 'select key, size from objects where bucket_name = "my-bucket" and key like "logs/%"'
```

Use a CTE:

```sh
sq -e '
with log_files as (
  select key, size
  from objects
  where bucket_name = "my-bucket" and key like "logs/%"
)
select replace(key, "logs/", "") as short_key, size
from log_files
'
```

The `sq` file also contains an interactive shell path with these behaviors:

- statements are submitted when they end with `;`
- dot commands begin with `.`
- `.mode table`, `.mode line`, and `.mode csv` switch output formatting

## What Works Today

The current codebase is best understood as an in-progress query engine for S3 object listing.

The engine currently has a built-in `objects` table with this schema:

| column | type |
| --- | --- |
| `key` | `string` |
| `bucket_name` | `string` |
| `size` | `int` |

Under the hood, the S3 scan uses AWS SDK default configuration loading, so normal AWS
environment variables, shared config files, and standard credential resolution apply.

### SQL Features Verified In The Current Code

The following features are present in parser/planner/tests today:

- `select ... from ...`
- column aliases with `as`
- `where`
- `=`, `!=`, `>`, `>=`, `<`, `<=`
- `and`, `or`
- `like`
- `is null`
- `in`
- `with` common table expressions
- joins in the parser and logical planner
- scalar text functions:
  - `split_part(string, separator, index)`
  - `replace(string, source, target)`

Some of these features are farther along than others. The README examples stay close to the
S3 object workflow that is already visible in the implementation.


## How It Works

At a high level, the flow is:

1. parse SQL into an AST
2. bind names and types against the catalog
3. build a logical plan
4. run logical optimization
5. build a physical plan
6. execute against Arrow record batches
7. fetch S3 objects through the AWS SDK when scanning the `objects` table

Key packages:

- `parser`: lexer and parser
- `logical`: binding, logical plan construction, optimization
- `physical`: execution operators and S3 scan
- `queryexec`: high-level query entry point
- `function`: built-in scalar functions
- `catalog`: table metadata
- `cmd/sq`: current CLI entry point

## Project Status
This is still early-stage software. The important distinction is:

- the query engine direction is real and already implemented in pieces
- the CLI entry point exists, but the overall workflow is still early and evolving

