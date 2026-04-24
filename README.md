# sq

`sq` is a CLI for exploring Amazon S3 objects with SQL. It is still early-stage software, so
the interface and supported queries may change, but it is already useful for everyday S3
inspection.

Instead of stitching together `aws s3 ls`, `grep`, and shell pipelines, you can query S3
objects as rows in a table. That makes it easier to answer questions like:

- what objects exist under a prefix?
- which files are larger than a threshold?
- how can I reshape keys before exporting results?

## Install

```sh
go install github.com/ocowchun/sq/cmd/sq@latest
```

## Getting Started

`sq` uses the normal AWS SDK credential and config loading flow. That means it works with the
same environment variables, shared config files, and credential files you already use with the
AWS CLI.

Run a query by passing the SQL as a single argument:

```sh
sq 'select key, size from objects where bucket_name = "my-bucket"'
```

Use a specific AWS profile when needed:

```sh
sq -profile my-profile 'select key, size from objects where bucket_name = "my-bucket"'
```

## Example Queries

List objects in a bucket:

```sh
sq 'select key, size from objects where bucket_name = "my-bucket"'
```

Filter to a prefix:

```sh
sq 'select key, size from objects where bucket_name = "my-bucket" and key like "logs/%"'
```

Find larger objects:

```sh
sq 'select key, size from objects where bucket_name = "my-bucket" and size > 1048576'
```

Rewrite part of a key in the output:

```sh
sq '
select replace(key, "logs/", "") as short_key, size
from objects
where bucket_name = "my-bucket" and key like "logs/%"
'
```

Normalize key casing and inspect key length:

```sh
sq '
select lower(key) as lower_key, upper(key) as upper_key, length(key) as key_len
from objects
where bucket_name = "my-bucket"
'
```

Use a CTE:

```sh
sq '
with log_files as (
  select key, size
  from objects
  where bucket_name = "my-bucket" and key like "logs/%"
)
select key, size
from log_files
where size > 1048576
'
```

## The `objects` Table

Queries currently read from a built-in table named `objects`.

| column | type | meaning |
| --- | --- | --- |
| `key` | `string` | S3 object key |
| `bucket_name` | `string` | S3 bucket name |
| `size` | `int` | object size in bytes |

## Supported SQL Features

These are the SQL features you can use in queries today:

- `select ... from ...`
- column aliases with `as`
- `where`
- comparison operators: `=`, `!=`, `>`, `>=`, `<`, `<=`
- boolean operators: `and`, `or`
- `like`
- `is null`
- `in`
- `with` common table expressions
- joins: `inner join` and `left join`
- scalar text functions:
  - `length(string)`
  - `lower(string)`
  - `upper(string)`
  - `split_part(string, separator, index)`
  - `replace(string, source, target)`

## Interactive Mode

Run `sq` with no SQL argument to start the interactive shell:

```sh
sq
```

In interactive mode:

- submit a statement by ending it with `;`
- switch output format with `.mode table`, `.mode line`, or `.mode csv`
