# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

## Pinned S3 engine cache

The proxy and connection-lifetime tests use a loopback S3 fixture. Run them with
Python 3.11 or newer, a DuckDB package matching the built extension, and the
`httpfs` extension installed.

```sh
DUCKDB_DELTA_EXTENSION=/path/to/delta.duckdb_extension python3 test/python/test_pinned_s3_engine_cache.py
```
