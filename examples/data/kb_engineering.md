# Engineering Knowledge Base

## Data Pipeline Best Practices

When designing data pipelines, the most important principle is incremental processing.
Recomputing the entire dataset on every run is wasteful and slow. Track checksums or
hashes of each input file and only re-process files that have actually changed.
This is sometimes called change data capture or delta processing.

Use pandas for small-to-medium datasets that fit in memory. For larger datasets,
consider Spark or DuckDB which can process data larger than RAM. Pandas DataFrames
are convenient because they integrate well with SQLite via the `to_sql` and
`read_sql_query` methods.

## SQLite for Production

SQLite is underrated for production workloads. A single-writer SQLite database in
WAL mode can handle thousands of reads per second and hundreds of writes per second.
The FTS5 extension provides full-text search with BM25 ranking out of the box.
Many production systems use SQLite for caching, queues, and local search indexes.

Key SQLite settings for production:
- Enable WAL mode: `PRAGMA journal_mode=WAL`
- Use synchronous=NORMAL for speed: `PRAGMA synchronous=NORMAL`
- Set a reasonable cache size: `PRAGMA cache_size=-65536` (64MB)
- Enable foreign keys: `PRAGMA foreign_keys=ON`

## Python Packaging

Modern Python projects should use `pyproject.toml` instead of `setup.py`.
The `[project]` table replaces `setup()` calls. Use `pip install -e .` for
editable installs during development. Always pin your dependencies in a
`requirements.lock` or use a tool like `pip-compile` for reproducible builds.

## Monitoring and Observability

Every data pipeline should emit structured logs. Use Python's built-in `logging`
module with a JSON formatter for production. Track metrics like:
- Files processed per run
- Rows indexed per file
- Error rate by file type
- Duration per pipeline stage
- Delta size (how much actually changed vs total)

## Error Handling

Data pipelines should be resilient. If one file fails to parse, log the error
and continue with the rest. Never let a single bad file stop the entire pipeline.
Use try/except at the file level, not the pipeline level. Write failed files to
a dead-letter table so they can be reviewed and reprocessed.
