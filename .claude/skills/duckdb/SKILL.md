---
name: duckdb
description: Query the Lite app's DuckDB database safely while the collector is running
argument-hint: [SQL query or description of what to look up]
disable-model-invocation: false
---

# Query Lite's DuckDB Database

Safely query the PerformanceMonitor Lite DuckDB database using Python. This skill exists because DuckDB only allows one read-write connection at a time. When Lite is running (collecting data), it holds the write connection. External readers MUST connect in read-only mode.

## Database Location

Since the #1832 fix (data moved OUT of Velopack's install root, which Setup.exe deletes), the store lives at:
```
%LOCALAPPDATA%\PerformanceMonitorLite-Data\monitor.duckdb
```
Builds from BEFORE that fix (3.3.0 and earlier) keep it at the old path, and the file only moves when a fixed build first runs:
```
%LOCALAPPDATA%\PerformanceMonitorLite\monitor.duckdb
```
**Check the -Data path first; fall back to the old path if it does not exist.**

## How to Query (ALWAYS use this pattern)

Use Python with the `duckdb` module. **ALWAYS connect in read-only mode.**

```python
python -c "
import duckdb, os
la = os.environ['LOCALAPPDATA']
p = la + '/PerformanceMonitorLite-Data/monitor.duckdb'
if not os.path.exists(p):
    p = la + '/PerformanceMonitorLite/monitor.duckdb'
con = duckdb.connect(p, read_only=True)
result = con.execute('YOUR SQL HERE').fetchall()
for row in result:
    print(row)
con.close()
"
```

## CRITICAL RULES

1. **ALWAYS use `read_only=True`** -- without this, the connection will be blocked by Lite's write lock and hang or fail
2. **NEVER use `duckdb.connect()` without `read_only=True`** -- the default is read-write which WILL conflict with the running app
3. **Use forward slashes** in the path (Python on Windows handles this fine)
4. **Close the connection** when done -- don't leave read locks dangling
5. **Use Python** -- duckdb 1.4.4 is installed

## Common Queries

List all tables:
```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name;
```

List columns for a table:
```sql
SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'TABLE_NAME' ORDER BY ordinal_position;
```

List all views:
```sql
SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW' ORDER BY table_name;
```

Row counts:
```sql
SELECT table_name, estimated_size FROM duckdb_tables() ORDER BY estimated_size DESC;
```

## DuckDB vs SQL Server Syntax Notes

- String concatenation: `||` (not `+`)
- ILIKE for case-insensitive LIKE
- `EPOCH_MS(timestamp_col)` to convert to epoch
- `strftime('%Y-%m-%d %H:%M:%S', ts)` for formatting
- No `TOP N` -- use `LIMIT N` instead
- `EXCLUDE` clause: `SELECT * EXCLUDE (col1, col2) FROM table`

