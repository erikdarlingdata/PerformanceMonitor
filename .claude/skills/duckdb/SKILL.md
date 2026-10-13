---
name: duckdb
description: Query the Lite app's DuckDB database safely while the collector is running
argument-hint: [SQL query or description of what to look up]
disable-model-invocation: false
---

# Query Lite's DuckDB Database

Safely query the PerformanceMonitor Lite DuckDB database using Python. This skill exists because DuckDB only allows one read-write connection at a time. When Lite is running (collecting data), it holds the write connection, so an external reader connects in read-only mode.

## Database Location

The store lives at:
```
%LOCALAPPDATA%\PerformanceMonitorLite-Data\monitor.duckdb
```
Lite 3.3.0 and earlier kept it at the path below, and the file moves to the path above only when a newer build first runs. Check the path above first, and fall back to this one when it does not exist:
```
%LOCALAPPDATA%\PerformanceMonitorLite\monitor.duckdb
```

## How to Query

Use Python with the `duckdb` module, always with this pattern:

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

## Rules

1. **Pass `read_only=True` to `duckdb.connect()`.** The default is read-write. Lite's write lock blocks a read-write connection, and it hangs or fails.
2. **Use forward slashes** in the path (Python on Windows handles this fine)
3. **Close the connection** when done, so no read lock is left dangling
4. **Use Python** with its `duckdb` module

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

