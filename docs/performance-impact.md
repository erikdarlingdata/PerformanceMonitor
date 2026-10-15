# Performance Impact

Part of the [Performance Monitor documentation](../README.md#documentation).

### On Monitored Servers

- All queries use `READ UNCOMMITTED` isolation
- Configurable collection intervals
- Lite: max 7 concurrent SQL connections, 30-second command timeout
- Darling: the service collects on the shared default cadences; TimescaleDB compresses the store

### Local Resources (Lite)

- DuckDB: ~50–200 MB per server per week
- Parquet archives: ~10x compression with ZSTD
- ScottPlot charts use hardware-accelerated rendering
