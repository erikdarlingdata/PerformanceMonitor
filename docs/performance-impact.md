# Performance Impact

Part of the [Performance Monitor documentation](../README.md#documentation).

### On Monitored Servers

- All queries that run on a SQL Server target use `READ UNCOMMITTED` isolation
- Configurable collection intervals
- Lite: max 7 concurrent SQL connections, 30-second command timeout
- Darling: the service collects on the shared default cadences. TimescaleDB compresses the store

### Local Resources (Lite)

- DuckDB: ~50–200 MB per server per week
- Parquet archives: ~10x compression with ZSTD
- Charts do not use the GPU. Lite sets WPF to software rendering for the whole window, and ScottPlot draws each chart on the CPU through SkiaSharp. The Darling viewer does the same.
