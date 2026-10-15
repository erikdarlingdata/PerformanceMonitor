# Troubleshooting

Part of the [Performance Monitor documentation](../README.md#documentation).

### Lite

Application logs are written to the `logs/` folder. Collection success/failure is also logged to the `collection_log` table in DuckDB.

Verbosity is the `log_minimum_level` key in `settings.json` (no UI — see `Lite/config/settings.sample.json`), one of `Trace`, `Debug`, `Information`, `Warning`, `Error`, `Critical` or `None`. It defaults to `Information` and is read at startup, so a change needs a restart.

Set it to `Debug` when you need the per-database collection timing lines — `[server] collector [db] => N rows (sql:Nms, duckdb:Nms)`, one per database per collector per cycle. They are below the default because at that rate they bury the collection failures the log is usually opened for; the aggregate they decompose is in `collection_log`'s `duration_ms`, which is recorded at every level.

Common issues:

1. **No data after connecting** — Wait for the first collection cycle (1–5 minutes). Check logs for connection errors.
2. **Query Store tab empty** — Query Store must be enabled on the target database (`ALTER DATABASE [YourDB] SET QUERY_STORE = ON`).
3. **Blocked process reports empty** — Lite attempts to auto-configure the blocked process threshold to 5 seconds via `sp_configure`. On **AWS RDS**, `sp_configure` is not available — set `blocked process threshold (s)` through an RDS Parameter Group (see [Platform Notes](platform-notes.md#platform-notes) below). On **Azure SQL Database**, the threshold is fixed at 20 seconds and cannot be changed. If you still see no data on other platforms, verify the login has `ALTER SETTINGS` permission.
4. **Connection failures** — Verify network connectivity, firewall rules, and that the login has the required [permissions](permissions.md#permissions). For Azure SQL Database, use a contained database user with `VIEW DATABASE STATE`. Error 40615 means the server firewall did not allow the client's IP address. Add a server-level rule, or a database-level rule in master.
5. **FinOps Index Analysis hangs, times out, or returns `Msg 229` on `sql_expression_dependencies`** — see [FinOps Index Analysis](permissions.md#finops-index-analysis-per-database-grants) below for the full per-database grant set that fixes both failure modes.

### Darling

For a bug report, run the service with `--diagnostics-bundle <path>`. It writes one JSON file with the store's state, collection health, slow reads, statement history and the recent service log. It replaces server, host, database and login names and IP addresses with aliases and leaves credentials out. It checks the finished file for every name it knows and writes nothing if one remains.

Darling troubleshooting (service logs, store connectivity, permissions) is in the [Darling operator guide](../Darling/README.md). **Dashboard** (Full edition) troubleshooting is in [deprecated/Dashboard/README.md](../deprecated/Dashboard/README.md).
