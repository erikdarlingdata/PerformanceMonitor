# Troubleshooting

Part of the [Performance Monitor documentation](../README.md#documentation).

### Lite

Application logs are written to `%LOCALAPPDATA%\PerformanceMonitorLite-Data\logs`, one `lite_<date>.log` file per day. Lite keeps each file for 7 days. In the sidebar, **View Log** opens today's file and **Open Log Folder** opens the folder. Collection success/failure is also logged to the `collection_log` table in DuckDB.

Verbosity is the `log_minimum_level` key in `settings.json` (no UI — see `Lite/config/settings.sample.json`), one of `Trace`, `Debug`, `Information`, `Warning`, `Error`, `Critical` or `None`. It defaults to `Information` and is read at startup, so a change needs a restart.

Set it to `Debug` when you need the per-database collection timing lines — `[server] collector [db] => N rows (sql:Nms, duckdb:Nms)`, one per database per collector per cycle. They are below the default because at that rate they bury the collection failures the log is usually opened for; the aggregate they decompose is in `collection_log`'s `duration_ms`, which is recorded at every level.

Common issues:

1. **No data after connecting** — Wait for the first collection cycle (1–5 minutes). Check logs for connection errors.
2. **Query Store tab empty** — Query Store must be enabled on the target database (`ALTER DATABASE [YourDB] SET QUERY_STORE = ON`).
3. **Blocked process reports empty** — Lite attempts to auto-configure the blocked process threshold to 5 seconds via `sp_configure`. On **AWS RDS**, `sp_configure` is not available — set `blocked process threshold (s)` through an RDS Parameter Group (see [Platform Notes](platform-notes.md#platform-notes) below). On **Azure SQL Database**, the threshold is fixed at 20 seconds and cannot be changed. If you still see no data on other platforms, verify the login has `ALTER SETTINGS` permission.
4. **Connection failures** — Verify network connectivity, firewall rules, and that the login has the required [permissions](permissions.md#permissions). For Azure SQL Database, use a contained database user with `VIEW DATABASE STATE`. Error 40615 means the server firewall did not allow the client's IP address. Add a server-level rule, or a database-level rule in master.
5. **FinOps Index Analysis hangs, times out, or returns `Msg 229` on `sql_expression_dependencies`** — see [FinOps Index Analysis](permissions.md#finops-index-analysis-per-database-grants) below for the full per-database grant set that fixes both failure modes.
6. **A server reachable only over a named pipe will not connect** — Lite and the Darling service connect to a remote SQL Server over TCP only. Turn on TCP/IP for the instance in SQL Server Configuration Manager. If the port is not 1433, write the host as `name,port`. A host that starts with `np:`, `lpc:` or `admin:`, or a path that starts with `\` or `/`, is refused.

### Lite stops at start with a CET message

Symptom: Lite stops at start. The exit code is `0x80131506` and stderr shows this message:

```text
Fatal error. Your Windows doesn't fully support CET. Please install all available Windows updates.
```

`Setup.exe` then ends with "Install Partially Succeeded" and gives no reason, because Velopack shows a fixed message when the app's install step fails. Velopack's velopack.log records the same exit code.

Cause: .NET 10 apps need current Windows updates to start on a CPU with CET (Control-flow Enforcement Technology, a hardware protection). Without them, Lite stops at start. We saw this on Windows Server 2022 build 20348.1366. The exit code is `0x80131506` and stderr shows this message:

Fix: install all available Windows updates and restart. If you cannot do that yet, there is a short-term workaround. Run it from an elevated PowerShell before `Setup.exe` or the first start. It turns off one protection (`UserShadowStack`, the hardware-enforced stack protection) for the Lite exe only:

```powershell
Set-ProcessMitigation -Name PerformanceMonitorLite.exe -Disable UserShadowStack
```

This weakens Lite's protection against stack-corruption attacks, so treat it as a stopgap. To undo it once Windows is patched, remove the values the first command wrote:

```powershell
Remove-ItemProperty -Path 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Image File Execution Options\PerformanceMonitorLite.exe' -Name MitigationOptions, MitigationAuditOptions, EAFModules -ErrorAction SilentlyContinue
```

On Windows Server 2022 build 20348.5622, `Set-ProcessMitigation -Remove -Disable UserShadowStack` did not remove the setting (it returned `C000000D`). `Set-ProcessMitigation -Reset` failed too ("Invalid input parameters"). The registry way above worked. The `Set-ProcessMitigation -Disable` command writes three values (`MitigationOptions`, `MitigationAuditOptions`, `EAFModules`) under the exe's key in `Image File Execution Options`. The undo removes only those values and leaves any other value on that key alone. Restart the program afterwards. The first command is described in Microsoft's [Set-ProcessMitigation](https://learn.microsoft.com/en-us/powershell/module/processmitigations/set-processmitigation) reference.

### Darling

For a bug report, run the service with `--diagnostics-bundle <path>`. It writes one JSON file with the store's state, collection health, slow reads, statement history and the recent service log. It replaces server, host, database and login names and IP addresses with aliases and leaves credentials out. It checks the finished file for every name it knows and writes nothing if one remains.

A remote SQL Server reachable only over a named pipe does not connect. Lite and the Darling service connect to a remote SQL Server over TCP only. Turn on TCP/IP for the instance in SQL Server Configuration Manager. If the port is not 1433, write the host as `name,port`. A host that starts with `np:`, `lpc:` or `admin:`, or a path that starts with `\` or `/`, is refused.

Darling troubleshooting (service logs, store connectivity, permissions) is in the [Darling operator guide](../Darling/README.md). **Dashboard** (Full edition) troubleshooting is in [deprecated/Dashboard/README.md](../deprecated/Dashboard/README.md).
