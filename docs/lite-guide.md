# Quick Start — Lite

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

1. Download **[`PerformanceMonitorLite-lite-Setup.exe`](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest)** — a self-contained build, so there is **no .NET runtime to install first**.
2. Run the installer — it installs to `%LocalAppData%\PerformanceMonitorLite`, adds **Start Menu** and **Desktop** shortcuts, and registers the app under **Apps & Features** so it shows up in Windows search and can be uninstalled normally. Auto-update is wired in. Your data goes in `%LocalAppData%\PerformanceMonitorLite-Data`, a separate folder the installer never touches.
3. Launch from the Start Menu or Desktop shortcut.
4. Click **+ Add Server**, enter connection details, test, save.
5. Double-click the server in the sidebar to connect.

Data starts flowing within 1–5 minutes. That's it. No installation on your server, no Agent jobs, no sysadmin required.

**Taking the portable ZIP (`PerformanceMonitorLite-<version>.zip`) instead?** It is a self-contained win-x64 build too, so there is **nothing to install first** — unzip it anywhere and run `PerformanceMonitorLite.exe`. A stock Windows Server with no .NET on it at all is fine.

`READ-ME-FIRST.txt` still ships in the ZIP beside `PerformanceMonitorLite.exe`, now saying there is nothing to install rather than listing downloads. Monitored SQL Servers need nothing installed on them either way.

Windows itself must have its updates installed. If Lite stops at start with a CET message, see [Lite stops at start with a CET message](troubleshooting.md#lite-stops-at-start-with-a-cet-message).

### Upgrade Lite

**Upgrading from zip?** Click **Import Settings** then **Import Data** in the sidebar. Settings imports server connections, alert thresholds, SMTP config, and schedules. Data imports historical DuckDB + Parquet archives. **Auto-update users** (installed via Setup.exe) get updates automatically — no manual import needed.

**Import Settings** needs the folder that holds `config\servers.json`. **Import Data** needs the folder that holds `monitor.duckdb`. For a Lite older than 3.4.0, both are the same folder: the one you unzipped, or `%LOCALAPPDATA%\PerformanceMonitorLite`. From 3.4.0 on, `monitor.duckdb` is in `%LOCALAPPDATA%\PerformanceMonitorLite-Data`, so Import Data accepts that folder. The server list is in `%ProgramData%\PerformanceMonitorLite\config`, so Import Settings finds no `servers.json` in that data folder unless an older copy is left there. To move a current install to another PC, copy the folders instead, as in [Back up Lite and move it to another PC](#back-up-lite-and-move-it-to-another-pc).

#### Upgrading an install older than 3.4.0

Do not run `Setup.exe` over a Lite version older than 3.4.0. Before 3.4.0, Lite kept its DuckDB store, Parquet archive, logs, and `settings.json` inside `%LOCALAPPDATA%\PerformanceMonitorLite`. That folder is also the install folder, and `Setup.exe` deletes it before the new build starts. The history is then lost. Lite 3.4.0 and later keep that data in `%LOCALAPPDATA%\PerformanceMonitorLite-Data`, so `Setup.exe` does not touch it.

To find your version, click **About** in the left sidebar. The window shows `Version 3.x.y`.

If your version is older than 3.4.0, use one of these two ways:

1. Click **About**, then install the update from there. This upgrades in place and keeps your data.
2. Close Lite and copy `%LOCALAPPDATA%\PerformanceMonitorLite` to a safe place. Then run `Setup.exe`. `Setup.exe` deletes the original folder, so nothing is restored on its own. After Lite starts, click **Import Settings** and then **Import Data** in the sidebar, and point both at your copy.

After option 1, the first start of 3.4.0 or later moves the old data to the new folder. Your server list and your saved passwords are not in the install folder, so a `Setup.exe` upgrade does not delete them.

You can also extract the portable ZIP over your existing copy to upgrade in place.

### Where Lite keeps its files

| What | Where |
|---|---|
| Collected data (`monitor.duckdb`) | `%LOCALAPPDATA%\PerformanceMonitorLite-Data` |
| Parquet archive | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\archive` |
| Logs (`lite_<date>.log`) | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\logs` |
| Settings (`settings.json`, `collection_schedule.json`, `ignored_wait_types.json`) | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config` |
| Server list (`servers.json`) and credential profiles (`profiles.json`) | `%ProgramData%\PerformanceMonitorLite\config`, shared by every Windows user on the machine |
| SQL and Entra passwords, the SMTP password, and webhook URLs | Windows Credential Manager, per Windows user |
| The installed app (`Setup.exe` install) | `%LOCALAPPDATA%\PerformanceMonitorLite` |

Lite keeps archived data for 3 months. This is a fixed value. `settings.json` has no retention key, and the Settings window has no retention control. Lite deletes an archive file when the date in its file name is more than 3 months back. A row can therefore stay a little longer than 3 months.

Lite keeps each log file for 7 days. To open the logs, click **View Log** or **Open Log Folder** in the sidebar.

### Lite Data Storage

All data is stored in `%LOCALAPPDATA%\PerformanceMonitorLite-Data\` — a different folder from the install directory (`%LOCALAPPDATA%\PerformanceMonitorLite\`), so neither an in-app update nor re-running `Setup.exe` can disturb it. Data from an older install is moved into the new folder automatically the first time this version starts.

- **Hot data** in DuckDB 1.5.6: non-blocking checkpoints, free block reuse, stable file size without periodic resets
- **Archive** to Parquet with ZSTD compression (~10x reduction) — automatic monthly compaction keeps file count low (~75 files vs thousands)
- **Retention**: 3-month calendar-month rolling window. Retention is fixed at 3 months and cannot be changed.
- **Memory limit**: DuckDB's memory limit is a setting (Settings, Dashboard Defaults, "DuckDB memory limit"). The default is 2 GB, and it can go from 1 GB up to 80% of memory. A change applies after a restart.
- Typical size: ~50–200 MB per server per week

For the full list of folders, see [Where Lite keeps its files](#where-lite-keeps-its-files). To remove a server or uninstall, see [Remove a server, uninstall, and delete the data](#remove-a-server-uninstall-and-delete-the-data). To back up Lite or move it to another PC, see [Back up Lite and move it to another PC](#back-up-lite-and-move-it-to-another-pc).

### Back up Lite and move it to another PC

To back up Lite, close Lite and copy two folders:

- `%LOCALAPPDATA%\PerformanceMonitorLite-Data`
- `%ProgramData%\PerformanceMonitorLite\config`

To move Lite to another PC:

1. Install Lite on the new PC, start it once, and close it.
2. Copy the two folders from the old PC over the same paths on the new PC.
3. Start Lite. It gives the copied data folder a new install id, because the old id belongs to the old machine.
4. Enter the SQL and Entra passwords again for each server. Windows Credential Manager passwords do not move with the folders. Servers that use Windows Authentication need nothing. If you use email alerts or webhooks, enter the SMTP password and webhook URLs again.

Do not use **Import Settings** for this move. It reads `config\servers.json` under the folder that you select. The data folder has no current server list, because the list is in `%ProgramData%`.

### Remove a server, uninstall, and delete the data

Do these steps in this order. Lite can only remove its Extended Events sessions from a server while Lite is still installed.

1. Right-click each server in the sidebar and select **Remove**. Lite then drops this install's Extended Events sessions on that server and deletes the stored password for that server. The two shared sessions, `PerformanceMonitor_Deadlock` and `PerformanceMonitor_BlockedProcess`, stay on the server. See [Removing a server](lite-extended-events.md#removing-a-server). To change a server instead, select **Edit** in the same menu.
2. Close Lite. If a minimized copy is in the system tray, open it and close it too.
3. Uninstall Lite from Apps & Features. For the portable ZIP, delete the folder that you unzipped.
4. To delete the history, delete `%LOCALAPPDATA%\PerformanceMonitorLite-Data`.
5. To delete the server list, delete `%ProgramData%\PerformanceMonitorLite\config`.
6. To delete the SMTP password and webhook URLs, open Windows Credential Manager. Delete the entries whose names start with `PerformanceMonitorLite_`.

Uninstalling does not delete the data folder or the server list. If you uninstall before you remove the servers, the Extended Events sessions stay on those servers. Lite has no command that drops them later, so see [Cleaning up by hand](lite-extended-events.md#cleaning-up-by-hand).

### Lite Configuration

| File | Location | Purpose |
|---|---|---|
| `servers.json` | `%ProgramData%\PerformanceMonitorLite\config\` (machine-wide) | Server connections, shared across all Windows users on the machine. Passwords stay per-user in Windows Credential Manager. Optional **Utility Database** per server for community procs installed outside master. |
| `settings.json` | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\` (per-user) | MCP server, startup behavior, alert thresholds, SMTP configuration |
| `collection_schedule.json` | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\` (per-user) | Per-collector enable/disable and frequency. A collector that runs once a day or less often can also have a `run_at` time, so it runs in a quiet hour. That time is 24-hour `HH:MM` on the monitored server's clock, and you set it in the schedule editor. Each server starts at that time plus its own spread of under 60 minutes, and a run can start up to 60 minutes after that slot, so a run can start nearly 2 hours after `run_at`. It runs only while Lite is open. A day is skipped only when Lite was closed from the slot until 60 minutes after it. |
| `ignored_wait_types.json` | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\` (per-user) | 126 benign wait types excluded by default |

When a second Windows user on the same machine launches Lite, they see the shared `servers.json` immediately. SQL Auth and Entra MFA passwords are scoped to each user's own Credential Manager, so they'll be prompted once per server; Windows Auth works without any prompt.

**Always On AG?** Enable **ReadOnlyIntent** in the connection settings to route Lite's monitoring queries to a readable secondary, keeping the primary clear. Enable **MultiSubnetFailover** for multi-subnet failover scenarios.

**Query Store from a replica.** Set **QS replica** (a host, `host,port` or listener name) on a SQL Server registration to read Query Store from a readable secondary instead of the primary. Empty, the default, reads the primary and changes nothing. Azure SQL Database never reads a replica. The replica host gets the same login and password as the server, so it must accept them. Collection health shows the newest Query Store run's `read_source` (for example `replica 12, primary 2`) in the Note column and the MCP `get_collection_health` row, only when a run has one.

### Appearance

Lite ships three themes — **Dark** (the default), **Light** and **Cool Breeze** — picked from **Settings → Color theme**, applied live. Every surface in the app is painted from one palette of eighteen named colors per theme, and you can adjust twelve of them yourself (#3577), per theme, with a reset to the shipped values.

Open **Settings**, expand **Colors** beneath the theme drop-down, and you get one row per color for the theme currently showing: a swatch, the hex, a picker (hue / saturation / brightness sliders) and the **measured contrast ratio** against the surface that color renders on — page background against text, accent against the ink that sits on it, each status color against the page. Green is 4.50:1 or better (WCAG AA for text), amber is 3.00–4.49 (fine for a marker, hard as text), red is under 3.00. The readout is a measurement, not a gate: **Apply** always works. **Reset to default** returns the current theme to its shipped palette and leaves your other themes alone. **Open file** opens the JSON in your editor; anything you save there applies live too.

The file is `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\theme-overrides.json`, beside `settings.json`. It is keyed by theme, then by color, and holds only what you changed:

```json
{
  "Dark": {
    "AccentColor": "#3A7BD5",
    "AccentForegroundColor": "#FFFFFF"
  },
  "CoolBreeze": {
    "WarningColor": "#B45309"
  }
}
```

Values are `#RRGGBB` (`#AARRGGBB` is accepted). A key that is not one of the twelve, a value that is not a color, a theme that is not one of the three, or a file that is not JSON is skipped with a line in the log and never stops Lite from starting. Deleting the file, or a theme's block, is the same as Reset.

| Key | Group | What it paints |
|---|---|---|
| `BackgroundColor` | Backgrounds | The window and panel background most text sits on |
| `BackgroundLightColor` | Backgrounds | Cards, settings sections, buttons at rest, headers — a step lighter than the page |
| `BackgroundDarkColor` | Backgrounds | The sidebar, grid rows and other recessed areas — a step darker than the page |
| `ForegroundColor` | Text | Primary text: values, headers, labels |
| `ForegroundDimColor` | Text | Captions and secondary labels |
| `AccentColor` | Accent | The selected tab, the selected grid row, highlighted combo items, accent buttons, links |
| `AccentForegroundColor` | Accent | The ink for text and glyphs on an accent fill |
| `AlternatingRowColor` | Rows | Every other grid row |
| `SuccessColor` | Status | Healthy / OK markers and the Done row mark |
| `WarningColor` | Status | Warning markers and the To Do row mark |
| `ErrorColor` | Status | Error / critical markers and the Do Not Do row mark |
| `InfoColor` | Status | Informational markers |

The other six palette colors (`AccentHoverColor`, `AccentPressedColor`, `BackgroundLighterColor`, `ForegroundMutedColor`, `BorderColor`, `BorderLightColor`) are derived tones — a hover is the accent lightened a step, a border is the background lifted a step — and follow the theme author's values; they are not adjustable, and neither is any single control (the selected tab, one button): the knobs are the palette, and every surface built from it follows. A handful of component-specific brushes the themes paint with a fixed value (the plan viewer's panels, the time-range slicer, the deadlock graph) are likewise not derived from the palette and do not move.

### Lite Collectors

More than 40 collectors run on independent, configurable schedules (the long-running-query completion trace is opt-in and ships disabled):

| Collector | Default | Source |
|---|---|---|
| query_snapshots | 1 min | `sys.dm_exec_requests` + `sys.dm_exec_sessions` |
| blocked_process_report | 1 min | XE ring buffer session |
| waiting_tasks | 1 min | `sys.dm_os_waiting_tasks` |
| wait_stats | 1 min | `sys.dm_os_wait_stats` (deltas) |
| latch_stats | 1 min | `sys.dm_os_latch_stats` (deltas) |
| spinlock_stats | 1 min | `sys.dm_os_spinlock_stats` (deltas) |
| cpu_scheduler_stats | 1 min | `sys.dm_os_schedulers` runnable/blocked/queued task counts (not Azure SQL DB) |
| long_query_completions | 1 min (opt-in, ships off) | dedicated completion-trace XE (`rpc_completed`/`sql_batch_completed` over a duration threshold, plus `attention`) |
| attention | 1 min | dedicated `PerformanceMonitor_Attention` XE session (`attention`: queries a client cancelled or timed out, with the client application, host, database and statement; feeds the Client Timeouts alert) |
| query_stats | 1 min | `sys.dm_exec_query_stats` (deltas) |
| procedure_stats | 1 min | `sys.dm_exec_procedure_stats` (deltas) |
| cpu_utilization | 1 min | `sys.dm_os_ring_buffers` scheduler monitor |
| database_states | 1 min | `sys.databases` state per database — feeds the database offline/unhealthy alert (not Azure SQL DB) |
| file_io_stats | 1 min | `sys.dm_io_virtual_file_stats` (deltas) |
| memory_stats | 1 min | `sys.dm_os_sys_memory` + memory counters |
| memory_grant_stats | 1 min | `sys.dm_exec_query_memory_grants` |
| tempdb_stats | 1 min | `sys.dm_db_file_space_usage` + `tempdb.sys.database_files` (the ROWS files' growth ceiling) |
| perfmon_stats | 1 min | `sys.dm_os_performance_counters` (deltas) |
| deadlocks | 5 min | dedicated `PerformanceMonitor_Deadlock` XE session (`xml_deadlock_report`; `database_xml_deadlock_report` on Azure SQL DB) |
| dmv_blocking_snapshot | 1 min | `sys.dm_os_waiting_tasks` + `sys.dm_exec_*` (always-on blocking fallback when the blocked-process-report XE is unavailable) |
| ag_replica_states | 1 min | `sys.dm_hadr_availability_replica_states` + `sys.availability_replicas` (Availability Group replica health; zero rows without AGs, not Azure SQL DB) |
| ag_database_replica_states | 1 min | `sys.dm_hadr_database_replica_states` (per-database send/redo queues, rates, secondary lag; zero rows without AGs, not Azure SQL DB) |
| session_stats | 5 min | `sys.dm_exec_sessions` active session tracking |
| session_summary_stats | 5 min | `sys.dm_exec_sessions` top app/host/database summary |
| memory_clerks | 5 min | `sys.dm_os_memory_clerks` |
| memory_pressure_events | 5 min | `sys.dm_os_ring_buffers` RING_BUFFER_RESOURCE_MONITOR |
| query_store | 5 min | Query Store DMVs (per database) |
| plan_correction | 5 min | `sys.dm_db_tuning_recommendations` + `sys.database_automatic_tuning_options` (automatic plan correction: FORCE_LAST_GOOD_PLAN enablement plus the engine's live regression recommendations, with the regressed query's text resolved through Query Store; SQL Server 2017+, Azure SQL DB and Azure SQL Managed Instance) |
| plan_cache_stats | 5 min | `sys.dm_exec_cached_plans` (single-use vs reused plan-cache bloat) |
| system_health_events | 5 min | `system_health` XE ring buffer (not Azure SQL DB) |
| default_trace_events | 5 min | default trace via `sys.fn_trace_gettable` (not Azure SQL DB) |
| job_history | 5 min | `msdb.dbo.sysjobhistory` retained job-run history (not Azure SQL DB) |
| agent_status | 5 min | `sys.dm_server_services` + `msdb.dbo.sysjobschedules` (not Azure SQL DB / RDS) |
| running_jobs | 5 min | `msdb` job history with duration vs avg/p95 (not Azure SQL DB / AWS RDS) |
| database_size_stats | 1 hour | `sys.master_files` + `FILEPROPERTY` + `dm_os_volume_stats` |
| pvs_stats | 1 hour | `sys.dm_tran_persistent_version_store_stats` + `sys.databases` (ADR persistent version store size and cleanup state per database; SQL Server 2019+ only, always collected on Azure SQL DB) |
| query_store_health | 1 hour | `sys.database_query_store_options` per database (actual vs desired state, readonly_reason, storage used vs cap, cleanup thresholds, runtime-stats interval length; SQL Server 2016+, one row per database with OFF recorded explicitly) |
| server_properties | on connect | `SERVERPROPERTY()` hardware and licensing metadata |
| index_object_stats | Daily | `sys.dm_db_partition_stats` + `sys.dm_db_index_usage_stats` + `sys.dm_db_index_operational_stats` |
| server_config | On connect | `sys.configurations` |
| database_config | On connect | `sys.databases` |
| database_scoped_config | On connect | Database-scoped configurations |
| trace_flags | On connect | `DBCC TRACESTATUS` |

Darling runs these same collectors across a fleet of servers. It adds five more that Lite does not have:

- `query_store_forced_plans` reads each database's forced plans.
- `query_store_hints` reads each database's Query Store hints (SQL Server 2022 and later).
- `identity_capacity` reads how full each database's identity columns are.
- `database_protection` reads the last good CHECKDB and the last full and log backup of each database.
- `agent_alerts` reads the SQL Server Agent alerts and the operators they notify.

Darling also runs more than 25 PostgreSQL collectors. See the [Darling collector reference](../Darling/README.md).

The Extended Events sessions that Lite creates on a monitored server, and how to remove them, are on their own page: [The Extended Events Sessions Lite Creates](lite-extended-events.md).
