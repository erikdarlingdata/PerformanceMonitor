# Quick Start — Lite

Part of the [Performance Monitor documentation](../README.md#documentation).

1. Download **[`PerformanceMonitorLite-lite-Setup.exe`](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest)** — a self-contained build, so there is **no .NET runtime to install first**.
2. Run the installer — it installs to `%LocalAppData%\PerformanceMonitorLite`, adds **Start Menu** and **Desktop** shortcuts, and registers the app under **Apps & Features** so it shows up in Windows search and can be uninstalled normally. Auto-update is wired in. Your data goes in `%LocalAppData%\PerformanceMonitorLite-Data`, a separate folder the installer never touches.
3. Launch from the Start Menu or Desktop shortcut.
4. Click **+ Add Server**, enter connection details, test, save.
5. Double-click the server in the sidebar to connect.

Data starts flowing within 1–5 minutes. That's it. No installation on your server, no Agent jobs, no sysadmin required.

**Taking the portable ZIP (`PerformanceMonitorLite-<version>.zip`) instead?** It is a self-contained win-x64 build too, so there is **nothing to install first** — unzip it anywhere and run `PerformanceMonitorLite.exe`. A stock Windows Server with no .NET on it at all is fine.

That was not always true: through 3.5.0 the ZIP was a portable build with no runtime of its own, and on a machine without .NET it died on the host’s own `You must install .NET to run this application` before a line of Lite’s code ran. Pinning the ZIP to win-x64 removed the requirement **and halved the download** — the old build shipped ~537&nbsp;MB of `runtimes\` for macOS, Linux, ARM and musl targets Windows can never load, which cost far more than bundling the runtime does. See [`Lite/README.md`](../Lite/README.md#prerequisites) for the detail.

**Upgrading from zip?** Click **Import Settings** then **Import Data** in the sidebar and point both at your old Lite folder. Settings imports server connections, alert thresholds, SMTP config, and schedules. Data imports historical DuckDB + Parquet archives. **Auto-update users** (installed via Setup.exe) get updates automatically — no manual import needed.

> **Warning — upgrading an install older than the data-relocation fix:** on those versions Lite kept its data *inside* the install directory, and re-running `Setup.exe` over an existing install deletes that directory before the new build ever runs. It takes the DuckDB store, the Parquet archive, the logs, and `settings.json` with it. Upgrade **in place** instead: the **About** button in the left sidebar downloads and applies the update without touching your data, or extract the portable ZIP over your existing copy. From this release on, data lives outside the install directory and is moved there automatically on first start, so `Setup.exe` is safe again. (Your monitored-server list is unaffected either way — it lives in `%ProgramData%\PerformanceMonitorLite\config` — and so are the passwords in Windows Credential Manager.)

**Always On AG?** Enable **ReadOnlyIntent** in the connection settings to route Lite's monitoring queries to a readable secondary, keeping the primary clear. Enable **MultiSubnetFailover** for multi-subnet failover scenarios.

**Query Store from a replica.** Set **QS replica** (a host, `host,port` or listener name) on a SQL Server registration to read Query Store from a readable secondary instead of the primary. Empty, the default, reads the primary and changes nothing. Azure SQL Database never reads a replica. The replica host gets the same login and password as the server, so it must accept them. Collection health shows the newest Query Store run's `read_source` (for example `replica 12, primary 2`) in the Note column and the MCP `get_collection_health` row, only when a run has one.

### Lite Collectors

43 collectors run on independent, configurable schedules (the long-running-query completion trace is opt-in and ships disabled):

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

Darling runs these same 43 collectors across a fleet of servers, plus a 44th, `query_store_forced_plans`, which reads each database's forced plans. Lite does not have it. Darling also runs 29 PostgreSQL collectors. See the [Darling collector reference](../Darling/README.md).

### The Extended Events Sessions Lite Creates

Each Lite install has an eight-character id. Lite makes it at the first start and keeps it in `install-id.json`, in the data folder `%LOCALAPPDATA%\PerformanceMonitorLite-Data\`. The id appears in the name of each session that belongs to this install alone. Two installs that monitor one server never share such a session, so one install cannot drop or stop the other's.

If Lite cannot read `install-id.json` (another program holds it open, or the read fails), it leaves the file as it is and uses no id until it can read it. If it cannot save a new id, it does not use that id either. With no id, Lite creates, starts and drops no session of its own, and the long-query trace records why as a fault. The next collection cycle tries again, at most once a minute. The log says it once as a Warning with the error, then at Debug until the id is read. A file that holds something other than an id record still gets a new id.

#### The sessions

- `PerformanceMonitor_Lite_<id>_LongQueryCompletions` is this install's long-query trace. Lite creates it while the opt-in `long_query_completions` collector is on. It is created with `STARTUP_STATE = OFF`, so it stays stopped after a server restart. Each check starts it again when it finds it stopped.
- `PerformanceMonitor_Deadlock` and `PerformanceMonitor_BlockedProcess` keep their shared names, because Darling and other installs read them too. Lite never drops them. It checks each one whenever its collector runs. It creates a missing one and starts a stopped one, so a deliberate stop does not last.
- `PerformanceMonitor_Attention` is the client-timeout session that the `attention` collector reads. It also keeps its shared name, and Lite never drops it. Lite checks it while the `attention` collector is on. It creates a missing one with `STARTUP_STATE = ON` and starts a stopped one. On Azure SQL Database, Lite creates one in every monitored database. If a session of that name has an older definition, Lite drops it and creates it again once for each run of the app.
- On Azure SQL Database, a shared session can stay unusable after a start. Lite then creates `PerformanceMonitor_Lite_<id>_Deadlock`, `PerformanceMonitor_Lite_<id>_BlockedProcess` or `PerformanceMonitor_Lite_<id>_Attention` in that database, with `STARTUP_STATE = OFF`, and reads that one. When the shared session works again, Lite switches back and drops its own fallback.

#### The old shared session

Earlier versions shared one session, `PerformanceMonitor_LongQueryCompletions`, between all installs. An upgraded install drops it once for each registration and database. It then records that drop and never touches the session again. If an older install creates it again later, Lite logs one Information line per start and leaves the session alone. To remove it, run the statements under "Cleaning up by hand" below.

#### Removing a server

Removing a server drops this install's sessions on it. These are the long-query session and its own deadlock, blocked-process and client-timeout fallbacks. Lite makes one attempt, within 15 seconds. A failed drop is logged and never stops the removal.

Lite leaves a session that another registration of this install keeps. On premises, it also leaves the session when it cannot tell whether another registration of this install shares the instance. That happens when the instance name of this server is not known. Lite logs the reason. The three shared sessions stay on the server.

#### Registrations that share an instance

On premises, a registration with the trace off leaves the long-query session in place when another registration of this install has the trace on. Both registrations must point at the same instance. Lite compares the last known `@@SERVERNAME` of each one.

#### Read-only intent on Azure SQL Database

A session cannot be created over a read-only connection. For a registration with read-only intent, Lite creates the session definition over a connection without the intent. The definition replicates to the read-only replica. Lite then starts the session over the registration's own connection. A drop stops the session on the replica first, then drops it on the primary. Microsoft describes the method in [Monitor read-only replicas with Extended Events](https://learn.microsoft.com/en-us/azure/azure-sql/database/read-scale-out). On Hyperscale with several high-availability replicas, a read-only connection lands on one replica that the app cannot choose, so the stop reaches only that one. Microsoft Learn describes no way to address a single high-availability replica; it says the read-intent workload is distributed arbitrarily across them ([Connect to an HA replica](https://learn.microsoft.com/en-us/azure/azure-sql/database/service-tier-hyperscale-replicas#connect-to-an-ha-replica)).

A Managed Instance registration with read-only intent needs the trace started on the primary first. Lite does not do that, so the registration gets the read-only message (error 3906). A registration without the intent can land on a read-only database, such as a geo-secondary. It gets one clear message, and Lite retries the create every hour.

#### Azure SQL Database limits

Microsoft Learn lists these caps under "Resource governance" on [Extended Events in Azure SQL](https://learn.microsoft.com/en-us/azure/azure-sql/database/xevent-db-diff-from-svr). A database holds at most 100 started sessions. An elastic pool holds at most 100 database-scoped sessions. Session memory is capped at 128 MB per database and 512 MB per pool. In a dense pool, a start can fail below 100 sessions.

Each install uses one long-query session per database while the trace is on. It adds up to three fallbacks while a shared session is unusable. A failed create or start on Azure SQL Database carries a sentence that names these caps.

#### Known limits

- An older Darling with the trace on loses capture once for each upgraded install, until it reconnects.
- An older Lite creates the old session again within one collection cycle.
- Two registrations of one instance in one install drop the old session twice.
- A lost record costs one more drop. This happens when you recreate the Lite data folder.
- On RDS Multi-AZ, the drop reaches the primary only. The copy on the standby stays stopped after a failover, unless an older install starts it.
- On Hyperscale with several high-availability replicas, the stop reaches only the replica that a read-only connection lands on. A copy of the session that runs on another replica is not stopped. Microsoft Learn describes no way to address one high-availability replica: [Connect to an HA replica](https://learn.microsoft.com/en-us/azure/azure-sql/database/service-tier-hyperscale-replicas#connect-to-an-ha-replica).
- On Azure SQL Database, two registrations of one database with different logins share one session name, as before.

#### Clones

A clone of the whole machine, such as a VM snapshot, keeps the machine name and the Windows user. It also keeps the install id. The clone and the original then use the same session names on every server both monitor.

To give the clone its own id, delete `install-id.json` and restart Lite. Lite makes a new id at start. A data folder that you copy to another machine gets a new id on its own. So does a data folder that another Windows user opens. Import Settings does not carry the file.

#### Cleaning up by hand

Lite has no command that drops sessions, so run the statements below yourself. Replace `<id>` with the `id` value in `install-id.json`. Each statement checks that the session exists first, so a run that finds nothing changes nothing.

The login needs the permission that [Microsoft Learn's `DROP EVENT SESSION` page](https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-event-session-transact-sql) requires. On SQL Server and Azure SQL Managed Instance that is `DROP ANY EVENT SESSION` (SQL Server 2022 and later) or `ALTER ANY EVENT SESSION`. On Azure SQL Database it is `DROP ANY DATABASE EVENT SESSION`, in each database.

Server scope (SQL Server, Azure SQL Managed Instance and AWS RDS). The first statement drops the old shared session. The second drops this install's own session. Lite makes the deadlock, blocked-process and client-timeout fallbacks in Azure SQL Database only, so a server has none to drop.

```sql
IF EXISTS (SELECT 1/0 FROM sys.server_event_sessions AS ses WHERE ses.name = N'PerformanceMonitor_LongQueryCompletions')
    DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON SERVER;

IF EXISTS (SELECT 1/0 FROM sys.server_event_sessions AS ses WHERE ses.name = N'PerformanceMonitor_Lite_<id>_LongQueryCompletions')
    DROP EVENT SESSION [PerformanceMonitor_Lite_<id>_LongQueryCompletions] ON SERVER;
```

Database scope (Azure SQL Database). Run these five statements in each monitored database.

```sql
IF EXISTS (SELECT 1/0 FROM sys.database_event_sessions AS des WHERE des.name = N'PerformanceMonitor_LongQueryCompletions')
    DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON DATABASE;

IF EXISTS (SELECT 1/0 FROM sys.database_event_sessions AS des WHERE des.name = N'PerformanceMonitor_Lite_<id>_LongQueryCompletions')
    DROP EVENT SESSION [PerformanceMonitor_Lite_<id>_LongQueryCompletions] ON DATABASE;

IF EXISTS (SELECT 1/0 FROM sys.database_event_sessions AS des WHERE des.name = N'PerformanceMonitor_Lite_<id>_Deadlock')
    DROP EVENT SESSION [PerformanceMonitor_Lite_<id>_Deadlock] ON DATABASE;

IF EXISTS (SELECT 1/0 FROM sys.database_event_sessions AS des WHERE des.name = N'PerformanceMonitor_Lite_<id>_BlockedProcess')
    DROP EVENT SESSION [PerformanceMonitor_Lite_<id>_BlockedProcess] ON DATABASE;

IF EXISTS (SELECT 1/0 FROM sys.database_event_sessions AS des WHERE des.name = N'PerformanceMonitor_Lite_<id>_Attention')
    DROP EVENT SESSION [PerformanceMonitor_Lite_<id>_Attention] ON DATABASE;
```

If the registration used read-only intent, first stop the session over a read-only connection with `ALTER EVENT SESSION [name] ON DATABASE STATE = STOP;`. Then run the drop over a connection without the intent, which reaches the primary. On Hyperscale with several high-availability replicas, the stop reaches only the replica that a read-only connection lands on. A copy of the session that runs on another replica is not stopped. Microsoft Learn describes no way to address one high-availability replica: [Connect to an HA replica](https://learn.microsoft.com/en-us/azure/azure-sql/database/service-tier-hyperscale-replicas#connect-to-an-ha-replica).

The shared `PerformanceMonitor_Deadlock`, `PerformanceMonitor_BlockedProcess` and `PerformanceMonitor_Attention` sessions need the same statements with those names. Run them only when no other install monitors the server, because every install that monitors it uses them.

### Lite Data Storage

All data is stored in `%LOCALAPPDATA%\PerformanceMonitorLite-Data\` — a different folder from the install directory (`%LOCALAPPDATA%\PerformanceMonitorLite\`), so neither an in-app update nor re-running `Setup.exe` can disturb it. Data from an older install is moved into the new folder automatically the first time this version starts.

- **Hot data** in DuckDB 1.5.5 — non-blocking checkpoints, free block reuse, stable file size without periodic resets
- **Archive** to Parquet with ZSTD compression (~10x reduction) — automatic monthly compaction keeps file count low (~75 files vs thousands)
- **Retention**: 3-month calendar-month rolling window
- **Memory limit**: DuckDB's memory limit is a setting (Settings, Dashboard Defaults, "DuckDB memory limit"). The default is 2 GB, and it can go from 1 GB up to 80% of memory. A change applies after a restart.
- Typical size: ~50–200 MB per server per week

### Lite Configuration

| File | Location | Purpose |
|---|---|---|
| `servers.json` | `%ProgramData%\PerformanceMonitorLite\config\` (machine-wide) | Server connections, shared across all Windows users on the machine. Passwords stay per-user in Windows Credential Manager. Optional **Utility Database** per server for community procs installed outside master. |
| `settings.json` | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\` (per-user) | Retention, MCP server, startup behavior, alert thresholds, SMTP configuration |
| `collection_schedule.json` | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\` (per-user) | Per-collector enable/disable and frequency. A collector that runs once a day or less often can also have a `run_at` time, so it runs in a quiet hour. That time is 24-hour `HH:MM` on the monitored server's clock, and you set it in the schedule editor. Each server starts at that time plus its own spread of under 60 minutes, and a run can start up to 60 minutes after that slot, so a run can start nearly 2 hours after `run_at`. It runs only while Lite is open. A day is skipped only when Lite was closed from the slot until 60 minutes after it. |
| `ignored_wait_types.json` | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\` (per-user) | 126 benign wait types excluded by default |

When a second Windows user on the same machine launches Lite, they see the shared `servers.json` immediately. SQL Auth and Entra MFA passwords are scoped to each user's own Credential Manager, so they'll be prompted once per server; Windows Auth works without any prompt.
