# SQL Server Performance Monitor

<p align="center">
  <a href="https://github.com/erikdarlingdata/PerformanceMonitor/stargazers"><img src="https://img.shields.io/github/stars/erikdarlingdata/PerformanceMonitor?style=for-the-badge&logo=github&color=gold&logoColor=black" alt="GitHub Stars"></a>
  <a href="https://github.com/erikdarlingdata/PerformanceMonitor/network/members"><img src="https://img.shields.io/github/forks/erikdarlingdata/PerformanceMonitor?style=for-the-badge&logo=github" alt="GitHub Forks"></a>
  <a href="https://github.com/erikdarlingdata/PerformanceMonitor/blob/main/LICENSE"><img src="https://img.shields.io/github/license/erikdarlingdata/PerformanceMonitor?style=for-the-badge" alt="License: MIT"></a>
  <a href="https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest"><img src="https://img.shields.io/github/v/release/erikdarlingdata/PerformanceMonitor?style=for-the-badge" alt="Latest Release"></a>
  <a href="https://github.com/erikdarlingdata/PerformanceMonitor/issues"><img src="https://img.shields.io/github/issues/erikdarlingdata/PerformanceMonitor?style=for-the-badge" alt="Open Issues"></a>
  <a href="https://github.com/erikdarlingdata/PerformanceMonitor/commits/main"><img src="https://img.shields.io/github/last-commit/erikdarlingdata/PerformanceMonitor?style=for-the-badge" alt="Last Commit"></a>
  <a href="https://github.com/erikdarlingdata/PerformanceMonitor/actions/workflows/build.yml"><img src="https://img.shields.io/github/actions/workflow/status/erikdarlingdata/PerformanceMonitor/build.yml?style=for-the-badge&label=CI" alt="CI"></a>
</p>
<p align="center">
  <a href="https://x.com/erikdarlingdata"><img src="https://img.shields.io/badge/Follow_%40ErikDarlingData-black?style=for-the-badge&logo=x&logoColor=white" alt="Follow @ErikDarlingData on X"></a>
  <a href="https://www.youtube.com/@ErikDarlingData"><img src="https://img.shields.io/badge/YouTube-Subscribe-red?style=for-the-badge&logo=youtube&logoColor=white" alt="YouTube Subscribe"></a>
  <a href="https://www.linkedin.com/in/erik-darling-data/"><img src="https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=for-the-badge&logo=linkedin&logoColor=white" alt="LinkedIn Connect"></a>
  <a href="https://erikdarling.com"><img src="https://img.shields.io/badge/Blog-erikdarling.com-FF6B35?style=for-the-badge&logo=wordpress&logoColor=white" alt="Blog"></a>
</p>

**Free, open-source monitoring that replaces the tools charging you thousands per server per year.** Specialized collectors, real-time alerts, and a built-in MCP server for AI analysis. Nothing phones home. Your data stays on your server and your machine.

**Supported:** SQL Server 2016–2025 | Azure SQL Managed Instance | AWS RDS for SQL Server | Azure SQL Database (Lite and Darling)

![Landing page with server health cards](Screenshots/Screenshot%20Dashboard%20landing%20page%20with%20server%20health%20cards.jpg)

---

## Editions

Pick by how you want collection to run — the monitoring brain (collectors, alert engine, plan analysis, MCP tools) is shared across all three at the library level.

| | **[Lite](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest)** — flagship | **[Darling](Darling/README.md)** — headless | **[Dashboard](deprecated/Dashboard/README.md)** — *deprecated* |
|---|---|---|---|
| **How it runs** | Single desktop app monitors remotely, on demand | Windows service collects 24/7 into a central store; detached viewer reads it from any seat | SQL-Server-installed database + Agent collectors, separate viewer app |
| **Installs on your server?** | No | No | Yes (a `PerformanceMonitor` database) |
| **Stores data** | Local DuckDB + Parquet | Bundled PostgreSQL + TimescaleDB | In the target SQL Server |
| **Best for** | Quick triage, Azure SQL DB, locked-down servers, consultants, firefighting | Always-on monitoring of many servers from one service | *Existing installs only — new deployments should use Lite or Darling* |
| **Requires** | `VIEW SERVER STATE` ([permissions](#permissions)) | `VIEW SERVER STATE` + a place to run the service | SQL Agent ([Dashboard docs](deprecated/Dashboard/README.md)) |

> **⚠️ The "Full" Dashboard edition is deprecated.** Existing installs keep working and remain on bug-fix support, but it is no longer the recommended path and — as of v3.3.0 — the Dashboard and its CLI installer are **no longer included in release assets**. The last shipped builds are in the [v3.2.0 release](https://github.com/erikdarlingdata/PerformanceMonitor/releases/tag/v3.2.0), and both remain buildable from the repo. New deployments should use **Lite** or **Darling**. Its docs live with the code: **[deprecated/Dashboard/README.md](deprecated/Dashboard/README.md)** (the app, tabs, permissions) and **[deprecated/Installer/README.md](deprecated/Installer/README.md)** (the CLI database installer).

**👉 Not sure? [Start with Lite.](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest)** One download, nothing installed on your server, data flowing in under 5 minutes.

All editions include real-time alerts (system tray + email + webhooks), charts and graphs, dark and light themes, CSV export, and a built-in MCP server for AI-powered analysis with tools like Claude. All release binaries are digitally signed via [SignPath](https://signpath.io) — no more Windows SmartScreen warnings.

---

## What People Are Saying

> *"You guys make us DBAs look like absolute rockstars. I'm over here getting showered with praise, and all I do is use your scripts and follow your advice."*

> *"replaced SentryOne and had it running in 10 minutes"*

> *"I've had enough time to gather data and converse with Claude on this. It helped a lot to zone in on CPU starvation from the hypervisor on which the VM runs. IT team currently investigating the host configuration."*

---

## What You Get

🔍 **Specialized collectors** on configurable schedules — wait stats, query performance, blocking chains, deadlock graphs, memory grants, file I/O, tempdb, perfmon counters, FinOps/capacity, and more. Query text and execution plan collection can be disabled per-collector for sensitive environments.

🚨 **Real-time alerts** for blocking, deadlocks, and high CPU — system tray notifications, styled HTML emails with full XML attachments, and webhook notifications for external integrations

📊 **NOC-style overview** with green/yellow/red health cards, auto-refresh, configurable time ranges, and dark/light themes

📋 **Graphical plan viewer** with native ShowPlan rendering, 30-rule PlanAnalyzer, operator-level cost breakdown, and a standalone mode for opening `.sqlplan` files without a server connection

💡 **Recommendations engine (advise-and-act)** — a dedicated Recommendations tab surfaces prioritized findings from your own monitoring data with the reasoning behind each one, and can apply selected fixes directly. Destructive changes (like enabling Read Committed Snapshot Isolation) are gated behind an informed-consent dialog that spells out both the risk of acting and the risk of doing nothing.

🤖 **Built-in MCP server** with read-only tools for AI analysis — ask Claude Code or Cursor "what are the top wait types on my server?" and get answers from your actual monitoring data

🧰 **Community tools installed automatically** — sp_WhoIsActive, sp_BlitzLock, sp_HealthParser, sp_HumanEventsBlockViewer

🔒 **Your data never leaves** — no telemetry, no cloud dependency, no phoning home. Credentials stored in Windows Credential Manager with OS-level encryption.

---

## More Screenshots

### Lite Edition — Query Performance
![Lite Edition — Query Performance](Screenshots/Lite%20Edition%20%E2%80%94%20Query%20Performance.jpg)

### Graphical Plan Viewer
![Graphical plan viewer with missing index suggestions and operator analysis](Screenshots/New%20Query%20Plan%20Viewer.jpg)

### Alert Notifications
![Alert notification](Screenshots/Screenshot%20alert%20notification%20or%20email.jpg)

### MCP Server — AI-Powered Analysis
![MCP server analysis](Screenshots/Screenshot%20MCP%20server%20analysis.jpg)

---

## Quick Start — Lite

1. Download **[`PerformanceMonitorLite-win-Setup.exe`](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest)** — a self-contained build, so there is **no .NET runtime to install first**.
2. Run the installer — it installs to `%LocalAppData%\PerformanceMonitorLite`, adds **Start Menu** and **Desktop** shortcuts, and registers the app under **Apps & Features** so it shows up in Windows search and can be uninstalled normally. Auto-update is wired in. Your data goes in `%LocalAppData%\PerformanceMonitorLite-Data`, a separate folder the installer never touches.
3. Launch from the Start Menu or Desktop shortcut.
4. Click **+ Add Server**, enter connection details, test, save.
5. Double-click the server in the sidebar to connect.

Data starts flowing within 1–5 minutes. That's it. No installation on your server, no Agent jobs, no sysadmin required.

**Taking the portable ZIP (`PerformanceMonitorLite-<version>.zip`) instead?** It is a self-contained win-x64 build too, so there is **nothing to install first** — unzip it anywhere and run `PerformanceMonitorLite.exe`. A stock Windows Server with no .NET on it at all is fine.

That was not always true: through 3.5.0 the ZIP was a portable build with no runtime of its own, and on a machine without .NET it died on the host’s own `You must install .NET to run this application` before a line of Lite’s code ran. Pinning the ZIP to win-x64 removed the requirement **and halved the download** — the old build shipped ~537&nbsp;MB of `runtimes\` for macOS, Linux, ARM and musl targets Windows can never load, which cost far more than bundling the runtime does. See [`Lite/README.md`](Lite/README.md#prerequisites) for the detail.

**Upgrading from zip?** Click **Import Settings** then **Import Data** in the sidebar and point both at your old Lite folder. Settings imports server connections, alert thresholds, SMTP config, and schedules. Data imports historical DuckDB + Parquet archives. **Auto-update users** (installed via Setup.exe) get updates automatically — no manual import needed.

> **Warning — upgrading an install older than the data-relocation fix:** on those versions Lite kept its data *inside* the install directory, and re-running `Setup.exe` over an existing install deletes that directory before the new build ever runs. It takes the DuckDB store, the Parquet archive, the logs, and `settings.json` with it. Upgrade **in place** instead: the **About** button in the left sidebar downloads and applies the update without touching your data, or extract the portable ZIP over your existing copy. From this release on, data lives outside the install directory and is moved there automatically on first start, so `Setup.exe` is safe again. (Your monitored-server list is unaffected either way — it lives in `%ProgramData%\PerformanceMonitorLite\config` — and so are the passwords in Windows Credential Manager.)

**Always On AG?** Enable **ReadOnlyIntent** in the connection settings to route Lite's monitoring queries to a readable secondary, keeping the primary clear. Enable **MultiSubnetFailover** for multi-subnet failover scenarios.

### Lite Collectors

42 collectors run on independent, configurable schedules (the long-running-query completion trace is opt-in and ships disabled):

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
| plan_correction | 5 min | `sys.dm_db_tuning_recommendations` + `sys.database_automatic_tuning_options` (automatic plan correction: FORCE_LAST_GOOD_PLAN enablement plus the engine's live regression recommendations, with the regressed query's text resolved through Query Store) |
| plan_cache_stats | 5 min | `sys.dm_exec_cached_plans` (single-use vs reused plan-cache bloat) |
| system_health_events | 5 min | `system_health` XE ring buffer (not Azure SQL DB) |
| default_trace_events | 5 min | default trace via `sys.fn_trace_gettable` |
| job_history | 5 min | `msdb.dbo.sysjobhistory` retained job-run history (not Azure SQL DB) |
| agent_status | 5 min | `sys.dm_server_services` + `msdb.dbo.sysjobschedules` (not Azure SQL DB / RDS) |
| running_jobs | 5 min | `msdb` job history with duration vs avg/p95 |
| database_size_stats | 1 hour | `sys.master_files` + `FILEPROPERTY` + `dm_os_volume_stats` |
| pvs_stats | 1 hour | `sys.dm_tran_persistent_version_store_stats` + `sys.databases` (ADR persistent version store size and cleanup state per database; SQL Server 2019+ only, always collected on Azure SQL DB) |
| query_store_health | 1 hour | `sys.database_query_store_options` per database (actual vs desired state, readonly_reason, storage used vs cap, cleanup thresholds, runtime-stats interval length; SQL Server 2016+, one row per database with OFF recorded explicitly) |
| server_properties | on connect | `SERVERPROPERTY()` hardware and licensing metadata |
| index_object_stats | Daily | `sys.dm_db_partition_stats` + `sys.dm_db_index_usage_stats` + `sys.dm_db_index_operational_stats` |
| server_config | On connect | `sys.configurations` |
| database_config | On connect | `sys.databases` |
| database_scoped_config | On connect | Database-scoped configurations |
| trace_flags | On connect | `DBCC TRACESTATUS` |

Darling runs this same shared collector set across a fleet of servers (latch stats, spinlock stats, CPU scheduler, plan cache, and system_health parsing are now part of the shared catalog above, collected by Lite too) — see the [Darling collector reference](Darling/README.md).

### Lite Data Storage

All data is stored in `%LOCALAPPDATA%\PerformanceMonitorLite-Data\` — a different folder from the install directory (`%LOCALAPPDATA%\PerformanceMonitorLite\`), so neither an in-app update nor re-running `Setup.exe` can disturb it. Data from an older install is moved into the new folder automatically the first time this version starts.

- **Hot data** in DuckDB 1.5.2 — non-blocking checkpoints, free block reuse, stable file size without periodic resets
- **Archive** to Parquet with ZSTD compression (~10x reduction) — automatic monthly compaction keeps file count low (~75 files vs thousands)
- **Retention**: 3-month calendar-month rolling window
- Typical size: ~50–200 MB per server per week

### Lite Configuration

| File | Location | Purpose |
|---|---|---|
| `servers.json` | `%ProgramData%\PerformanceMonitorLite\config\` (machine-wide) | Server connections, shared across all Windows users on the machine. Passwords stay per-user in Windows Credential Manager. Optional **Utility Database** per server for community procs installed outside master. |
| `settings.json` | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\` (per-user) | Retention, MCP server, startup behavior, alert thresholds, SMTP configuration |
| `collection_schedule.json` | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\` (per-user) | Per-collector enable/disable and frequency |
| `ignored_wait_types.json` | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\` (per-user) | 124 benign wait types excluded by default |

When a second Windows user on the same machine launches Lite, they see the shared `servers.json` immediately. SQL Auth and Entra MFA passwords are scoped to each user's own Credential Manager, so they'll be prompted once per server; Windows Auth works without any prompt.

---

## Quick Start — Darling (headless)

**Darling** is the always-on edition for teams that want 24/7 collection without a desktop app driving it: a Windows service collects from your servers around the clock into a central PostgreSQL store (TimescaleDB is detected and adopted automatically for compression and chunk-based retention), and a detached WPF viewer reads that store from any seat. It runs the same monitoring brain as Lite — collectors, alert engine, and analysis pipeline shared at the library level — with alerts over email and webhooks (Teams, Slack, PagerDuty, and generic HTTP POST) and the same MCP tool surface available on request. An optional built-in read-only **web dashboard** (off by default, its own port 5153) serves the fleet overview, per-server drill-down, and alert history to any browser, so operators can watch the fleet without installing the viewer.

1. Download **`PerformanceMonitorDarling-<version>.zip`** from the [latest release](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest) — the signed service and viewer with the bundled PostgreSQL + TimescaleDB runtime beside the service exe, so a from-zero install needs no database provisioning.
2. Copy `darling.sample.json` to `darling.json` and add your servers (and optional SMTP / webhook delivery). In managed mode the service unpacks and runs its own PostgreSQL — no external database to set up.
3. Run the service (console for a trial, or install it as a Windows service). It seeds the store and begins collecting on the same default cadences and retention horizons as a fresh Lite install.
4. Start the **Darling Viewer**. It is in the same zip, under `viewer\`, and `install-darling.ps1` leaves a Desktop shortcut. On the service host there is nothing to point at anything: it finds the same `darling.json`, derives the store connection, and opens on the fleet. For a seat on another machine, run `--export-viewer-config` on the service host and copy the folder it writes.
5. Optionally turn on the two off-by-default surfaces: `--enable-web` serves the browser dashboard on port 5153, `--enable-mcp` serves the MCP endpoint on 5152. Both take effect live, no restart.

**Never done this before?** [**docs/uat-onboarding.md**](docs/uat-onboarding.md) is the ordered path from a downloaded zip to all three surfaces — the WPF viewer, the web dashboard, and MCP — with the log line, HTTP response, or screen that proves each step worked, and the handful of things that reliably catch people out.

Configuration is a single JSON file with no schedule knobs. See the **[Darling operator guide](Darling/README.md)** for the configuration reference, permissions, and operations.

---

## Edition Comparison

| Capability | Lite | Darling | Dashboard *(deprecated)* |
|---|---|---|---|
| Target server installation | None | None | Required |
| Runs collection | On-demand desktop app | 24/7 Windows service | SQL Agent on the target |
| Multi-server from one seat | Built-in | Built-in (central store) | Per-server install |
| Data storage | DuckDB + Parquet (local) | PostgreSQL + TimescaleDB (bundled) | SQL Server (on target) |
| Azure SQL Database | Supported | Supported | Not supported |
| Azure SQL MI / AWS RDS | Supported | Supported | Supported |
| Graphical plan viewer | Built-in, 30-rule PlanAnalyzer | Built-in, 30-rule PlanAnalyzer | Built-in, 30-rule PlanAnalyzer |
| Standalone plan viewer | Open/paste/drag `.sqlplan` | Open/paste/drag `.sqlplan` | Open/paste/drag `.sqlplan` |
| Alerts (tray + email + webhooks) | Yes | Email + webhooks (headless) | Yes |
| Themes | Dark and light | Dark and light | Dark and light |
| Portability | Single executable | Portable service + viewer zip | Server-bound |
| MCP server (LLM integration) | Built-in (77 tools) | On request | Built into Dashboard (66 tools) |

---

## Tabs

The **Lite** app and the **Darling** viewer share the same tab layout (the viewer is Lite's front end reading a Postgres store instead of local DuckDB):

| Tab | Contents |
|---|---|
| **Overview** | 2x2 resource chart grid (CPU, Memory, Wait Stats, TempDB) with drill-down |
| **Active Queries** | Running queries with session details, wait types, blocking, DOP, memory grants |
| **Wait Stats** | Filterable wait statistics chart with delta calculations |
| **CPU** | SQL Server CPU vs Other Processes over time |
| **Memory** | Physical memory overview, SQL Server memory trend, memory clerk breakdown, memory pressure events |
| **Queries** | Performance trends, top queries and procedures by duration, Query Store integration, query heatmap |
| **File I/O** | Read/write I/O trends per database file |
| **TempDB** | Space usage breakdown and TempDB file I/O |
| **Blocking** | Blocking/deadlock trends, blocked process reports, deadlock history, visual block-chain & deadlock-graph viewers |
| **Perfmon** | Selectable SQL Server performance counters over time |
| **Configuration** | Server configuration, database configuration, scoped configuration, trace flags |
| **FinOps** | Utilization & provisioning analysis, database resource breakdown, storage growth (7d/30d), idle database detection, index analysis via sp_IndexCleanup, per-object table/index size, growth, usage, and locking/contention analysis, application connections, server inventory, cost optimization recommendations, column-level filtering on all grids |
| **Recommendations** | Prioritized findings drawn from collected metrics, grouped into incidents, each card showing the affected database, the recommendation, the reasoning behind it, and a copyable MCP investigation prompt |

Both feature auto-refresh, configurable time ranges, chart drill-down to Active Queries, right-click CSV export, system tray integration, dark and light themes, and timezone display options (server time, local time, or UTC). The Darling viewer adds a fleet sidebar and per-server tabs; see [Darling/README.md](Darling/README.md). The deprecated Dashboard's six-tab-group layout is documented in [deprecated/Dashboard/README.md](deprecated/Dashboard/README.md).

---

## Alerts & Notifications

Every edition includes a real-time alert engine that monitors for performance issues and sends notifications via system tray balloons (Lite/Dashboard), email, and webhooks.

### Alert Types

| Metric | Default Threshold | Description |
|---|---|---|
| **Blocking** | 5 seconds | Fires when the longest blocked session exceeds the threshold |
| **Deadlocks** | 1 | Fires when new deadlocks are detected since the last check |
| **Poison waits** | 100 ms avg | Fires when any poison wait type exceeds the average-ms-per-wait threshold |
| **Long-running queries** | 5 minutes | Fires when any query exceeds the elapsed-time threshold |
| **TempDB space** | 80% | Fires when TempDB usage exceeds the percentage threshold. Measured against tempdb's **growth ceiling** (`SUM(max_size)` over the ROWS files) where there is one, and against the current allocation where the files grow without limit — so the percentage means "distance to the point where tempdb cannot grow further" on every engine |
| **Long-running agent jobs** | 3× average | Fires when a job's current duration exceeds a multiple of its historical average |
| **High CPU** | 80% | Fires when total CPU (SQL + other) exceeds the threshold |
| **Volume free space** | 10% or 5 GB free | Fires when a monitored volume's free space drops below the percentage or absolute threshold (either check can be disabled). Never fires on Azure SQL Database. |
| **Failed agent job** | 60-minute lookback | Fires when a SQL Agent job run fails within the lookback window. Skipped on Azure SQL Database. |
| **Server unreachable** | N/A | Fires when a monitored server goes offline or comes back online |
| **Collection stopped** | No run in 30 min | Fires when collection stalls (Agent/service stopped or collectors erroring). App-computed, so it survives the collector being off; clears with a "Collection Resumed" notice. Never fires on Azure SQL Database; degrades gracefully where msdb is restricted (e.g. AWS RDS). |

All thresholds are configurable in Settings.

**Poison wait types** monitored: [`THREADPOOL`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-wait-stats-transact-sql#threadpool) (worker thread exhaustion), [`RESOURCE_SEMAPHORE`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-wait-stats-transact-sql#resource_semaphore) (memory grant pressure), and [`RESOURCE_SEMAPHORE_QUERY_COMPILE`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-wait-stats-transact-sql#resource_semaphore_query_compile) (compilation memory pressure). These waits indicate severe resource starvation and should never occur under normal operation.

### Notification Channels

- **System tray** — balloon notifications with a configurable per-metric cooldown (default: 5 minutes)
- **Email (SMTP)** — styled HTML emails with a configurable per-metric cooldown (default: 15 minutes), plus configurable SMTP settings (server, port, SSL, authentication, recipients)
- **Microsoft Teams** — styled card messages with color-coded severity indicators sent to Teams channels via incoming webhooks
- **Slack** — styled messages with color-coded severity indicators sent to Slack channels via incoming webhooks
- **PagerDuty** — native Events API v2 integration that triggers PagerDuty incidents with proper severity mapping (critical/error/warning/info) and automatic incident correlation via dedup_key. Supports both US and EU data centers
- **Generic webhook** — HTTP POST to any configurable endpoint for integration with other alerting systems (Opsgenie, n8n, custom automation, etc.)

All cooldown periods are independently configurable in Settings under the Performance Alerts section.

#### PagerDuty Setup

To receive alerts in PagerDuty:

1. In PagerDuty, create a service and add an **Events API v2** integration
2. Copy the 32-character **Integration Key** (routing key)
3. In Performance Monitor Settings, enable PagerDuty notifications and paste the routing key
4. Check **Use EU data center** if your PagerDuty account is in the EU region
5. Click **Send Test Notification** to verify the integration

PagerDuty alerts include the metric name, server, current value, threshold, and severity. Repeated alerts for the same incident automatically correlate into a single PagerDuty incident via the dedup_key, preventing alert fatigue.

### Email Alerts

Alert emails include:

- **Metric summary** — what triggered the alert, current value vs threshold
- **Detail section** — recent blocking chains or deadlock participants with query text, wait times, lock modes, database names, and client application
- **XML attachment** — full `blocked_process_report.xml` or `deadlock_graph.xml` for offline analysis

### Alert Behavior

- **Resolved notifications** — when a condition clears (e.g., blocking ends), a "Cleared" notification fires
- **Server silencing** — right-click a server to acknowledge alerts, silence all alerts, or unsilence
- **Always-on** — the alert engine runs independently of which tab is active, including when minimized to the system tray
- **Alert history** — Lite logs alerts to DuckDB (`config_alert_log`); Darling logs to its Postgres store; both are accessible via MCP
- **Alert muting** — create rules to suppress specific recurring alerts while still logging them. Rules match on server name, metric type, database, query text, wait type, or job name (AND logic across fields). Access via Settings → Manage Mute Rules, or right-click an alert in the Alert History tab. The context menu offers **Mute This Alert** (pre-fills server + metric) and **Mute Similar Alerts** (pre-fills metric only, matching across all servers). Muted alerts appear grayed out and are still recorded for auditability. Rules support optional expiration (1h, 24h, 7 days, or permanent).
- **Alert details** — right-click any alert in the Alert History tab and choose **View Details** for core fields (time, server, metric, value, threshold, notification type, status) plus context-sensitive details that vary by metric.

---

## Agent Job Monitoring

Every edition monitors currently running SQL Agent jobs and flags jobs running longer than expected.

| Metric | How It Works |
|---|---|
| **Current duration** | Elapsed time since the job started |
| **Average duration** | Historical mean from successful completions in `msdb.dbo.sysjobhistory` |
| **p95 duration** | 95th percentile from historical completions |
| **Running long flag** | Set when current duration exceeds the p95 threshold |

Lite and Darling query `msdb` directly on each collection cycle; all editions expose this data through the MCP `get_running_jobs` tool. Gracefully skipped on Azure SQL Database, AWS RDS for SQL Server, and environments without SQL Server Agent.

---

## MCP Server (LLM Integration)

Every edition includes an embedded [Model Context Protocol](https://modelcontextprotocol.io) server that exposes monitoring data to LLM clients like Claude Code and Cursor.

The tools answer from the monitor's own collected store, and that is the boundary worth stating up front: **no MCP tool runs SQL an AI client wrote against your monitored servers.** The handful of write-capable tools (Darling's view authoring, alert tuning, and server onboarding) change the monitor's own configuration, and the only live contact with a monitored server — the analysis plan fetch and the onboarding connection probe — runs the product's own fixed, read-only queries under the same least-privilege monitoring login the collectors use.

### Setup

1. Enable the MCP server in Settings (checkbox + port, default `5151`)
   - The port must be between **1024** and **65535**. Ports 0–1023 are well-known privileged ports reserved by the operating system.
   - On save, the app checks whether the chosen port is already in use and warns you if there is a conflict.
   - On startup, the app verifies the port is available before starting the MCP server.
2. Register with Claude Code:

```
claude mcp add --transport http --scope user sql-monitor http://localhost:5151/
```

3. Open a new Claude Code session and ask questions like:
   - "What servers are being monitored?"
   - "What are the top wait types on my server?"
   - "Are there any blocking or deadlock issues?"
   - "Show me CPU utilization for the last 4 hours"
   - "What are the most expensive queries by CPU?"

### Available Tools

**Lite** exposes 77 tools; **Darling** exposes the analysis + data-read surface on request; the deprecated **Dashboard** exposes 66 (see [deprecated/Dashboard/README.md](deprecated/Dashboard/README.md)). Core tools are shared.

| Category | Tools |
|---|---|
| Discovery | `list_servers` |
| Health | `get_server_summary`, `get_collection_health`, `get_daily_summary` |
| Alerts | `get_alert_history`, `get_alert_settings`, `get_mute_rules` |
| Waits | `get_wait_stats`, `get_wait_types`, `get_wait_trend`, `get_waiting_tasks` |
| Queries | `get_top_queries_by_cpu`, `get_top_procedures_by_cpu`, `get_query_store_top`, `get_query_duration_trend`, `get_query_trend` |
| Active Queries | `get_active_queries` |
| CPU | `get_cpu_utilization` |
| Memory | `get_memory_stats`, `get_memory_trend`, `get_memory_clerks`, `get_memory_grants`, `get_resource_semaphore` |
| Blocking | `get_deadlocks`, `get_deadlock_detail`, `get_blocked_process_reports`, `get_blocked_process_xml`, `get_blocking_trend`, `get_deadlock_trend` |
| I/O | `get_file_io_stats`, `get_file_io_trend` |
| TempDB | `get_tempdb_trend` |
| Perfmon | `get_perfmon_stats`, `get_perfmon_trend` |
| Jobs | `get_running_jobs` |
| Configuration | `get_server_config`, `get_database_config`, `get_database_scoped_config`, `get_query_store_health`, `get_trace_flags` |
| Server Info | `get_server_properties`, `get_database_sizes` |
| Object/Index Stats | `get_table_index_sizes`, `get_index_usage`, `get_object_locking` |
| Sessions | `get_session_stats` |
| System Events | `get_memory_pressure_events` |
| Latches & Spinlocks | `get_latch_stats`, `get_spinlock_stats` |
| Plan Cache & Scheduler | `get_plan_cache_bloat`, `get_cpu_scheduler_pressure` |
| Long Queries | `get_long_query_completions` |
| Default Trace | `get_default_trace_events` |
| Config Changes | `get_server_config_changes`, `get_database_config_changes`, `get_trace_flag_changes` |
| Health Parser | `get_health_parser_system_health`, `get_health_parser_severe_errors`, `get_health_parser_io_issues`, `get_health_parser_scheduler_issues`, `get_health_parser_memory_conditions`, `get_health_parser_cpu_tasks`, `get_health_parser_memory_broker`, `get_health_parser_memory_node_oom` |
| Plan Analysis | `analyze_query_plan`, `analyze_procedure_plan`, `analyze_query_store_plan`, `analyze_plan_xml`, `get_plan_xml` |
| Diagnostic Analysis | `analyze_server`, `get_analysis_facts`, `compare_analysis`, `audit_config`, `get_analysis_findings`, `mute_analysis_finding` |

Most tools accept optional `server_name` and `hours_back` parameters. If only one server is configured, `server_name` is auto-resolved. Every tool that takes `hours_back` also takes an optional `as_of` — an ISO-8601 UTC instant that moves the END of the window off "now", so a past incident is one call (`as_of` its end, `hours_back` its length) rather than a very wide window filtered by hand. The MCP server binds to `localhost` only and does not accept remote connections. (Darling adds windowed-trend and fleet-overview tools plus agent-driven write tools — Custom Views authoring, alert-settings and mute-rule tuning, and bulk add/remove servers — and supports an opt-in LAN endpoint — see [Darling/README.md](Darling/README.md).)

---

## Performance Impact

### On Monitored Servers

- All queries use `READ UNCOMMITTED` isolation
- Configurable collection intervals
- Lite: max 7 concurrent SQL connections, 30-second command timeout
- Darling: the service collects on the shared default cadences; TimescaleDB compresses the store

### Local Resources (Lite)

- DuckDB: ~50–200 MB per server per week
- Parquet archives: ~10x compression with ZSTD
- ScottPlot charts use hardware-accelerated rendering

---

## Troubleshooting

### Lite

Application logs are written to the `logs/` folder. Collection success/failure is also logged to the `collection_log` table in DuckDB.

Common issues:

1. **No data after connecting** — Wait for the first collection cycle (1–5 minutes). Check logs for connection errors.
2. **Query Store tab empty** — Query Store must be enabled on the target database (`ALTER DATABASE [YourDB] SET QUERY_STORE = ON`).
3. **Blocked process reports empty** — Lite attempts to auto-configure the blocked process threshold to 5 seconds via `sp_configure`. On **AWS RDS**, `sp_configure` is not available — set `blocked process threshold (s)` through an RDS Parameter Group (see [Platform Notes](#platform-notes) below). On **Azure SQL Database**, the threshold is fixed at 20 seconds and cannot be changed. If you still see no data on other platforms, verify the login has `ALTER SETTINGS` permission.
4. **Connection failures** — Verify network connectivity, firewall rules, and that the login has the required [permissions](#permissions). For Azure SQL Database, use a contained database user with `VIEW DATABASE STATE`.
5. **FinOps Index Analysis hangs, times out, or returns `Msg 229` on `sql_expression_dependencies`** — see [FinOps Index Analysis](#finops-index-analysis-per-database-grants) below for the full per-database grant set that fixes both failure modes.

**Darling** troubleshooting (service logs, store connectivity, permissions) is in the [Darling operator guide](Darling/README.md). **Dashboard** (Full edition) troubleshooting is in [deprecated/Dashboard/README.md](deprecated/Dashboard/README.md).

---

## Authentication

Every edition supports five authentication types, defined once in `PerformanceMonitor.Common.AuthenticationTypes` and shared by Lite, Darling, the Dashboard, and the CLI installer:

| Type | Interactive? | Credential stored? | Where |
|---|---|---|---|
| Windows | No | None | — |
| SQL Server | No | Password | Windows Credential Manager |
| Entra ID (MFA) | Yes, once per session | None | — |
| Service Principal | No | Client secret | Windows Credential Manager |
| Managed Identity | No | None | — |

**Managed Identity and Service Principal** are non-interactive Azure AD (Entra ID) authentication modes, added for fleet onboarding of Azure SQL Database / Managed Instance without a per-server interactive MFA prompt (see [#1038](https://github.com/erikdarlingdata/PerformanceMonitor/issues/1038)). Both map directly to `Microsoft.Data.SqlClient`'s native `SqlAuthenticationMethod` (`ActiveDirectoryServicePrincipal` / `ActiveDirectoryManagedIdentity`) — PerformanceMonitor never acquires, caches, or stores a token itself; the official Microsoft driver handles that internally.

- **Managed Identity** requires the machine running the app/service to itself be an Azure resource (VM, App Service, etc.) with a system- or user-assigned managed identity. That identity is then provisioned as a user directly on each target database (see [Permissions](#permissions) below). Nothing is stored locally.
- **Service Principal** uses an Entra app registration's client id + secret. The client id is non-secret and stored in config; the secret is stored only in Windows Credential Manager, same as a SQL auth password.

### Credential Profiles (Lite, fleet onboarding)

For a fleet of servers sharing one identity — one managed identity or one service principal used across many Azure SQL databases — Lite has **Credential Profiles**: a named, reusable credential that any number of server entries can reference instead of each one carrying its own inline auth. Create one under **Manage Servers → Credential Profiles…**, then point server entries at it. Profiles live in `profiles.json` alongside `servers.json`; a Managed Identity profile stores no secret, and Service Principal / SQL Server profile secrets live in Windows Credential Manager, never in the JSON file.

---

## Permissions

### Lite / Darling (On-Premises)

Nothing is installed on the target server. Below is the full least-privilege grant set, verified live against SQL Server 2025 with a scratch login ([#1823](https://github.com/erikdarlingdata/PerformanceMonitor/issues/1823)). **This block is the authoritative copy** — Lite and Darling need the identical grants, so the Darling operator guide points here rather than keeping a second list that can drift out of step:

```sql
USE [master];

/* Server-scoped DMVs - the required core. Also implies VIEW DATABASE STATE in every database. */
GRANT VIEW SERVER STATE TO [YourLogin];

/* Enter every current and future database, with no per-database users to maintain. The
   per-database collectors (database scoped config, index/object stats, database size,
   Query Store) skip databases the login cannot enter. */
GRANT CONNECT ANY DATABASE TO [YourLogin];

/* Catalog visibility. Catalog views enforce permissions by HIDING ROWS, not by erroring:
   without this, sys.tables / sys.indexes (the index and object collectors) and the AG
   catalog views return zero rows that look exactly like "no data" (see the AG note below). */
GRANT VIEW ANY DEFINITION TO [YourLogin];

/* The deadlock / blocked-process Extended Events sessions. */
GRANT ALTER ANY EVENT SESSION TO [YourLogin];

/* Optional: the default-trace collector reads sys.traces, which requires ALTER TRACE -
   VIEW SERVER STATE does not cover it. Note ALTER TRACE is not read-only (it also permits
   creating and altering traces, and implies SHOWPLAN in every database); withhold it and
   the collector records a PERMISSIONS skip instead. */
GRANT ALTER TRACE TO [YourLogin];

/* Optional: lets the app bootstrap 'blocked process threshold (s)' to 5 when it is still 0.
   sp_configure + RECONFIGURE require ALTER SETTINGS; without it the bootstrap is logged and
   skipped, blocked process reports stay empty, and you set the threshold yourself (or via an
   RDS Parameter Group). Blocking stays visible either way through the DMV blocking snapshot. */
GRANT ALTER SETTINGS TO [YourLogin];

/* Optional: SQL Agent job monitoring + failed-job alerts. Direct table grants, deliberately
   NOT SQLAgentReaderRole: that role gates the sp_help_job* procedures, which this product
   never calls, and grants NO SELECT on the tables the collectors actually read - with only
   the role, every Agent collector fails with error 229. */
USE [msdb];
CREATE USER [YourLogin] FOR LOGIN [YourLogin];
GRANT SELECT ON dbo.sysjobs         TO [YourLogin];
GRANT SELECT ON dbo.sysjobactivity  TO [YourLogin];
GRANT SELECT ON dbo.sysjobhistory   TO [YourLogin];
GRANT SELECT ON dbo.sysjobschedules TO [YourLogin];
GRANT SELECT ON dbo.syscategories   TO [YourLogin];
GRANT SELECT ON dbo.syssessions     TO [YourLogin];
GRANT EXECUTE ON dbo.agent_datetime TO [YourLogin];
```

Two operational notes. The msdb grants live inside a system database that SQL Server setup can rewrite — re-check them after a CU or version upgrade. And if a security review rejects `CONNECT ANY DATABASE`, the documented fallback is a real user per database (the same shape as the [FinOps loop below](#finops-index-analysis-per-database-grants), minus the `sql_expression_dependencies` grant) — at the cost of not covering databases created later.

**Availability Groups need a second grant.** The `ag_replica_states` / `ag_database_replica_states` collectors read the AG *catalog views* (`sys.availability_groups`, `sys.availability_replicas`) alongside the `sys.dm_hadr_*` DMVs. The DMVs are covered by `VIEW SERVER STATE`, but [the catalog views require `VIEW ANY DEFINITION`](https://learn.microsoft.com/en-us/sql/database-engine/availability-groups/windows/monitor-availability-groups-transact-sql) — and catalog views enforce that by *hiding rows*, not by raising an error. So on a real AG cluster a login with only `VIEW SERVER STATE` gets zero rows, which looks exactly like a server with no availability groups. If your AG dashboards are empty, this grant is why.

Darling uses the same target-server grants; its bundled PostgreSQL store and service account are covered in the [Darling operator guide](Darling/README.md). The deprecated Full edition's install/least-privilege grants are in [deprecated/Dashboard/README.md](deprecated/Dashboard/README.md).

### FinOps Index Analysis (per-database grants)

Applies to **all editions**. The FinOps Index Analysis tab runs `sp_IndexCleanup` against each user database you ask it to inspect, executing as your app login. The server-level grants above (`VIEW SERVER STATE` and friends) are *not* sufficient on their own — the login also needs a user mapping in every user database it will analyze, plus `VIEW DATABASE STATE`, `VIEW DEFINITION`, and `SELECT` on `sys.sql_expression_dependencies` in each.

The third grant is the easy one to miss: by default only members of `db_owner` have `SELECT` on `sys.sql_expression_dependencies`, and `VIEW DEFINITION` does not include it. `sp_IndexCleanup` queries that catalog view (via three-part name to the target database) when checking for computed columns and check constraints that reference UDFs, so the failure only surfaces on databases that actually have those — which is why a smoke-test database may pass and a real workload database fails with `Msg 229`.

For each target user database:

```sql
USE [YourTargetDatabase];
CREATE USER [SQLServerPerfMon] FOR LOGIN [SQLServerPerfMon];
GRANT VIEW DATABASE STATE                       TO [SQLServerPerfMon];
GRANT VIEW DEFINITION                           TO [SQLServerPerfMon];
GRANT SELECT ON sys.sql_expression_dependencies TO [SQLServerPerfMon];
```

Or apply broadly with `sp_MSforeachdb`:

```sql
EXEC sp_MSforeachdb N'
USE [?];
IF DB_ID() > 4 AND DATABASEPROPERTYEX(DB_NAME(), ''Updateability'') = ''READ_WRITE''
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = ''SQLServerPerfMon'')
        CREATE USER [SQLServerPerfMon] FOR LOGIN [SQLServerPerfMon];
    GRANT VIEW DATABASE STATE                       TO [SQLServerPerfMon];
    GRANT VIEW DEFINITION                           TO [SQLServerPerfMon];
    GRANT SELECT ON sys.sql_expression_dependencies TO [SQLServerPerfMon];
END';
```

**Symptoms if missing.** Two distinct failure modes depending on which grant is absent:

- *No user mapping in the target database* — `sp_IndexCleanup` can hang at 100% CPU with no waits and never return, instead of failing fast with `Msg 916`. It's a SQL Server engine bug where a permission check at execute time is misclassified as "this plan needs to be recompiled," producing an infinite recompile loop. Reproduces on SQL Server 2016 SP3 through 2025 CU4.
- *User is mapped with `VIEW DATABASE STATE` + `VIEW DEFINITION` but no `SELECT` on `sys.sql_expression_dependencies`* — fails fast with `Msg 229, Level 14, State 5: The SELECT permission was denied on the object 'sql_expression_dependencies'` the moment a database with a UDF-bound computed column or check constraint is reached.

Adding all three grants eliminates both. See issue [#915](https://github.com/erikdarlingdata/PerformanceMonitor/issues/915) for the full diagnosis.

### Azure SQL Database (Lite / Darling)

Azure SQL Database doesn't support server-level logins. Create a **contained database user** directly on the target database:

```sql
-- Connect to your target database (not master)
CREATE USER [SQLServerPerfMon] WITH PASSWORD = 'YourStrongPassword';
GRANT VIEW DATABASE STATE TO [SQLServerPerfMon];
GRANT VIEW DEFINITION     TO [SQLServerPerfMon];
```

For [Managed Identity or Service Principal](#authentication) authentication, create the contained user from the identity's display name instead of a password — an Azure AD admin must already be configured on the logical server:

```sql
-- Connect to your target database (not master)
CREATE USER [your-managed-identity-or-app-registration-name] FROM EXTERNAL PROVIDER;
GRANT VIEW DATABASE STATE TO [your-managed-identity-or-app-registration-name];
GRANT VIEW DEFINITION     TO [your-managed-identity-or-app-registration-name];
```

For a large fleet, grant an Entra **group** instead of provisioning each identity individually where your architecture allows it. SQL Agent and msdb are not available on Azure SQL Database — those collectors are skipped automatically.

> `VIEW DEFINITION` is the database-scoped form of the catalog-visibility grant explained above: without it, `sys.tables` / `sys.indexes` return zero rows to the index and object collectors — silently, not as an error. `VIEW DATABASE STATE` is what lets the `sys.dm_os_*` DMVs return server hardware inventory (CPU count, physical memory, socket and core topology) and memory metrics on each monitored database. If the contained user lacks it, edition, version, and storage still resolve from permission-free scalars, so the FinOps **Server Inventory** grid keeps the server's row and shows a non-alarming "Hardware Note" that hardware inventory is unavailable, instead of dropping the entire row (#1535).

### Azure SQL Managed Instance

Works like on-premises. Use server-level logins with `VIEW SERVER STATE`. SQL Agent is available.

### AWS RDS for SQL Server

For ongoing collection, `VIEW SERVER STATE` and msdb access work the same as on-premises, but `sp_configure` is not available (use RDS Parameter Groups instead — see [Platform Notes](#platform-notes)).

---

## Platform Notes

### AWS RDS Parameter Group Configuration

`sp_configure` is not available on AWS RDS for SQL Server. Features that depend on server-level configuration must be set through **AWS RDS Parameter Groups** instead.

**Blocked process threshold** — Enables blocked-process-report collection (the richer XE-sourced blocking detail). Without it the blocked-process-report XE will not fire on RDS, but blocking is still captured by the always-on `dmv_blocking_snapshot` collector, so the blocking grid and block-chain viewer stay populated regardless.

1. Open the [AWS RDS Console](https://console.aws.amazon.com/rds/) and navigate to **Parameter groups**
2. Create a new parameter group (or modify the one attached to your instance) — Family `sqlserver-ee-16.0` (or your edition/version), Type DB Parameter Group
3. Search for `blocked process threshold (s)` and set it to `5` (seconds)
4. Apply the parameter group to your RDS instance (may require a reboot if the parameter is static)
5. Verify: `SELECT c.name, c.value_in_use FROM sys.configurations AS c WHERE c.name = N'blocked process threshold (s)';`

**Deadlocks** — No parameter group configuration is required. The SQL Server deadlock monitor runs automatically on all platforms, and the `xml_deadlock_report` Extended Event fires without any threshold setting.

**Azure SQL Database** — The blocked process threshold is fixed at 20 seconds and cannot be changed. The `blocked_process_report` event fires automatically when blocking exceeds this duration.

---

## Folder Structure

```
PerformanceMonitor/
│
│   Lite Edition — standalone desktop app, nothing installed on server
├── Lite/                  # Lite desktop application (WPF)
│
│   Darling Edition — headless service + bundled Postgres + viewer
├── Darling/               # Service, storage, analysis, viewer (see Darling/README.md)
│
│   Shared monitoring brain (collectors, alerting, analysis, MCP, UI)
├── PerformanceMonitor.*/  # Shared libraries used by every edition
│
│   Full Edition (deprecated) — server-installed collectors + separate dashboard
├── install/               # SQL installation scripts (Full edition)
├── upgrades/              # Version-specific upgrade scripts (Full edition)
├── deprecated/Installer/  # CLI installer for the Full edition database (see deprecated/Installer/README.md)
├── deprecated/Installer.Core/  # Shared installation library (CLI + Dashboard)
├── deprecated/Dashboard/  # Full edition dashboard app (see deprecated/Dashboard/README.md)
│
└── README.md              # This file
```

---

## Building from Source

All projects target .NET 10.0.

```
# Lite Edition
dotnet build Lite/PerformanceMonitorLite.csproj

# Darling Edition (service + viewer)
dotnet build Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj
dotnet build Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj

# Full Edition (deprecated) — Dashboard app + CLI installer
dotnet build deprecated/Dashboard/Dashboard.csproj
dotnet publish deprecated/Installer/PerformanceMonitorInstaller.csproj -c Release
```

---

## Support & Sponsorship

**This project is free and open source under the MIT License.** The software is fully functional with no features withheld — every user gets the same tool, same collectors, same MCP integration.

However, some organizations have procurement or compliance policies that require a formal vendor relationship, a support agreement, or an invoice on file before software can be deployed to production. If that sounds familiar, two commercial support tiers are available:

| Tier | Annual Cost | What You Get |
|------|-------------|--------------|
| **Supported** | $500/year | Email support (2-business-day response), compatibility guarantees for new SQL Server versions, vendor agreement and invoices for compliance, unlimited instances |
| **Priority** | $2,500/year | Next-business-day email response, quarterly live Q&A sessions, early access to new features, roadmap input, unlimited instances |

Both tiers cover unlimited SQL Server instances. The software itself is identical — commercial support is about the relationship, not a feature gate.

**[Read more about the free tool and commercial options](https://erikdarling.com/free-sql-server-performance-monitoring/)** | **[Purchase a support subscription](https://training.erikdarling.com/sql-monitoring)**

If you find the project valuable, you can also support continued development:

| | |
|---|---|
| **Sponsor on GitHub** | [Become a sponsor](https://github.com/sponsors/erikdarlingdata) to fund new features, ongoing maintenance, and SQL Server version support. |
| **Consulting Services** | [Hire me](https://training.erikdarling.com/sqlconsulting) for hands-on consulting if you need help analyzing the data this tool collects, or expert assistance fixing the issues it uncovers. |

Neither sponsorship nor consulting is required — use the tool freely.

---

## Third-Party Components

### sp_WhoIsActive

- **Author:** Adam Machanic | **License:** GPLv3
- **Repository:** https://github.com/amachanic/sp_whoisactive

### DarlingData

- **Author:** Erik Darling (Darling Data, LLC) | **License:** MIT
- **Repository:** https://github.com/erikdarlingdata/DarlingData

### SQL Server First Responder Kit

- **Author:** Brent Ozar Unlimited | **License:** MIT
- **Repository:** https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for complete license texts.

---

## Sponsors

<table>
  <tr>
    <td><a href="https://signpath.io"><img src="docs/signpath_logo.svg" alt="SignPath" width="40"></a></td>
    <td>Free code signing on Windows provided by <a href="https://signpath.io">SignPath.io</a>, certificate by <a href="https://signpath.org">SignPath Foundation</a></td>
  </tr>
</table>

---

## License

Copyright (c) 2026 Darling Data, LLC. Licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Author

Erik Darling — [erikdarling.com](https://erikdarling.com) — [Darling Data, LLC](https://darlingdata.com)
