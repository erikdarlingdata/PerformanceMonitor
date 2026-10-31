# Tabs

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

The **Lite** app and the **Darling** viewer have the same main-window tabs and nearly the same tabs for one server. The viewer is Lite's front end reading a Postgres store instead of local DuckDB.

### Main window tabs

| Tab | Contents |
|---|---|
| **Overview** | One card per monitored server. The cards can be sorted by CPU % or by name and searched by name. The Darling viewer adds a fleet roll-up above the cards. It shows server counts by health band, fleet totals for blocking and deadlocks, and a worst-first Needs Attention list that opens a server's tab |
| **Alert History** | Logged alerts with a time range, details, dismiss, and mute actions |
| **Job History** | SQL Agent job runs retained from `msdb` |
| **Agent Alerts** | Darling viewer only. One server's SQL Server Agent alerts and the gaps in them. A gap is a missing alert for severities 19 to 25 or errors 823 to 825. It is also an alert that tells no operator, a disabled alert, or an alert that fired and told no one. Pick the server at the top. Azure SQL Database and Azure SQL Managed Instance have no Agent alerts, and the tab says so |
| **Database Protection** | Darling viewer only. One server's last full backup, last log backup and last good CHECKDB for each database, with the age of each. Overdue databases are listed first. A full backup is overdue after 7 days, a log backup after 60 minutes and CHECKDB after 14 days. A database that is offline, restoring or a secondary is not judged, and a database in simple recovery expects no log backup. On Azure SQL Database and Azure SQL Managed Instance the age is shown with the platform named and is never called overdue. On Amazon RDS the backup ages are shown the same way and never called overdue, but CHECKDB is judged as on SQL Server. Pick the server at the top |
| **Fleet Sweeps** | Darling viewer only. The fleet sweep engine's runs inside a time range (one hour by default), the selected sweep in full (changes since the previous sweep, per-server verdicts, the would-have-paged ledger and instrument liveness), and the watch-item worklist. Read only, with the same rows as the web page |
| **Identity Capacity** | Darling viewer only. One server's identity columns that are more than 10 percent used, with a server list and a Refresh button. Each column shows its database, table, type and percent used. It also shows the values left, the rate per day and the days until it runs out. The band is warning from 70 percent and critical from 90 percent. Below the list, each database shows its scan state: scanned clean, has columns above 10 percent, or not yet scanned. A server with no scan shows the reason in place of the lists. Read only, with the same figures as the web page |
| **Alert Rules** | Darling viewer only. A window opened from Alert History, next to Manage Mute Rules. It lists the custom alert rules with their status, scope, expiry, state and last fire. You can add a rule, add one from a template, edit, pause, resume and delete. The form uses the same metric catalog, checks, templates and expiry rules as the web page. The buttons are on only when the store login can insert, update and delete on the rules table |
| **Custom Views** | Darling viewer only. A window opened from Alert History, next to Known Patterns. It lists the shared dashboards and notebooks by folder. You can create, edit, rename, move, delete, import and export them, with the same checks as the web page. **Run view** draws the saved view for one server or for all servers. Table panels are grids, line panels are charts, stat panels are tiles, and a notebook also shows its text cells. A panel that reads a fixed report instead of the composer says "Open it in the web dashboard." and is not run. The viewer has no personal views |
| **Availability Groups** | Replica and per-database health from the Availability Group collectors. The tab is hidden until there is Availability Group data |
| **FinOps** | Twelve sub-tabs. Utilization, Database Resources, Storage Growth, Locking & Contention, Database Sizes, and Version Store (PVS). Optimization, High Impact, Application Connections, and Server Inventory. Index Analysis (runs sp_IndexCleanup) and Recommendations. Column-level filtering on most grids |
| **Recommendations** | Prioritized findings drawn from collected metrics, grouped into incidents, each card showing the affected database, the recommendation, the reasoning behind it, and a copyable MCP investigation prompt |

A **Plan Viewer** tab also appears in the main window when you open a plan from one of these tabs. It is hidden otherwise.

### Web dashboard only

These parts of the web dashboard are not in the Darling viewer, each for a reason.

- **The visual composer and the Alert Notebook.** Both sit on the service's read dispatch, which the viewer does not have. The viewer edits a view as JSON instead.
- **Fixed-report panels in a view.** They need the same read dispatch, so the viewer shows a pointer to the web dashboard.
- **The store-host card.** It checks the machine the service runs on. The viewer cannot see that machine.
- **Backtest.** It is an MCP tool only, so neither the web dashboard nor the viewer has a page for it.

In the viewer, the alert detail window has an "Open this server's tab at this time" link. It does the job of the web Triage page. It opens the tab for the alert's metric over the hour before the alert and the quarter hour after it.

### Tabs for one server

Opening a server from the Overview cards shows these tabs. This list is Lite's. The viewer differences follow the table.

| Tab | Contents |
|---|---|
| **Overview** | Five stacked charts on one shared time axis: SQL CPU, Wait Stats, Blocking, Buffer Pool, and I/O Latency |
| **Wait Stats** | Filterable wait statistics chart with delta calculations |
| **Queries** | Sub-tabs: Performance Trends, Active Queries, Top Queries by Duration, Top Procedures by Duration, Query Store by Duration, Plan Corrections, and Query Heatmap. Active Queries shows running queries with session details, wait types, blocking, DOP, and memory grants |
| **Plan Viewer** | Opens a query plan. It is empty until you choose View Plan or Get Actual Plan on a query row |
| **CPU** | SQL Server CPU vs Other Processes over time |
| **Memory** | Sub-tabs: Overview (physical memory and the SQL Server memory trend), Memory Clerks, Memory Grants, and Memory Pressure Events |
| **File I/O** | Read/write I/O per database file. Sub-tabs: File I/O Latency and File I/O Throughput |
| **tempdb** | Space usage breakdown and tempdb file I/O |
| **Blocking** | Sub-tabs: Trends, Current Waits, Blocked Process Reports, Deadlocks, and Blocking Stats. It has the visual block-chain and deadlock-graph viewers |
| **Perfmon** | Selectable SQL Server performance counters over time |
| **Running Jobs** | SQL Agent jobs that are running now, with start time, current duration, average and P95 duration, and percent of average. It needs access to `msdb` |
| **Configuration** | Sub-tabs: Server Configuration, Database Configuration, Scoped Configuration, Query Store, Automatic Tuning, and Trace Flags |
| **Daily Summary** | A composite health summary for each day (times in UTC). Click a day for detail and drill-in |
| **Latches & Spinlocks** | Latch classes from the latest snapshot, with wait deltas and wait times |
| **CPU Scheduler** | A scheduler chart and a table of the latest scheduler metrics, with warning rows highlighted |
| **Plan Cache** | Total plans, oldest plan age, and a plan cache bloat level |
| **Session Stats** | Session counts over time, with the top application, top host, and databases |
| **Collection Health** | Sub-tabs: Health Summary, Collection Log, and Duration Trends |
| **System Events** | Sub-tabs: Corruption Events, Contention Events, Scheduler Issues, Severe Errors, Memory Conditions, Memory Broker, Memory Node OOM, Significant Waits, CPU Tasks, I/O Issues, and Default Trace |
| **Configuration Changes** | Sub-tabs: Server Config Changes, Database Config Changes, and Trace Flag Changes |
| **Long Queries** | Completions of RPC calls and batches that ran over the duration threshold, with the statement, duration, CPU, and logical reads |

In the Darling viewer, the tabs for one server differ from Lite in these ways:

- CPU has two sub-tabs: CPU Utilization and CPU Scheduler. There is no separate CPU Scheduler tab.
- Memory has a fifth sub-tab, Plan Cache. There is no separate Plan Cache tab.
- Queries adds the sub-tabs Current Active Queries, Query Store Regressions, and Query Store Clutter.

Both feature auto-refresh, chart drill-down to Active Queries, right-click CSV export, system tray integration, Dark, Light and Cool Breeze themes with user-adjustable palette colors (see [Appearance](lite-guide.md#appearance)), and timezone display options (server time, local time, or UTC).

Both also share one time range picker with the web pages. It has presets, calendar periods, and typed ranges such as `last 3 days` or `since 9am`. A text column's filter lists its values to tick, like Excel, and stays set across refreshes and restarts.

Lite has Investigations and Manage Query Tags buttons in its sidebar. The Darling viewer opens Investigations from Settings.

The Darling viewer adds a fleet sidebar and per-server tabs, and it shows a PostgreSQL target on its own set of tabs. See [Darling/README.md](../Darling/README.md). The deprecated Dashboard's six-tab-group layout is documented in the Dashboard README inside the [frozen source zip](../deprecated/README.md).

### The Darling viewer, tab by tab

This section moved here from the operator page for the viewer, so that the tab descriptions are in one place. It describes the Darling viewer only.

Each per-server tab has these inner tabs (**Session Stats**, **Configuration Changes**, **System Events**, **Latches & Spinlocks** and **Long Queries** are not described below):

| Inner tab | Contents |
|---|---|
| **Overview** | Correlated, X-axis-synced timeline lanes over the last 24 hours — CPU % (SQL Server vs SQL+other Total), total wait ms/sec, blocking + deadlocking, buffer pool MB, and file-I/O latency — each with a ±2σ baseline band and anomaly markers, all sharing one crosshair so a spike in one lane lines up against the others |
| **Wait Stats** | A searchable wait-type picker (poison + usual-suspect + `PAGELATCH_` defaults, checked-to-top, a 30-type selection guide) beside a per-**type** trend chart for the checked types over the last 24 hours, with a Wait Time (ms/sec) ↔ Avg Wait Time (ms/wait) metric toggle — the per-type companion to the Overview's single total-wait lane |
| **Queries** | Sub-tabs over the last 24 hours — **Performance Trends** (a 2×2 of per-second trend charts: query duration, procedure duration, Query Store duration, execution count), **Active Queries** (the ~26-column filterable snapshot grid of captured running queries with a time-range slicer, a **Latest Snapshot** button that re-reads the newest stored capture, and per-row Estimated / Actual plan buttons that open the stored plan in the Plan Viewer), **Top Queries by Duration** (the full query-stats grid with in-grid bar cells for executions/CPU/duration/reads and a CPU-by-database breakdown), **Top Procedures by Duration**, **Query Store by Duration**, **Query Store Regressions**, **Query Store Clutter** (#3797 — which databases make this server's Query Store catalog expensive: per database the collector's read cost, plan churn and options row with the verdict and its reasons, a readable secondary carried as a row with the architectural reason on it rather than filtered out, and the per-server `QDS_*` / memory-clerk overhead proxy in a block of its own; select a row for the recommendation), **Plan Corrections**, and **Query Heatmap** (query counts per 5-minute bin × per-execution magnitude bucket, by a chosen metric; right-click a cell to drill into Active Queries for that window) — the three per-source grids each carry a time-range slicer (drag to narrow the window) and a shared **Compare** control that overlays the current window against a baseline period (yesterday, last week, or same day last week), flagging new and vanished queries |
| **Plan Viewer** | Hosts execution plans as closable sub-tabs (the shared plan-viewer control, the same one Lite and the Dashboard use). Right-click a **Top Queries** or **Query Store** row and choose **View Plan** to open the plan the service captured for it (`query_stats.query_plan_xml` / `query_store_stats.query_plan_text`); Top Queries rows also carry a **Query Plan** column whose Download button saves the stored plan as a `.sqlplan` file (enabled only when a plan was captured). Top Procedures and the blocking / deadlock reports deliberately do **not** surface a plan here — procedure plans aren't stored, and blocked-process / deadlock rows carry only a `sql_handle` (not plan XML); resolving either to a plan needs a live SQL connection the viewer never makes. "Get Actual Plan" (a live re-execution) is likewise out |
| **CPU** | Raw per-sample CPU utilization (SQL Server vs other processes) over the last 24 hours — every ring-buffer sample, full-bleed as two series; the Overview's CPU lane plots the same raw samples compactly (SQL vs SQL+other Total) with a baseline |
| **Memory** | Sub-tabs over the last 24 hours — **Overview** (a summary strip of physical / SQL Server / target / buffer pool / plan cache / page-file memory plus the system memory state and model, over a Total-vs-Target-vs-Buffer-Pool memory trend with a memory-grants overlay), **Memory Clerks** (a searchable clerk-type picker — top-5 default, checked-to-top, clear-only-the-filtered — beside a per-clerk memory trend for the checked clerks with a non-buffer-pool total and top-clerk summary), **Memory Grants** (per-resource-pool grant sizing — available / granted / used MB — and activity — grantees / waiters / timeouts / forced grants), **Memory Pressure Events** (hour-bucketed stacked bars of `RING_BUFFER_RESOURCE_MONITOR` pressure, SQL Server vs OS, medium vs severe), **Resource Semaphore** (resource semaphore pressure over the window, and the newest resource semaphore snapshot), and **Plan Cache** (the plan cache by type at the newest snapshot) |
| **File I/O** | Sub-tabs over the last 24 hours — **Latency** (per-file read and write latency, with a dashed queued-I/O overlay) and **Throughput** (per-file read and write MB/s) — the top 10 files by activity |
| **tempdb** | Stacked charts over the last 24 hours — space usage (user / internal objects / version store), total allocated size, and per-file I/O latency |
| **Blocking** | Sub-tabs over the last 24 hours — **Trends** (lock-wait rate, blocking incidents, deadlocks), **Current Waits** (waiting-task duration by wait type, blocked sessions by database), **Blocked Process Reports** (the full ~25-column filterable grid — XE reports preferred with the always-on DMV blocking snapshot merged in as fallback, each row badged with its source, a time-range slicer, per-row report-XML save, and long-block highlighting; double-click or right-click **View Block Chain** to reconstruct and draw the blocking chain the row belongs to), and **Deadlocks** (one filterable row per process parsed from each deadlock graph, a slicer, per-row graph-XML save; double-click or right-click **View Deadlock Graph** to draw the deadlock graph) |
| **Perfmon** | A searchable counter picker with the shared counter packs (General Throughput, Memory Pressure, CPU / Compilation, I/O Pressure, TempDB Pressure, Lock / Blocking) beside a per-counter trend for the checked counters (up to 12) over the last 24 hours — each series shaped by its stored counter type: a rate counter per second, a gauge as its level, anything else as its per-interval delta (V132; rows written before it fall back to the `/sec` name proxy) |
| **Running Jobs** | Latest snapshot of currently-running SQL Agent jobs — start time, current vs average vs p95 duration, % of average, and a highlighted row when a job is running past its p95 (a store-derived banner appears when the service's login lacks msdb access) |
| **Configuration** | Column-filterable snapshot grids of the server's latest capture — server configuration (`sys.configurations`), database configuration (28 columns of `sys.databases`), database-scoped configuration, and trace flags. The web page also shows two Identity Capacity panels. They hold the columns above 10 percent and each database's scan state, as the Identity Capacity tab does. It also shows the SQL Server Agent alerts and the gaps in them here, and a Health Checks panel with the last full backup, log backup and CHECKDB of each database, overdue ones first |
| **Daily Summary** | A one-row roll-up of the selected day (default today, UTC, with a date picker) — total wait time, the top wait type, distinct query count, deadlock / blocking-event / high-CPU-sample counts, collector errors, and an overall health band |
| **Collection Health** | Sub-tabs — **Health Summary** (a 7-day per-collector roll-up: run / success / error counts, failure rate, average duration, last success / run / error, and a health band of HEALTHY / WARNING / STALE / FAILING / NEVER_RUN / NO_PERMISSIONS / EXTENSION_MISSING — double-click a collector to open its full run history), **Collection Log** (the recent run log with per-run SQL and store-write timings and row counts), **Duration Trends** (a per-collector success-duration scatter), and **Sweep Pressure** (SQL Server only: the sweep verdict, busy percent, peak cycle and peak risk, and the three heaviest collectors, over the last 7 days whatever the time range is) |

The aggregate tabs — **Overview** and **Alerts** span every server; **Recommendations** has its own server selector, independent of the sidebar. **Job History** and **FinOps** are not described below:

| Tab | Contents |
|---|---|
| **Overview** | A card per registered server (all servers, not the sidebar selection): server name + status dot, CPU (total non-idle with the SQL-only number alongside), memory, blocking and deadlock counts over the last hour, and last-collection time, each colour-banded (CPU ≥ 80% red / ≥ 50% amber / green; blocking and deadlocks red-or-amber when present) with a red **Offline** overlay. Status is derived from **collection freshness** — the newest `collection_log` age — rather than a live ping (the viewer never connects to the monitored servers): fresh is Online, older than twice the fastest collector's one-minute cadence is a Warning, and no recent collection is Offline. **Double-click a card** to open that server's tab. Refreshes every 30 seconds |
| **Recommendations** | The latest analysis run's findings for the tab's **own selected server** — a server selector independent of the sidebar, a Refresh button, and a status line showing the last analysis time — re-skinned to Lite's advise-only **card** design: a scrollable list of collapsible **incident** sections, each holding severity-banded cards (a severity badge, the affected `[database]`, the title, and the advice). Every card offers **Ask AI** (copies an MCP investigation prompt referencing `analyze_server` / `get_analysis_findings`); a card whose stored remediation carries a copy-paste statement also offers **Copy fix** (copies the suggested T-SQL). Advise-only — the viewer never applies anything, and there is no mute affordance here (alert muting lives on the Alerts surface). A **Generate now** button asks the service to run an analysis at once, and the service also runs one on its own 30-minute cadence |
| **Alerts** | The full alert history from `config_alert_log` across **all servers** (newest first, selectable time range), with a Server column and a Server filter. Double-click a row (or **View Details**) for a modal detail window showing the alert's stored detail and structured advice / remediation / drill-down from its dedup-fingerprint context. **Dismiss Selected / Dismiss All** hide alerts from the view (a durable `dismissed` flag on `config_alert_log`); column filters, Copy Cell/Row/All, and Export to CSV match Lite's grid. Right-click to **Mute This Alert** or **Mute Similar** (metric-only), and a **Manage Mute Rules** button opens the mute-rule editor |

The Darling web dashboard has these pages in its sidebar:

- Fleet Overview
- Availability Groups (shown once the store holds Availability Group data)
- FinOps, with the same twelve sub-tabs as the table above
- Fleet Sweeps
- Alert History and Alert Rules
- Investigations (requests, briefs, reports and feedback for your own AI agent)
- Admin, with the tabs Servers, Notification Routes, Alert Settings, Report Feedback, Practice Scores, Pending Notes, Threshold Overrides, Agent changes, Server actions, and Environment Profiles
- Custom Views, Mute Rules, Known Patterns, and Manage Tags (create, rename, recolour, move under another tag, put sibling tags in order with Move up and Move down, delete, and assign servers), Query Tags (the tag tree, hand assignments and rules), and Query Tag Usage (resource use by tag)
- Job History

The page for one SQL Server has these tabs:

- Overview, Wait Stats, CPU, Memory, and Blocking
- File I/O, I/O Trends, Queries, and Configuration
- Config Changes, Activity, and System Events
- Collection Health and Recommendations (accounts that can edit get Generate now, which queues the same analysis run as the Viewer; every finding has Ask AI, which copies the same prompt)

The page for one PostgreSQL server has these tabs: Overview, Activity, Vacuum, Waits, I/O, Replication, Storage, and Configuration.
