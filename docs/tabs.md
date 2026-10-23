# Tabs

Part of the [Performance Monitor documentation](../README.md#documentation).

The **Lite** app and the **Darling** viewer have the same main-window tabs and nearly the same tabs for one server. The viewer is Lite's front end reading a Postgres store instead of local DuckDB.

### Main window tabs

| Tab | Contents |
|---|---|
| **Overview** | One card per monitored server. The cards can be sorted by CPU % or by name and searched by name. The Darling viewer adds a fleet roll-up above the cards. It shows server counts by health band, fleet totals for blocking and deadlocks, and a worst-first Needs Attention list that opens a server's tab |
| **Alert History** | Logged alerts with a time range, details, dismiss, and mute actions |
| **Job History** | SQL Agent job runs retained from `msdb` |
| **Availability Groups** | Replica and per-database health from the Availability Group collectors. The tab is hidden until there is Availability Group data |
| **FinOps** | Twelve sub-tabs. Utilization, Database Resources, Storage Growth, Locking & Contention, Database Sizes, and Version Store (PVS). Optimization, High Impact, Application Connections, and Server Inventory. Index Analysis (runs sp_IndexCleanup) and Recommendations. Column-level filtering on most grids |
| **Recommendations** | Prioritized findings drawn from collected metrics, grouped into incidents, each card showing the affected database, the recommendation, the reasoning behind it, and a copyable MCP investigation prompt |

A **Plan Viewer** tab also appears in the main window when you open a plan from one of these tabs. It is hidden otherwise.

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

Both feature auto-refresh, chart drill-down to Active Queries, right-click CSV export, system tray integration, Dark, Light and Cool Breeze themes with user-adjustable palette colors (see [Themes and colors](../Lite/README.md#themes-and-colors)), and timezone display options (server time, local time, or UTC).

Both also share one time range picker with the web pages. It has presets, calendar periods, and typed ranges such as `last 3 days` or `since 9am`. A text column's filter lists its values to tick, like Excel, and stays set across refreshes and restarts.

Lite has Investigations and Manage Query Tags buttons in its sidebar. The Darling viewer opens Investigations from Settings.

The Darling viewer adds a fleet sidebar and per-server tabs, and it shows a PostgreSQL target on its own set of tabs. See [Darling/README.md](../Darling/README.md). The deprecated Dashboard's six-tab-group layout is documented in [deprecated/Dashboard/README.md](../deprecated/Dashboard/README.md).

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
- Collection Health and Recommendations

The page for one PostgreSQL server has these tabs: Overview, Activity, Vacuum, Waits, I/O, Replication, Storage, and Configuration.
