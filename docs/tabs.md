# Tabs

Part of the [Performance Monitor documentation](../README.md#documentation).

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
| **FinOps** | Twelve sub-tabs. Utilization, Database Resources, Storage Growth, Locking & Contention, Database Sizes, and Version Store (PVS). Optimization, High Impact, Application Connections, and Server Inventory. Index Analysis (runs sp_IndexCleanup) and Recommendations. Column-level filtering on most grids |
| **Recommendations** | Prioritized findings drawn from collected metrics, grouped into incidents, each card showing the affected database, the recommendation, the reasoning behind it, and a copyable MCP investigation prompt |
| **Alert History** | Logged alerts with a time range, details, dismiss, and mute actions |
| **Job History** | SQL Agent job runs retained from `msdb` |
| **Availability Groups** | Replica and per-database health from the Availability Group collectors |

Both feature auto-refresh, chart drill-down to Active Queries, right-click CSV export, system tray integration, dark and light themes with user-adjustable palette colors (see [Themes and colors](../Lite/README.md#themes-and-colors)), and timezone display options (server time, local time, or UTC).

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
