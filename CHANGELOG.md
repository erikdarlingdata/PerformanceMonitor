# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.0] - 2026-02-20

### Important

- **Schema upgrade**: The `collect.memory_stats` table gains two new columns (`total_physical_memory_mb`, `committed_target_memory_mb`). The upgrade script runs automatically via the CLI/GUI installer and uses `IF NOT EXISTS` checks, so it is safe to re-run. On servers with very large `memory_stats` tables this ALTER may take a moment.

### Added

- Physical Memory, SQL Server Memory, and Target Memory columns in Memory Overview ([#140])
- Current Configuration view (Server Config, Database Config, Trace Flags) in Dashboard Overview ([#143])
- Popup column filters and right-click context menus in all drill-down history windows ([#206])
- Consistent popup column filters across all Dashboard grids — replaced remaining TextBox-in-header filters and added filters to Trace Flags ([#200])
- 7-day time filter option in drill-down queries ([#165])
- Alert badge/count on sidebar Alerts button ([#109])
- Missing poison wait defaults in wait stats picker ([#188])

### Changed

- Default Trace tabs moved from Resource Metrics to Overview section ([#169])
- Trends tab shown first in Locking section ([#171])
- Wait stats cap raised from 20 to 30 (Dashboard) / 50 (Lite) so poison waits are never dropped ([#139])
- Settings time range dropdown now matches dashboard button options ([#210])
- "Total Executions" label in drill-down summaries renamed to clarify meaning ([#194])
- WAITFOR sessions excluded from long-running query alerts ([#151])

### Fixed

- Deadlock XML processor timezone mismatch — sp_BlitzLock returning 0 results because UTC dates were passed instead of local time
- Sidebar alert badge not updating when alerts dismissed from server sub-tabs ([#214])
- Sidebar alert badge not clearing on acknowledge ([#186])
- NOC deadlock/blocking showing "just now" for stale events instead of actual timestamp ([#187])
- NOC deadlock severity using extended events timestamp ([#170])
- Newly added servers not appearing on Overview until app restart ([#199])
- Double-click on column header incorrectly triggering row drill-down ([#195])
- Squished drill-down charts — now use proportional sizing ([#166])
- Unreliable chart tooltips — now use X-axis proximity matching ([#167])
- Query Trace Patterns showing empty despite data existing ([#168])
- Drill-down windows: removed inline plan XML, added time range filtering, aggregated by collection_time ([#189])
- Row clipping in Default Trace and Current Configuration grids ([#183], [#184])
- Numeric filter negative range parsing ([#113])
- MCP shutdown deadlock risk ([#112])
- Lite DBNull cast error in database_config collector on SQL 2016 Express ([#192])
- DuckDB concurrent file access IO errors ([#164])

## [1.2.0] - 2026-02-15

### Added

- Alert types, alerts history view, column filtering, and dismiss/hide for alerts ([#52], [#56])
- Average ms per wait chart toggle in both apps ([#22])
- Collection Health tab in Lite UI ([#39])
- Collector performance diagnostics in Lite UI ([#40])
- Hover tooltips on all Dashboard charts ([#70])
- Minimize-to-tray setting added to Lite ([#53])
- Persist dismissed alerts across app restarts ([#44])
- Locale-aware date/time formatting throughout UI ([#41])
- 24-hour format in time range picker ([#41])
- CI pipelines for build validation, SQL install testing, and DuckDB schema tests
- Expanded Lite database config collector to 28 sys.databases columns ([#142])
- Parquet archive visibility and scheduled DuckDB database compaction ([#160], [#161])
- DuckDB checkpoint optimization and collection timing accuracy
- Installer `--reset-schedule` flag to reset collection schedule on re-install

### Fixed

- Deadlock charts not populating data ([#73])
- Chart X-axis double-converting custom range to server time ([#49])
- query_cost overflow in memory grant collector ([#47])
- XE ring buffer query timeouts on large buffers ([#37])
- Dashboard sub-tab badge state and DuckDB migration for dismissed column
- Lite duplicate blocking/deadlock events from missing WHERE clause ([#61])
- Procedure_stats_collector truncation on DDL triggers ([#69])
- DataGrid row height increased from 25 to 28 to fix text clipping
- Skip offline servers during Lite collection and reduce connection timeout ([#90])
- Mutex crash on Lite app exit ([#89])
- Permission denied errors handled gracefully in collector health ([#150])

## [1.1.0] - 2026-02-13

### Added

- Hover tooltips on all multi-series charts — Wait Stats, Sessions, Latch Stats, Spinlock Stats, File I/O, Perfmon, TempDB ([#21])
- Microsoft Entra MFA authentication for Azure SQL DB connections in Lite ([#20])
- Column-level filtering on all 11 Lite DataGrids ([#18])
- Chart visual parity — Material Design 300 color palette, data point markers, consistent grid styling ([#16])
- Smart Select All for wait types + expand from 12 to 20 wait types ([#12])
- Trend chart legends always visible in Dashboard ([#11])
- Per-server collector health in Lite status bar ([#5])
- Server Online/Offline status in Lite overview ([#2])
- Check for updates feature in both apps ([#1])
- High DPI support for both Dashboard and Lite

### Fixed

- Query text off-by-one truncation ([#25])
- Blocking/deadlock XML processors truncating parsed data every run ([#23])
- WAITFOR queries appearing in top queries views ([#4])
- Wait type Clear All not refreshing search filter in Dashboard

## [1.0.0] - 2026-02-11

### Added

- Full Edition: Dashboard + CLI/GUI Installer with 30+ automated SQL Agent collectors
- Lite Edition: Agentless monitoring with local DuckDB storage
- Support for SQL Server 2016-2025, Azure SQL DB, Azure SQL MI, AWS RDS
- Real-time charts and trend analysis for wait stats, CPU, memory, query performance, index usage, file I/O, blocking, deadlocks
- Email alerts for blocking, deadlocks, and high CPU
- MCP server integration for AI-assisted analysis
- System tray operation with background collection and alert notifications
- Data retention with configurable automatic cleanup
- Delta normalization for per-second rate calculations
- Dark theme UI

[1.3.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/erikdarlingdata/PerformanceMonitor/releases/tag/v1.0.0
[#1]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1
[#2]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2
[#4]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/4
[#5]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/5
[#11]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/11
[#12]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/12
[#16]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/16
[#18]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/18
[#20]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/20
[#21]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/21
[#22]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/22
[#23]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/23
[#25]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/25
[#37]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/37
[#39]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/39
[#40]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/40
[#41]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/41
[#44]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/44
[#47]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/47
[#49]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/49
[#52]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/52
[#53]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/53
[#56]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/56
[#61]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/61
[#69]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/69
[#70]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/70
[#73]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/73
[#85]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/85
[#86]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/86
[#89]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/89
[#90]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/90
[#109]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/109
[#112]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/112
[#113]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/113
[#139]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/139
[#140]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/140
[#142]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/142
[#143]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/143
[#150]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/150
[#151]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/151
[#160]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/160
[#161]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/161
[#164]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/164
[#165]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/165
[#166]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/166
[#167]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/167
[#168]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/168
[#169]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/169
[#170]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/170
[#171]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/171
[#183]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/183
[#184]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/184
[#186]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/186
[#187]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/187
[#188]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/188
[#189]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/189
[#192]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/192
[#194]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/194
[#195]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/195
[#199]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/199
[#200]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/200
[#206]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/206
[#210]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/210
[#214]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/214
