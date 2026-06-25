# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Lite and Dashboard: the low-disk (Volume Free Space) alert is now severity-graded instead of always INFO** ([#1136]) — the metric name was absent from the shared severity map, so every low-disk alert fell through to the default INFO tier (blue, lowest severity) regardless of how full the volume was. A volume nearing full stops data/log file growth — transactions fail and the database can go into recovery/suspect — so this under-prioritized a potentially critical condition, including for downstream severity-based webhook routing (Teams/Slack). The alert now renders **WARNING** for a normal breach and **CRITICAL** when the worst breached volume is critically low (≤ 3% free **or** ≤ 2 GB free — a second, lower tier beneath the user-configured fire threshold). The critical grading lives in the shared `LowDiskAlertGate.IsCriticallyLow`, and the tier rides through to the email badge/accent, Teams card, and Slack sidebar via an `AlertContext.SeverityOverride`, so the metric name (hence mute rules, cooldown, and Alert-History matching) is unchanged. The existing "notify only on a fresh or worsening breach" firing rule is untouched — this is purely the severity classification. Fixed identically in both apps. Covered by `AlertSeverityTests` and `LowDiskAlertGateTests`
- **Lite and Dashboard: the execution-plan viewer is now one shared control** ([#1205]) — Dashboard and Lite each carried a near-identical 6-file `PlanViewerControl`; the plan parsing/layout/analysis already lived in the shared `PerformanceMonitor.PlanAnalysis`, so only the WPF shell was duplicated. That shell is now a single `PerformanceMonitor.Ui.PlanViewerControl` referenced by both apps, so it can no longer drift. Two host-visible effects: (1) **Lite's multi-statement picker becomes a sortable grid** — for a plan with more than one statement, Lite previously used a `Statements:` dropdown and now shows Dashboard's collapsible **Statements** panel (a sortable grid of #, query text, CPU/Elapsed/UDF or estimated cost, and Critical/Warning counts, with right-click → Copy Query Text), so you can sort a batch by cost or runtime to find the heavy statement; single-statement plans are unchanged (no panel). (2) The shared control parses and analyzes **off the UI thread**, so opening a large showplan no longer briefly freezes the window. The theme-change handler also moves to a `Cleanup()`-at-teardown lifecycle in both apps — it is no longer detached on `Unloaded` (which a `TabControl` raises on every tab switch, the previous Dashboard behavior that could leave a backgrounded plan tab no longer re-theming); each app now calls `Cleanup()` at every real teardown (plan window/tab close, server-tab close, and the malformed-XML error path)
- **Lite and Dashboard: "Show Active Queries at This Time" right-click drill-down now works on every resource chart** ([#1208]) — the chart drill-down (right-click a point to jump to Active Queries scoped to that ±30-minute window, originally [#682]) was wired on only a subset of charts and had drifted between the two apps. A full audit of every chart surfaced three problems, all fixed: (1) the Dashboard **Resources/TempDB tab** drill-down was dead plumbing — the control defined the `AddDrillDown` mechanism and a `ChartDrillDownRequested` event that the server tab even subscribed to, but never wired a single chart, so none of its 11 charts could drill down; (2) the TempDB **Allocated MB** chart (both apps) and Lite's **Memory Pressure Events** chart had no right-click menu at all (no save/export *or* drill-down) because they were never registered — they now get the full menu plus a new hover helper so the click can resolve a timestamp; (3) bidirectional parity drift — Lite lacked it on Memory Clerks/Grants/Pressure and Current/Lock Waits while Dashboard had them, and Dashboard lacked it on TempDB Stats while Lite had it. The drill-down is now present on every time-series resource chart in both apps (memory clerks/grants/pressure, tempdb size/stats/file-I/O latency, file-I/O latency + throughput, perfmon, current waits, and — Dashboard only — session/latch/spinlock stats and plan cache; Lite's Lock Wait routes to *Show Blocking* to match Dashboard). The query/procedure/Query Store/execution **duration-trend** charts are intentionally excluded because the grid→slicer overlay already routes there, as are Collector Duration and the system-health event charts. Lite consolidates the new navigation into one shared `OnActiveQueriesDrillDown` handler rather than duplicating the existing CPU/Memory/TempDB drill-down bodies
- **Lite and Dashboard: the Overview correlated-timeline lanes get the same Active Queries drill-down** ([#1210]) — follow-on to [#1208]. The five synchronized lanes (CPU, Wait Stats, Blocking, Memory, File I/O) are a deliberately stripped-chrome view that disables all chart mouse input for its shared crosshair, so they had no right-click at all — and in Lite that control *is* the **Overview / default landing tab** (Dashboard's 2×2 Overview grid already drilled, so this also closes a cross-app gap). Each lane now has a minimal right-click menu with just *Show Active Queries at This Time*; because pan/zoom is disabled the clicked time is read straight from the X axis, and it routes to the same ±30-minute Active Queries navigation as every other chart. The synchronized crosshair is unaffected — it is mouse-move driven, independent of the right-click handler

### Fixed

- **Lite: the `index_object_stats` collector no longer times out and returns zero index data on larger estates** ([#1135]) — the v3.0.0 collector ran its entire multi-database sweep as **one** `SqlCommand` under the global 30s `CommandTimeoutSeconds`, cursoring over every online database into a `#temp` and returning a single final `SELECT`. Because nothing streamed back until the end, the 30s was a *cumulative, all-or-nothing* budget across every database — on a server with sizable/many databases the sweep blew past 30s, failed with `Execution Timeout Expired` (SQL `#-2`), and discarded results from **every** database, not just the slow one. Enabled by default and "never-run = due immediately," it failed on first connect right after upgrade and kept retrying the timeout. Now the collector runs **one command per database** (mirroring the Query Store collector): on-prem enumerates databases then sends each through `[db].sys.sp_executesql`, Azure SQL DB connects to each database individually, and each database has its own command, timeout, and `try/catch` — so a slow or inaccessible database fails only itself and the rest still persist. Within each database the three DMVs (`sys.dm_db_partition_stats`, `sys.dm_db_index_usage_stats`, `sys.dm_db_index_operational_stats`) are staged into `#temp` tables with single scans and then joined, giving the optimizer real cardinality and avoiding the bad plans the old single monolithic multi-DMV join produced on large databases (the `sp_IndexCleanup` technique). The collector also gets a dedicated 300s timeout (matching the FinOps `sp_IndexCleanup` path) instead of the 30s meant for lightweight DMV reads. The Dashboard's equivalent SQL collector (`install/55`) — which was not subject to the bug (it runs under SQL Agent and persists per database) — was brought to parity with the same DMV-staging technique for plan quality on large databases
- **Dashboard: the execution-plan node "actual of estimated rows" badge no longer over-reports for multi-execution operators** ([#1205]) — Dashboard compared an operator's *total* actual rows (summed across every execution) against the *per-execution* estimate, so any operator that runs more than once — e.g. the inner side of a nested-loops join — looked far off: a seek executed 1,000 times showed ~1,000x its estimate and turned red even when each execution matched the estimate exactly. It now divides actual rows by execution count before comparing, matching the per-execution estimate — the math Lite's viewer already used and that `PlanAnalyzer` applies elsewhere. Surfaced while consolidating the two apps' plan viewers into the shared control; Lite's badge was already correct and is unchanged

## [3.0.0] - 2026-06-15

### Important

- **Major release — 2.11.0 → 3.0.0, no breaking changes.** This version rolls up a codebase-wide correctness and security hardening pass (the `code-review-*` series spanning the SQL schema, collectors, and views, the installer, the Lite and Dashboard services, and the shared libraries); a major UI-responsiveness overhaul that moves the data path off the WPF dispatcher in both apps; new object- and index-level collection (per-table / per-index size, growth, usage, and locking/contention); the rebuilt Recommendations / Apply Fix engine (advise-and-act, with safe and destructive fixes appliable behind informed two-sided consent); and a batch of smaller fixes and features. Nothing here is a breaking change — existing installations upgrade in place via `upgrades/2.11.0-to-3.0.0/` (typed blocked-process columns, a nullable host-CPU column, the `TRANSACTION_MUTEX` ignored wait, and new server-health columns), and the Dashboard and Lite apps auto-update over the top

### Fixed

- **Lite and Dashboard: Azure SQL Database shows its real product name in FinOps → Server Inventory** — the Edition column displayed the legacy `SQL Azure` value that `SERVERPROPERTY('Edition')` returns for Azure SQL DB; it now reads `Azure SQL Database (<service tier>)` (e.g. `Azure SQL Database (General Purpose)`), derived from `DATABASEPROPERTYEX(DB_NAME(), 'Edition')`, for any engine-edition-5 instance. Normalized at every edition display/storage site across **both** apps — the live inventory queries (Lite + Dashboard) and the SQL-side collectors (`install/42`, `install/53`) plus Lite's `server_properties` collector — so the value is consistent app-wide; on-prem editions are unchanged. (The licensing-recommendation queries are left raw and identical in both apps: they only do an `Enterprise` substring check and never display the edition for Azure.)
- **Dashboard: "Deadlocks Cleared" no longer flaps right after every deadlock** ([#1091]) — deadlock detection is edge-triggered off a delta against the cumulative perfmon counter, so the check immediately after a deadlock saw a zero delta and fired a *"Deadlocks Cleared"* notification ~one interval (≈60s) after every *"Deadlock Detected"*. The alert now stays active and clears only once a deadlock-quiet window (1 hour) has elapsed since the last new deadlock, so the detect/clear pair lines up with Lite, whose rolling 1-hour count drains about an hour after the last deadlock. Each new deadlock resets the window. The clear message is now *"No deadlocks in the last hour"* (was *"No deadlocks since last check"*). Covered by `DeadlockAlertClearPolicyTests`
- **Lite: blocking and deadlock alerts no longer re-fire for the same events every cooldown** ([#1091]) — the overview alert engine treated the blocking and deadlock counts as a level: each check compared the rolling **1-hour** count against the threshold, so a single deadlock (or blocked-process report) kept the count above the threshold for the whole hour it lingered in the window, and the alert re-fired every cooldown (the reporter saw the same "2 deadlocks in the last hour" notification every five minutes for an hour). The Dashboard already edge-triggers off a delta; Lite now does too. Both alerts are gated by a new `RollingCountAlertGate` that fires only when the rolling count climbs above the count recorded at the last fired alert — a genuinely new event. The watermark decays as old events age out of the window (so a later rise re-alerts), resets when the window empties, and advances only when an alert actually fires (so an event arriving during a cooldown is reported once the cooldown elapses rather than being swallowed). Covered by `RollingCountAlertGateTests`
- **Lite and Dashboard: low-disk (Volume Free Space) alert no longer re-fires every cooldown for a standing full volume** — a breached volume is a sustained condition, but the alert engine treated free space as a level and re-fired every `AlertCooldownMinutes` (default 5) for as long as the volume stayed below threshold. Besides the repeated tray/email, every cycle wrote a fresh Alert-History row, so dismissing the alert appeared not to work — the dismissed row was immediately replaced by an identical, newer one. The alert is now gated by a shared `LowDiskAlertGate` that notifies only on a **fresh** breach or one that has **worsened** by at least 1 percentage point of free space below the last-alerted level, and clears its watermark when the volume recovers — mirroring the failed-job watermark and the [#1091] rolling-count edge trigger. Fixed identically in both apps. Covered by `LowDiskAlertGateTests`
- **Lite and Dashboard: low-disk and failed-Agent-job conditions now light the server tab badge** ([#754]/[#749]) — the per-server tab badge was driven only by blocking, deadlocks, CPU, and memory, so a server whose only problem was a full volume or a failed Agent job showed **no tab indicator** — you couldn't tell which server was affected at a glance (the alert surfaced only as a one-shot tray toast and an Alert-History row). Both apps now fold the alert engine's active low-disk / failed-job state into the badge: it lights while the breach (or a failure within the lookback window) is active, auto-clears when the disk recovers or the failure ages out, and acknowledges/silences exactly like the other badges. This is the persistent-indicator complement to the low-disk re-fire fix above — alert once, then stay quietly flagged instead of re-nagging. Covered by `AlertBadgeConditionTests`
- **Lite: blocking/deadlock XE sessions now self-heal and failures are surfaced** ([#1086]) — the `PerformanceMonitor_BlockedProcess` and `PerformanceMonitor_Deadlock` Extended Events sessions were created only when a server tab was opened; the recurring background collection loop never created or retried them. A server monitored without an open tab (e.g. app minimized to tray after a restart), or a first attempt that failed (connection not ready, missing `ALTER ANY EVENT SESSION`), left blocking/deadlock capture permanently dead — while the collectors read the non-existent ring buffer, got zero rows, and reported **OK**. The session ensure now runs inside the collector itself on every cycle (cheap existence check once created), so both the tab-open path and the background loop create/start/retry it. A failed ensure can no longer be masked: it fails the collector run, shows in the status-bar collector health (including permission failures, which previously didn't count as "erroring"), and fires a one-time tray notification ("Capture Not Running") on the transition. The Azure SQL DB database-scoped sessions also gain `STARTUP_STATE = ON` so they restart automatically after a failover
- **Dashboard: blocking/deadlock XE sessions self-heal, Azure SQL DB sessions are actually created, and a missing session raises a Capture Down alert** — same silent-failure family as [#1086], worse on the Dashboard side. (1) The server-scoped sessions were created once at install and never re-ensured: if later stopped or dropped, `collect.blocked_process_xml_collector` and `collect.deadlock_xml_collector` swallowed the missing-session error and logged `SUCCESS` with zero rows forever. Both procs now ensure (create/start) the session at the top of every run. (2) On Azure SQL DB, the code comments claimed the database-scoped sessions were "auto-created by the collection procedures" — nothing anywhere created them, so blocking/deadlock capture was 100% non-functional on Azure SQL DB; the procs now create and start them (`database_xml_deadlock_report` for deadlocks — the Azure read also filtered on the wrong event name and would have returned nothing even with a session present). (3) Honest logging: when the session is genuinely absent and can't be created (typically missing `ALTER ANY EVENT SESSION` on-prem / `CREATE ANY DATABASE EVENT SESSION` on Azure SQL DB), the run logs `SESSION_MISSING` with the real error instead of `SUCCESS`. (4) The alert engine reads that status and raises a **Capture Down** alert through the standard pipeline — snoozable tray notification, email, webhook, alert history, cooldown, and mute — with a **Capture Restored** clear when the session comes back. Note: on Azure SQL DB the blocked-process *threshold* cannot be set via `sp_configure` and Microsoft documents no default, so the blocked-process session may exist yet capture nothing there; deadlock capture has no such dependency
- **Blocked-process and deadlock XML processors no longer loop on un-parseable events** — the second-phase parsers (`collect.process_blocked_process_xml` → `sp_HumanEventsBlockViewer`, and `collect.process_deadlock_xml` → `sp_BlitzLock`) only marked a captured event processed when the parse produced at least one row. Events that legitimately yield zero — a *self-block* or non-lock wait (e.g. a memory-grant `RESOURCE_SEMAPHORE` wait that tripped `blocked_process_threshold`, which SQL Server reports as a session blocked by itself), or a deadlock graph the parser can't reconstruct — were never marked, so every collection cycle re-ran the CPU-intensive parser over the same dead events and re-logged a perpetual `NO_RESULTS` while the staging table never drained. Both processors now mark events processed after any clean parse run and log `SUCCESS`; genuine parse failures still roll back and retry. Separately, the blocked-process processor's parse window was half-open (`event_time < @end_date`), so a batch of reports sharing one timestamp — the common case, since a blocked-process monitor loop emits every report at a single instant — fell outside `[MIN, MAX)` and was silently dropped; the upper bound is now inclusive (matching the deadlock processor). Covered by `tools/test_blocked_process_processor.sql` using real self-block and two-session samples

- **Lite and Dashboard UI no longer goes blank or disappears after sleep/wake** ([#1050]) — closing a laptop lid (or locking the screen) and then resuming could leave the app running with no usable window: notifications kept firing but the window was gone from the desktop and taskbar, and relaunching showed an empty window until a full exit/restart. Two causes, both fixed. (1) WPF's GPU render thread can lose its rendering surface across a sleep/wake or RDP reconnect and never recover, leaving a live-but-blank window; both apps now use software rendering (`RenderOptions.ProcessRenderMode = SoftwareOnly`) to remove the GPU dependency — charts are unaffected because ScottPlot already renders via SkiaSharp. (2) When Windows turned the sleep-driven minimize into a hidden window, the minimize-to-tray logic left it hidden with no automatic way back; a new shared resume guard now restores the window from the tray on resume/unlock if it was visible beforehand (a window the user deliberately sent to the tray is left alone)
- **"Silence All Alerts" now suppresses email too** ([#1035]) — right-clicking a monitored instance and choosing *Silence All Alerts* hid tray notifications and Alerts-tab badges, but two email paths ignored the silenced state and kept sending: connection up/down emails (*Server Unreachable* / *Server Restored*) and analysis-finding emails (the narrative findings from the analysis engine, which include CPU/memory/blocking stories). Only the threshold-alert path (High CPU, blocking, deadlocks, etc.) honored silencing. Both gaps are closed — a silenced server now produces no tray, email, or alert-history row from any path. The analysis path was the likely source of the reporter's "High CPU" email, since the threshold-based High CPU alert was already suppressed. The shared `AnalysisNotificationService` (used by Lite too) gains an optional per-server silence predicate; Lite has no silencing feature and passes none
- **Dashboard time labels are now consistently 24-hour** ([#1012]) — the time-range header at the top of each tab (e.g. *"Original: May 28, 11:30 PM – May 29, 1:30 AM (PST)"*) and the Query Performance heatmap x-axis tick labels used `h:mm tt`, while every other timestamp in the app (footer "Last refresh", DataGrid columns, slicer, tooltips, logs) already used 24-hour `HH:mm`/`HH:mm:ss`. The AM/PM marker was also being truncated in the column shown by the reporter. Normalized the four outliers to `HH:mm` to match the rest of the app. The Lite heatmap had the same `h:mm tt` straggler — fixed alongside
- **Lite UI no longer freezes during archival** ([#979]) — archival held DuckDB's exclusive write lock across the entire export-to-Parquet step, blocking every UI query (tab switches showed the spinning wheel, worse with more monitored servers). Export-to-Parquet only reads the database, so it now runs under a shared read lock concurrently with the UI; only the brief `DELETE` takes the exclusive write lock
- **Lite FinOps no longer recommends an edition downgrade on an Availability Group secondary** ([#980]) — the licensing recommendations suggested "downgrade to Standard to save $X/mo" for any Enterprise instance, with no AG awareness. On a secondary replica that advice is misleading — every replica in an AG must run the same edition. FinOps now detects the AG replica role and, on a secondary, shows an informational note instead of the downgrade/savings estimate
- **Lite alert emails no longer re-fire after an app restart** ([#981]) — the per-metric email cooldown lived only in memory, so restarting Lite cleared it and an alert sent minutes earlier could be sent again immediately. The cooldown is now seeded from `config_alert_log` (the most recent successful send for that server/metric) the first time each alert is evaluated, so it survives restarts
- **Dashboard alert emails no longer re-fire after an app restart** — brings Dashboard `EmailAlertService` to parity with the Lite-side persistence introduced in [#981]. The cooldown is now seeded from the in-memory alert log (loaded from `alert_history.json` on startup) the first time each `{serverId}:{metricName}` key is evaluated
- **Analysis-finding notification cooldowns now persist across restarts on both Lite and Dashboard** — the per-finding re-notification cooldown in `AnalysisNotificationService` lived only in memory, so restarting either app cleared it and a finding that had just fired (and entered its `AnalysisNotifyCooldownMinutes` cooldown) could re-notify immediately. The cooldown now seeds lazily from the alert log (Lite: `config_alert_log`; Dashboard: `alert_history.json`) on first lookup per finding, mirroring the email-cooldown pattern from #981. Entries past 2× the cooldown window are pruned on each notify cycle so the dictionary stays bounded
- **Data Retention job no longer fails with `xp_delete_file` error 22049** ([#972]) — the trace-file cleanup added in v2.11.0 passed a wildcard path to `xp_delete_file`, raising an uncatchable `Msg 22049` that failed the entire `PerformanceMonitor - Data Retention` Agent job on every run once any `Monitor_LongQueries_*.trc` files existed. `xp_delete_file` also cannot delete `.trc` files at all — it only accepts SQL Server backup files and Maintenance Plan report files — so that cleanup step has been removed from `config.data_retention`
- **Codebase-wide correctness and security hardening pass** — a broad review (the `code-review-*` PR series, #1093–#1108) fixed defects across the stack without changing behavior users depend on:
    - **Shared libraries** — defects in the extracted `PerformanceMonitor.Analysis` / `.PlanAnalysis` / `.Ui` / `.Common` code
    - **Dashboard** — timezone and CPU-path defects
    - **Lite** — services, analysis, and UI defects, plus `ArchiveService` data-loss / corruption fixes
    - **Installer** — CLI version-detection and failure-handling
    - **SQL** — high-impact collector defects, view / analyzer crashes (including a Linux CPU gap), and schema / job / validation defects
- **FinOps no longer recommends downgrading to Standard Edition on a server running Availability Groups** ([#1085]) — an Enterprise instance with no TDE was told to "review whether Standard Edition would meet workload requirements" even when it was running AGs, which Standard supports only in the limited Basic Availability Groups form. FinOps now counts advanced (non-basic) AGs via `sys.availability_groups.basic_features` and, when any are present, appends a caveat naming the AG count and Standard's Basic-AG limitations (two replicas, one database per group, no readable secondary), retitles the finding to "review Availability Group requirements before downgrading," and lowers its confidence — the savings estimate is retained. The Dashboard, which previously had no AG awareness at all, was brought to full parity and also gains the [#980] AG-secondary informational note it never received
- **Server-tab alert badge is now clearable** ([#1092]) — the red alert badge on a server tab could previously only be cleared through an undiscoverable right-click menu. Left-clicking the badge now acknowledges and clears it (hand cursor, *"Click to dismiss · Right-click for options"* tooltip), and Alert History **Dismiss All** clears the matching server badge(s) too. A follow-up ([#1122]) closed the last gap: **Dismiss Selected** now also clears the badge for every distinct server represented in the dismissed rows. On the Dashboard, which already had richer auto-resolving badges, this added the missing left-click affordance for parity
- **Long-running-query alert no longer constantly trips on CDC capture jobs** ([#1096]) — the Change Data Capture capture job runs as a continuous SQL Agent session (`sp_MScdc_capture_job` → `sp_cdc_scan`), so its elapsed time permanently exceeded the long-running-query threshold and the alert fired non-stop; none of the four existing `wait_type`-based exclusions caught it. Both apps gain an **Exclude CDC capture jobs** toggle (default on) that identifies the capture session server-side by decoding its Agent `program_name` to a `job_id` and matching `msdb.dbo.cdc_jobs` (`job_type = 'capture'`), falling back to a whole-text match when msdb is unreadable or `cdc_jobs` doesn't yet exist — so it stays CDC-specific and never hides unrelated Agent jobs. Dashboard filters the live DMV query inline; Lite computes a per-row `is_cdc_capture` flag in the collector (its snapshots store only statement-level text) and filters on read

### Changed

- **Plan parsing / analysis extracted to shared library `PerformanceMonitor.PlanAnalysis`** — the previously duplicated `ShowPlanParser`, `PlanAnalyzer`, `BenefitScorer`, `PlanLayoutEngine`, and `PlanModels` pairs across `Dashboard/Services` + `Dashboard/Models` and `Lite/Services` + `Lite/Models` are now one copy referenced by both apps via `<ProjectReference>`. The new library targets `net10.0` (no WPF) and has zero dependency on `PerformanceMonitor.Analysis` — the two shared libraries are independent. ~5,100 LOC of byte-equivalent duplication eliminated. The `planalyzer-sync-checker` agent is retired (no copies to sync). `ActualPlanExecutor` stays per-app this release because it calls `ReproScriptBuilder` (Class B, drifted between Lite and Dashboard); both will be extracted in a follow-up PR once `ReproScriptBuilder` is reconciled and a logging abstraction is designed
- **`PlanIconMapper` split to break a shared-library WPF dependency** — `ShowPlanParser` calls `PlanIconMapper.GetIconName` to populate `PlanNode.IconName` during parse, but the rest of `PlanIconMapper` is WPF-bound (`GetIcon` returns `BitmapImage`). The pure-data half (the `IconMap` dictionary + the `GetIconName` lookup) is now `IconNameMapper` inside `PerformanceMonitor.PlanAnalysis`. The per-app `PlanIconMapper.GetIcon(string iconName)` is unchanged; the per-app `GetIconName` forwarder is gone (`ShowPlanParser` calls `IconNameMapper.GetIconName` directly, and there were no other callers)
- **Analysis engine extracted to shared library `PerformanceMonitor.Analysis`** — the previously duplicated `FactScorer`, `RelationshipGraph`, `InferenceEngine`, `AnalysisModels`, `IFactCollector`, `IPlanFetcher`, and `BlockingChainReconstructor` pairs across `Dashboard/Analysis/` and `Lite/Analysis/` are now one copy referenced by both apps and both test projects via `<ProjectReference>`. The new library targets `net10.0` (no WPF) so it can be picked up by future non-WPF consumers without a multi-target rewrite. The `blocking-reconstructor-sync-checker` agent is retired (no copies to sync). `BlockingChainReconstructorTests` ported to `Dashboard.Tests` (10 tests) as part of the same change — Dashboard now exercises the same reconstruction coverage as Lite. `AnalysisService` and the DB-bound adapters (`*FactCollector`, `*DrillDownCollector`, `*FindingStore`, `*AnomalyDetector`, `*BaselineProvider`, `*PlanFetcher`) stay per-app because they bind to `DuckDBConnection` vs `SqlConnection`. `PlanAnalyzer` and its `planalyzer-sync-checker` are outside this extraction's scope and stay
- **Trace files are now bounded at the source** ([#972]) — `collect.trace_management_collector` creates the long-query trace with a rollover file-count cap (`@filecount`, via the new `@max_files` parameter, default 5), so SQL Server itself deletes the oldest `.trc` file as the trace rolls. The scheduled collector also now issues `START` instead of `RESTART`: it keeps one trace running rather than tearing it down and spawning a fresh timestamped trace — and a fresh batch of orphaned files — every cycle
- **Blocked-process reports expose blocker-side fields as typed columns** — `collect.blocking_BlockedProcessReport` now carries `blocking_spid`, `blocking_last_tran_started`, `blocking_status`, `blocked_sql_text`, and `blocking_sql_text` populated at insert time from `blocked_process_report_xml`. Existing rows are backfilled idempotently by the 2.11.0 → 3.0.0 upgrade script
- **Blocking-chain reconstruction now reads typed columns from `collect.blocking_BlockedProcessReport`** instead of re-parsing `blocked_process_report_xml` on every analysis cycle — eliminates up to 5000 `XElement.Parse` calls per `BLOCKING_CHAIN` fact collection. The Dashboard `BlockedProcessXmlParser` has been deleted; the Lite collection-time parser is unchanged (Lite has no SQL-side staging table and still parses once at collect time)
- **Analysis minimum-data threshold lowered to 24 hours** — `Lite/Analysis/AnalysisService.cs` and `Dashboard/Analysis/AnalysisService.cs` now require 24 hours of collected data before analysis runs, down from 72. Validated empirically as sufficient for fraction-of-period calculations, so a fresh install starts producing findings after one day instead of three
- **Major UI-responsiveness overhaul — the data path now runs off the WPF dispatcher in both apps** — DuckDB.NET is synchronous, so in Lite `await _dataService.X()` completed on the calling (UI) thread, and a single DuckDB connection open under load is ~750 ms; the result was multi-hundred-millisecond to multi-second UI freezes on the per-minute pipeline, refreshes, and alert checks. The fix moves the work onto pool threads (`Task.Run`) across the board: Lite's background collect/checkpoint/archive pipeline, the full-refresh fan-out, the 60-second sub-tab refreshes, picker charts, the overview sweep, timeline lanes, connect, and the FinOps and Recommendations reads; the Dashboard's `ServerTab` row materialization and its execution-plan parse/analyze; and — found later by wall-clock thread-time profiling under a HammerDB TPC-C load — the alert-check / overview-sweep DuckDB queries that were still on the dispatcher ([#1121], which cut the worst measured dispatcher stall from ~1.2 s to under 10 ms). Lite also skips the heavy refresh for non-selected (hidden) server tabs, the shared crosshair/hover hot path was made cheaper for both apps, and Dashboard timers gained re-entrancy guards. A cluster of long-session memory leaks that progressively degraded responsiveness was fixed alongside ([#1116]): an Alerts-tab `DispatcherTimer` that kept ticking after the tab closed, unbounded per-run alert-key dictionaries, a tray-service handler re-subscribed on every theme change, and plan-viewer controls leaked through a static theme event. (The related sleep/wake blank-window and software-rendering fix is tracked separately under [#1050] above.) Net effect: the UI stays responsive under heavy collection and query load

### Added

- **`tools/Remove-OrphanedTraceFiles.ps1`** ([#972]) — one-time cleanup script for `Monitor_LongQueries_*.trc` files left on disk by versions through 2.11.0. Run it on the SQL Server host; it skips files belonging to a running trace and files that are in use
- **`FactAdvice` and `FactRemediation` in `PerformanceMonitor.Analysis`** — new shared-library data layer that maps every scorable fact-key to a Headline / Investigation / Remediation advice block, plus a copy-paste-ready `sp_query_store_force_plan` T-SQL generator for `PLAN_REGRESSION` findings (gated to that single fact-key in v1; `PARAMETER_SENSITIVITY` deliberately does not generate plan-force T-SQL because forcing locks in the wrong plan for some parameter values). Drill-down collectors now also project `best_plan_id` (via `MAX(plan_id)` in the plan-dedup CTE) so the generated EXEC carries the integer ID `sp_query_store_force_plan` actually accepts, not just the hash. Lite's `BuildContext` now mirrors Dashboard's — both apps emit a Diagnosis card at `Details[0]` carrying Story / Severity / Notify threshold / Confidence / Facts / Database / Window before the drill-down items. The rendering surfaces that consume this data (email HTML, plain-text email, Teams + Slack webhook payloads, in-app Alert Details window) ship in a separate follow-up PR
- **Object- and index-level collection: sizes, growth, usage, and locking/contention** ([#1103]) — both apps gain a daily collector that snapshots per-table and per-index storage (`sys.dm_db_partition_stats`), index usage (`sys.dm_db_index_usage_stats` — seeks/scans/lookups/updates), and per-object locking/latch/escalation (`sys.dm_db_index_operational_stats` — row-lock waits, page-latch waits, lock-escalation attempts), all from stock DMVs verified stable from SQL Server 2016 through 2025 and on Azure SQL DB / Managed Instance. On-prem and MI iterate user databases (honoring the collector exclusion list); Azure SQL DB uses its single-database branch. Dashboard collects into `collect.index_object_stats` (`install/55_collect_index_object_stats.sql`, scheduled daily with 90-day retention picked up by dynamic retention); Lite collects into DuckDB with archival registered. Three new FinOps sub-tabs in each app — **Object Sizes & Growth** (per-table size plus 7-/30-day growth and daily rate), **Index Usage** (Unused / Write-only / Active classification), and **Locking & Contention** (top-contended indexes) — plus MCP read tools (`get_table_index_sizes`, `get_index_usage`, `get_object_locking`). Because the daily snapshots are cumulative, the new **object-growth** (`ANOMALY_OBJECT_GROWTH`, a table grew >100 MB and ≥20% day-over-day) and **lock-contention** (`ANOMALY_OBJECT_CONTENTION`, an index gained ≥60s of new row-lock wait) alerts are delta-based (the two most recent snapshots, reset-guarded) and flow through the existing anomaly → `AnalysisNotificationService` pipeline. Thresholds are fixed constants in this release; making them user-configurable is a follow-up
- **Recommendations / Apply Fix engine (advise-and-act rebuild)** — the analysis engine's advisory output is now a first-class **Recommendations** surface in both apps, alongside Critical Issues. Each finding renders as a card with a plain-language Headline / Investigation / Remediation block (from the `FactAdvice`/`FactRemediation` shared-library data layer) and routes the reader into the relevant in-app view or MCP tool instead of dumping raw DMV queries. Advise-only recommendations include server-config advisories (MAXDOP / cost threshold for parallelism / max server memory), per-database config (autogrowth, percent-growth on large files), server-health facts (Lock Pages in Memory, Instant File Initialization, recent memory dumps), and missing-index / plan-warning recommendations mined from collected plans — missing-index `CREATE` statements are surfaced as copy-paste text. A subset is **appliable in place behind informed, two-sided consent**: always-safe `ALTER DATABASE SET` config fixes, and the destructive **RCSI** (enable read-committed snapshot) and **clear cached plan** (`DBCC FREEPROCCACHE` / unforce) fixes, which gate behind an acknowledge-each-risk dialog that quantifies both the risk of changing and the risk of doing nothing from the finding's own monitoring data. The advice and remediation T-SQL also render across every notification surface — email (HTML and plain text), Teams and Slack webhook payloads, and the in-app Alert Details window — and through the `analyze_server` and `get_analysis_findings` MCP tools
- **Low volume free-space alert** ([#754]) in both apps — a new **Volume Free Space** alert (default on) fires when a monitored server's disk volume drops below a free-space percentage **or** a fixed GB amount (set either threshold to `0` to disable that dimension; if both are set, either breach fires). It reads the per-volume size/free data already collected by the database-size collector, evaluates every volume on the server, and fires one alert per server naming the worst (lowest-free) volume with up to five breaching volumes in the context — with the same cooldown, mute, alert-history, tray, and email plumbing as the existing tempdb-space alert. Defaults: 10% / 5 GB. Azure SQL DB has no volume data, so the alert never fires there
- **Failed SQL Agent job alert** ([#749]) in both apps — complements the existing job-duration alerts with a **Failed Agent Job** alert (default on) that issues a live `msdb.dbo.sysjobhistory` query at alert-check time for job-outcome rows (`step_id = 0`, `run_status = 0`) that failed within a configurable look-back window (default 60 minutes). The read degrades gracefully when the login lacks msdb / `SQLAgentReaderRole` access (returns empty, never faults the alert cycle) and is skipped entirely on Azure SQL DB, which has no SQL Agent
- **Installer: optional custom data/log file locations** ([#768]) — two optional CLI flags, `--data-path` and `--log-path` (both `--flag VALUE` and `--flag=VALUE` forms accepted), place the `PerformanceMonitor` database's `.mdf`/`.ldf` on specific server-side volumes at install time; an omitted flag falls back to the instance default path as before. The paths apply only on first creation (the create block is guarded by `IF DB_ID(N'PerformanceMonitor') IS NULL`), and Azure SQL Managed Instance ignores them. The path is validated and escaped (control characters and the dangerous filename characters are rejected; single quotes are doubled in both the C# injection layer and the dynamic `CREATE DATABASE`) because a data-file `FILENAME` literal cannot be parameterized

[#749]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/749
[#754]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/754
[#768]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/768
[#972]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/972
[#979]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/979
[#980]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/980
[#981]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/981
[#1012]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1012
[#1035]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1035
[#1050]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1050
[#1085]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1085
[#1086]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1086
[#1091]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1091
[#1092]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1092
[#1096]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1096
[#1103]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1103
[#1116]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1116
[#1121]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1121
[#1122]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1122
[#1135]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1135
[#1136]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1136
[#1205]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1205
[#1208]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1208
[#1210]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1210

## [2.11.0] - 2026-05-19

### Important

- **.NET 10 upgrade** — Dashboard, Lite, Installer, and Installer.Core now target `net10.0` (Windows projects target `net10.0-windows`). Building from source now requires the .NET 10 SDK; CI is pinned to 10.0.204 via `global.json` for reproducible builds. End users running prebuilt Velopack installers do not need to install anything separately — runtime is bundled ([#958])
- **Setup.exe is now the recommended install path** for Dashboard and Lite — the README steers users to the Velopack `Setup.exe`, which installs to `%LocalAppData%`, registers the apps under Apps & Features, creates Start Menu and Desktop shortcuts, and wires up auto-update. Portable ZIPs are still produced for both apps (CI release pipeline and local build scripts) as a fallback for advanced or air-gapped users. The Installer ZIP (CLI installer + SQL scripts) is unchanged
- **Shared `servers.json` location** — Dashboard and Lite now store `servers.json` under `%ProgramData%\PerformanceMonitor{Dashboard,Lite}\` so every Windows user on the same machine shares one server list. First run migrates an existing per-user `servers.json` to the new location and grants Authenticated Users Modify on the directory. SQL credentials remain per-user in Windows Credential Manager — each DBA re-enters SQL passwords on first connect; Windows Auth works with no re-entry

### Added

- **One-click snooze from the alert tray popup** in Lite — snooze an alert directly from the tray notification balloon without opening the main window ([#944])
- **Snooze hint in email and Teams/Slack alert payloads** — alert messages now show the snooze duration / scheduled wake time when an alert is fired while a snooze is active ([#944])
- **Process memory logging per collection cycle** in Lite — the collector now logs working set and private bytes at the end of each cycle, making it easier to track memory growth in long-running sessions

### Changed

- **Lite compaction memory tuning** ([#933]) — multiple changes to make parquet compaction robust on wide-row tables and large datasets:
    - Cap the main collector connection's `memory_limit` and raise it transiently only for the `COPY` step
    - Detect compaction `EXCLUDE` columns per merge step instead of once up front
    - Raise the compaction `memory_limit` floor to 4 GB
    - Set DuckDB `temp_directory` explicitly so spill files don't blow the OS temp drive
    - Compact parquet in size-budgeted batches instead of one mega-batch
- **Trace collectors honor `config.collector_database_exclusions`** ([#887] follow-up) — the trace-file based collectors now filter against the exclusions table, matching the behavior of the eight DMV-based per-database collectors shipped in v2.9.0
- **InstallerGui project directory removed** — the WPF InstallerGui was retired in v2.9.0 in favor of the Dashboard's integrated Add Server dialog. The project directory has now been deleted from the repo
- **Build warnings cleaned up** across Lite, Dashboard, and Installer ([#945])
- **GitHub Actions runners bumped** to Node 24-compatible major versions to silence deprecation warnings

### Fixed

- **Re-run `installation_history` column widening** for servers that crossed v2.4.0 → v2.5.0 before PR #828's fix shipped in v2.7.0. Those servers ran the original widen script as a no-op against `master`, then advanced their installer_version past 2.5, so the now-fixed script never reapplied. Adds an idempotent ALTER under an `IF EXISTS` guard checking `max_length = 510` ([#828])
- **Mute rules preserved across size-triggered DuckDB reset** in Lite — when the local DuckDB exceeded the configured size budget and was reset, mute rules were being lost. They now survive the reset ([#938])
- **Chart tooltips break after tab switch** — root-cause fix for the popup-wedge issue first patched in v2.10.0. Both the Memory tab handlers and `CorrelatedCrosshairManager` are now resilient to tab churn ([#916], [#937])
- **Stale `Monitor_LongQueries_*.trc` files cleaned up** by `config.data_retention` — the trace-file cleanup step previously left old `.trc` files behind on disk ([#951])
- **Nullability guards** added to the remaining comparison overlay tasks that were producing CS86xx warnings

[#828]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/828
[#887]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/887
[#916]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/916
[#933]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/933
[#937]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/937
[#938]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/938
[#944]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/944
[#945]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/945
[#951]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/951
[#958]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/958

## [2.10.0] - 2026-05-04

### Fixed

- **Memory tab tooltip** stops working after switching away and returning to the tab. Both Dashboard and Lite Memory tab crosshair tooltip handlers now reattach correctly on tab re-entry; the same popup-wedge fix is also applied to `CorrelatedCrosshairManager` ([#916])
- **FinOps memory recommendation** now bases sizing on a 7-day P95 of memory samples instead of a single snapshot, so recommendations no longer swing based on instantaneous workload state. Applied in both Dashboard and Lite ([#917])

### Changed

- **Per-database grants for FinOps Index Analysis** documented in the README — sp_IndexCleanup-backed Index Analysis requires per-database `EXECUTE` grants on each user database you want to analyze ([#915])

[#915]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/915
[#917]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/917

## [2.9.0] - 2026-04-29

### Important

- **Breaking change to `config.data_retention`** — the `@truncate_all` parameter has been removed. Pass `@retention_days = 0` for the same behavior. `@retention_days = NULL` (default) respects per-collector retention from `config.collection_schedule` with a 30-day fallback for unscheduled tables; `@retention_days = N > 0` overrides every table to N days. Any existing Agent jobs or scripts calling `data_retention @truncate_all = 1` need to be updated ([#900])
- **New `config.collector_database_exclusions` table** for per-database collector exclusions. Eight per-database collectors filter against this table; system databases remain hard-skipped by the collectors themselves. Existing installs get the table on the next upgrade — `install/01_install_database.sql` and `config.ensure_config_tables` both create it under an `IF OBJECT_ID … IS NULL` guard ([#887])

### Added

- **Per-database collector exclusions** — exclude noisy or unimportant databases from per-database collectors. Dashboard side adds `config.collector_database_exclusions` and filters 8 collectors (`query_stats`, `query_store`, `procedure_stats`, `file_io_stats`, `waiting_tasks`, `database_configuration`, `database_size_stats`, `server_properties`). Lite side adds an `ExcludedDatabases` list per server in `servers.json` and filters 9 collectors ([#887])
- **`Off` collection preset** — `EXECUTE config.apply_collection_preset @preset_name = N'Off'` disables every collector in one call. Pair with a second Agent job that applies a non-`Off` preset at the start of your active window for overnight / quiet-hours scoping. Non-`Off` presets now also set `enabled = 1` across the board so the switch from `Off → Balanced` reliably resumes collection ([#888])
- **Purge Now action** in Manage Servers — confirm dialog with a mode picker (Use configured / 1 / 3 / 7 / Custom / All) drives `config.data_retention`; right-click menu on the Manage Servers grid mirrors every per-row action (Edit, Toggle Favorite, Check Server Version, Purge Now, Remove) ([#900])
- **Total non-idle CPU on Lite Overview** — headline value shows total CPU with the SQL-only value alongside (e.g. `64% (SQL 60%)`); new `CpuAlertMode` dropdown in Settings → Alerts (Total / SqlOnly) drives both the alert evaluator and headline color; tray notifications and email alerts label the value as "Total CPU" or "SQL CPU" ([#899])
- **Resume gap detection** — `query_stats`, `procedure_stats`, and `query_store` collectors skip the historical sweep on first run after an Off preset, Agent stoppage, or server reboot. When the last successful run is older than 5× the configured `frequency_minutes` (floored at 30 minutes), the cutoff clamps to `SYSDATETIME()` so only forward-going data is collected on resume — preventing the tempdb blowout that hit the original reporter ([#892])
- **Right-click View Plan** on Dashboard Blocked Process Reports (View Blocked Plan + View Blocking Plan), Dashboard Deadlocks, and Lite Deadlocks grids. Plan lookup hits `sys.dm_exec_query_stats` + `sys.dm_exec_text_query_plan` on the monitored server, falling back to `executionStack/frame` entries when the process-level `sql_handle` is empty or evicted ([#880])
- **Open Log Folder** sidebar button in Lite — opens `%LocalAppData%\PerformanceMonitorLite\logs\` in Explorer for grabbing historical logs to attach to bug reports. Sits below View Log, which retains its existing behavior of opening today's log file ([#873])
- **Installed Version column** in the Manage Servers grid for both Dashboard and Lite. Dashboard shows the PerformanceMonitor database version on each server (probed in parallel via `GetInstalledVersionAsync`, with `Not installed` / `Unavailable` fallbacks). Lite shows the running app's own version on every row, mirroring Full's column header for consistency.
- **Lite-style server card indicators in Full** — back-ported the Ellipse-with-DataTriggers status dot (Online/Offline/Warning/Unknown) and the right-aligned favorite star from Lite to the Full Dashboard's server list, matching Lite's visual treatment.
- **Architecture overview** at `docs/how-collection-works.md` covering the minute loop, dispatcher, collector shape, `config.collection_schedule`, retention, and the Dashboard read path

### Changed

- **PlanIconMapper synced** with PerformanceStudio v1.9.0 improvements — columnstore storage type on scan/delete/insert/update/merge operators routes to `columnstore_index_*` icons (covers CCI and NCCI); `Parallelism` operator subtypes (Repartition Streams, Distribute Streams, Gather Streams) get their own icons
- **`Microsoft.Data.SqlClient` 6.1.4 → 7.0.1** — major-version bump. Azure/Entra dependencies were split out of the core package in 7.0; `Microsoft.Data.SqlClient.Extensions.Azure 1.0.0` added to Dashboard, Lite, and Installer.Core for `ActiveDirectoryInteractive` connections
- **`ModelContextProtocol` 0.7.0-preview.1 → 1.2.0** — off the preview tag and onto stable 1.x in Dashboard and Lite
- **`DuckDB.NET` 1.5.0 → 1.5.2** in Lite — fixes unbounded row group growth on indexed tables under repeated load+insert cycles, memory leaks and race conditions in prepared statements, WAL checkpoint marking, and Windows UTF-8/UTF-16 handling
- **`Microsoft.Extensions.*` 10.0.5 → 10.0.7**, **`System.Text.Json` 10.0.5 → 10.0.7**, **`ScottPlot.WPF` 5.1.57 → 5.1.58** — patch-level bumps with no expected behavioral change
- **Theme polish** on grids and plan viewer in Dashboard and Lite — thanks [@ClaudioESSilva](https://github.com/ClaudioESSilva) ([#889])

### Fixed

- **Install loop timeout** raised from 5 minutes to 1 hour. `install/98_validate_installation.sql` runs every enabled collector with `@debug = 1` in a single batch; on large databases (reporter had 7.2M rows in `collect.query_stats`, 4.4M in `collect.query_store_data`) this took ~9 minutes and was blowing the 5-minute timeout, failing the install or upgrade ([#884])

[#873]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/873
[#880]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/880
[#884]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/884
[#887]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/887
[#888]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/888
[#889]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/889
[#892]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/892
[#899]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/899
[#900]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/900

## [2.8.0] - 2026-04-22

### Important

- **New nonclustered indexes** on `collect.query_stats`, `collect.procedure_stats`, and `collect.query_store_data` to eliminate Eager Index Spools in Dashboard grid queries. On large installations these indexes may take several minutes to build; the upgrade script uses `ONLINE = ON` on Enterprise/Developer/Azure editions and falls back to offline on Standard/Web ([#835])

### Added

- **Memory Pressure Events in Lite** — the collector, chart, and `get_memory_pressure_events` MCP tool previously only in the Full Edition are now available in Lite ([#865])
- **Grid auto-scrolling** in Lite and Dashboard ([#843]) — thanks [@ClaudioESSilva](https://github.com/ClaudioESSilva)

### Changed

- **PlanAnalyzer and BenefitScorer** synced with PerformanceStudio's Apr 9–16 improvements
- **Query/Procedure/Query Store stats** refactored to a phased DECOMPRESS approach; removed unhelpful `WAITFOR DECOMPRESS` filters
- **Query/Procedure/Query Store grids** capped to TOP 500 to prevent UI freezes on large datasets
- **Server tabs lazy-load** — only the visible server tab loads on startup; remaining tabs load on first visit
- **Webhook URLs (Dashboard)** encrypted with DPAPI via Windows Credential Manager — Lite webhook URLs remain in plaintext settings for now
- **DuckDB queries hardened** — parameterized values, escaped paths, fixed `IsArchiving` race
- **Lite chart axes and sub-tab styling** polished, then ported to Dashboard

### Fixed

- **Memory Pressure Events chart filter** was dropping valid rows; added MCP interpretation guidance ([#865])
- **FinOps recommendation severity sort order** in Lite and Dashboard ([#872])
- **Overview crosshair** disappearing after tab switches or layout passes
- **Blocked process report plan lookup** returning the wrong plan ([#867])
- **FinOps TDE recommendation** flagging Standard edition on SQL Server 2019+ where TDE is free ([#854])
- **Azure SQL DB collector** falls back to single-database mode when `master` is inaccessible ([#857])
- **Azure SQL DB query snapshots** scoped to the current database ([#857])
- **Azure SQL DB query snapshot prefilter** — request set is narrowed into `#temp` before joining DMVs to avoid Azure-specific execution plan issues ([#857])
- **Azure SQL DB live query plans** — now skipped gracefully instead of erroring ([#857])
- **Azure SQL DB memory_stats collector** — dropped `sys.dm_os_schedulers` which is blocked on elastic-pool contained users regardless of DB-scoped grants ([#857])
- **Non-transient permission denials** now stop collector retries instead of looping forever ([#857])

[#835]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/835
[#843]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/843
[#854]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/854
[#857]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/857
[#865]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/865
[#867]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/867
[#872]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/872

## [2.7.0] - 2026-04-13

### Added

- **Host OS column** in Server Inventory for both Dashboard and Lite ([#748], [#823])
- **Offline community script support** via `community/` directory for user-contributed scripts ([#814], [#822])
- **MultiSubnetFailover connection option** in Dashboard and Lite for Always On availability groups ([#813], [#821])

### Changed

- **PlanAnalyzer and ShowPlanParser** synced from PerformanceStudio with latest improvements ([#816])
- **MCP query tools** optimized for large databases ([#826])
- **Add Server dialog UX** improved with inline connection status and full-height window
- **"CPUs" renamed to "Logical CPUs"** for clarity in Lite ([#825])

### Fixed

- **Dashboard auto-refresh stalling under load** — replaced DispatcherTimer with async Task.Delay loop to prevent priority starvation during heavy chart rendering ([#833], [#834])
- **Lite auto-refresh silently skipping** every tick ([#824])
- **Deadlock count not resetting** between collections ([#803], [#820])
- **Upgrade filter skipping patch versions** during version comparison ([#817], [#819])
- **Upgrade script executing against master** instead of PerformanceMonitor database ([#828])
- **Duplicate release builds** triggering on both created and published events

[#748]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/748
[#803]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/803
[#813]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/813
[#814]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/814
[#816]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/816
[#817]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/817
[#819]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/819
[#820]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/820
[#821]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/821
[#822]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/822
[#823]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/823
[#824]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/824
[#825]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/825
[#826]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/826
[#828]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/828
[#833]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/833
[#834]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/834

## [2.6.0] - 2026-04-08

### Added

- **Correlated timeline lanes** on Lite Overview and Dashboard — synchronized CPU, memory, waits, and TempDB trend lanes for at-a-glance correlation ([#688])
- **Dynamic baselines and anomaly detection** in Lite and Dashboard — automatic baseline calculation with anomaly highlighting on key metrics ([#692], [#693])
- **Query grid comparison** — before/after comparison mode for query grids in Lite and Dashboard with global Compare dropdown ([#687])
- **Nonclustered index count badge** on modification operators in plan viewer ([#788])
- **Upgrade detection in Edit Server** dialog — see pending upgrades without adding a new server ([#772])
- **CLI installer interactive mode** prompts for trust-cert and encryption settings ([#784])
- **SignPath code signing** — release binaries are now digitally signed via the [SignPath FOSS](https://signpath.io) program

### Changed

- **PlanAnalyzer Rule 3 (Serial Plan)** comprehensively refined — severity demotion for TRIVIAL and 0ms plans, `CouldNotGenerateValidParallelPlan` treated as actionable, all 25 `NonParallelPlanReason` values now covered
- **PlanAnalyzer warning rules** ported from PerformanceStudio improvements
- **Text readability** — replaced all muted/dim text colors with full foreground colors for readability

### Fixed

- **Embedded resource upgrade discovery** broken — upgrades silently returned zero results for Dashboard installs ([#772])
- **Archive compaction OOM** on large parquet groups
- **CLI installer argument parsing** treating flags as positional args ([#786])
- **Lite long-running query alerts** firing on stale DuckDB snapshots
- **FinOps Enterprise feature detection** now queries all databases and filters to TDE only ([#780])
- **Second launch error** — now brings existing window to foreground instead ([#769])
- **Overview tab Memory Grant** showing 0 for all timestamps ([#776])
- **Lite FinOps Enterprise features** query error on servers without `database_id` column ([#777])
- **Collector health status** incorrect for on-load collectors
- **CSV and clipboard exports** writing `System.Windows.Controls.StackPanel` as column headers instead of actual header text ([#805])

[#687]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/687
[#688]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/688
[#692]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/692
[#693]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/693
[#769]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/769
[#772]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/772
[#776]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/776
[#777]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/777
[#780]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/780
[#784]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/784
[#786]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/786
[#788]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/788
[#805]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/805

## [2.5.0] - 2026-03-30

### Important

- **InstallerGui retired**: The standalone GUI installer has been removed. Installation, upgrade, and uninstall are now handled directly from the Dashboard's Add Server dialog, powered by the new Installer.Core shared library. The CLI installer continues to work as before. ([#755])

### Added

- **Dashboard integrated installer** — Add Server dialog now installs, upgrades, and uninstalls PerformanceMonitor directly, replacing the standalone InstallerGui ([#755])
- **Installer.Core shared library** — shared installation logic used by both the CLI installer and Dashboard ([#755])
- **Overview tab** for Lite with 2x2 resource chart grid (CPU, Memory, Wait Stats, TempDB) ([#689])
- **Chart drill-down** on CPU, Memory, TempDB, Blocking, and Deadlock charts in both Dashboard and Lite — right-click any chart point to jump to Active Queries for that time window ([#682])
- **Grid-to-slicer overlay** for Query Stats, Procedure Stats, and Query Store tabs — click a row to overlay its trend on the slicer chart ([#683])
- **Query heatmap** tab in both Dashboard and Lite — visual heat map of query activity over time ([#739], [#743])
- **Webhook notifications** for alerts — configurable webhook endpoint for alert delivery ([#725])
- **Per-server collector schedule intervals** — customize collection frequency per server ([#703])
- **Investigate button** in Critical Issues grid — jump directly to relevant tab from an alert ([#684])
- **Dismiss Selected** context menu and View Log sidebar button for alert management ([#718], [#740])
- **Alert archival awareness** — dismissed_archive_alerts sidecar table, source column for live vs archived alerts, stale-data indicator, structured telemetry ([#718])
- **Dashboard read-only connection intent** — connections use `ApplicationIntent=ReadOnly` where supported ([#728])
- FUNDING.yml for GitHub Sponsors ([#752])

### Changed

- **Installer architecture** refactored: CLI installer is now a thin wrapper over Installer.Core ([#755])
- **DuckDB memory capped** at 2 GB during parquet compaction to prevent out-of-memory on large archives ([#758])
- **Text rendering** improved with `TextOptions.TextFormattingMode="Display"` for sharper text ([#710])
- **installation_history version columns** widened from nvarchar(255) to nvarchar(512) to handle long @@VERSION strings ([#712])

### Fixed

- **Memory leaks in Lite** — delta cache, event handlers, and chart helpers properly disposed ([#758])
- **Doomed transaction errors** in delta framework and ensure_collection_table — ROLLBACK now occurs before error logging ([#756])
- **XACT_STATE check** added after third-party stored procedure calls (sp_HumanEventsBlockViewer, sp_BlitzLock) to prevent doomed transaction errors ([#695])
- **CREATE DATABASE failure** when model database has large default file sizes ([#676])
- **CPU metrics mixed** for different Azure SQL databases on the same logical server ([#680])
- **Azure SQL DB vCore** FinOps calculations incorrect for serverless/vCore tiers ([#736])
- **Webhook alert recording** not persisting correctly ([#726])
- **Drill-down timezone** misalignment between chart and detail view ([#747], [#750])
- **Drill-down refresh** losing context on auto-refresh ([#744])
- **Drill-down target** incorrectly routing Memory to Memory Grants instead of Active Queries ([#706])
- **Heatmap colorbar stacking** when switching between servers ([#746])
- **Display mode pickers** not reflecting current state on tab switch ([#751])
- **Slicer custom range** handling and sub-hour display issues ([#704])
- **Overlay selection** lost on Dashboard auto-refresh ([#683])
- **Numeric values** in alert details treated as strings instead of numbers ([#732])
- **FinOps VM right-sizing** query error — `PERCENTILE_CONT` missing required `OVER()` clause
- **FinOps Enterprise features** query error on AWS RDS — `database_id` column not present in `sys.dm_db_persisted_sku_features` on RDS
- **FinOps right-click copy** broken on all Dashboard FinOps grids — context menu walked to row instead of grid
- **FinOps recommendation error logs** now include server name for easier troubleshooting

### Deprecated

- **InstallerGui** — removed from the solution and build pipeline. Use the Dashboard or CLI installer instead. ([#755])

[#676]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/676
[#680]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/680
[#682]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/682
[#683]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/683
[#684]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/684
[#689]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/689
[#695]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/695
[#703]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/703
[#704]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/704
[#706]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/706
[#710]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/710
[#712]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/712
[#718]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/718
[#725]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/725
[#726]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/726
[#728]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/728
[#732]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/732
[#736]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/736
[#739]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/739
[#740]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/740
[#743]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/743
[#744]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/744
[#746]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/746
[#747]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/747
[#750]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/750
[#751]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/751
[#752]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/752
[#755]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/755
[#756]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/756
[#758]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/758

## [2.4.0] - 2026-03-23

### Important

- **Lite data directory moved**: Lite now stores all data (config, DuckDB, archives, logs) in `%LOCALAPPDATA%\PerformanceMonitorLite\` instead of alongside the executable. This enables auto-update support. Existing users upgrading from the zip should use **Import Settings** and **Import Data** to bring over their configuration and historical data from the old install folder.
- **Auto-update (Windows)**: Both Dashboard and Lite now include Velopack auto-update. Users who install via the new Setup.exe will receive update notifications and can download + apply updates from within the app. Existing zip distribution continues to work as before.

### Added

- **Velopack auto-update** for Dashboard and Lite — check on startup, download + apply from About window with confirmation dialog before restart ([#635])
- **Per-tab time range slicers** on Dashboard and Lite query tabs — filter data directly on each tab without changing global time range ([#655], [#662])
- **Time display picker** (Local/UTC/Server) in Dashboard and Lite toolbars ([#646])
- **Import Settings** — renamed from "Import Connections", now also copies `settings.json`, `collection_schedule.json`, `ignored_wait_types.json`, and `alert_state.json` from a previous install
- **Alert muting improvements** — pre-fill context fields (database, query, wait type, job name) from alert detail text, configurable default expiration for new mute rules, tooltip on query text field ([#642])
- **Missing date columns** on Query Stats and Procedure Stats tabs (`creation_time`, `last_execution_time`) ([#649], [#651], [#654])
- **Trace pattern drill-down** now includes `CollectionTime` and `NtUserName` columns ([#663])
- **DataGrid sort preservation** across auto-refresh — sort order no longer resets when data refreshes ([#659])
- **CLI installer**: colored output (green/red/yellow) and version check on startup ([#639])
- **GUI installer**: version check on startup
- **Growth rate and VLF count** columns in Database Sizes (from v2.3.0 nightly, now in upgrade path) ([#567])
- `llms.txt` and `CITATION.cff` for project discoverability ([#630])

### Changed

- **Lite data directory** moved to `%LOCALAPPDATA%\PerformanceMonitorLite\` for Velopack compatibility
- **Delta gap detection** added to all cumulative-counter collectors (file I/O, wait stats, query stats, procedure stats, memory grants) — prevents inflated spikes after app restart ([#633])
- **File I/O NULL fallbacks** improved when `sys.master_files` is inaccessible — falls back to `DB_NAME()` and `File_{id}` instead of generic "Unknown" ([#633])
- **Running jobs collector** skipped gracefully when login lacks msdb access ([#656])
- NuGet packages updated to latest minor versions ([#653])

### Fixed

- **Installer writing SUCCESS when files fail** — CLI tolerated 1 failure in automated mode, GUI had a similar workaround. Now any failure = not success.
- **Query stats collector causing SQL dumps** on passive mirror servers — removed `dm_exec_plan_attributes` CROSS APPLY, uses temp table of ONLINE database IDs instead ([#632])
- **Trigger name extraction** fails when comment before `CREATE TRIGGER` contains " ON " ([#666])
- **FinOps expensive queries** DuckDB error — query referenced `statement_start_offset` column that doesn't exist in schema
- **Imported parquet files** not recognized by archive compaction — added regex patterns for `imported_` prefix
- **Auto-refresh after Import Data** — views now refresh immediately after import completes

[#630]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/630
[#632]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/632
[#633]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/633
[#635]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/635
[#639]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/639
[#642]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/642
[#646]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/646
[#649]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/649
[#651]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/651
[#653]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/653
[#654]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/654
[#655]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/655
[#656]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/656
[#659]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/659
[#662]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/662
[#663]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/663
[#666]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/666
[#567]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/567

## [2.3.0] - 2026-03-18

### Important

- **Schema upgrade**: Six columns widened across three tables (`query_stats`, `cpu_scheduler_stats`, `waiting_tasks`, `database_size_stats`) to match DMV documentation types. These are in-place ALTER COLUMN operations — fast on any table size, no data migration. Upgrade scripts run automatically via the CLI/GUI installer.
- **SQL Server version check**: Both installers now reject SQL Server 2014 and earlier before running any scripts, with a clear error message. Azure MI (EngineEdition 8) is always accepted. ([#543])
- **Installer adversarial tests**: 35 automated tests covering upgrade failures, data survival, idempotency, version detection fallback, file filtering, restricted permissions, and more. These run as part of pre-release validation. ([#543])

### Added

- **ErikAI analysis engine** — rule-based inference engine for Lite that scores server health across wait stats, CPU, memory, I/O, blocking, tempdb, and query performance. Surfaces actionable findings with severity, detail, and recommended actions. Includes anomaly detection (baseline comparison for acute deviations), bad actor detection (per-query scoring for consistently terrible queries), and CPU spike detection for bursty workloads. ([#589], [#593])
- **ErikAI Dashboard port** — full analysis engine ported to Dashboard with SQL Server backend ([#590])
- **FinOps cost optimization recommendations** — Phase 1-4 checks: enterprise feature audit, CPU/memory right-sizing, compression savings estimator, unused index cost quantification, dormant database detection, dev/test workload detection, VM right-sizing, storage tier optimization, reserved capacity candidates ([#564])
- **FinOps High Impact Queries** — 80/20 analysis showing which queries consume the most resources across all dimensions ([#564])
- **FinOps dollar-denominated cost attribution** — per-server monthly cost setting with proportional database-level breakdown ([#564])
- **On-demand plan fetch** for bad actor and analysis findings — click to retrieve execution plans for flagged queries ([#604])
- **Plan analysis integration** — findings include execution plan analysis when plans are available ([#594])
- **Server unreachable email alerts** — Dashboard sends email (not just tray notification) when a monitored server goes offline or comes back online ([#529])
- **Column filters on all FinOps DataGrids** — filter funnel icons on every column header across all 7 FinOps grids in Lite and Dashboard ([#562])
- **Column filters on Dashboard** IdleDatabases, TempDB, and Index Analysis grids
- **Lite data import** — "Import Data" button brings in monitoring history from a previous Lite install via parquet files, preserving trend data across version upgrades ([#566])
- **Per-server Utility Database setting** — Lite can call community stored procedures (sp_IndexCleanup) from a database other than master ([#555])
- **SQL Server version check** in both CLI and GUI installers — rejects 2014 and earlier with a clear message ([#543])
- **Execution plan analysis MCP tools** for both Dashboard and Lite
- **Full MCP tool coverage** — Dashboard expanded from 28 to 57 tools, Lite from 32 to 51 tools ([#576], [#577])
- **Self-sufficient analyze_server drill-down** — MCP tool returns complete analysis, not breadcrumb trail ([#578])
- **NuGet package dependency licenses** in THIRD_PARTY_NOTICES.md

### Changed

- **Azure SQL DB FinOps** — all collectors (database sizes, query stats, file I/O) now connect to each database individually instead of only querying master. Server Inventory uses dynamic SQL to avoid `sys.master_files` dependency. ([#557])
- **Index Analysis scroll fix** — both summary and detail grids now use proportional heights instead of Auto, so they scroll independently with large result sets ([#554])
- **Dashboard Add Server dialog** — increased MaxHeight from 700 to 850px so buttons are visible when SQL auth fields are shown
- **GUI installer** — Uninstall button now correctly enables after a successful install
- **GUI installer** — fixed encryption mapping and history logging ([#612])
- **Dashboard visible sub-tab only refresh** on auto-refresh ticks ([#528])
- Analysis engine decouples data maturity check from analysis window

### Fixed

- **Installer dropping database on every upgrade** — `00_uninstall.sql` excluded from install file list, installer aborts on upgrade failure, version detection fallback returns "1.0.0" instead of null ([#538], [#539])
- **SQL dumps on mirroring passive servers** from FinOps collectors ([#535])
- **RetrievedFromCache** always showing False ([#536])
- **Arithmetic overflow** in query_stats collector for dop/thread columns ([#547])
- **Lite perfmon chart bugs** and Dashboard ScottPlot crash handling ([#544], [#545])
- **PLE=0 scoring bug** — was scored as harmless, now correctly flagged ([#543])
- **PercentRank >1.0** bug in HealthCalculator
- **6 verified Lite bugs** from code review ([#611])
- **Enterprise feature audit text** — partitioning is not Enterprise-only
- **FinOps collector scheduling**, server switch, and utilization bugs
- **Dashboard drill-down** Unicode arrow in story path split
- **Empty DataGrid scrollbar artifacts** — hide grids when empty across all FinOps tabs
- **Query preview** — truncated in row, full text in tooltip

[#529]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/529
[#535]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/535
[#536]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/536
[#538]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/538
[#539]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/539
[#543]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/543
[#544]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/544
[#545]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/545
[#547]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/547
[#554]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/554
[#555]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/555
[#557]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/557
[#562]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/562
[#564]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/564
[#566]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/566
[#576]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/576
[#577]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/577
[#578]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/578
[#528]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/528
[#589]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/589
[#590]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/590
[#593]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/593
[#594]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/594
[#604]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/604
[#611]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/611
[#612]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/612

## [2.2.0] - 2026-03-11

**Contributors:** [@HannahVernon](https://github.com/HannahVernon), [@ClaudioESSilva](https://github.com/ClaudioESSilva), [@dphugo](https://github.com/dphugo), [@Orestes](https://github.com/Orestes) — thank you!

### Important

- **Schema upgrade**: Three large collection tables (`query_stats`, `procedure_stats`, `query_store_data`) are migrated to use `COMPRESS()` for query text and plan columns. The upgrade performs a table swap (create new → migrate data → rename) which may take several minutes on large tables. A `row_hash` column is added for deduplication. Three new tracking tables are also created. Volume stats columns are added to `database_size_stats`. Upgrade scripts run automatically via the CLI/GUI installer and use idempotent checks.

  Compression results measured on a production instance:

  | Table | Compressed | Uncompressed | Ratio |
  |---|---|---|---|
  | query_stats | 18.0 MB | 339.0 MB | 18.8x |
  | query_store_data | 13.5 MB | 258.0 MB | 19.1x |
  | **Total** | **31.5 MB** | **597 MB** | **~19x** |

### Added

- **FinOps monitoring tab** — database size tracking, server properties, storage growth analysis (7d/30d), index analysis with unused/duplicate/compressible detection, utilization efficiency, idle database identification, and estate-level resource views ([#474])
- **Named collection presets** — Aggressive, Balanced, and Low-Impact schedule profiles via `config.apply_collection_preset` ([#454])
- **Entra ID interactive MFA authentication** in both CLI and GUI installers for Azure SQL MI connections ([#481])
- **MCP port validation** — TCP port conflict detection, range validation (1024+), Auto port button, and auto-restart on settings change ([#453])
- **Alert database exclusion filters** — filter blocking and deadlock alerts by database in both Dashboard and Lite ([#410], [#412])
- **Configurable alert cooldown periods** for tray notifications and email alerts
- **Wait stats query drill-down** — click a wait type to see the queries causing it ([#372])
- **Configurable long-running query settings** — max results, WAITFOR/backup/diagnostics exclusions ([#415])
- **Uninstall option** in both CLI and GUI installers ([#431])
- **Session stats collector** for active session tracking ([#474])
- **LOB compression and deduplication** for query stats tables to reduce storage ([#419])
- **Volume-level drive space** enrichment in database size stats via `dm_os_volume_stats`
- **GUI installer installation history** logging to `config.installation_history` ([#414])
- **ReadOnlyIntent connection option** — Lite connections can set `ApplicationIntent=ReadOnly` for automatic read routing to Always On AG readable secondaries ([#515])
- **Alert muting** — mute individual alerts or create pattern-based mute rules by server, metric, database, or application. Manage Mute Rules window with enable/disable toggle. Alert history detail view with double-click drill-down and context-sensitive detail text. Poison wait type documentation links. ([#512])
- **SignPath code signing** — all release binaries (Dashboard, Lite, Installers) are digitally signed, eliminating Windows SmartScreen warnings ([#511])
- CI version bump check on PRs to main
- Permissions section in README with least-privilege setup ([#421])

### Changed

- **Utilization tab redesigned** — ported to Dashboard with aligned metrics between apps ([#478])
- PlanAnalyzer rules synced from PerformanceStudio — Rule 5 message format, seek predicate parsing, spool labels, unmatched index detail ([#416], [#475], [#480])
- Data retention now purges processed XE staging rows
- GeneratedRegex conversion for compile-time regex patterns ([#346], [#420])
- Server health card width increased from 260 to 300 for less text truncation ([#489])
- User's locale used for date/time formatting in WPF bindings ([#459])
- XML processing instructions stripped from sql_command/sql_text display
- Parameterized queries in blocking/deadlock alert filtering
- **DuckDB 1.5.0 upgrade** — non-blocking checkpointing eliminates read stalls during WAL flushes, free block reuse stabilizes database file size without archive-and-reset cycles ([#516])
- **Automatic parquet compaction** — archive files are merged into monthly files after each archive cycle, reducing file count from 2,600+ to ~75 and eliminating per-file metadata overhead on glob scans ([#516])

  Combined with the UI responsiveness overhaul (#510), Lite's refresh cycle improved 13-26x:

  | Metric | Before | After |
  |---|---|---|
  | Lite `RefreshAllDataAsync` | 6-13s | < 500ms |
  | Parquet files scanned per query | 233 | 19 |
  | Archive-and-reset frequency | 21/day | ~0 |
  | `v_wait_stats` query time | 1,700ms | 27ms |

- **Monthly archive retention** — switched from 90-day file-age deletion to 3-month calendar-month rolling window, aligned with compacted monthly filenames ([#516])
- **Lite status bar** shows used data size vs file size (e.g., "Database: 175.5 / 423.8 MB") via DuckDB `pragma_database_size()` ([#517])
- **Query Store collector diagnostics** — reader/append/flush timing breakdown logged when collection exceeds 2 seconds, for identifying SQL Server DMV contention under heavy workloads ([#518])
- SSMS-parity edge tooltips on plan viewer operator connections and ManyToMany indicator always shown for merge join operators ([#504])
- **Lite UI responsiveness overhaul** — visible-tab-only refresh, sub-tab awareness, Query Store collector optimization (NULL plan XML + LOOP JOIN hint), and DuckDB write reduction ([#510])

  Timer tick improvements measured under TPC-C load on SQL2022:

  | Scenario | Before | After | Improvement |
  |---|---|---|---|
  | Lite idle | 6-13s | 546-750ms | ~90% |
  | Lite under TPC-C | 6-13s | ~3s | ~70% |
  | Dashboard idle | 5.6s | 0.6-0.8s | 86% |
  | Dashboard under TPC-C | 5.6s | 1.8-2.0s | 64% |

  Query Store collector specifically:

  | Metric | Before | After |
  |---|---|---|
  | query_store collector total | 6-18s | ~600ms |
  | query_store SQL time | 374-1,104ms | ~300ms (LOOP JOIN hint) |
  | query_store DuckDB write | 6-16s | ~75-230ms (NULL plan XML) |

### Fixed

- **UI hang** when opening Dashboard tab for offline server — replaced synchronous `.GetAwaiter().GetResult()` with proper `await` ([#477])
- **First-collection spike** skewing PerfMon, wait stats, file I/O, memory grant, query stats, and procedure stats charts — first cumulative value now treated as baseline ([#482])
- **Wait type filter TextBox** too small to read ([#488])
- **Poison wait false positives** and alert log parsing ([#445], [#448])
- **RID Lookup** analyzer rule matching new PhysicalOp label ([#429])
- **procedure_stats** plan query using DECOMPRESS after compression migration
- **database_size_stats** InvalidCastException on compatibility_level
- **Deadlock filter** using wrong column reference in `GetFilteredDeadlockCountAsync`
- **RESTORING database** filter added to waiting_tasks collector ([#430])
- Custom TrayToolTip crash — replaced with plain ToolTipText ([#422])
- **Lite tab switch freeze** — added `_isRefreshing` guard to prevent tab switch handler from competing with timer ticks for DuckDB connection, eliminating "not responding" hangs ([#510])
- DuckDB read lock acquisition resilience
- Formatted duration columns sorting alphabetically instead of numerically
- Settings window staying open on validation errors
- Deserialization clamping and validation abort issues
- **sp_IndexCleanup** summary grid column mapping off-by-one, expanded both grids to show all columns from both result sets ([#503])
- **Rule 22 table variable** false positive on modification operators — INSERT/UPDATE/DELETE on table variables is expected ([#513])
- **ComboBox focus steal** in plan viewer stealing keyboard focus from other controls ([#508])
- **DOP 2 skew** false positive — parallel skew rule no longer fires at DOP 2 ([#508])
- **ReadOnlyIntent connections** sharing server_id in DuckDB when the same server was added with and without ReadOnlyIntent ([#521])

[2.2.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v2.1.0...v2.2.0

## [2.1.0] - 2026-03-04

### Important

- **Schema upgrade**: The `config.collection_schedule` table gains two new columns (`collect_query`, `collect_plan`) for optional query text and execution plan collection. Both default to enabled to preserve existing behavior. Upgrade scripts run automatically via the CLI/GUI installer and use idempotent checks.

### Added

- **Light theme and "Cool Breeze" theme** — full light mode support for both Dashboard and Lite with live preview in settings ([#347])
- **Standalone Plan Viewer** — open, paste (Ctrl+V), or drag & drop `.sqlplan` files independent of any server connection, with tabbed multi-plan support ([#359])
- **Time display mode toggle** — show timestamps in Server Time, Local Time, or UTC with timezone labels across all grids and tooltips ([#17])
- **30 PlanAnalyzer rules** — expanded from 12 to 30 rules covering implicit conversions, GetRangeThroughConvert, lazy spools, OR expansion, exchange spills, RID lookups, and more ([#327], [#349], [#356], [#379])
- **Wait stats banner** in plan viewer showing top waits for the query ([#373])
- **UDF runtime details** — CPU and elapsed time shown in Runtime Summary pane when UDFs are present ([#382])
- **Sortable statement grid** and canvas panning in plan viewer ([#331])
- **Comma-separated column filters** — enter multiple values separated by commas in text filters ([#348])
- **Optional query text and plan collection** — per-collector flags in `config.collection_schedule` to disable query text or plan capture ([#337])
- **`--preserve-jobs` installer flag** — keep existing SQL Agent job schedules during upgrade ([#326])
- **Copy Query Text** context menu on Dashboard statements grid ([#367])
- **Server list sorting** by display name in both Dashboard and Lite ([#30])
- **Warning status icon** in server health indicators ([#355])
- Reserved threads and 10 missing ShowPlan XML attributes in plan viewer ([#378])
- Nightly build workflow for CI ([#332])

### Changed

- PlanAnalyzer warning messages rewritten to be actionable with expert-guided per-rule advice ([#370], [#371])
- PlanAnalyzer rule tuning: time-based spill analysis (Rule 7), lowered parallel skew thresholds (Rule 8), memory grant floor raised to 1GB/4GB (Rule 9), skip PROBE-only bitmap predicates (Rule 11) ([#341], [#342], [#343], [#358])
- First-run collector lookback reduced from 3-7 days to 1 hour for faster initial data ([#335])
- Plan canvas aligns top-left and resets scroll on statement switch ([#366])
- Plan viewer polish: index suggestions, property panel improvements, muted brush audit ([#365])
- Add Server dialog visual parity between Dashboard and Lite with theme-driven PasswordBox styling ([#289])

### Fixed

- **OverflowException** on wait stats page with large decimal values — SQL Server `decimal(38,24)` exceeding .NET precision ([#395])
- **SQL dumps** on mirroring passive servers with RESTORING databases ([#384])
- **UI hang** when adding first server to Dashboard ([#387])
- **UTC/local timezone mismatch** in blocked process XML processor ([#383])
- **AG secondary filter** skipping all inaccessible databases in cross-database collectors ([#325])
- DuckDB column aliases in long-running queries ([#391])
- sp_server_diagnostics and WAITFOR excluded from long-running query alerts ([#362])
- UDF timing units corrected: microseconds to milliseconds ([#338])
- DuckDB migration ordering after archive-and-reset ([#314])
- Int16 cast error in long-running query alerts ([#313])
- Missing dark mode on 19 SystemEventsContent charts ([#321])
- Missing tooltips on charts after theme changes ([#319])
- Operator time per-thread calculation synced across all plan viewers ([#392])
- Theme StaticResource/DynamicResource binding fix for runtime theme switching
- Memory grant MB display, missing index quality scoring, wildcard LIKE detection ([#393])
- **Installer validation** reporting historical collection errors as current failures — now filters to current run only ([#400])
- **query_snapshots schema mismatch** after sp_WhoIsActive upgrade — collector auto-recreates daily table when column order changes ([#401])
- **Missing upgrade script** for `default_trace_events` columns (`duration_us`, `end_time`) on 2.0.0→2.1.0 upgrade path ([#400])

## [2.0.0] - 2026-02-25

### Important

- **Schema upgrade**: The `collect.memory_grant_stats` table gains new delta columns and drops unused warning columns. The `collect.session_wait_stats` table, its collector procedure, reporting view, and schedule entry are removed (zero UI coverage). Upgrade scripts run automatically via the CLI/GUI installer and use idempotent checks.

### Added

- **Graphical query plan viewer** — native ShowPlan rendering in both Dashboard and Lite with SSMS-parity operator icons, properties panel, tooltips, warning/parallelism badges, and tabbed plan display ([#220])
- **Actual execution plan support** — execute queries with SET STATISTICS XML ON to capture actual plans, with loading indicator and confirmation dialog ([#233])
- **PlanAnalyzer** — automated plan analysis with rules for missing indexes, eager spools, key lookups, implicit conversions, memory grants, and more
- **Current Active Queries live snapshot** — real-time view of running queries with estimated/live plan download ([#149])
- **Memory clerks tab** in Lite with picker-driven chart ([#145])
- **Current Waits charts** in Blocking tab for both Dashboard and Lite ([#280])
- **File I/O throughput charts** — read/write throughput trends, file-level latency breakdown, queued I/O overlay ([#281])
- **Memory grant stats charts** — standardized collection with delta framework integration and trend visualization ([#281])
- **CPU scheduler pressure status** — real-time scheduler, worker, runnable task counts with color-coded pressure level below CPU chart
- **Collection log drill-down** and daily summary in Lite ([#138])
- **Collector duration trends chart** in Dashboard Collection Health ([#138])
- **Themed perfmon counter packs** — 14 new counters with organized themed groups ([#255])
- **User-configurable connection timeout** setting ([#236])
- **Per-collector retention** — uses per-collector retention from `config.collection_schedule` in data retention ([#237])
- **Query identifiers** in drill-down windows — query hash, plan hash, SQL handle visible for identification ([#268])
- **Trace pattern drill-down** with missing columns and query text tooltips ([#273])
- **Query Store Regressions drill-down** with TVF rewrite for performance ([#274])
- **CLI `--help` flag** for installer ([#111])
- Sort arrows, right-aligned numerics, and initial sort indicators across all grids ([#110])
- Copyable plan viewer properties ([#269])
- Standardized chart save/export filenames between Dashboard and Lite ([#284])
- Full Dashboard column parity for query_stats, procedure_stats, and query_store_stats
- Min/max extremes surfaced in both apps — physical reads, rows, grant KB, spills, CLR time, log bytes ([#281])

### Changed

- Query Store detection uses `sys.database_query_store_options` instead of `sys.databases.is_query_store_on` for Azure SQL DB compatibility ([#287])
- Config tab consolidation, DB drop on server remove, DuckDB-first plan lookups, procedure stats parity
- Collector health status now detects consecutive recent failures — 5+ consecutive errors = FAILING, 3+ = WARNING
- Plan buttons now show a MessageBox when no plan is available instead of silently doing nothing
- CSV export uses locale-appropriate separators for non-US locales ([#240])
- Query Store Regressions and Query Trace Patterns migrated to popup grid filtering ([#260])
- NuGet packages updated; xUnit v3 migration

### Fixed

- **DuckDB file corruption** during maintenance — ReaderWriterLockSlim coordination, archive-all-and-reset at 512MB replaces compaction ([#218])
- Archive view column mismatch, wait_stats thread-safety, and percent_complete type cast ([#234])
- Collector health status bar text color ([#234])
- View Plan for Query Store and Query Store Regressions tabs ([#261])
- Query Store drill-down time filter alignment with main view ([#263])
- Execution count mismatches between main views and drill-downs
- Drill-down chart UX — sparse data markers, hover tooltips, window sizing ([#271])
- Truncated status text in Add Server dialog ([#257])
- Scrollbar visibility, self-filtering artifacts, missing columns, and context menus ([#245], [#246], [#247], [#248])
- query_stats and procedure_stats collectors ignoring recent queries
- Blank tooltips on warning and parallel badge icons
- Missing chart context menu on File I/O Throughput charts in Lite

### Removed

- `collect.session_wait_stats` table, `collect.session_wait_stats_collector` procedure, `report.session_wait_analysis` view, and schedule entry — zero UI coverage, never surfaced in Dashboard or Lite ([#281])

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

[2.1.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v1.3.0...v2.0.0
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
[#218]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/218
[#220]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/220
[#233]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/233
[#234]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/234
[#236]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/236
[#237]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/237
[#240]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/240
[#245]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/245
[#246]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/246
[#247]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/247
[#248]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/248
[#255]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/255
[#257]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/257
[#260]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/260
[#261]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/261
[#263]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/263
[#268]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/268
[#269]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/269
[#271]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/271
[#273]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/273
[#274]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/274
[#280]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/280
[#281]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/281
[#284]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/284
[#287]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/287
[#313]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/313
[#314]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/314
[#17]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/17
[#30]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/30
[#319]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/319
[#321]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/321
[#325]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/325
[#326]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/326
[#327]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/327
[#331]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/331
[#332]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/332
[#335]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/335
[#337]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/337
[#338]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/338
[#341]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/341
[#342]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/342
[#343]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/343
[#347]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/347
[#348]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/348
[#349]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/349
[#355]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/355
[#356]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/356
[#358]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/358
[#359]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/359
[#362]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/362
[#365]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/365
[#366]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/366
[#367]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/367
[#370]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/370
[#371]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/371
[#373]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/373
[#378]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/378
[#379]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/379
[#382]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/382
[#383]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/383
[#384]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/384
[#387]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/387
[#391]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/391
[#392]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/392
[#393]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/393
[#289]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/289
[#395]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/395
[#400]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/400
[#401]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/401
[#410]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/410
[#412]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/412
[#414]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/414
[#415]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/415
[#416]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/416
[#419]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/419
[#420]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/420
[#421]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/421
[#422]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/422
[#429]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/429
[#430]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/430
[#431]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/431
[#445]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/445
[#448]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/448
[#453]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/453
[#454]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/454
[#459]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/459
[#474]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/474
[#475]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/475
[#477]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/477
[#478]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/478
[#480]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/480
[#481]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/481
[#482]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/482
[#488]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/488
[#489]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/489
[#503]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/503
[#504]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/504
[#508]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/508
[#510]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/510
[#512]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/512
[#511]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/511
[#513]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/513
[#515]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/515
[#516]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/516
[#517]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/517
[#518]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/518
[#521]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/521
