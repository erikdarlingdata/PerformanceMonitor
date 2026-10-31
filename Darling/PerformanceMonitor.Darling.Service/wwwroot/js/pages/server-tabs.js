/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The server page's TAB REGISTRIES — the web port of the desktop viewer's per-server TabControl
 * (Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.xaml, ~65 TabItems across 38 partials).
 *
 * There are TWO, and serverTabsFor(card) picks between them from the fleet card's server-derived `is_postgres`
 * (#2530). SERVER_TABS is the SQL Server registry and also the default for a card that makes no engine claim —
 * the name is kept for that second job, because "no claim" has always rendered these tabs and still should.
 * POSTGRES_TABS is the eight-tab PostgreSQL registry; its own header says why that is the answer and not twelve.
 *
 * Every entry is `{ id, label, note?, build(server, ctx) }` and `build` returns an array of nodes, almost all of
 * them PANEL DESCRIPTORS run through the unmodified renderPanel (the #1563 seam): a `read` naming an MCP tool
 * served at GET /api/read/{read}, `params`, and a `viz` from the four-kind registry in panels.js. No fifth viz
 * kind was added — see the PR for why the property-grid shape that tempted one is served honestly by `stat`
 * (the reads that return a flat object) and `table` (the reads that already return rows).
 *
 * `ctx.hours` is the page's time range, the web twin of ViewerServerTab.TimeRange.cs's preset picker. Panels
 * whose read takes no time window ignore it and say so in their subtitle, because a panel labelled "last 6
 * hours" that is really a latest-snapshot read is a lie the reader cannot see.
 *
 * TWO HONESTY RULES run through this file:
 *   1. Every panel carries an `emptyText` saying WHY it could be empty in the reader's own terms. A read that
 *      returns the {status,message} envelope supplies its own (better) sentence and renderPanel shows that
 *      instead; emptyText only covers the data-arrived-but-zero-rows case, which would otherwise read as a
 *      blank rectangle.
 *   2. A tab whose desktop twin does something the browser genuinely cannot do carries a `note` naming it and
 *      pointing at the desktop viewer. A reader told "open the desktop viewer for plan analysis" is better
 *      served than one given a web page that looks like plan analysis and is not.
 *
 * R4 (XSS): every value reaches the DOM through renderPanel/util.el's text path. The custom cell renderers here
 * (query text, XML) build a <pre> through el(), the sentinel renderers build a text node, and none of them
 * touches innerHTML.
 */

import { el, makeActivatable, noticeStrip, daysText } from "../util.js";
import { worstFirst } from "../panels.js";
import { memoryGrantChartPanels } from "./memory-grant-charts.js";
import { statsHistoryColumn } from "./stats-history.js";
import { pgPlanColumn } from "./pg-plan-viewer.js";
import { analysisFindingsTab } from "./analysis-findings.js";
import { collectorRunBar, runNowColumns } from "./collector-run.js";
import { windowParams } from "../time-slicer.js";
import { blockChainColumn } from "./block-chain.js";
import { blockingPlanColumns, deadlockPlanColumn, planColumn, procedurePlanColumn, queryStorePlanColumn } from "./plan-viewer.js";
import { queryStoreHistoryColumn } from "./query-store-history.js";
import { queryHeatmapCard } from "./query-heatmap.js";
import { systemEventsChartSpecs, COUNTERS_INTRO, counterChartsDrawn } from "./system-events-charts.js";
import { SQL_TAB_REACH, POSTGRES_TAB_REACH, withTabReach } from "./server-tab-reach.js";
import { PG_EVICTION_STATS, PG_BLOCKING_STATS, PG_DATABASE_STATS, PG_QUERY_DURATION_TREND_COLUMNS, PG_DATABASE_TREND_COLUMNS, PG_DEADLOCK_GRAPH_COLUMNS, PG_DEADLOCK_COLUMNS, PG_LOG_EVENT_COLUMNS, PG_COLUMN_STATS_COLUMNS, PG_PREDICATE_COLUMNS, PG_LOCK_STATS_COLUMNS, PG_PLAN_COLUMNS, PG_PLAN_CAPTURE_READINESS_COLUMNS, PG_LOGGING_AUDIT_COLUMNS, PG_TOP_QUERY_COLUMNS, PG_BLOCKING_CHAIN_COLUMNS, PG_BLOCKING_CYCLE_COLUMNS, PG_DATABASE_COLUMNS } from "./server-tabs/pg-activity.js";
import { PG_SERVER_CONFIG_COLUMNS, PG_CONFIG_CHANGE_COLUMNS } from "./server-tabs/pg-config.js";
import { PG_IO_SUMMARY_STATS, PG_IO_TREND_COLUMNS, PG_WRITE_STATS, PG_BUFFER_USAGE_COLUMNS, PG_IO_COLUMNS } from "./server-tabs/pg-io.js";
import { momentStat, PG_CPU_SERIES, PG_EXTENSION_CENSUS_COLUMNS, PG_EXTENSION_COLUMNS } from "./server-tabs/pg-overview.js";
import { PG_REPLICATION_STATS_COLUMNS, PG_SLOT_COLUMNS } from "./server-tabs/pg-replication.js";
import { PG_BLOAT_STATS, PG_INDEX_USAGE_STATS, PG_INDEX_BLOAT_COLUMNS, PG_BLOAT_COLUMNS, PG_INDEX_USAGE_COLUMNS } from "./server-tabs/pg-storage.js";
import { PG_WRAPAROUND_THRESHOLD_STATS, PG_SESSION_STATE_STATS, PG_AUTOVACUUM_COLUMNS, PG_WRAPAROUND_COLUMNS, PG_XMIN_COLUMNS, PG_SESSION_STATE_COLUMNS } from "./server-tabs/pg-vacuum.js";
import { PG_WAIT_TREND_COLUMNS, PG_WAIT_SAMPLING_COLUMNS, PG_KERNEL_COLUMNS, PG_WAIT_COLUMNS } from "./server-tabs/pg-waits.js";
import { fanout, topRankingLabel, rankedParams, withTagPick, rankedCard, tablePanel, stat, linePanel, SNAPSHOT, PROPERTY_STATS, ALERT_READ_PANEL, COLLECTION_CAVEATS_PANEL, SWEEP_STATS, SKIPPED_CYCLES_STATS, withWaitLink, TOP_QUERY_COLUMNS, TOP_QUERY_GROUPS, TOP_PROC_COLUMNS, TOP_PROC_GROUPS, serverTrendPanel, seriesTrendPanel, FINDING_COLUMNS, COLLECTION_LOG_COLUMNS, selectionInside, COLLECTOR_COLUMNS, HEAVIEST_COLUMNS, PG_WRAPAROUND_STATS, PG_XMIN_STATS, PG_SLOT_STATS, PG_AUTOVACUUM_STATS } from "./server-tabs/shared.js";
import { perfmonPanel, SESSION_STATS, sessionStatsTrendPanel, APPLICATION_COLUMNS, JOB_COLUMNS, JOB_HISTORY_COLUMNS, INDEX_COLUMNS } from "./server-tabs/sql-activity.js";
import { slicedPanel, DEADLOCKS_EMPTY_TEXT, COUNT_SERIES, BLOCKING_REPORT_SERIES, BLOCKING_BY_SOURCE, LOCK_WAIT_SERIES, WAITING_TASK_SERIES, BLOCKED_SESSION_SERIES, BLOCKING_SEVERITY_SERIES, DEADLOCK_SEVERITY_SERIES, BLOCKING_GROUPS, BLOCKING_COLUMNS, DEADLOCK_COLUMNS, deadlockGraphsPanel, BPR_COLUMNS, OBJECT_LOCK_COLUMNS } from "./server-tabs/sql-blocking.js";
import { SERVER_CHANGE_COLUMNS, DB_CHANGE_COLUMNS, TRACE_FLAG_CHANGE_COLUMNS } from "./server-tabs/sql-changes.js";
import { SERVER_CONFIG_COLUMNS, DB_CONFIG_COLUMNS, scopedConfigPanel, QS_HEALTH_COLUMNS, TRACE_FLAG_COLUMNS, AUDIT_COLUMNS, identityCapacityPanels, AGENT_ALERT_SPECS, DATABASE_PROTECTION_SPECS } from "./server-tabs/sql-config.js";
import { SCHEDULER_STATS, CPU_SERIES } from "./server-tabs/sql-cpu.js";
import { HEALTH_CPU_SERIES, HEALTH_ENTRY_COLUMNS, SEVERE_ERROR_COLUMNS, SCHEDULER_ISSUE_COLUMNS, IO_ISSUE_COLUMNS, CPU_TASK_COLUMNS, MEMORY_CONDITION_COLUMNS, MEMORY_CONDITION_GROUPS, MEMORY_BROKER_COLUMNS, SIGNIFICANT_WAIT_COLUMNS, MEMORY_OOM_COLUMNS, MEMORY_OOM_GROUPS, DEFAULT_TRACE_COLUMNS } from "./server-tabs/sql-events.js";
import { pickCollector, collectionLogPanel } from "./server-tabs/sql-health.js";
import { fileIoPanel, TEMPDB_SERIES, FILE_IO_COLUMNS, DB_SIZE_COLUMNS, TABLE_SIZE_COLUMNS, PVS_COLUMNS } from "./server-tabs/sql-io.js";
import { MEMORY_STATS, PLAN_CACHE_STATS, MEMORY_SERIES, CLERK_COLUMNS, GRANT_WINDOW_COLUMNS, SEMAPHORE_WINDOW_COLUMNS, GRANT_COLUMNS, SEMAPHORE_COLUMNS, memoryClerksTrendPanel, memoryPressurePanels, CACHE_TYPE_COLUMNS } from "./server-tabs/sql-memory.js";
import { overviewStripsPanel, dailySummaryPanels, OVERVIEW_STATS } from "./server-tabs/sql-overview.js";
import { topQueriesPanel, tagFilteredCard, slicedTagCard, DURATION_SERIES, EXECUTION_RATE_SERIES, activeQueriesPanel, QUERY_STORE_REGRESSION_GROUPS, QUERY_STORE_REGRESSION_COLUMNS, QUERY_HEATMAP_COLUMNS, QUERY_STORE_COLUMNS, QS_CLUTTER_COLUMNS, QS_OVERHEAD_WAIT_COLUMNS, QS_CLERK_STATS, LONG_QUERY_GROUPS, LONG_QUERY_COLUMNS, PLAN_CORRECTION_GROUPS, PLAN_CORRECTION_COLUMNS, AUTO_TUNING_COLUMNS } from "./server-tabs/sql-queries.js";
import { waitsPanel, WAITING_TASK_COLUMNS, LATCH_COLUMNS, SPINLOCK_COLUMNS, namedTrendPanel } from "./server-tabs/sql-waits.js";
export { serverTrendPanel, seriesTrendPanel } from "./server-tabs/shared.js";
export { perfmonPanel, drawPerfmonTrends, sessionStatsTrendPanel } from "./server-tabs/sql-activity.js";
export { fileIoGrain, fileIoPanel } from "./server-tabs/sql-io.js";
export { pressureEventBuckets, memoryClerksTrendPanel, drawClerkTrends, memoryPressurePanels } from "./server-tabs/sql-memory.js";
export { dailySummaryPanels } from "./server-tabs/sql-overview.js";
export { topQueriesPanel } from "./server-tabs/sql-queries.js";
export { waitsPanel, drawWaitTrends, namedTrendPanel, drawNamedTrends } from "./server-tabs/sql-waits.js";

/* ─────────────────────────── the tabs ─────────────────────────── */

/**
 * The tab registry, in the order the desktop viewer presents them. `build(server, ctx)` returns the tab's nodes;
 * `ctx` is `{ hours, label }` — the page's time range and its human label.
 */
export const SERVER_TABS = [
  {
    id: "overview",
    label: "Overview",
    build: (server, ctx) => [
      stat("Overview", "get_server_summary", { server }, OVERVIEW_STATS, SNAPSHOT, 2),
      stat("Server Properties", "get_server_properties", { server }, PROPERTY_STATS, SNAPSHOT, 2),
      /* #6291: the desktop Overview's five stacked strips on one time axis and one crosshair. The CPU, Memory, Blocked process
         report and Deadlocks charts that sat here live on the CPU, Memory, Blocking and Deadlocks tabs (and the File I/O tab
         keeps both latency charts), so nothing is unreachable; the strips replace the four separate Overview charts and the File I/O panel. */
      overviewStripsPanel(server, ctx),
      tablePanel(
        "Analysis Findings",
        "get_analysis_findings",
        { server, hours: ctx.hours },
        "findings",
        FINDING_COLUMNS,
        ctx.label,
        "No findings in this window. Findings are written by the analysis pass, which needs at least 24 hours of collected history."
      ),
      /* #2484: today's tile and the month range behind the desktop viewer's Performance Calendar. #3905: both
         from ONE fetch of get_daily_summary_range, today's tile being the range's anchor-day row, where they
         used to be two reads (get_daily_summary beside the range) that cost the store the day's aggregate
         twice per load. See dailySummaryPanels. */
      ...dailySummaryPanels(server),
    ],
  },

  {
    id: "waits",
    label: "Wait Stats",
    build: (server, ctx) => [
      waitsPanel(server, ctx),
      tablePanel(
        "Waiting Tasks",
        "get_waiting_tasks",
        { server, hours: ctx.hours, limit: 30 },
        "tasks",
        withWaitLink(WAITING_TASK_COLUMNS, server, "collection_time"),
        ctx.label,
        "No waiting tasks were captured in this window."
      ),
      tablePanel(
        "Latch Stats",
        "get_latch_stats",
        { server, hours: ctx.hours, top: 10 },
        "latches",
        LATCH_COLUMNS,
        ctx.label,
        "No latch classes accumulated waits in this window."
      ),
      namedTrendPanel(server, ctx, "latch"),
      tablePanel(
        "Spinlock Stats",
        "get_spinlock_stats",
        { server, hours: ctx.hours, top: 10 },
        "spinlocks",
        SPINLOCK_COLUMNS,
        ctx.label,
        "No spinlocks recorded collisions in this window."
      ),
      namedTrendPanel(server, ctx, "spinlock"),
    ],
  },

  {
    id: "cpu",
    label: "CPU",
    build: (server, ctx) => [
      linePanel("CPU Utilization", "get_cpu_utilization", { server, hours: ctx.hours }, "samples", "sample_time", CPU_SERIES, {
        subtitle: ctx.label,
        format: "pct",
        unit: "%",
        span: 2,
        emptyText: "No CPU samples in this window.",
      }),
      stat("Scheduler Pressure", "get_cpu_scheduler_pressure", { server }, SCHEDULER_STATS, SNAPSHOT, 2),
      serverTrendPanel(server, ctx, "cpu_scheduler"),
      /* #4231: `noteKey` (#3278) carries `truncation_note` — raw query_stats/procedure_stats are dropped at
         4 days once the rollups are armed, and a window asking further back than that silently served less
         than it asked for. Null when the floor did not bite, the same as every other noteKey panel. */
      rankedCard("queries", (ranking, picker, w, onRow) =>
        tablePanel(
          "Top Queries by " + topRankingLabel(ranking),
          "get_top_queries_by_cpu",
          rankedParams(windowParams({ server, hours: w.hours, top: 20, detail: "full" }, w), ranking, "queries"),
          "queries",
          [...TOP_QUERY_COLUMNS, planColumn(server), statsHistoryColumn("queries", server, w)],
          w.label,
          "No query stats in this window. Delta-based collection needs at least two cycles (~30 minutes) before it reports non-zero values.",
          2,
          "truncation_note",
          null,
          TOP_QUERY_GROUPS,
          picker,
          null,
          null,
          false,
          onRow
        ),
        { server, ctx }
      ),
      rankedCard("procedures", (ranking, picker, w, onRow) =>
        tablePanel(
          "Top Procedures by " + topRankingLabel(ranking),
          "get_top_procedures_by_cpu",
          rankedParams(windowParams({ server, hours: w.hours, top: 20, detail: "full" }, w), ranking, "procedures"),
          "procedures",
          [...TOP_PROC_COLUMNS, procedurePlanColumn(server), statsHistoryColumn("procedures", server, w)],
          w.label,
          "No procedure stats in this window. Delta-based collection needs at least two cycles (~30 minutes).",
          2,
          "truncation_note",
          null,
          TOP_PROC_GROUPS,
          picker,
          null,
          null,
          false,
          onRow
        ),
        { server, ctx }
      ),
    ],
  },

  {
    id: "memory",
    label: "Memory",
    build: (server, ctx) => [
      stat("Memory", "get_memory_stats", { server }, MEMORY_STATS, SNAPSHOT, 2),
      linePanel("Memory Trend", "get_memory_trend", { server, hours: ctx.hours }, "trend", "time", MEMORY_SERIES, {
        subtitle: ctx.label,
        format: "mb",
        span: 2,
        emptyText: "No memory samples in this window.",
      }),
      memoryClerksTrendPanel(server, ctx),
      serverTrendPanel(server, ctx, "plan_cache"),
      tablePanel(
        "Memory Clerks",
        "get_memory_clerks",
        { server },
        "clerks",
        CLERK_COLUMNS,
        SNAPSHOT,
        "No memory clerks in the latest snapshot — the clerk collector may not have run yet.",
        1
      ),
      ...memoryPressurePanels(server, ctx),
      /* #3653: TWO READS UNDER ONE WINDOW (#3637), rendered as the two things they are. The memory-grant
         pair answers with `grants[]` - the NEWEST snapshot in the window, one row per pool (per semaphore and
         pool on the semaphore lens), every row stamped with the same instant - and `window[]` - one row per
         pool aggregating EVERY snapshot in the window: the most sessions ever seen waiting and when, the peak
         granted, the floor of available, timeouts and forced grants summed across the window's intervals.

         Until #3653 the Memory Grants panel drew `grants[]` as a LINE chart under the page's range label.
         Every row carried the same collection_time, so the "series" was one x-position with a dot per pool,
         captioned as if it spanned the window - one instant drawn as a trend. The Resource Semaphore table
         beside it told the same lie more quietly: `grants[]` rows under a window subtitle. `window[]` is not a
         series either - it is the window's aggregates per pool, which is what the read chose to store the
         deltas for - so the honest rendering is two TABLES per read, window first: "was there pressure",
         then "is there pressure now", the reading order the read's own description gives.

         #6299 added the per-collection trend that comment said this service did not serve: get_memory_grants
         with `chart: true` answers one point per bucket per pool across the WHOLE window, and the two charts
         below draw that (memory-grant-charts.js). That does not repeat #3653's mistake: the removed chart
         drew `grants[]`, one snapshot's rows all stamped with the same instant, as a window trend; these
         points each carry their own collection time, and a window with no snapshot draws the empty text,
         never a line of zeros. The two tables below are unchanged. */
      ...memoryGrantChartPanels(server, ctx),
      ...fanout("get_memory_grants", { server, hours: ctx.hours }, [
        {
          title: "Memory Grant Pressure",
          subtitle: ctx.label + ", every snapshot in the window, per resource pool",
          viz: "table",
          rowsKey: "window",
          columns: GRANT_WINDOW_COLUMNS,
          emptyText: "No memory grant snapshots in this window.",
        },
        {
          title: "Memory Grants",
          subtitle: "newest snapshot in " + ctx.label + ", per resource pool - a moment, not the window",
          viz: "table",
          windowNote: false,
          rowsKey: "grants",
          columns: GRANT_COLUMNS,
          emptyText: "No memory grant snapshot in this window.",
        },
      ]),
      ...fanout("get_resource_semaphore", { server, hours: ctx.hours }, [
        {
          title: "Resource Semaphore Pressure",
          subtitle: ctx.label + ", every snapshot in the window, per semaphore and pool",
          viz: "table",
          rowsKey: "window",
          columns: SEMAPHORE_WINDOW_COLUMNS,
          emptyText: "No resource-semaphore snapshots in this window.",
        },
        {
          title: "Resource Semaphore",
          subtitle: "newest snapshot in " + ctx.label + ", per semaphore and pool - a moment, not the window",
          viz: "table",
          windowNote: false,
          rowsKey: "grants",
          columns: SEMAPHORE_COLUMNS,
          emptyText: "No resource-semaphore snapshot in this window.",
        },
      ]),
      ...fanout("get_plan_cache_bloat", { server, hours: ctx.hours }, [
        { title: "Plan Cache", subtitle: ctx.label, viz: "stat", stats: PLAN_CACHE_STATS },
        {
          title: "Plan Cache by Type",
          subtitle: ctx.label,
          viz: "table",
          rowsKey: "cache_types",
          columns: CACHE_TYPE_COLUMNS,
          emptyText: "No plan-cache breakdown in this window.",
        },
      ]),
    ],
  },

  {
    id: "blocking",
    label: "Blocking",
    /* get_blocking_stats, which draws Blocking Severity and Deadlock Severity, takes any window, and its tables keep
       more than this page's widest Range. The note names that Range from the presets (tabNote), so narrowing them
       cannot leave it wrong. */
    note: (widestHours) =>
      "Blocked-process reports are shown here as their captured XML. A Blocking row's Block chain button shows its lead-blocker tree, " +
      "as the desktop viewer does. A deadlock graph can be drawn from its row. This page shows at most " +
      daysText(widestHours) +
      ". A Custom View can show more blocking and deadlock history.",
    build: (server, ctx) => [
      linePanel("Blocked process report trend", "get_blocking_trend", { server, hours: ctx.hours }, "trend", "time", BLOCKING_REPORT_SERIES, {
        subtitle: ctx.label,
        format: "int",
        unit: "reports",
        atTimeItem: "blocking",
        sourceKey: "source",
        bySource: BLOCKING_BY_SOURCE,
        emptyText: "No blocked process reports in this window — an empty trend here means none were captured, not that nothing was collected.",
      }),
      linePanel("Deadlock trend", "get_deadlock_trend", { server, hours: ctx.hours }, "trend", "time", COUNT_SERIES, {
        subtitle: ctx.label,
        format: "int",
        atTimeItem: "deadlocks",
        emptyText: DEADLOCKS_EMPTY_TEXT,
        emptyWins: true,
      }),
      /* #6301 slice 2b: the Blocking and Deadlocks grids sit under a time slicer each (the strips are the two trends' own counts, read once over the
         page range), and a drag on either narrows both grids, the graphs and the report XML for this server together. */
      slicedPanel("blocking", { server, ctx }, (w) =>
        tablePanel(
          "Blocking",
          "get_blocking",
          windowParams({ server, hours: w.hours, limit: 30 }, w, true),
          "events",
          [...BLOCKING_COLUMNS, ...blockingPlanColumns(server), blockChainColumn(server)],
          w.label,
          "No blocking events in this window.",
          2,
          "separately_monitored_note",
          null,
          BLOCKING_GROUPS
        )
      ),
      slicedPanel("deadlocks", { server, ctx }, (w) =>
        tablePanel(
          "Deadlocks",
          "get_deadlocks",
          windowParams({ server, hours: w.hours, limit: 20 }, w, true),
          "deadlocks",
          [...DEADLOCK_COLUMNS, deadlockPlanColumn(server)],
          w.label,
          DEADLOCKS_EMPTY_TEXT,
          2,
          "separately_monitored_note",
          null,
          null,
          null,
          null,
          null,
          true
        )
      ),
      /* #2484: the Current Waits tab the viewer has and the browser did not. ONE read, two panels --
         via fanout, not two linePanel() calls, because the tab must not fetch the same read twice (there is
         a pin for it, and the reason is real: this read returns both series in one payload precisely so
         they cannot be looked at separately). A wait spike with no blocked sessions is a resource wait;
         the same spike with them is contention. */
      ...fanout("get_current_waits_trend", { server, hours: ctx.hours }, [
        {
          title: "Waiting Tasks",
          subtitle: ctx.label,
          dbScope: "server",
          viz: "line",
          rowsKey: "waiting_tasks",
          xKey: "collection_time",
          series: WAITING_TASK_SERIES,
          emptyText:
            "Nothing was waiting in this window. If the server has never been sampled the read says so " +
            "explicitly rather than reporting this as an all-clear.",
        },
        {
          title: "Blocked Sessions",
          subtitle: ctx.label,
          viz: "line",
          rowsKey: "blocked_sessions",
          xKey: "collection_time",
          series: BLOCKED_SESSION_SERIES,
          format: "int",
          emptyText: "No blocked sessions in this window.",
        },
      ]),
      /* #2484: the aggregate lock-wait lane, the third chart on the viewer's Blocking Trends tab. Charts ONE
         numeric key: since #3897 the read returns the whole LCK family as one bucketed series (its wait_types[]
         legend names the types that waited), where it used to return a row per (collection, wait type) that
         this chart drew as one jagged line. get_wait_trend can chart one LCK type; this is the whole family. */
      linePanel("Lock Waits", "get_lock_wait_trend", { server, hours: ctx.hours }, "trend", "collection_time", LOCK_WAIT_SERIES, {
        subtitle: ctx.label,
        atTimeItem: "blocking",
        emptyText:
          "No lock waits in this window. If wait stats have never been collected for this server the read " +
          "says so explicitly rather than reporting an absence of lock contention.",
      }),
      /* #2484: severity, the companion to the two count trends at the top of this tab. One read, two
         charts, via fanout for the same reason as above. */
      ...fanout("get_blocking_stats", { server, hours: ctx.hours }, [
        {
          title: "Blocking Severity",
          subtitle: ctx.label,
          viz: "line",
          atTimeItem: "blocking",
          sourceKey: "source",
          rowsKey: "blocking_duration",
          xKey: "time",
          series: BLOCKING_SEVERITY_SERIES,
          emptyText:
            "No blocking in this window. If neither capture path has ever produced a row the read says " +
            "so explicitly rather than reporting a clean bill of health.",
        },
        {
          title: "Deadlock Severity",
          subtitle: ctx.label,
          dbScope: "unfiltered",
          viz: "line",
          atTimeItem: "deadlocks",
          rowsKey: "deadlock_severity",
          xKey: "time",
          series: DEADLOCK_SEVERITY_SERIES,
          emptyText: DEADLOCKS_EMPTY_TEXT,
          emptyWins: true,
        },
      ]),
      slicedPanel("deadlocks", { server, ctx }, (w) => deadlockGraphsPanel(server, w), false),
      slicedPanel(
        "blocking",
        { server, ctx },
        (w) =>
          tablePanel(
            "Blocked Process Reports",
            "get_blocked_process_xml",
            windowParams({ server, hours: w.hours, limit: 5 }, w, true),
            "reports",
            BPR_COLUMNS,
            w.label,
            "No blocked-process report XML in this window. The report is only written when the blocked process threshold is configured on the target."
          ),
        false
      ),
      tablePanel(
        "Object Contention",
        "get_object_locking",
        { server },
        "objects",
        OBJECT_LOCK_COLUMNS,
        "daily collection",
        "No lock-wait rows recorded. Index and object stats are collected daily.",
        2,
        "optimized_locking_note",
        /* #4925: a master target keeps its separately monitored databases' rows here; this line says why. */
        ["separately_monitored_note"]
      ),
    ],
  },

  {
    id: "io",
    label: "File I/O",
    build: (server, ctx) => [
      fileIoPanel(server, ctx),
      tablePanel(
        "File I/O Stats",
        "get_file_io_stats",
        { server },
        "files",
        FILE_IO_COLUMNS,
        SNAPSHOT,
        "No file I/O rows in the latest snapshot."
      ),
      linePanel("tempdb", "get_tempdb_trend", { server, hours: ctx.hours }, "trend", "time", TEMPDB_SERIES, {
        subtitle: ctx.label,
        format: "mb",
        span: 2,
        emptyText: "No tempdb samples in this window.",
      }),
      /* `noteKey` carries the read's own note when a Hyperscale log file is in the snapshot: its size is n/a
         (log service) and it is left out of these totals, so the page says so instead of looking short. */
      tablePanel(
        "Database Sizes",
        "get_database_sizes",
        { server },
        "databases",
        DB_SIZE_COLUMNS,
        SNAPSHOT,
        "No database sizes in the latest snapshot.",
        1,
        "note"
      ),
      tablePanel(
        "Table & Index Sizes",
        "get_table_index_sizes",
        { server },
        "tables",
        TABLE_SIZE_COLUMNS,
        "daily collection",
        "No object size rows recorded. Index and object stats are collected daily."
      ),
      tablePanel(
        "Persistent Version Store",
        "get_pvs_stats",
        { server },
        "databases",
        PVS_COLUMNS,
        SNAPSHOT,
        "No PVS rows. The collector reads a SQL Server 2019+ DMV, and a server with Accelerated Database Recovery off has nothing to report."
      ),
    ],
  },

  /* #6296, #6297: the desktop's File I/O Throughput sub-tab and tempdb charts. Their own tab, not panels on File I/O: get_server_trend
     reaches 7 days where the File I/O tab's reads reach 30, and a tab's reach is the smallest of its reads. */
  {
    id: "iotrends",
    label: "I/O Trends",
    build: (server, ctx) => [
      seriesTrendPanel(server, ctx, "file_io_throughput"),
      serverTrendPanel(server, ctx, "tempdb_size"),
      seriesTrendPanel(server, ctx, "tempdb_file_io"),
    ],
  },

  {
    id: "queries",
    label: "Queries",
    note: "Plan analysis, cached-plan retrieval and actual-plan re-execution are in the desktop viewer, not on this page.",
    noteInfo: "They need a plan renderer and a connection back to the monitored server. Apart from the Active Queries live read (Run live now, for accounts that can edit; otherwise use the viewer's Current Active Queries), this page only shows collected data.",
    build: (server, ctx) => [
      activeQueriesPanel(server, ctx),
      /* #2484: the viewer's Performance Trends tab is four charts over three reads. Duration and the
         execution rate come from ONE payload -- via fanout, not two linePanel() calls, because the tab must not
         fetch the same read twice. They get separate charts rather than separate series because ms/sec and
         executions/sec are two units, and one y-domain cannot hold both honestly. */
      ...fanout("get_query_duration_trend", { server, hours: ctx.hours }, [
        {
          title: "Query Duration Trend",
          subtitle: ctx.label,
          viz: "line",
          rowsKey: "trend",
          xKey: "time",
          series: DURATION_SERIES,
          format: "ms",
          span: 2,
          emptyText: "No query duration samples in this window.",
        },
        {
          title: "Executions per Second",
          subtitle: ctx.label,
          viz: "line",
          rowsKey: "trend",
          xKey: "time",
          series: EXECUTION_RATE_SERIES,
          span: 2,
          emptyText: "No query executions recorded in this window.",
        },
      ]),
      linePanel(
        "Procedure Duration Trend",
        "get_procedure_duration_trend",
        { server, hours: ctx.hours },
        "trend",
        "time",
        DURATION_SERIES,
        {
          subtitle: ctx.label,
          format: "ms",
          span: 2,
          emptyText:
            "No stored-procedure activity in this window. A server that runs no procedures lands here too, " +
            "and the read says which of the two it found.",
        }
      ),
      linePanel(
        "Query Store Duration Trend",
        "get_query_store_duration_trend",
        { server, hours: ctx.hours },
        "trend",
        "time",
        DURATION_SERIES,
        {
          subtitle: ctx.label,
          format: "ms",
          span: 2,
          emptyText:
            "No Query Store activity in this window. If Query Store is off on this server's databases the " +
            "read says so rather than showing an empty chart.",
        }
      ),
      /* #2520: the same table this tab always had, now with the drill-down attached. The CPU tab keeps the
         plain table — the drill-down belongs where the second question gets asked, and two of these would
         mean two fetches of get_top_queries_by_cpu for one page's worth of the same twenty rows. */
      topQueriesPanel(server, ctx),
      /* #4231: same `truncation_note` disclosure as the CPU tab's plain tables. */
      rankedCard("procedures", (ranking, picker, w, onRow) =>
        tablePanel(
          "Top Procedures by " + topRankingLabel(ranking),
          "get_top_procedures_by_cpu",
          rankedParams(windowParams({ server, hours: w.hours, top: 20, detail: "full" }, w), ranking, "procedures"),
          "procedures",
          [...TOP_PROC_COLUMNS, procedurePlanColumn(server), statsHistoryColumn("procedures", server, w)],
          w.label,
          "No procedure stats in this window. Delta-based collection needs at least two cycles (~30 minutes).",
          2,
          "truncation_note",
          null,
          TOP_PROC_GROUPS,
          picker,
          null,
          null,
          false,
          onRow
        ),
        { server, ctx }
      ),
      /* #6301: under the time slicer. The grid reads the slicer's window (`w`), the History button on a row reads that same window, and a row pick
         overlays that query's history on the strip. */
      slicedTagCard(
        "querystore",
        { server, ctx },
        (picker, w, onRow) =>
          tablePanel(
            "Query Store",
            "get_query_store_top",
            withTagPick(windowParams({ server, hours: w.hours, top: 20 }, w), "querystore"),
            "queries",
            [...QUERY_STORE_COLUMNS, queryStorePlanColumn(server), queryStoreHistoryColumn(server, w)],
            w.label,
            "No Query Store rows in this window.",
            2,
            "truncation_note",
            /* #5094: when whole days came from the daily summary the answer says so (`approximate`), and its note is
               drawn above the grid as text, so the totals are not read as exact. Null (no line) on an exact answer. */
            ["approximation_note"],
            null,
            picker,
            null,
            null,
            false,
            onRow
          ),
        true
      ),
      /* #2484: the Query Store Regressions tab -- the only tab in the per-server page that was entirely
         unreachable from a browser rather than merely reduced. Built with tablePanel(), not an object literal:
         mount() stringifies anything it cannot consume, so a literal renders as [object Object] and never
         fetches the read at all. */
      tablePanel(
        "Query Store Regressions",
        "get_query_store_regressions",
        { server, hours: ctx.hours, limit: 50 },
        "regressions",
        QUERY_STORE_REGRESSION_COLUMNS,
        ctx.label,
        "No query regressed against its baseline in this window. A server with no earlier history has nothing to compare against.",
        2,
        /* #6654: the read leaves out databases the server dropped; the note names them (drawn on an empty answer too, panels.js). */
        "databases_left_out_note",
        null,
        QUERY_STORE_REGRESSION_GROUPS
      ),
      /* #3797: the clutter view. ONE fetch, three panels — the per-database rows, the per-server QDS wait
         block and the per-server memory clerk — because the read composes four arms over two raw hypertables
         and three separate fetches of it would pay for that three times on one tab (the fanout() reason).

         include_fleet_median is deliberately NOT sent. It walks query_store_stats and wait_stats fleet-wide
         over the window, which on a large store is the read deadline's whole budget, and a server tab is a
         per-server question; fleet_median therefore reads null here with the payload's own note saying it was
         not computed, which an agent calling the tool can ask for and this page does not.

         The last two panels are SERVER blocks beside DATABASE rows, and their titles say so: the QDS_* waits
         and the Query Store memory clerk are instance-wide, and nothing on this page attributes them to a
         database. Each shows the payload's own note as its notice — the wait block because the proxy is the
         NON-sleep QDS types only and a reader who takes it for the whole of Query Store's cost is reading it
         wrong, the clerk because its absence is a RANK (outside the collector's top 25) and not a zero. */
      ...fanout("get_query_store_clutter", { server, hours: ctx.hours, limit: 50 }, [
        {
          title: "Query Store Clutter",
          subtitle: ctx.label + ", per database, worst first",
          viz: "table",
          rowsKey: "databases",
          floorKey: "window",
          columns: QS_CLUTTER_COLUMNS,
          noteKey: "server_note",
          emptyText:
            "No Query Store rows, no Query Store collection run and no Query Store health capture for this " +
            "server in this window. The read says which of those it found rather than showing an empty grid.",
        },
        {
          title: "Query Store Overhead (per server)",
          subtitle: ctx.label,
          dbScope: "server",
          viz: "table",
          rowsKey: "qs_overhead.wait_stats.included",
          floorKey: "window",
          columns: QS_OVERHEAD_WAIT_COLUMNS,
          noteKey: "qs_overhead.wait_stats.excluded_note",
          emptyText:
            "No non-sleep QDS_* wait deltas in this window. Query Store's four sleep waits are dropped at " +
            "collection on purpose, so their absence here is the filter working, not a quiet server.",
        },
        {
          title: "Query Store Memory Clerk",
          subtitle: SNAPSHOT,
          span: 1,
          dbScope: "server",
          viz: "stat",
          stats: QS_CLERK_STATS,
          noteKey: "qs_overhead.memory_clerk.note",
          emptyText:
            "The Query Store memory clerk was not among the top 25 clerks over 1 MB in any capture in this window, so this is a rank, not a zero.",
        },
      ]),
      /* #2484, #6293: the Query Heatmap, drawn as the Viewer draws it: a colour grid of query counts by time and
         magnitude bucket, with a metric picker and a "Show as table" switch to the table of the same cells (the
         earlier #2484 design kept the plot desktop-only; that is overruled). The card is in pages/query-heatmap.js. */
      queryHeatmapCard({
        server,
        hours: ctx.hours,
        label: ctx.label,
        columns: QUERY_HEATMAP_COLUMNS,
        emptyText:
          "No query executed in this window. Query stats are collected every cycle whether or not anything " +
          "ran, so the read distinguishes a server nobody collected from one that was simply idle.",
      }),
      tagFilteredCard("longqueries", (picker) =>
        tablePanel(
          "Long Query Completions",
          "get_long_query_completions",
          withTagPick({ server, hours: ctx.hours, limit: 30 }, "longqueries"),
          "completions",
          LONG_QUERY_COLUMNS,
          ctx.label,
          "No long-running completions in this window. This collector is opt-in and off by default.",
          2,
          null,
          null,
          LONG_QUERY_GROUPS,
          picker
        )
      ),
      /* One read, two panels. get_plan_corrections returns both arrays, and automatic_tuning comes from an
         unconditional latest-snapshot query that ignores hours/limit entirely — so the second fetch was paying
         for the recommendations work twice to render a slice that never varied with the window. */
      ...fanout("get_plan_corrections", { server, hours: ctx.hours, limit: 50 }, [
        {
          title: "Plan Corrections",
          subtitle: ctx.label,
          viz: "table",
          rowsKey: "recommendations",
          columns: PLAN_CORRECTION_COLUMNS,
          groups: PLAN_CORRECTION_GROUPS.groups,
          defaultGroups: PLAN_CORRECTION_GROUPS.defaultGroups,
          emptyText: "No tuning recommendations in this window.",
        },
        {
          title: "Automatic Tuning",
          subtitle: SNAPSHOT,
          span: 1,
          viz: "table",
          windowNote: false,
          rowsKey: "automatic_tuning",
          columns: AUTO_TUNING_COLUMNS,
          emptyText: "No per-database FORCE_LAST_GOOD_PLAN state recorded.",
        },
      ]),
    ],
  },

  {
    id: "config",
    label: "Configuration",
    build: (server) => [
      stat("Server Properties", "get_server_properties", { server }, PROPERTY_STATS, SNAPSHOT, 2),
      tablePanel(
        "Configuration Audit",
        "audit_config",
        { server },
        "recommendations",
        AUDIT_COLUMNS,
        SNAPSHOT,
        "The audit found nothing to flag."
      ),
      tablePanel(
        "Server Configuration",
        "get_server_config",
        { server },
        "settings",
        SERVER_CONFIG_COLUMNS,
        SNAPSHOT,
        "No sp_configure snapshot yet."
      ),
      tablePanel(
        "Database Configuration",
        "get_database_config",
        { server },
        "databases",
        DB_CONFIG_COLUMNS,
        SNAPSHOT,
        "No database configuration snapshot yet."
      ),
      scopedConfigPanel(server),
      tablePanel(
        "Query Store Health",
        "get_query_store_health",
        { server },
        "databases",
        QS_HEALTH_COLUMNS,
        "hourly collection",
        "No Query Store health rows yet."
      ),
      tablePanel(
        "Trace Flags",
        "get_trace_flags",
        { server },
        "trace_flags",
        TRACE_FLAG_COLUMNS,
        SNAPSHOT,
        "No trace flags are enabled on this server.",
        1
      ),
      /* #6873: the last full backup, log backup and CHECKDB per database, one read feeding two panels. The state words, the age text and
         the platform label are the service's (DatabaseProtectionBands), the same ones the Viewer shows. A server with no capture, a
         refused collector or an engine that is not collected answers with its own sentence in each panel. */
      ...fanout("get_database_protection", { server }, DATABASE_PROTECTION_SPECS),
      /* #6874: the SQL Server Agent alerts and the gaps in them, one read feeding three panels. A server with no Agent alerts at all
         (Azure SQL Database, Managed Instance) or whose collector is refused answers with its own sentence in every panel. */
      ...fanout("get_agent_alerts", { server }, AGENT_ALERT_SPECS),
      /* #6875: identity column capacity, one read feeding two panels (the columns, then each database's scan state). */
      ...identityCapacityPanels(server),
    ],
  },

  {
    id: "changes",
    label: "Config Changes",
    build: (server, ctx) => [
      tablePanel(
        "Server Configuration Changes",
        "get_server_config_changes",
        { server, hours: ctx.hours },
        "changes",
        SERVER_CHANGE_COLUMNS,
        ctx.label,
        "No server configuration changed in this window."
      ),
      tablePanel(
        "Database Configuration Changes",
        "get_database_config_changes",
        { server, hours: ctx.hours },
        "changes",
        DB_CHANGE_COLUMNS,
        ctx.label,
        "No database configuration changed in this window."
      ),
      tablePanel(
        "Trace Flag Changes",
        "get_trace_flag_changes",
        { server, hours: ctx.hours },
        "changes",
        TRACE_FLAG_CHANGE_COLUMNS,
        ctx.label,
        "No trace flags changed in this window."
      ),
    ],
  },

  {
    id: "activity",
    label: "Activity",
    build: (server, ctx) => [
      perfmonPanel(server, ctx),
      sessionStatsTrendPanel(server, ctx),
      ...fanout("get_session_stats", { server }, [
        { title: "Sessions", subtitle: SNAPSHOT, viz: "stat", stats: SESSION_STATS },
        {
          title: "Sessions by Application",
          subtitle: SNAPSHOT,
          viz: "table",
          rowsKey: "applications",
          columns: APPLICATION_COLUMNS,
          emptyText: "No application rows in the latest session snapshot.",
        },
      ]),
      tablePanel(
        "Running Jobs",
        "get_running_jobs",
        { server },
        "jobs",
        JOB_COLUMNS,
        SNAPSHOT,
        runningJobsEmptyText(ctx),
        2,
        null,
        null,
        null,
        null,
        null,
        null,
        ctx.offline === true
      ),
      tablePanel(
        "Job History",
        "get_job_history",
        { server, hours: ctx.hours, limit: 100 },
        "runs",
        JOB_HISTORY_COLUMNS,
        ctx.label + ", newest 100 runs (steps and job outcomes)",
        "No SQL Agent job runs were retained in this window."
      ),
      tablePanel(
        "Index Usage",
        "get_index_usage",
        { server },
        "indexes",
        INDEX_COLUMNS,
        "daily collection",
        "No index usage rows recorded. Index and object stats are collected daily."
      ),
    ],
  },

  {
    id: "events",
    label: "System Events",
    note:
      "These are the system_health session and default trace, parsed on read.",
    build: (server, ctx) => [
      /* The CPU chart reads the whole window as time buckets (chart: true, #6158); a limit of 50 drew only the newest
         few hours of a day. The entries table below keeps the newest-rows read. */
      ...fanout("get_health_parser_system_health", { server, hours: ctx.hours, chart: true }, [
        {
          title: "system_health CPU",
          subtitle: ctx.label,
          viz: "line",
          rowsKey: "entries",
          xKey: "event_time",
          series: HEALTH_CPU_SERIES,
          format: "pct",
          unit: "%",
          emptyText: "No system_health entries in this window.",
        },
        /* The Corruption and Contention Events charts (#6295), off the same fetch. */
        ...systemEventsChartSpecs(ctx.label),
      ], { text: COUNTERS_INTRO, when: counterChartsDrawn }),
      ...fanout("get_health_parser_system_health", { server, hours: ctx.hours, limit: 50 }, [
        {
          title: "system_health Entries",
          subtitle: ctx.label,
          viz: "table",
          rowsKey: "entries",
          columns: HEALTH_ENTRY_COLUMNS,
          emptyText: "No system_health entries in this window.",
        },
      ]),
      tablePanel(
        "Severe Errors",
        "get_health_parser_severe_errors",
        { server, hours: ctx.hours, limit: 50 },
        "errors",
        SEVERE_ERROR_COLUMNS,
        ctx.label,
        "No severe errors in this window — the healthy state for this read."
      ),
      tablePanel(
        "Scheduler Issues",
        "get_health_parser_scheduler_issues",
        { server, hours: ctx.hours, limit: 50 },
        "issues",
        SCHEDULER_ISSUE_COLUMNS,
        ctx.label,
        "No scheduler issues in this window — the healthy state for this read."
      ),
      tablePanel(
        "I/O Issues",
        "get_health_parser_io_issues",
        { server, hours: ctx.hours, limit: 50 },
        "issues",
        IO_ISSUE_COLUMNS,
        ctx.label,
        "No I/O issues in this window — the healthy state for this read."
      ),
      tablePanel(
        "CPU Tasks",
        "get_health_parser_cpu_tasks",
        { server, hours: ctx.hours, limit: 50 },
        "events",
        CPU_TASK_COLUMNS,
        ctx.label,
        "No CPU task events in this window."
      ),
      tablePanel(
        "Memory Conditions",
        "get_health_parser_memory_conditions",
        { server, hours: ctx.hours, limit: 50 },
        "events",
        MEMORY_CONDITION_COLUMNS,
        ctx.label,
        "No memory condition events in this window.",
        2, null, null, MEMORY_CONDITION_GROUPS
      ),
      tablePanel(
        "Memory Broker",
        "get_health_parser_memory_broker",
        { server, hours: ctx.hours, limit: 50 },
        "events",
        MEMORY_BROKER_COLUMNS,
        ctx.label,
        "No memory broker events in this window."
      ),
      tablePanel(
        "Memory Node OOM",
        "get_health_parser_memory_node_oom",
        { server, hours: ctx.hours, limit: 50 },
        "events",
        MEMORY_OOM_COLUMNS,
        ctx.label,
        "No memory node OOM events in this window — the healthy state for this read.",
        2, null, null, MEMORY_OOM_GROUPS
      ),
      /* #2484: the ninth member of the get_health_parser_* family. Built with tablePanel(), not a bare object
         literal -- mount() stringifies anything it cannot consume, so a literal renders as [object Object]
         and never fetches the read at all. */
      tablePanel(
        "Significant Waits",
        "get_health_parser_significant_waits",
        { server, hours: ctx.hours, limit: 50 },
        "waits",
        withWaitLink(SIGNIFICANT_WAIT_COLUMNS, server, "event_time"),
        ctx.label,
        "No significant waits (a real session waiting 500 ms+ on a non-idle wait type) in this window."
      ),
      tablePanel(
        "Default Trace",
        "get_default_trace_events",
        { server, hours: ctx.hours, limit: 100 },
        "events",
        DEFAULT_TRACE_COLUMNS,
        ctx.label,
        "No significant default trace events in this window."
      ),
    ],
  },

  {
    id: "health",
    label: "Collection Health",
    build: (server, ctx) => [
      /* #5941: "Run all collectors now" (an edit seat only) and the status and results of any run started here. */
      collectorRunBar(server),
      /* One read, three panels. get_collection_health rolls up seven days of collector logs AND computes sweep
         pressure; these are three slices of that single payload, so three descriptors meant running the tab's
         heaviest query three times to open it. */
      ...fanout("get_collection_health", { server }, [
        { title: "Sweep Pressure", subtitle: "trailing 7 days", viz: "stat", stats: SWEEP_STATS },
        /* #5735: the collection loop's skipped slots, from the same get_collection_health read (its `skipped_cycles`
           block, always present: count 0 is a real answer). Its own panel because its window is 24 hours, not the 7 days
           beside it. */
        { title: "Skipped Cycles", subtitle: "trailing 24 hours", viz: "stat", stats: SKIPPED_CYCLES_STATS },
        /* #3013: the alerting layer's own store reads, which appear on no other health surface. Its own
           panel rather than a tile on Sweep Pressure specifically because of the SUBTITLE: these figures are
           in-memory counts since the service started, and inheriting "trailing 7 days" would have made the
           panel assert a window it did not measure. Shared const so the SQL Server and PostgreSQL tabs cannot
           drift apart on it. */
        ALERT_READ_PANEL,
        COLLECTION_CAVEATS_PANEL,
        {
          title: "Collectors",
          subtitle: "trailing 7 days",
          viz: "table",
          rowsKey: "collectors",
          columns: [...COLLECTOR_COLUMNS, ...runNowColumns(server)],
          /* Click-through 10: worst first, not A to Z. */
          orderRows: worstFirst,
          emptyText: "No collection log rows for this server yet.",
          /* #5227: a row is the way into that collector's run history (the Collection Log below). */
          onRow: (row, tr) => {
            if (!row || !row.collector) return;
            tr.style.cursor = "pointer";
            tr.setAttribute("title", "Show the runs of " + row.collector + " in the Collection Log below");
            /* Enter/Space as well as a click; a click that ends a text selection inside the row is the user copying, not picking. */
            makeActivatable(tr, (e) => {
              if (e && e.type === "click" && selectionInside(tr)) return;
              pickCollector(server, row.collector);
            });
          },
        },
        {
          title: "Heaviest Collectors",
          subtitle: "trailing 7 days",
          viz: "table",
          rowsKey: "sweep_pressure.heaviest_collectors",
          columns: HEAVIEST_COLUMNS,
          emptyText: "No per-collector timings recorded yet.",
        },
      ]),
      /* #2484: the RAW per-run log under the rollup above. Its own read, not a slice of the fanout --
         the rollup aggregates seven days into one row per collector, and no projection of it can give
         back the individual runs. This is the tab people reach for when the rollup says HEALTHY and
         collection still looks wrong, and until now the WPF viewer was the only way to it. */
      collectionLogPanel(server, ctx),
      /* #6298: the desktop's Duration Trends chart, the longest run of each collector over the range. */
      seriesTrendPanel(server, ctx, "collector_duration"),
    ],
  },

  /* #4843: the desktop viewer's analysis Recommendations view (pages/analysis-findings.js). */
  analysisFindingsTab,
];

/* ─────────────────────────── the PostgreSQL tabs ─────────────────────────── */

/**
 * The PostgreSQL registry (#2530). EIGHT tabs against the SQL Server registry's twelve, and the difference is the
 * design rather than a shortfall: parity was explicitly not the constraint. Bloat, wraparound, the xmin horizon
 * and autovacuum have no SQL Server analogue and are what actually pages a PostgreSQL DBA; tempdb, Query Store,
 * trace flags, plan cache and the system_health ring buffer have no PostgreSQL analogue, and rendering them at a
 * PostgreSQL target is the defect this registry removes rather than a shape to reproduce.
 *
 * Every get_pg_* read the service serves lands on exactly one of these tabs, and a pin asserts it in both
 * directions — derived from the dispatch, so a NEW PostgreSQL read cannot ship reachable only through MCP,
 * which is how the first eight spent three releases. It has already caught one: #2539's
 * get_pg_database_stats landed on dev while this was in review, and the pin refused the merge until the
 * Activity tab showed it.
 *
 * THE ENGINE-NEUTRAL BORROWING, and why it is only three reads. get_collection_health, get_collection_log and
 * get_analysis_findings read the collection log and the findings store — neither is a SQL Server collector's
 * output — so they answer a PostgreSQL target honestly and are the same panels the SQL Server Overview and
 * Collection Health tabs build. Nothing else is shared. get_server_summary and get_daily_summary LOOK
 * engine-neutral and are not: they roll up SQL Server wait types, memory pressure and deadlocks, and at a
 * PostgreSQL target they answer `unavailable` with a sentence about a collector that will never run.
 */
export const POSTGRES_TABS = [
  {
    id: "overview",
    label: "Overview",
    build: (server, ctx) => [
      /* The vitals are the three Tier 0 outage predictors plus the backlog that feeds two of them, as tiles
         rather than the tables the owning tabs carry: the question on a triage screen is "is anything wrong",
         not "which table". Each read is fetched again by the tab that owns it — the same deliberate duplication
         the SQL Server Overview makes with file I/O and findings. The cost is one read per tab actually opened;
         the alternative is an Overview that cannot answer its own question. */
      momentStat(
        "Freeze Headroom",
        "get_pg_wraparound_risk",
        { server, hours: ctx.hours },
        PG_WRAPAROUND_STATS,
        ctx.label,
        1,
        "No freeze-headroom samples in this window."
      ),
      /* #2719: Aurora only. get_pg_cpu_utilization reads AWS Performance Insights' os.cpuUtilization.total.avg
         over the RDS/PI API, which self-hosted PostgreSQL has no route to at all — not gated by major version
         or by an extension the way the rest of this tab's panels are, but by the target being Amazon Aurora in
         the first place. On a stock PostgreSQL target the panel is permanently empty and says so in its own
         words, the same treatment the Waits tab gives get_pg_wait_stats. */
      linePanel(
        "Instance CPU",
        "get_pg_cpu_utilization",
        { server, hours: ctx.hours },
        "samples",
        "sample_time",
        PG_CPU_SERIES,
        {
          subtitle: ctx.label + ", Amazon Aurora only (AWS Performance Insights)",
          format: "pct",
          unit: "%",
          emptyText: "No CPU samples in this window. This is an Amazon Aurora feature — on a stock PostgreSQL target this panel is permanently empty.",
          /* Click-through 3: on a server that is not Aurora the read answers "not collected", and the panel is not drawn. */
          hideWhenNotCollected: true,
          atTime: false,
        }
      ),
      momentStat(
        "xmin Horizon",
        "get_pg_xmin_horizon",
        { server, hours: ctx.hours },
        PG_XMIN_STATS,
        ctx.label,
        1,
        "Nothing held the xmin horizon back in this window."
      ),
      /* The counts are over the rows RETURNED, not over the database: the read ranks worst-first and caps at
         `limit`, and table_count / past_threshold_count / growing_count are all computed after that cap. So the
         subtitle says "worst 20" and the labels say "of those" — a tile reading "20 tables" on a server with
         4,000 tables behind on vacuum would be the most reassuring wrong number on the page. */
      stat(
        "Autovacuum Backlog",
        "get_pg_autovacuum_health",
        { server, hours: ctx.hours, limit: 20 },
        PG_AUTOVACUUM_STATS,
        ctx.label + ", worst 20 tables",
        1,
        "No table has dead tuples, pending analyze work, inserts since its last vacuum, or autovacuum disabled. The collector records only tables with pending work, so this is the healthy answer rather than missing data."
      ),
      stat(
        "Replication Slots",
        "get_pg_replication_slots",
        { server, hours: ctx.hours },
        PG_SLOT_STATS,
        ctx.label,
        1,
        "This server has no replication slots. A slot is the thing that retains WAL indefinitely, so none is one fewer way to fill a disk."
      ),
      tablePanel(
        "Analysis Findings",
        "get_analysis_findings",
        { server, hours: ctx.hours },
        "findings",
        FINDING_COLUMNS,
        ctx.label,
        "No findings in this window. On a PostgreSQL target the analysis pass writes the three Tier 0 outage predictors — wraparound, the xmin horizon and replication-slot retention — and nothing else yet, so an empty grid here is narrower than it looks."
      ),
      /* The SQL Server registry gives collection health a tab of its own. Six tabs is not enough to spend one
         on it, and on a PostgreSQL target it is the FIRST question anyone asks — eight collectors, several of
         them gated off by major version or by Aurora-ness — so it lands on the Overview instead. One read,
         three panels, for the reason fanout exists. */
      collectorRunBar(server),
      ...fanout("get_collection_health", { server }, [
        { title: "Sweep Pressure", subtitle: "trailing 7 days", viz: "stat", stats: SWEEP_STATS },
        /* #5735: the collection loop's skipped slots, from the same get_collection_health read (its `skipped_cycles`
           block, always present: count 0 is a real answer). Its own panel because its window is 24 hours, not the 7 days
           beside it. */
        { title: "Skipped Cycles", subtitle: "trailing 24 hours", viz: "stat", stats: SKIPPED_CYCLES_STATS },
        /* #3013: the alerting layer's own store reads, which appear on no other health surface. Its own
           panel rather than a tile on Sweep Pressure specifically because of the SUBTITLE: these figures are
           in-memory counts since the service started, and inheriting "trailing 7 days" would have made the
           panel assert a window it did not measure. Shared const so the SQL Server and PostgreSQL tabs cannot
           drift apart on it. */
        ALERT_READ_PANEL,
        COLLECTION_CAVEATS_PANEL,
        {
          title: "Collectors",
          subtitle: "trailing 7 days",
          viz: "table",
          rowsKey: "collectors",
          columns: [...COLLECTOR_COLUMNS, ...runNowColumns(server)],
          /* Click-through 10: worst first, not A to Z. */
          orderRows: worstFirst,
          emptyText: "No collection log rows for this server yet.",
        },
        {
          title: "Heaviest Collectors",
          subtitle: "trailing 7 days",
          viz: "table",
          rowsKey: "sweep_pressure.heaviest_collectors",
          columns: HEAVIEST_COLUMNS,
          emptyText: "No per-collector timings recorded yet.",
        },
      ]),
      tablePanel(
        "Collection Log",
        "get_collection_log",
        { server, hours: ctx.hours, limit: 200 },
        "runs",
        COLLECTION_LOG_COLUMNS,
        "individual runs, newest first, over the selected window",
        "No collector runs in the selected window. A collector gated off for this engine writes no log row at all rather than a zero-row success, so an absence here is the gate working."
      ),
      /* #2629: the extension inventory. On Overview because this is the "what IS this server" tab, and
         because it is the answer to most of the empty panels elsewhere on the page — a state of
         'available' means the files are there and one CREATE EXTENSION fills a grid that currently reads
         as a permanent absence. */
      /* `noteKey` for the same reason the index bloat panel has one (#3434). Rows here are the PRODUCT of
         databases and extension names - 102 names per database on the measured fleet - so 50 rows is one
         database's slice of a multi-database host, and the ordering puts the non-relevant INSTALLED rows
         behind every non-relevant AVAILABLE one, so what the cap removes first is precisely what an
         install census needs. The read computes all of that (#3425): its note carries the truncation
         caveat, the reach verdict saying whether a larger limit would help, and the pointer to
         install_census and database_name. Without the note this page rendered a capped census with no
         caveat at all, which is a weaker version of the same defect - a page that looks complete. */
      ...fanout("get_pg_extensions", { server, hours: ctx.hours, limit: 50 }, [
        {
          title: "Extensions",
          subtitle: ctx.label + ", per database; 'available' means CREATE EXTENSION would work",
          viz: "table",
          rowsKey: "extensions",
          columns: PG_EXTENSION_COLUMNS,
          emptyText:
            "No extension inventory in this window. It is collected once a day, so a short window can be empty.",
          noteKey: "note",
        },
        {
          title: "Extension Install Census",
          subtitle: "one row per extension created anywhere on this server; NO row limit touches this",
          viz: "table",
          rowsKey: "install_census",
          columns: PG_EXTENSION_CENSUS_COLUMNS,
          emptyText:
            "Nothing is created on this server beyond what PostgreSQL installs itself, or the inventory has not been collected yet. This census counts only extensions somebody created: the available-but-absent ones are the actionable rows and they sort first in the grid above.",
        },
      ]),
    ],
  },

  {
    id: "activity",
    label: "Activity",

    /* HALF of this tab is Aurora-only, and only half. get_pg_blocking runs everywhere — including on
       standbys, where a recovery conflict is blocking that happens nowhere else — while get_pg_top_queries
       is fed by pg_statement_stats, whose AppliesTo gate is target.IsAurora, so it reads
       aurora_stat_statements() and has no core-PostgreSQL equivalent in any version. Same treatment as the
       Waits tab, for the same reason: the panel self-explains via not_collected, and the note says so before
       the reader clicks. Saying it here rather than only in the panel matters more on THIS tab than on
       Waits, because the rest of the tab does fill, so a reader could reasonably read one empty grid among
       three as a fault. */
    note: "The blocking panels cover every PostgreSQL target. Top Query Shapes needs Amazon Aurora and stays empty elsewhere.",
    noteInfo: "Top Query Shapes comes from aurora_stat_statements(), which core PostgreSQL does not have. The blocking panels above it work everywhere, standbys included.",
    build: (server, ctx) => [
      /* One read, three panels, and the FIRST of them is the denominator. get_pg_blocking is a periodic SAMPLE,
         not an event log: PostgreSQL records nothing unless asked, so "no chains" is ambiguous between a quiet
         server and a collector that never ran, and only the capture counts separate them. Showing the chains
         without the counts is the shape of answer this read was built to refuse. */
      ...fanout("get_pg_blocking", { server, hours: ctx.hours, limit: 50 }, [
        {
          title: "Blocking Sampling",
          subtitle: ctx.label + ", the denominator behind the two grids below",
          viz: "stat",
          stats: PG_BLOCKING_STATS,
          span: 2,
        },
        {
          title: "Blocking Chains",
          subtitle: ctx.label + ", root blocker attributed",
          viz: "table",
          rowsKey: "chains",
          columns: PG_BLOCKING_CHAIN_COLUMNS,
          emptyText:
            "No blocking chain was sampled in this window. Read that against the capture count above: with captures and no chains, nobody was blocked at any moment the collector looked.",
        },
        {
          title: "Lock Cycles",
          subtitle: ctx.label + ", mutual waits with no root",
          viz: "table",
          rowsKey: "cycles",
          columns: PG_BLOCKING_CYCLE_COLUMNS,
          emptyText:
            "No lock cycle was sampled in this window — the healthy state. A cycle has no root blocker to attribute, which is why it is a separate grid rather than a chain with a missing root.",
        },
      ]),
      /* One fetch, two panels: the eviction pass count and the statement cap in a tile (the figures the desktop
         viewer's note names), then the grid. #4677: the caveat the read builds (null when the counter was read and
         no pass happened, the unknown sentence when it was never observed) renders above the rows. */
      ...fanout("get_pg_top_queries", { server, hours: ctx.hours, limit: 20 }, [
        {
          title: "Statement Evictions",
          subtitle: ctx.label + ", pg_stat_statements passes",
          viz: "stat",
          stats: PG_EVICTION_STATS,
          span: 1,
          emptyText: "No eviction figures: the extension's eviction counter was not read in this window.",
        },
        {
          title: "Top Query Shapes",
          subtitle: ctx.label + ", by total execution time",
          viz: "table",
          rowsKey: "queries",
          columns: PG_TOP_QUERY_COLUMNS,
          emptyText: "No query statistics in this window.",
          noteKey: "evictions.note",
        },
      ]),
      /* Directly under the query shapes, joined on queryid: a plan only means something beside the
         statement it belongs to. The plan JSON is REDACTED at collection - query text dropped, literals
         replaced - so nothing customer-specific reaches this grid, and the empty text names the usual
         cause rather than implying the server is quiet. The Plan cell shows the row's own redacted JSON;
         no second read. */
      tablePanel(
        "Captured Plans",
        "get_pg_plans",
        { server, hours: ctx.hours, limit: 10 },
        "plans",
        [...PG_PLAN_COLUMNS, pgPlanColumn(server)],
        ctx.label + ", grouped by plan shape - plans are redacted at collection",
        "No captured plans. Usually auto_explain is not loaded, or the monitoring login cannot read the server log; on Aurora and RDS there is no log file to read at all."
      ),
      /* #3070: directly under the plans, because it is the answer to the grid above being empty and to the
         deadlock grid further down being empty. The Windows Viewer puts this panel on its Vacuum tab, which
         is a different registry with a different shape; here it belongs beside the two reads whose absence
         it explains, and its own empty text is what stops that explanation being circular. Every facet is
         shown, satisfied ones included — a list of only the failures cannot show that capture is working. */
      tablePanel(
        "Plan Capture Readiness",
        "get_pg_plan_capture_readiness",
        { server, hours: ctx.hours, limit: 25 },
        "facets",
        PG_PLAN_CAPTURE_READINESS_COLUMNS,
        ctx.label + ", the newest reading of each facet in it; in the order to fix them, top to bottom; the remedy is per facet, and on Aurora/RDS it says which changes need a parameter group and a reboot",
        "No readiness state collected. The collector has not run for this server yet, or the window is shorter than its hourly run."
      ),
      /* #3607: the rest of the logging surface, directly under plan-capture readiness because it is the
         other half of the same onboarding question - is this target telling us everything it could. Judged
         from the newest stored pg_server_config snapshot rather than collected, so it takes no window; the
         read reports the snapshot time as captured_at. Every setting is shown whatever its
         verdict, for the reason the readiness grid shows satisfied facets: a list of only the failures
         cannot show that a target IS instrumented. */
      tablePanel(
        "Logging Settings Audit",
        "get_pg_logging_audit",
        { server },
        "facets",
        PG_LOGGING_AUDIT_COLUMNS,
        "newest configuration snapshot; verdict describes the log lines each setting writes - partial means a threshold is filtering and is the recommended posture for log_min_duration_statement; the remedy is worded for this server's hosting flavour",
        "No configuration snapshot to audit. pg_server_config runs hourly unless its schedule was changed, so a server registered within the last collection interval has not reached its first collection - this is the absence of evidence, not a verdict about the server's logging."
      ),
      /* Directly UNDER the query shapes, because that is the question it answers (#2539). A statement whose
         time makes no sense from its row count usually spilled, and pg_stat_database's temp counters are the
         only evidence of that we collect — the statement stats themselves cannot see it. The deadlock and
         rollback counters ride along because they come from the same free read; the raw blks_hit / blks_read
         columns do not, because `cache_hit_pct` and the read's own `cache_finding` say what they are for
         without a third and fourth column of block counts to be misread as a workload measure. */
      ...fanout("get_pg_database_stats", { server, hours: ctx.hours, limit: 20 }, [
        {
          title: "Database Activity",
          subtitle: ctx.label + ", totals over every database that moved",
          viz: "stat",
          stats: PG_DATABASE_STATS,
          span: 2,
          emptyText: "No database recorded transactions, block accesses, temp files or deadlocks in this window.",
        },
        {
          title: "By Database",
          subtitle: ctx.label + ", biggest temp-file spiller first, then by blocks read",
          viz: "table",
          rowsKey: "databases",
          columns: PG_DATABASE_COLUMNS,
          emptyText:
            "No per-database activity in this window. These are changes over the window, so one snapshot is not enough.",
        },
      ]),
      /* #2663 the same counters over TIME, directly under the window they summarise. This is where the
         cache hit ratio becomes usable at all: pg_stat_database's counters are cumulative since the last
         reset, so the ratio above is a lifetime average that barely moves - a database that fell off a
         cliff an hour ago still reads 99% because of the weeks behind it. Differenced per interval, the
         cliff is on screen. Same for spills: a weekly total cannot tell one bad afternoon from a leak. */
      tablePanel(
        "Database Trend",
        "get_pg_database_trend",
        { server, hours: ctx.hours },
        "points",
        PG_DATABASE_TREND_COLUMNS,
        ctx.label + ", the biggest temp-file spiller in this window, or the busiest by block access when nothing spilled; each row's hit ratio is its own intervals', not the lifetime average, and Worst Hit % is its worst single interval",
        "No differenced intervals for this database in the window. A trend needs at least two snapshots, so a short window is legitimately empty here."
      ),
      /* #2629: sampled lock activity. On Activity rather than a Blocking tab because PostgreSQL has no
         Blocking tab — get_pg_blocking's chains live here too — and because this answers a different
         question from that one: which modes and relations are contended OVER TIME, rather than who is
         blocking whom right now. */
      tablePanel(
        "Lock Activity (sampled)",
        "get_pg_lock_stats",
        { server, hours: ctx.hours, limit: 25 },
        "locks",
        PG_LOCK_STATS_COLUMNS,
        ctx.label + ", a sample of pg_locks rather than an event log; ungranted rows are the contended ones",
        "No lock activity sampled in this window. Locks are sampled, so one taken and released between two captures does not appear."
      ),
      /* #2629: what the queries above actually FILTER on. Placed on Activity rather than Storage because
         it is evidence about the workload, not about the disk — and it belongs beside top queries, since
         a predicate row and a query row are two views of the same execution. */
      tablePanel(
        "Predicate Selectivity",
        "get_pg_predicate_stats",
        { server, hours: ctx.hours, limit: 25 },
        "predicates",
        PG_PREDICATE_COLUMNS,
        ctx.label + ", sampled counts - scale by 1/sample rate and treat the product as an estimate",
        "No predicate statistics in this window. pg_qualstats samples executions (1% by default) and needs shared_preload_libraries plus a restart, so an empty panel here usually means it is not loaded."
      ),
      tablePanel(
        "Column Statistics",
        "get_pg_column_stats",
        { server, hours: ctx.hours, limit: 25 },
        "columns",
        PG_COLUMN_STATS_COLUMNS,
        ctx.label + ", the numbers the planner turns into row estimates; n_distinct is a ratio when negative",
        "No column statistics in this window. They are collected once a day, for tables above a size floor and only where the monitoring login can read the table."
      ),
      /* #2661 deadlocks, on Activity beside blocking because they are the same subject at its limit: a
         blocking chain that resolved, and one the server had to break by cancelling somebody. Below
         blocking rather than above it because blocking is the far more common finding. */
      tablePanel(
        "Deadlocks",
        "get_pg_deadlocks",
        { server, hours: ctx.hours },
        "deadlocks",
        PG_DEADLOCK_COLUMNS,
        ctx.label + ", newest first; Sightings counts re-reads of the same report, not repeats",
        "No deadlock was reported in this window. That is the healthy answer - but it is the same shape as a server whose log cannot be read, which the Plan Capture Readiness panel above reports on because it reads the same file. pg_stat_database's deadlock counter is the independent check."
      ),
      /* #3601 the log, classified. Under the deadlocks because the two are the same log read two ways:
         the deadlock grid is one ERROR line's DETAIL graph parsed out whole, this is every classified line
         around it - the errors before it, the lock waits that preceded it, the connection churn beside it.
         The "check the error log" panel. Newest 50 across every family; the MCP read carries the family and
         severity filters, and total_events on its payload is the window's count, so the panel note says
         which of the two this grid is. */
      tablePanel(
        "Log Events",
        "get_pg_log_events",
        { server, hours: ctx.hours, limit: 50 },
        "events",
        PG_LOG_EVENT_COLUMNS,
        ctx.label + ", newest 50 across every family; messages as PostgreSQL wrote them, SQL in them normalized, the statement never stored; Sightings counts re-reads of the same line, not repeats",
        "No classified log event was stored in this window. That is the healthy answer for the error and lock-wait families - and it is also what a target with the relevant log_* setting off looks like (log_connections, log_lock_waits, log_temp_files, log_autovacuum_min_duration), or one whose log cannot be read, is not stamped UTC, or is not written in English: the Plan Capture Readiness panel above reads the same file and reports those three."
      ),
      /* #2663 the regression read: what ONE execution of the busiest statement cost, interval by interval.
         The statement grid above ranks by total time across the window, which hides a step change - a query
         that doubled halfway through still ranks where its average puts it. */
      tablePanel(
        "Query Duration Trend",
        "get_pg_query_duration_trend",
        { server, hours: ctx.hours },
        "points",
        PG_QUERY_DURATION_TREND_COLUMNS,
        ctx.label + ", the statement with the most execution time in this window; a step that persists is a plan or index change",
        "No statement recorded execution time in this window, so there is nothing to follow."
      ),
      /* The graphs themselves, under the summary - the same pairing the SQL Server Deadlocks tab makes.
         The read takes an optional hash, so this panel needs no drill-down plumbing: without one it
         answers with the most recent graphs. */
      tablePanel(
        "Deadlock Graphs",
        "get_pg_deadlock_detail",
        { server, limit: 5 },
        "deadlocks",
        PG_DEADLOCK_GRAPH_COLUMNS,
        "the whole wait graph as the server wrote it, including every participant's statement, SQL normalized",
        "No deadlock graph stored for this server."
      ),
    ],
  },

  {
    id: "vacuum",
    label: "Vacuum",
    note: "Sessions, the xmin horizon, the vacuum backlog and freeze headroom, in the order one leads to the next.",
    noteInfo:
      "A session holds a transaction open, which holds the xmin horizon back, which stops vacuum reclaiming rows, which lets freezing fall " +
      "behind and ends in wraparound. Each panel looks survivable alone. The first panel names the session, which is where the fix is made, " +
      "and says whether it pins anything: an open transaction with no snapshot and no transaction id costs vacuum nothing.",
    build: (server, ctx) => [
      /* Ordered by CAUSE, not by severity: session, then horizon, then backlog, then headroom.

         Session states leads because it is the link UPSTREAM of the horizon panel that used to open this
         tab. get_pg_xmin_horizon names the CLASS of holder (a session, a slot, standby feedback, a prepared
         transaction); this names WHICH session, and the remedy is always in whatever opened the
         transaction. It is also the only panel that can withhold the causal claim: a long
         idle-in-transaction session that pins nothing is the shape everybody misreads, and meeting it here
         is what stops the backlog two panels down being blamed on it. */
      ...fanout("get_pg_session_states", { server, hours: ctx.hours, limit: 25 }, [
        {
          title: "Sessions Holding a Transaction Open",
          subtitle: ctx.label + ", a sample at the collection interval rather than an event log",
          viz: "stat",
          stats: PG_SESSION_STATE_STATS,
          span: 2,
          emptyText:
            "No session held a transaction open past the collector's floor in this window. Sessions are sampled, so one that opened and closed between two captures left no trace.",
        },
        {
          title: "By Session",
          subtitle: ctx.label + ", horizon holders first, then the longest transaction",
          viz: "table",
          rowsKey: "sessions",
          columns: PG_SESSION_STATE_COLUMNS,
          emptyText:
            "No session held a transaction open past the collector's floor. Sessions are sampled, so one that opened and closed between two captures is not shown.",
        },
      ]),
      ...fanout("get_pg_xmin_horizon", { server, hours: ctx.hours }, [
        {
          title: "What Holds the Horizon",
          subtitle: ctx.label,
          viz: "stat",
          windowNote: false,
          stats: PG_XMIN_STATS,
          span: 2,
          emptyText:
            "Nothing held the xmin horizon back in this window — the healthy state. Vacuum can reclaim everything it finds.",
        },
        {
          title: "Horizon Holders",
          subtitle: ctx.label + ", every cause seen, not only the current winner",
          viz: "table",
          rowsKey: "holders",
          columns: PG_XMIN_COLUMNS,
          emptyText:
            "No holder in this window. Four unrelated causes produce the same symptom — a long-running session, a replication slot, standby feedback, a prepared transaction — so this grid names the cause rather than repeating that the horizon is old.",
        },
      ]),
      ...fanout("get_pg_autovacuum_health", { server, hours: ctx.hours, limit: 20 }, [
        {
          title: "Autovacuum Backlog",
          subtitle: ctx.label + ", worst 20 tables",
          viz: "stat",
          stats: PG_AUTOVACUUM_STATS,
          span: 2,
          emptyText:
            "No table has dead tuples, pending analyze work, inserts since its last vacuum, or autovacuum disabled — the healthy answer.",
        },
        {
          title: "Tables Behind",
          subtitle: ctx.label + ", ranked by how far past each table's own threshold",
          viz: "table",
          rowsKey: "tables",
          columns: PG_AUTOVACUUM_COLUMNS,
          emptyText:
            "No table is behind on vacuum or analyze. The collector records only tables with pending work, so an empty grid is the healthy case rather than a collector that has not run.",
        },
      ]),
      ...fanout("get_pg_wraparound_risk", { server, hours: ctx.hours }, [
        {
          title: "Freeze Headroom",
          subtitle: ctx.label,
          viz: "stat",
          windowNote: false,
          stats: PG_WRAPAROUND_STATS,
          span: 1,
          emptyText: "No freeze-headroom samples in this window.",
        },
        {
          /* The percentages above mean nothing without these four, and they are constants the READ ships, so
             that the browser is not the third place in this repo that decides where the failsafe engages. */
          title: "Where the Thresholds Are",
          subtitle: "PostgreSQL's own, not ours",
          viz: "stat",
          windowNote: false,
          stats: PG_WRAPAROUND_THRESHOLD_STATS,
          span: 1,
        },
        {
          title: "Per-Database Headroom",
          subtitle: ctx.label + ", XID and MultiXact freeze headroom",
          viz: "table",
          rowsKey: "databases",
          columns: PG_WRAPAROUND_COLUMNS,
          emptyText:
            "No per-database freeze headroom in this window. Both counters are tracked: a database can be comfortable on XIDs and in trouble on MultiXacts, which is why they are separate columns rather than one worst-of.",
        },
      ]),
    ],
  },

  {
    id: "waits",
    label: "Waits",
    /* THE DECISION, argued in the PR: this tab is SHOWN at every PostgreSQL target, including the stock ones
       where its read can never have content. get_pg_wait_stats reads aurora_stat_system_waits(), which core
       PostgreSQL has in no version, so on `postgres` the panel is permanently empty — and it is permanently
       empty WITH A SENTENCE, because #2532 taught the read to answer that state with `not_collected` naming
       the server, the engine, the collector and the exact Aurora surface, and saying "and never will". A panel
       that explains itself is not the defect #2530 was filed about; twelve unexplained blank SQL Server tabs
       were. Hiding it would buy a tidier stock-PostgreSQL page at the price of a tab set that changes shape
       between two PostgreSQL servers in one fleet, and of making the one Aurora-specific capability we have
       invisible to the operator best placed to want it. */
    note: "Wait-event sampling needs Amazon Aurora. On other PostgreSQL targets this tab stays empty.",
    noteInfo: "It comes from aurora_stat_system_waits(), which core PostgreSQL does not have.",
    build: (server, ctx) => [
      tablePanel(
        "Wait Events",
        "get_pg_wait_stats",
        { server, hours: ctx.hours, limit: 20 },
        "waits",
        PG_WAIT_COLUMNS,
        ctx.label + ", background-worker and client-idle waits already excluded by the collector",
        "No wait events in this window."
      ),
      /* #2629: the same tab carries the stock-PostgreSQL answer, so the two are read TOGETHER rather than
         one being a consolation for the other's blank. Exactly one of them fills on any given target — the
         Aurora grid above on Aurora, this one wherever pg_wait_sampling is loaded — and each explains its
         own emptiness in its own words, which is what makes a permanently-empty panel informative rather
         than a defect. */
      tablePanel(
        "Sampled Waits (pg_wait_sampling)",
        "get_pg_wait_sampling",
        { server, hours: ctx.hours, limit: 20 },
        "waits",
        PG_WAIT_SAMPLING_COLUMNS,
        ctx.label + ", sample counts from a periodic profiler; event type CPU means running, not waiting",
        "No sampled waits in this window."
      ),
      tablePanel(
        "OS CPU by Query (pg_stat_kcache)",
        "get_pg_kernel_stats",
        { server, hours: ctx.hours, limit: 20 },
        "queries",
        PG_KERNEL_COLUMNS,
        ctx.label + ", CPU measured by the operating system; device bytes are not logical I/O",
        "No per-query OS resource usage in this window.",
        2,
        null,
        null,
        null,
        null,
        null,
        "pg_stat_kcache is not installed on this server, so per-query OS CPU is not collected. Install the extension (it needs shared_preload_libraries and a restart) to fill this panel."
      ),
      /* #2663 the first PostgreSQL time series. The single-window panels above rank what this server waits
         on; this follows ONE of them over time, which is the question they cannot answer. Parameterised by
         event, so it needs one chosen - the toolbar default is the event that dominates the window. */
      tablePanel(
        "Wait Trend",
        "get_pg_wait_trend",
        { server, hours: ctx.hours },
        "points",
        PG_WAIT_TREND_COLUMNS,
        ctx.label + ", the event that dominates this window, over time; estimates from a sampling profiler, so the shape is the finding",
        "No samples for this wait event in the window. A trend needs at least two snapshots to difference, so a short window is legitimately empty here even while the event is being sampled."
      ),
    ],
  },

  {
    id: "io",
    label: "I/O",
    note: "PostgreSQL reports I/O by backend type, object and context, not by file.",
    noteInfo: "On Aurora the write columns are blank rather than zero: backends there do not write data files, storage does.",
    build: (server, ctx) => [
      ...fanout("get_pg_io_stats", { server, hours: ctx.hours, limit: 20 }, [
        {
          title: "I/O Summary",
          subtitle: ctx.label + ", differenced across the window",
          viz: "stat",
          stats: PG_IO_SUMMARY_STATS,
          span: 2,
          emptyText: "No I/O activity recorded in this window.",
        },
        {
          title: "By Backend, Object and Context",
          subtitle: ctx.label + ", busiest by read time first",
          viz: "table",
          rowsKey: "combinations",
          columns: PG_IO_COLUMNS,
          emptyText:
            "No I/O in this window. pg_stat_io needs PostgreSQL 16 or newer; on an older major the collector does not run at all, and the Collection Health panels on the Overview tab are where that shows.",
        },
      ]),
      /* #2629: what the I/O above is reading INTO, and what is writing it back out. Residency and
         checkpoint pressure are the two halves of a page's life either side of the I/O counters this tab
         already shows, so all three belong together. */
      tablePanel(
        "Buffer Pool Residency",
        "get_pg_buffer_usage",
        { server, hours: ctx.hours, limit: 25 },
        "relations",
        PG_BUFFER_USAGE_COLUMNS,
        ctx.label + ", residency is not read volume - a small hot table and a large one scanned once read alike",
        "No buffer pool contents recorded. This needs the pg_buffercache extension in the database the collector connects to."
      ),
      /* stat, not table: this read answers with ONE object describing the whole window, not a row set. */
      stat(
        "Checkpoints and WAL",
        "get_pg_write_stats",
        { server, hours: ctx.hours },
        PG_WRITE_STATS,
        ctx.label + ", requested checkpoints mean write volume filled max_wal_size before the interval elapsed",
        2,
        "No checkpoint or WAL activity differenced yet. These are differenced across snapshots, so a single collection has nothing to difference against and the window fills on the second."
      ),
      /* #2663 the I/O time series. Everything above this line describes one window; this follows one
         (backend_type, context) pair through it. The pair is the subject rather than the backend type alone
         because a hit ratio summed across contexts is meaningless - bulkread deliberately bypasses the
         buffer pool with a ring buffer, so folding its misses in with the normal context's understates both
         and the two have opposite remedies. No drill-down plumbing: the read chooses the busiest pair when
         the toolbar names none, and says which it chose. */
      tablePanel(
        "I/O Trend",
        "get_pg_io_trend",
        { server, hours: ctx.hours },
        "points",
        PG_IO_TREND_COLUMNS,
        ctx.label + ", the busiest backend and context in this window, per second; avg read ms is blank unless the server has track_io_timing on, which is off by default",
        "No I/O samples for this combination in the window. A trend needs at least two snapshots to difference, so a short window is legitimately empty here even while the combination is being collected."
      ),
    ],
  },

  {
    id: "replication",
    label: "Replication",
    build: (server, ctx) => [
      ...fanout("get_pg_replication_slots", { server, hours: ctx.hours }, [
        {
          title: "Slot Summary",
          subtitle: ctx.label,
          viz: "stat",
          stats: PG_SLOT_STATS,
          span: 2,
          emptyText:
            "This server has no replication slots. A slot is the thing that retains WAL indefinitely, so none is one fewer way to fill a disk.",
        },
        {
          title: "Slots",
          subtitle: ctx.label + ", most WAL retained first",
          viz: "table",
          rowsKey: "slots",
          columns: PG_SLOT_COLUMNS,
          emptyText:
            "No replication slots on this server. An inactive slot retains WAL forever and is the usual way a PostgreSQL instance fills its disk with nobody watching, so an empty grid here is good news.",
        },
      ]),
      /* #2629: the connected replicas, beside the slots. Two different questions and the DANGEROUS case is
         the disagreement — a slot that persists after its replica stops connecting retains WAL forever,
         and neither panel alone shows that. Slots first because that is the one that fills a disk. */
      tablePanel(
        "Connected Replicas",
        "get_pg_replication_stats",
        { server, hours: ctx.hours, limit: 25 },
        "replicas",
        PG_REPLICATION_STATS_COLUMNS,
        ctx.label + ", worst-in-window beside latest; lag is spiky and one sample catches one instant",
        "No replica was connected in this window. Expected on a server with no replicas - but if one is supposed to be attached, check the slots panel above: a slot with nothing connected to it retains WAL indefinitely."
      ),
    ],
  },

  /* Storage (#2541, #2542). Two reads, one question: where the space went and whether it is earning its
     keep. Bloat is deliberately NOT on the Vacuum tab despite being what vacuum lag costs - that tab is the
     cause chain read in causal order, and dropping the damage into the middle of it breaks the sequence
     that makes those three panels one story.

     Bloat is placed first because its number is an ESTIMATE and is the more dangerous of the two to act on:
     the summary tile carries the suppression count so a reader meets the caveat before any percentage. */
  {
    id: "storage",
    label: "Storage",
    build: (server, ctx) => [
      ...fanout("get_pg_table_bloat", { server, hours: ctx.hours, limit: 25 }, [
        {
          title: "Bloat Estimates",
          subtitle: ctx.label + ", estimates only - the tables are never read",
          viz: "stat",
          stats: PG_BLOAT_STATS,
          span: 2,
          emptyText:
            "No table of at least 1 MB was measured on this server. Below that floor bloat is not an actionable amount of space, so an empty panel here is the healthy answer rather than a missing read.",
        },
        {
          title: "By Table",
          subtitle: ctx.label + ", biggest estimated waste first; rows with no publishable estimate sort last",
          viz: "table",
          rowsKey: "tables",
          columns: PG_BLOAT_COLUMNS,
          emptyText:
            "No table of at least 1 MB was measured on this server. Bloat under a megabyte is not worth a maintenance window, so the collector does not store it.",
        },
      ]),
      ...fanout("get_pg_index_usage", { server, hours: ctx.hours, limit: 25 }, [
        {
          title: "Index Usage",
          subtitle: ctx.label + ", candidates are not conclusions",
          viz: "stat",
          stats: PG_INDEX_USAGE_STATS,
          span: 2,
          emptyText:
            "No index of at least 64 KB was recorded on this server. Below that floor an index costs effectively nothing to keep, so an empty panel here is the healthy answer rather than a missing read.",
        },
        {
          title: "By Index",
          subtitle: ctx.label + ", biggest unscanned first; invalid indexes on top",
          viz: "table",
          rowsKey: "indexes",
          columns: PG_INDEX_USAGE_COLUMNS,
          emptyText:
            "No index of at least 64 KB was recorded on this server, so there is nothing here to judge. This collector runs daily and is gated off on read replicas, where scan counts are the replica's own rather than the writer's.",
        },
      ]),
      /* #2629: index bloat, beside the table bloat above and the usage counts. The three answer one
         question in sequence — how much space, is it earning its keep, and is the index itself wasting it.

         #3234 made this an ESTIMATE from catalog statistics; it no longer walks a page, and there is no
         per-cycle measurement budget to be skipped by. The title, subtitle and empty text all described the
         old census until #3278 came through here.

         `noteKey` carries the server's coverage census (#3278). It has to come from the read rather than
         from the subtitle below, because the whole content of it is FIGURES about this server's population
         — and because this panel's rows are the one set on the page that cannot be read as a coverage
         claim: answerless rows sort FIRST by design, so a page capped at 25 is 100% of them whenever they
         outnumber 25, structurally rather than by chance. */
      tablePanel(
        "Index Bloat (estimated)",
        "get_pg_index_bloat",
        { server, hours: ctx.hours, limit: 25 },
        "indexes",
        PG_INDEX_BLOAT_COLUMNS,
        ctx.label + ", estimated from catalog statistics with no page reads; a row with a reason instead of a number has no answer, which is not the same as healthy",
        "Nothing recorded for this server yet. It is collected once a day, so a short window or a new install is empty here.",
        2,
        "note"
      ),
      /* THE ANSWERS, REACHED (#3434). The panel above keeps #3278's answerless-first order, and on a census
         whose answerless population outnumbers 25 that page is 100% of them - structurally rather than by
         chance - so every answered index sits behind a block no row cap on this page pages past. Measured
         on the fleet: 1,779 answerless rows ahead of 726 answers covering 213.8 GB on the largest census,
         and 268 ahead of 79 covering 191.0 GB on the next. answered_only (#3431) asks the read for that
         population directly, ranked by reclaimable bytes with nothing ahead of it.

         A SECOND PANEL rather than a filter on the first: the order above is deliberate and stays. And not
         a client-side merge of the two reads either - the caveat over each grid is composed by the SERVER
         from population figures the browser does not have, so merging two reads into one grid would mean
         merging two censuses into one sentence, and a note describing a population its grid is not showing
         is the very defect this closes.

         Not fanout(): that is several panels over ONE fetch, and these two differ in a PARAMETER, so there
         is no shared response to slice. */
      tablePanel(
        "Index Bloat (answered ranking)",
        "get_pg_index_bloat",
        { server, hours: ctx.hours, limit: 25, answered_only: true },
        "indexes",
        PG_INDEX_BLOAT_COLUMNS,
        ctx.label + ", only the indexes that have an answer, biggest reclaimable bytes first",
        "Nothing recorded for this server in this window. It is collected once a day.",
        2,
        "note"
      ),
    ],
  },

  /* #2658. The eighth tab, and the header's argument for seven admits it: what that argument rejects is
     reproducing SQL-Server-only CONCEPTS at a PostgreSQL target, not adding a question PostgreSQL genuinely
     has. "What is this server set to, and what changed" is one every engine has, and it was the only
     remaining get_pg_* pair with nowhere to land.

     Changes ABOVE current settings, deliberately. Somebody opening this tab during an incident is asking
     what moved, not what the server is; the full configuration is reference material and reads better as
     the thing underneath it. */
  {
    id: "config",
    label: "Configuration",
    build: (server, ctx) => [
      tablePanel(
        "Configuration Changes",
        "get_pg_server_config_changes",
        { server, hours: ctx.hours },
        "changes",
        PG_CONFIG_CHANGE_COLUMNS,
        ctx.label + ", newest first; the collector runs hourly unless its schedule was changed, so a change happened in the collection interval before it was seen",
        "No configuration parameter changed value in this window. This compares consecutive snapshots, so an unchanged server is legitimately empty here - it is a finding, not missing data."
      ),
      tablePanel(
        "Settings",
        "get_pg_server_config",
        { server },
        "settings",
        PG_SERVER_CONFIG_COLUMNS,
        "non-default first; pending restart means the file and the running server disagree",
        "No configuration snapshot has been collected yet. This collector runs hourly unless its schedule was changed, so a server registered within the last collection interval has not reached its first collection."
      ),
    ],
  },
];

/* Each tab's `reachReads` and `collector`, from server-tab-reach.js (#5562 r3): kept apart so a pin that counts a tab's fetches or
   reads its neighbouring fields as text never sees a reach list. The tab objects carry both at run time. */
withTabReach(SERVER_TABS, SQL_TAB_REACH);
withTabReach(POSTGRES_TABS, POSTGRES_TAB_REACH);

/**
 * The tab registry for a fleet card — the ONE place the engine branch lives.
 *
 * Only a POSITIVE PostgreSQL claim moves a server off the SQL Server registry. `is_postgres` is derived
 * server-side from `collect.servers.engine_kind` (#2530) and is false for a NULL kind, for a token this build
 * does not recognise, and for a card that never arrived — every one of which means "no claim", not "SQL
 * Server". Defaulting an unclaimed server to the SQL Server tabs is the pre-#2530 behaviour, unchanged, and it
 * is the right default while the claim is absent: a PostgreSQL target stamps its kind on its first connect, so
 * the unclaimed population is servers that have not connected since the rung landed, and guessing PostgreSQL
 * from an absence would break every one of them. The browser does not re-derive the boolean (R1) — the card
 * carries it, decoded on the server through MonitoredEngineKind.
 */
export function serverTabsFor(card) {
  return card && card.is_postgres === true ? POSTGRES_TABS : SERVER_TABS;
}

/** True only for a POSITIVE PostgreSQL claim on the card, the same test serverTabsFor makes (#5245: the server page's database
 *  filter reads the claim through this, so server.js never re-derives the boolean from the card itself). */
export function isPostgresTarget(card) {
  return !!card && card.is_postgres === true;
}

/** The tab for an id within a registry, falling back to the first (Overview) — an unknown/absent id is a deep
 *  link, not an error. `overview`, `activity`, `waits` and `io` exist in BOTH registries, so those deep links
 *  survive a server turning out to be the other engine; the rest fall back rather than break. */
export function findServerTab(id, tabs) {
  const registry = tabs || SERVER_TABS;
  return registry.find((t) => t.id === id) || registry[0];
}

/** What Running Jobs says when nothing is listed. "The normal state for most servers" is a claim about a server that is
 *  collecting; a server whose collection is stale (the fleet card's is_online false, the same signal the page header's
 *  Collectors chip reads) has no current reading, so it says so instead and the panel's sentence replaces the read's own
 *  empty message (tablePanel()'s emptyWins). */
export function runningJobsEmptyText(ctx) {
  return ctx && ctx.offline === true
    ? "No running jobs can be shown: collection for this server is stale, so the last snapshot is not a current reading. See Collection Health."
    : "No SQL Agent jobs were running at the last collection — the normal state for most servers.";
}

/** The tab's note as a rendered strip, or null. Kept here so the shell has no opinion about its wording. A note can be
 *  a function of `widestHours`, the widest window the page's Range offers, so a sentence that names it stays true
 *  when the presets change. */
export function tabNote(tab, widestHours) {
  const note = typeof tab.note === "function" ? tab.note(widestHours) : tab.note;
  if (!note) return null;
  const strip = noticeStrip(note);
  /* A longer background behind an info mark, not in the banner itself. */
  if (tab.noteInfo) strip.appendChild(el("span", { class: "info-tip", tabindex: "0", role: "img", title: tab.noteInfo, "aria-label": tab.noteInfo, text: " \u24d8" }));
  return strip;
}

