/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The relations a window-floor probe (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>) may read. Each is a
/// closed member that maps to ONE <c>v_</c> view with <c>server_id</c> and a time column (<c>collection_time</c>,
/// <c>capture_time</c> on the three config snapshots, or <c>event_time</c> on the two event tables: the column the
/// surface's grid filters on, <see cref="LocalDataService.QueryWindowRelationTimeColumn"/>), so no
/// caller hands a view name, and nothing a user typed, into the probe's SQL. #4231 began with the three RAW-ONLY
/// relations (<see cref="QueryStats"/>, <see cref="ProcedureStats"/>, <see cref="QueryStoreStats"/>): every
/// Queries-tab grid and MCP tool that reads one has no rollup underneath it to fall back on when the requested
/// window reaches past what the raw table actually retains. The Lite twin of Darling's Active Queries and Current
/// Waits data-start banners adds <see cref="QuerySnapshots"/> and <see cref="WaitingTasks"/>, and #4966 adds
/// <see cref="PlanCorrection"/> (Plan Corrections) and <see cref="MemoryPressureEvents"/> (Memory Pressure Events); the
/// Query Heatmap reads <c>v_query_stats</c> and asks the <see cref="QueryStats"/> question. A further surface adds its
/// own member and its arm in <c>QueryWindowRelationView</c>; <c>DataStartBannerTests</c> pins that every member names a
/// real archive view with the column the probe windows on. The Job History tab's note adds <see cref="JobHistory"/>
/// (#4966): its grid lists runs by the time each ran, which is the event time, and the probe measures the collector's
/// coverage (<c>collection_time</c>, the time a run was copied from msdb).
/// </summary>
public enum QueryWindowRelation
{
    QueryStats,
    // Group A (Queries tab: Plan Corrections)
    PlanCorrection,
    ProcedureStats,
    QueryStoreStats,
    // Group B (#4966): the Blocking tab's Blocked Process Reports and Deadlocks grids.
    /* The Blocked Process Reports grid: the XE collector's table (blocked_process_reports). The grid also lists the
       always-on DMV blocking snapshots' rows beside it, so its coverage is the EARLIER of this collector's and that
       one's (DmvBlockingSnapshots, QueryWindowRelationAlsoCoveredBy). Holds a row only when a block runs past the
       threshold, so coverage comes from collector runs. The grid filters on the report's own event_time. */
    BlockedProcessReports,
    /* The Deadlocks grid (deadlocks): a row only when one happens, so coverage comes from collector runs. The grid
       filters on the deadlock's own deadlock_time. */
    Deadlocks,
    /* The always-on DMV blocking snapshots (dmv_blocking_snapshots) that the Blocked Process Reports grid lists beside
       its XE reports. Not a surface of its own: it is probed only as the second collector behind
       BlockedProcessReports, for a server whose blocked process threshold is unset (so no XE report is ever stored) and
       for the stretch before the XE collector was switched on. Its event_time IS its collection_time. */
    DmvBlockingSnapshots,
    QuerySnapshots,

    /* Group C of #4966: Collection Health, System Events, Config Changes and Long Queries. */
    CollectionLog,
    SystemHealthEvents,
    DefaultTraceEvents,
    ServerConfig,
    DatabaseConfig,
    TraceFlags,
    LongQueryCompletions,

    WaitingTasks,
    MemoryPressureEvents,

    /* The Job History tab (#4966): the run history copied from msdb. */
    JobHistory
}

public partial class LocalDataService
{
    internal static string QueryWindowRelationView(QueryWindowRelation relation) => relation switch
    {
        QueryWindowRelation.QueryStats => "v_query_stats",
        // Group A (Queries tab: Plan Corrections)
        QueryWindowRelation.PlanCorrection => "v_plan_correction",
        QueryWindowRelation.ProcedureStats => "v_procedure_stats",
        QueryWindowRelation.QueryStoreStats => "v_query_store_stats",
        // Group B (#4966): the Blocking tab's Blocked Process Reports and Deadlocks grids.
        QueryWindowRelation.BlockedProcessReports => "v_blocked_process_reports",
        QueryWindowRelation.Deadlocks => "v_deadlocks",
        QueryWindowRelation.DmvBlockingSnapshots => "v_dmv_blocking_snapshots",
        QueryWindowRelation.QuerySnapshots => "v_query_snapshots",
        /* Group C of #4966: Collection Health, System Events, Config Changes and Long Queries. */
        QueryWindowRelation.CollectionLog => "v_collection_log",
        QueryWindowRelation.SystemHealthEvents => "v_system_health_events",
        QueryWindowRelation.DefaultTraceEvents => "v_default_trace_events",
        QueryWindowRelation.ServerConfig => "v_server_config",
        QueryWindowRelation.DatabaseConfig => "v_database_config",
        QueryWindowRelation.TraceFlags => "v_trace_flags",
        QueryWindowRelation.LongQueryCompletions => "v_long_query_completions",
        QueryWindowRelation.WaitingTasks => "v_waiting_tasks",
        QueryWindowRelation.MemoryPressureEvents => "v_memory_pressure_events",
        QueryWindowRelation.JobHistory => "v_job_history",
        _ => throw new ArgumentOutOfRangeException(nameof(relation), relation, "unknown QueryWindowRelation")
    };

    /// <summary>
    /// The collector whose runs <c>collection_log</c> records for a relation the probe reads by coverage
    /// (<see cref="QueryWindowRelation.QuerySnapshots"/>, <see cref="QueryWindowRelation.WaitingTasks"/>,
    /// <see cref="QueryWindowRelation.PlanCorrection"/>, <see cref="QueryWindowRelation.MemoryPressureEvents"/>, and #4966's
    /// System Events, Config Changes, Long Queries and Job History relations), or null for the three Queries-tab relations (the Query
    /// Heatmap reads the first of them) and <see cref="QueryWindowRelation.CollectionLog"/>, which keep the row-only
    /// probe. A closed map, so nothing a caller passes reaches the probe's SQL.
    /// </summary>
    internal static string? QueryWindowRelationCollector(QueryWindowRelation relation) => relation switch
    {
        // Group A (Queries tab: Plan Corrections)
        QueryWindowRelation.PlanCorrection => "plan_correction",
        QueryWindowRelation.QuerySnapshots => "query_snapshots",
        // Group B (#4966): the Blocking tab's Blocked Process Reports and Deadlocks grids.
        QueryWindowRelation.BlockedProcessReports => "blocked_process_report",
        QueryWindowRelation.Deadlocks => "deadlocks",
        QueryWindowRelation.DmvBlockingSnapshots => "dmv_blocking_snapshot",
        QueryWindowRelation.WaitingTasks => "waiting_tasks",
        QueryWindowRelation.MemoryPressureEvents => "memory_pressure_events",
        /* Group C of #4966. Every event and snapshot table below holds a row only when something happened or changed
           (system_health and default trace events, long-query completions, and the config snapshots, which the
           collectors take at load), so a collector's runs are what show the store covered a quiet start.
           CollectionLog is the run log itself, one dense row per run, so it keeps the row-only probe. */
        QueryWindowRelation.SystemHealthEvents => "system_health_events",
        QueryWindowRelation.DefaultTraceEvents => "default_trace_events",
        QueryWindowRelation.ServerConfig => "server_config",
        QueryWindowRelation.DatabaseConfig => "database_config",
        QueryWindowRelation.TraceFlags => "trace_flags",
        QueryWindowRelation.LongQueryCompletions => "long_query_completions",
        /* The Job History tab (#4966). job_history holds a row only when an Agent job ran, and the first collection copies the
           history msdb already holds, so a quiet start is covered by the collector's runs, not by a row near the window's start. */
        QueryWindowRelation.JobHistory => "job_history",
        _ => null
    };

    /// <summary>
    /// The time column of a relation's view that the probe compares against the window (#4966): the column the surface's
    /// grid filters on, so a banner never names a time later than the earliest row its grid shows. That is
    /// <c>collection_time</c> for most relations, <c>capture_time</c> for the three config snapshots (their collectors stamp
    /// it instead, <c>ICollectorSchemaInfo.PrefixTimeColumnName</c>, and the archive purges by it), and <c>event_time</c>
    /// for the event relations (#4989): the System Events grids that read system_health events, the Default Trace grid and
    /// the Blocked Process Reports grid's XE reports filter on the event's own time, not on the time a run stored it. The
    /// Deadlocks grid filters on <c>deadlock_time</c> in the same way (#4966). Both are the XE <c>@timestamp</c>, UTC, so
    /// neither is a server-clock relation (<see cref="QueryWindowRelationTimeIsServerLocal"/> stays false for them), a
    /// first run of either collector stores only <see cref="PerformanceMonitor.Collectors.CollectorContext.EventFallbackWindow"/>
    /// of history, and the archive still purges both tables by <c>collection_time</c>. The always-on DMV blocking
    /// snapshots' <c>event_time</c> is their own <c>collection_time</c>, so that relation keeps <c>collection_time</c>.
    /// A server's first run can store events from
    /// before itself, every row stamped with that run's <c>collection_time</c> while its <c>event_time</c> is older, and the
    /// probe reading <c>collection_time</c> there named the run above rows from before it. How far back depends on the
    /// collector: the Default Trace's first run stores the history the trace files already hold, which can go back days,
    /// while a first run of the system_health collector (and of Long Queries) reads back only
    /// <see cref="PerformanceMonitor.Collectors.CollectorContext.EventFallbackWindow"/>, 10 minutes. Long Queries stays on
    /// <c>collection_time</c>, the column its grid filters on; the grid's notice also takes the oldest completion it shows
    /// into account (<c>ServerTab.EarlierOfFloorAndRowShown</c>). The archive still purges those two tables by
    /// <c>collection_time</c>. The system_health <c>event_time</c> is the XE <c>@timestamp</c>,
    /// UTC, like every other column here. The Default Trace's <c>event_time</c> is the exception: it is the monitored server's
    /// wall clock as stored (its grid converts each row through the server's clock), so the probe does the same
    /// (<see cref="QueryWindowRelationTimeIsServerLocal"/>, #4989): it reads that relation's rows over the grid's padded
    /// server-local window, converts each one to UTC through the clock, and compares the converted times with the UTC
    /// window. Memory Pressure Events windows on <c>sample_time</c> (the ring-buffer event's own time, which can sit long
    /// before the first collection that stored it), so the probe asks the question the chart's own read asks. A closed map,
    /// so nothing a caller passes reaches the probe's SQL. The collector's own runs in <c>v_collection_log</c> always read
    /// <c>collection_time</c>, which is UTC.
    /// </summary>
    internal static string QueryWindowRelationTimeColumn(QueryWindowRelation relation) => relation switch
    {
        QueryWindowRelation.ServerConfig or QueryWindowRelation.DatabaseConfig or QueryWindowRelation.TraceFlags => "capture_time",
        QueryWindowRelation.SystemHealthEvents or QueryWindowRelation.DefaultTraceEvents or QueryWindowRelation.BlockedProcessReports => "event_time",
        QueryWindowRelation.Deadlocks => "deadlock_time",
        QueryWindowRelation.MemoryPressureEvents => "sample_time",
        _ => "collection_time"
    };

    /// <summary>
    /// The second relation whose coverage also feeds a grid (#4966), or null for every grid fed by one collector. Only the
    /// Blocked Process Reports grid has one: it lists the XE collector's reports and, beside them, the always-on DMV blocking
    /// snapshots (<see cref="GetRecentBlockedProcessReportsAsync"/>), which stand in for the XE reports where no blocked process
    /// threshold is set. The probe answers the EARLIER of the two collectors' coverage
    /// (<see cref="EarlierCoverageFloor"/>), so with XE collection off a range the DMV collector covers gets no notice, and one that
    /// starts before the DMV collector's coverage names it. The same rule as the Darling viewer's Blocked Process Reports grid.
    /// A closed map, so nothing a caller passes reaches the probe's SQL.
    /// </summary>
    internal static QueryWindowRelation? QueryWindowRelationAlsoCoveredBy(QueryWindowRelation relation) =>
        relation == QueryWindowRelation.BlockedProcessReports ? QueryWindowRelation.DmvBlockingSnapshots : null;

    /// <summary>The earlier of two probe answers, a null (nothing to report in the window) giving way to the other; null when both are.</summary>
    internal static DateTime? EarlierCoverageFloor(DateTime? first, DateTime? second) =>
        first is DateTime a && second is DateTime b ? (a <= b ? a : b) : first ?? second;

    /// <summary>
    /// Whether the relation's time column (<see cref="QueryWindowRelationTimeColumn"/>) holds the monitored server's LOCAL
    /// wall-clock time instead of UTC (#4989). Only the Default Trace does: <c>ft.StartTime</c> is stored as the trace wrote
    /// it, and <see cref="GetDefaultTraceEventsAsync"/> converts each row to UTC through the server's clock. Every other
    /// relation's column is UTC, system_health's included (the XE <c>@timestamp</c>), and keeps the probe's single-query path.
    /// </summary>
    internal static bool QueryWindowRelationTimeIsServerLocal(QueryWindowRelation relation) =>
        relation == QueryWindowRelation.DefaultTraceEvents;

    /// <summary>
    /// Where this server's data starts for the requested window, as far as any caller needs to know it. NULL when
    /// the server holds no row inside [<paramref name="startUtc"/>, <paramref name="endUtc"/>] at all (nothing was
    /// read; for the coverage surfaces, Active Queries, Current Waits, Plan Corrections and Memory Pressure Events, no
    /// row and no logged run of the collector either).
    /// <paramref name="startUtc"/> itself when the server also holds a row BEFORE the window (the window was
    /// served whole). Otherwise the server's first row inside the window (the data starts late). ONE probe shared by
    /// every Queries-tab grid (<c>Lite/Controls/ServerTab.*</c>), the Active Queries, Current Waits, Plan Corrections,
    /// Query Heatmap and Memory Pressure Events banners and every MCP tool that reads one of the <see cref="QueryWindowRelation"/> relations
    /// (<c>get_top_queries_by_cpu</c>, <c>get_top_procedures_by_cpu</c>, <c>get_query_store_top</c>), so there is
    /// one place that computes the floor rather than near-identical copies that can drift apart. Twin of Darling's
    /// <c>DarlingDataReader.GetQueryStoreWindowFloorAsync</c> (#2364) and of #4953's <c>DataWindowFloor</c>: Lite's
    /// <c>retention_days</c> is user-settable (lower it and the raw table is shorter than a "Last 7 days" ask), a
    /// custom range can start before the archive's oldest month
    /// (<see cref="RetentionService.ArchiveRetentionMonths"/>), and a server added mid-window has less history than
    /// the window asks for regardless of the setting. The silent cut #2364 fixed on Darling's
    /// <c>query_store_stats</c> can happen on any of these relations.
    ///
    /// <para>Reads the <c>v_</c> view, not the bare table: on a store with archived history that view is the
    /// hot table UNION ALL the parquet archive (<see cref="Database.DuckDbInitializer.CreateArchiveViewsAsync"/>),
    /// so a row found here is a row of everything the matching grid or tool read, archive included, never just
    /// the hot table's.</para>
    ///
    /// <para><b>Why it is not the oldest row.</b> Finding the server's oldest row means an unbounded
    /// <c>MIN(collection_time)</c>, and DuckDB has no <c>(server_id, collection_time)</c> index to stop at the first
    /// row, so it reads every archived row for the server. No caller needs it: a caller compares the result with the
    /// window's start (<see cref="Mcp.McpQueryTools.IsWindowTruncated"/>) and, to word the window it covered,
    /// clamps it (<see cref="Mcp.McpQueryTools.EffectiveWindowStart"/>), and any floor at or before the start gives
    /// the same output. Whether the data reaches back to the start is the whole question, and the first row inside
    /// the window alone cannot answer it on a sparse table (waiting_tasks holds a row only while something waits: a
    /// quiet first stretch puts the window's own first row far past its start while older rows are stored, and a
    /// probe that read only the window would raise a false banner).</para>
    ///
    /// <para><b>Active Queries, Current Waits, Plan Corrections and Memory Pressure Events read coverage, not
    /// rows.</b> query_snapshots and waiting_tasks hold a row only while something runs or waits, plan_correction only
    /// while the engine has a recommendation, and memory_pressure_events only where the ring buffer logged an event
    /// (windowed on <c>sample_time</c>, see <see cref="QueryWindowRelationTimeColumn"/>), so a server idle overnight, or
    /// one whose first waiting task came days after it was added, has no row near the window's start though the store
    /// covered it. For these four the collector's runs in <c>v_collection_log</c> count as well as rows: a run proves the collector was collecting
    /// then. The log survives exactly as long as the table: it is archived and deleted by the same single horizon
    /// (<see cref="RetentionService.ArchiveRetentionMonths"/>, the same monthly files), so a run older than the
    /// window means the table's rows from then on are still held, and the first run inside the window is where
    /// coverage starts, whether that is the server's first collection or the retention edge, whichever is later.
    /// For these four the probe answers NULL when the window holds no row and no run, the window's start when a
    /// row or run sits at or before it, and otherwise the first row or run inside the window, whichever is
    /// earlier. A covered window that holds no row (the collector ran, nothing waited) answers the start, so it
    /// shows no banner.</para>
    ///
    /// <para><b>The two steps.</b> The first is bounded on both sides, so DuckDB's zone-map statistics prune every
    /// row group and parquet file outside the window; it ends the probe when the window holds no row (NULL, never
    /// an old row or the start, either of which would read as the window having been served) or when its first row
    /// is already at the start. Only when that first row sits after the start does a second query ask whether the
    /// server holds ANY row before the start, <c>LIMIT 1</c>, which stops at the first hit.</para>
    ///
    /// <para><b>A time column in the server's wall clock (the Default Trace, #4989).</b> The Default Trace's <c>event_time</c>
    /// is the monitored server's local time as stored, and its grid (<see cref="GetDefaultTraceEventsAsync"/>) converts each
    /// row to UTC through the server's clock, so the probe does too, through <paramref name="serverClock"/>: the server's OWN
    /// clock (#4766), or <see cref="ServerTimeHelper.ActiveServerClock"/> when it is <c>null</c>, which is what that grid
    /// falls back to. For that relation the two steps above read the candidate rows in the grid's padded server-local window,
    /// convert each to UTC with the offset at the row's own date (so the answer is exact across a daylight-saving change), and
    /// take the earlier of the first row and the collector's first run, which is UTC, in C#
    /// (<c>GetServerLocalQueryWindowFloorAsync</c>). Every UTC relation, system_health's included (the XE <c>@timestamp</c>),
    /// keeps the single-query path and ignores the clock.</para>
    ///
    /// <para><b>A grid fed by two collectors (the Blocked Process Reports grid, #4966).</b> The grid lists the XE collector's
    /// reports and, beside them, the always-on DMV blocking snapshots, so for it the answer is the earlier of the two
    /// collectors' (<see cref="QueryWindowRelationAlsoCoveredBy"/>, <see cref="EarlierCoverageFloor"/>): a null answer from
    /// one (no row and no logged run of that collector in the window) gives way to the other's.</para>
    /// </summary>
    public async Task<DateTime?> GetQueryWindowFloorAsync(QueryWindowRelation relation, int serverId, DateTime startUtc, DateTime endUtc, ServerClock? serverClock = null)
    {
        var own = await GetOwnQueryWindowFloorAsync(relation, serverId, startUtc, endUtc, serverClock);
        return QueryWindowRelationAlsoCoveredBy(relation) is QueryWindowRelation also
            ? EarlierCoverageFloor(own, await GetOwnQueryWindowFloorAsync(also, serverId, startUtc, endUtc, serverClock))
            : own;
    }

    /// <summary>One relation's own probe, which <see cref="GetQueryWindowFloorAsync"/> documents. Each call opens and releases its
    /// own connection, so the two answers of a grid fed by two collectors never nest the non-recursive read lock.</summary>
    private async Task<DateTime?> GetOwnQueryWindowFloorAsync(QueryWindowRelation relation, int serverId, DateTime startUtc, DateTime endUtc, ServerClock? serverClock)
    {
        var view = QueryWindowRelationView(relation);
        using var _q = TimeQuery("GetQueryWindowFloorAsync", $"{view} window floor");
        using var connection = await OpenConnectionAsync();

        var collector = QueryWindowRelationCollector(relation);
        var timeColumn = QueryWindowRelationTimeColumn(relation);

        /* #4989: a time column that holds the server's wall clock (the Default Trace) goes through the clock, the way the
           grid reads it. Every UTC relation keeps the single-query path below. */
        if (QueryWindowRelationTimeIsServerLocal(relation))
        {
            return await GetServerLocalQueryWindowFloorAsync(
                connection, view, timeColumn, collector, serverId, startUtc, endUtc, serverClock ?? ServerTimeHelper.ActiveServerClock);
        }

        DateTime? firstInWindow;
        using (var windowCommand = connection.CreateCommand())
        {
            /* Coverage relations take the earlier of the first row and the collector's first run in the window.
               LEAST skips a NULL, so either one alone answers. */
            windowCommand.CommandText = collector is null
                ? $@"
SELECT MIN({timeColumn})
FROM {view}
WHERE server_id = $1
AND   {timeColumn} >= $2
AND   {timeColumn} <= $3"
                : $@"
SELECT LEAST(
    (SELECT MIN({timeColumn}) FROM {view} WHERE server_id = $1 AND {timeColumn} >= $2 AND {timeColumn} <= $3),
    (SELECT MIN(collection_time) FROM v_collection_log WHERE server_id = $1 AND collector_name = '{collector}' AND collection_time >= $2 AND collection_time <= $3))";
            windowCommand.Parameters.Add(new DuckDBParameter { Value = serverId });
            windowCommand.Parameters.Add(new DuckDBParameter { Value = startUtc });
            windowCommand.Parameters.Add(new DuckDBParameter { Value = endUtc });
            firstInWindow = await windowCommand.ExecuteScalarAsync() is DateTime first ? first : null;
        }

        if (firstInWindow is not DateTime firstRow || firstRow <= startUtc)
        {
            return firstInWindow;
        }

        using (var olderCommand = connection.CreateCommand())
        {
            olderCommand.CommandText = $@"
SELECT 1
FROM {view}
WHERE server_id = $1
AND   {timeColumn} < $2
LIMIT 1";
            olderCommand.Parameters.Add(new DuckDBParameter { Value = serverId });
            olderCommand.Parameters.Add(new DuckDBParameter { Value = startUtc });
            if (await olderCommand.ExecuteScalarAsync() is not null)
            {
                return startUtc;
            }
        }

        if (collector is null)
        {
            return firstRow;
        }

        /* No older row: an older run of the collector still proves the window is covered. */
        return await HasCollectorRunBeforeAsync(connection, collector, serverId, startUtc) ? startUtc : firstRow;
    }

    /// <summary>Whether the collector's runs in <c>v_collection_log</c> (<c>collection_time</c>, UTC) include one before
    /// <paramref name="startUtc"/>.</summary>
    private static async Task<bool> HasCollectorRunBeforeAsync(LockedConnection connection, string collector, int serverId, DateTime startUtc)
    {
        using var olderRunCommand = connection.CreateCommand();
        olderRunCommand.CommandText = $@"
SELECT 1
FROM v_collection_log
WHERE server_id = $1
AND   collector_name = '{collector}'
AND   collection_time < $2
LIMIT 1";
        olderRunCommand.Parameters.Add(new DuckDBParameter { Value = serverId });
        olderRunCommand.Parameters.Add(new DuckDBParameter { Value = startUtc });
        return await olderRunCommand.ExecuteScalarAsync() is not null;
    }

    /// <summary>
    /// <see cref="GetQueryWindowFloorAsync"/> for a relation whose time column is the monitored server's wall clock (#4989:
    /// the Default Trace's <c>event_time</c>). It answers what the UTC path answers, from the rows the grid shows
    /// (<see cref="GetDefaultTraceEventsAsync"/>), so a banner never names a time later than the earliest row its grid shows.
    /// <list type="number">
    /// <item>The candidate rows are read over the grid's pre-filter: the server-local bounds of the UTC window, an hour wider
    /// on each side, which covers a daylight-saving change inside the span. Each row is converted to UTC through
    /// <paramref name="clock"/>, which takes the offset in force at the row's own date (#4766), and the converted times give
    /// the earliest row inside [<paramref name="startUtc"/>, <paramref name="endUtc"/>] and whether a row sits before the
    /// start. The scan reads the one column over a span the grid reads whole.</item>
    /// <item>The collector's runs in <c>v_collection_log</c> are UTC, so they are not converted: the earlier of the first run
    /// and the first row is taken here, in C#, not by one SQL <c>LEAST</c> over a wall-clock column and a UTC one.</item>
    /// <item>When that first time is after the start, the server holds an older row if the scan found one that converts to a
    /// time before the start, or if any row sits below the pre-filter's lower bound (more than an hour before the start at
    /// any offset the clock holds, so it needs no conversion). Then an older run of the collector counts, as on the UTC path.</item>
    /// </list>
    /// </summary>
    private static async Task<DateTime?> GetServerLocalQueryWindowFloorAsync(
        LockedConnection connection, string view, string timeColumn, string? collector, int serverId,
        DateTime startUtc, DateTime endUtc, ServerClock clock)
    {
        /* The grid's pre-filter, bound for bound (GetDefaultTraceEventsAsync). */
        var padStart = clock.ToServerLocal(startUtc).AddHours(-1);
        var padEnd = clock.ToServerLocal(endUtc).AddHours(1);

        DateTime? firstRow = null;
        var olderRowInPad = false;
        using (var rowsCommand = connection.CreateCommand())
        {
            rowsCommand.CommandText = $@"
SELECT {timeColumn}
FROM {view}
WHERE server_id = $1
AND   {timeColumn} >= $2
AND   {timeColumn} <= $3";
            rowsCommand.Parameters.Add(new DuckDBParameter { Value = serverId });
            rowsCommand.Parameters.Add(new DuckDBParameter { Value = padStart });
            rowsCommand.Parameters.Add(new DuckDBParameter { Value = padEnd });
            using var reader = await rowsCommand.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var eventUtc = clock.ToUtc(reader.GetDateTime(0));
                if (eventUtc < startUtc)
                {
                    olderRowInPad = true;
                }
                else if (eventUtc <= endUtc && (firstRow is null || eventUtc < firstRow))
                {
                    firstRow = eventUtc;
                }
            }
        }

        DateTime? firstRun = null;
        if (collector is not null)
        {
            using var runCommand = connection.CreateCommand();
            runCommand.CommandText = $@"
SELECT MIN(collection_time)
FROM v_collection_log
WHERE server_id = $1
AND   collector_name = '{collector}'
AND   collection_time >= $2
AND   collection_time <= $3";
            runCommand.Parameters.Add(new DuckDBParameter { Value = serverId });
            runCommand.Parameters.Add(new DuckDBParameter { Value = startUtc });
            runCommand.Parameters.Add(new DuckDBParameter { Value = endUtc });
            firstRun = await runCommand.ExecuteScalarAsync() is DateTime run ? run : null;
        }

        var first = firstRow;
        if (firstRun is DateTime firstRunUtc && (first is null || firstRunUtc < first))
        {
            first = firstRunUtc;
        }

        /* As the UTC path: nothing in the window answers NULL, and a first time at the start is the answer. */
        if (first is not DateTime firstInWindow || firstInWindow <= startUtc)
        {
            return first;
        }

        if (olderRowInPad)
        {
            return startUtc;
        }

        using (var olderCommand = connection.CreateCommand())
        {
            olderCommand.CommandText = $@"
SELECT 1
FROM {view}
WHERE server_id = $1
AND   {timeColumn} < $2
LIMIT 1";
            olderCommand.Parameters.Add(new DuckDBParameter { Value = serverId });
            olderCommand.Parameters.Add(new DuckDBParameter { Value = padStart });
            if (await olderCommand.ExecuteScalarAsync() is not null)
            {
                return startUtc;
            }
        }

        return collector is not null && await HasCollectorRunBeforeAsync(connection, collector, serverId, startUtc)
            ? startUtc
            : firstInWindow;
    }
}
