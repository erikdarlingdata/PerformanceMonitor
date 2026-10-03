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
/// Waits data-start banners adds <see cref="QuerySnapshots"/> and <see cref="WaitingTasks"/>. A further surface adds
/// its own member and its arm in <c>QueryWindowRelationView</c>; <c>QueryWindowTruncationTests</c> pins that every
/// member names a real archive view.
/// </summary>
public enum QueryWindowRelation
{
    QueryStats,
    ProcedureStats,
    QueryStoreStats,
    QuerySnapshots,

    /* Group C of #4966: Collection Health, System Events, Config Changes and Long Queries. */
    CollectionLog,
    SystemHealthEvents,
    DefaultTraceEvents,
    ServerConfig,
    DatabaseConfig,
    TraceFlags,
    LongQueryCompletions,

    WaitingTasks
}

public partial class LocalDataService
{
    internal static string QueryWindowRelationView(QueryWindowRelation relation) => relation switch
    {
        QueryWindowRelation.QueryStats => "v_query_stats",
        QueryWindowRelation.ProcedureStats => "v_procedure_stats",
        QueryWindowRelation.QueryStoreStats => "v_query_store_stats",
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
        _ => throw new ArgumentOutOfRangeException(nameof(relation), relation, "unknown QueryWindowRelation")
    };

    /// <summary>
    /// The collector whose runs <c>collection_log</c> records for a relation the probe reads by coverage
    /// (<see cref="QueryWindowRelation.QuerySnapshots"/>, <see cref="QueryWindowRelation.WaitingTasks"/>, and #4966's
    /// System Events, Config Changes and Long Queries relations), or null for the three Queries-tab relations and
    /// <see cref="QueryWindowRelation.CollectionLog"/>, which keep the row-only probe. A closed map, so nothing a
    /// caller passes reaches the probe's SQL.
    /// </summary>
    internal static string? QueryWindowRelationCollector(QueryWindowRelation relation) => relation switch
    {
        QueryWindowRelation.QuerySnapshots => "query_snapshots",
        QueryWindowRelation.WaitingTasks => "waiting_tasks",
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
        _ => null
    };

    /// <summary>
    /// The time column of a relation's view that the probe compares against the window (#4966): the column the surface's
    /// grid filters on, so a banner never names a time later than the earliest row its grid shows. That is
    /// <c>collection_time</c> for most relations, <c>capture_time</c> for the three config snapshots (their collectors stamp
    /// it instead, <c>ICollectorSchemaInfo.PrefixTimeColumnName</c>, and the archive purges by it), and <c>event_time</c>
    /// for the two event relations (#4989): the System Events grids that read system_health events and the Default Trace
    /// grid filter on the event's own time, not on the time a run stored it. A server's first run stores the history the
    /// server already holds, every row stamped with that run's <c>collection_time</c> while its <c>event_time</c> goes back
    /// days, and the probe reading <c>collection_time</c> there named the run above rows from before it. The archive still
    /// purges those two tables by <c>collection_time</c>. The Default Trace's <c>event_time</c> is the monitored server's
    /// wall clock as stored (its grid converts each row through the server's clock), so on a server whose clock is not UTC
    /// the probe compares that wall time with the UTC window as it is. A closed map, so nothing a caller passes reaches the
    /// probe's SQL. The collector's own runs in <c>v_collection_log</c> always read <c>collection_time</c>.
    /// </summary>
    internal static string QueryWindowRelationTimeColumn(QueryWindowRelation relation) => relation switch
    {
        QueryWindowRelation.ServerConfig or QueryWindowRelation.DatabaseConfig or QueryWindowRelation.TraceFlags => "capture_time",
        QueryWindowRelation.SystemHealthEvents or QueryWindowRelation.DefaultTraceEvents => "event_time",
        _ => "collection_time"
    };

    /// <summary>
    /// Where this server's data starts for the requested window, as far as any caller needs to know it. NULL when
    /// the server holds no row inside [<paramref name="startUtc"/>, <paramref name="endUtc"/>] at all (nothing was
    /// read; for Active Queries and Current Waits, no row and no logged run of the collector either).
    /// <paramref name="startUtc"/> itself when the server also holds a row BEFORE the window (the window was
    /// served whole). Otherwise the server's first row inside the window (the data starts late). ONE probe shared by
    /// every Queries-tab grid (<c>Lite/Controls/ServerTab.*</c>), the Active Queries and Current Waits banners and
    /// every MCP tool that reads one of the <see cref="QueryWindowRelation"/> relations
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
    /// <para><b>Active Queries and Current Waits read coverage, not rows.</b> query_snapshots and waiting_tasks hold
    /// a row only while something runs or waits, so a server idle overnight, or one whose first waiting task came
    /// days after it was added, has no row near the window's start though the store covered it. For these two the
    /// collector's runs in <c>v_collection_log</c> count as well as rows: a run proves the collector was collecting
    /// then. The log survives exactly as long as the table: it is archived and deleted by the same single horizon
    /// (<see cref="RetentionService.ArchiveRetentionMonths"/>, the same monthly files), so a run older than the
    /// window means the table's rows from then on are still held, and the first run inside the window is where
    /// coverage starts, whether that is the server's first collection or the retention edge, whichever is later.
    /// For these two the probe answers NULL when the window holds no row and no run, the window's start when a
    /// row or run sits at or before it, and otherwise the first row or run inside the window, whichever is
    /// earlier. A covered window that holds no row (the collector ran, nothing waited) answers the start, so it
    /// shows no banner.</para>
    ///
    /// <para><b>The two steps.</b> The first is bounded on both sides, so DuckDB's zone-map statistics prune every
    /// row group and parquet file outside the window; it ends the probe when the window holds no row (NULL, never
    /// an old row or the start, either of which would read as the window having been served) or when its first row
    /// is already at the start. Only when that first row sits after the start does a second query ask whether the
    /// server holds ANY row before the start, <c>LIMIT 1</c>, which stops at the first hit.</para>
    /// </summary>
    public async Task<DateTime?> GetQueryWindowFloorAsync(QueryWindowRelation relation, int serverId, DateTime startUtc, DateTime endUtc)
    {
        var view = QueryWindowRelationView(relation);
        using var _q = TimeQuery("GetQueryWindowFloorAsync", $"{view} window floor");
        using var connection = await OpenConnectionAsync();

        var collector = QueryWindowRelationCollector(relation);
        var timeColumn = QueryWindowRelationTimeColumn(relation);
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
        return await olderRunCommand.ExecuteScalarAsync() is null ? firstRow : startUtc;
    }
}
