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
/// closed member that maps to ONE <c>v_</c> view with <c>server_id</c> and <c>collection_time</c> columns, so no
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
        QueryWindowRelation.WaitingTasks => "v_waiting_tasks",
        _ => throw new ArgumentOutOfRangeException(nameof(relation), relation, "unknown QueryWindowRelation")
    };

    /// <summary>
    /// Where this server's data starts for the requested window, as far as any caller needs to know it. NULL when
    /// the server holds no row inside [<paramref name="startUtc"/>, <paramref name="endUtc"/>] at all (nothing was
    /// read). <paramref name="startUtc"/> itself when the server also holds a row BEFORE the window (the window was
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

        DateTime? firstInWindow;
        using (var windowCommand = connection.CreateCommand())
        {
            windowCommand.CommandText = $@"
SELECT MIN(collection_time)
FROM {view}
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";
            windowCommand.Parameters.Add(new DuckDBParameter { Value = serverId });
            windowCommand.Parameters.Add(new DuckDBParameter { Value = startUtc });
            windowCommand.Parameters.Add(new DuckDBParameter { Value = endUtc });
            firstInWindow = await windowCommand.ExecuteScalarAsync() is DateTime first ? first : null;
        }

        if (firstInWindow is not DateTime firstRow || firstRow <= startUtc)
        {
            return firstInWindow;
        }

        using var olderCommand = connection.CreateCommand();
        olderCommand.CommandText = $@"
SELECT 1
FROM {view}
WHERE server_id = $1
AND   collection_time < $2
LIMIT 1";
        olderCommand.Parameters.Add(new DuckDBParameter { Value = serverId });
        olderCommand.Parameters.Add(new DuckDBParameter { Value = startUtc });
        return await olderCommand.ExecuteScalarAsync() is null ? firstRow : startUtc;
    }
}
