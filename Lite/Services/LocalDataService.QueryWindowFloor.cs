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
    /// Where this server's data starts for the requested window: the oldest <c>collection_time</c> the server holds
    /// at or before <paramref name="endUtc"/>, UNBOUNDED BELOW, and NULL when the server has no row inside
    /// [<paramref name="startUtc"/>, <paramref name="endUtc"/>] at all. ONE probe shared by every Queries-tab grid
    /// (<c>Lite/Controls/ServerTab.*</c>), the Active Queries and Current Waits banners and every MCP tool that
    /// reads one of the <see cref="QueryWindowRelation"/> relations (<c>get_top_queries_by_cpu</c>,
    /// <c>get_top_procedures_by_cpu</c>, <c>get_query_store_top</c>), so there is one place that computes the
    /// floor rather than near-identical copies that can drift apart. Twin of Darling's
    /// <c>DarlingDataReader.GetQueryStoreWindowFloorAsync</c> (#2364) and of #4953's <c>DataWindowFloor</c>: Lite's
    /// <c>retention_days</c> is user-settable (lower it and the raw table is shorter than a "Last 7 days" ask), a
    /// custom range can start before the archive's oldest month
    /// (<see cref="RetentionService.ArchiveRetentionMonths"/>), and a server added mid-window has less history than
    /// the window asks for regardless of the setting. The silent cut #2364 fixed on Darling's
    /// <c>query_store_stats</c> can happen on any of these relations.
    ///
    /// <para>Reads the <c>v_</c> view, not the bare table: on a store with archived history that view is the
    /// hot table UNION ALL the parquet archive (<see cref="Database.DuckDbInitializer.CreateArchiveViewsAsync"/>),
    /// so the floor this returns is the true floor of everything the matching grid or tool read, archive
    /// included, never just the hot table's.</para>
    ///
    /// <para><b>Why unbounded below.</b> The oldest row INSIDE the window is right only for a dense table. On a
    /// sparse one (waiting_tasks holds a row only while something waits) a quiet first stretch puts the window's own
    /// oldest row far past its start while the store still holds older rows for the same server, so nothing is
    /// missing, and a probe bounded below at the window's start raises a false banner. The oldest row at or before
    /// the window's end answers "does the data reach back to the start" for dense and sparse tables alike. A caller
    /// compares the result with the window's start (<see cref="Mcp.McpQueryTools.IsWindowTruncated"/>) and, to word
    /// the window it covered, clamps it (<see cref="Mcp.McpQueryTools.EffectiveWindowStart"/>): a floor before the
    /// start means the window's start is covered, never that the window began earlier.</para>
    ///
    /// <para><b>Why NULL first.</b> NULL keeps its old meaning, "the window holds nothing, so nothing was read".
    /// A server whose rows all end before the window would otherwise answer its oldest row, which reads as the whole
    /// window having been served. The existence check is bounded on both sides, so DuckDB's zone-map statistics
    /// prune every row group and parquet file outside the window and <c>LIMIT 1</c> stops at the first hit; the
    /// unbounded MIN runs only for a server that has a row in the window. Measured on a seeded store (three monthly
    /// parquet files plus a 7-day hot table, 40 servers, 29M rows) the pair answers in tens of milliseconds, about
    /// 60 to 75 ms for a server with 90 days of rows and under 25 ms for a new or retired one; the numbers are in
    /// the pull request that added it.</para>
    /// </summary>
    public async Task<DateTime?> GetQueryWindowFloorAsync(QueryWindowRelation relation, int serverId, DateTime startUtc, DateTime endUtc)
    {
        var view = QueryWindowRelationView(relation);
        using var _q = TimeQuery("GetQueryWindowFloorAsync", $"{view} MIN(collection_time) window floor");
        using var connection = await OpenConnectionAsync();

        using (var existsCommand = connection.CreateCommand())
        {
            existsCommand.CommandText = $@"
SELECT 1
FROM {view}
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
LIMIT 1";
            existsCommand.Parameters.Add(new DuckDBParameter { Value = serverId });
            existsCommand.Parameters.Add(new DuckDBParameter { Value = startUtc });
            existsCommand.Parameters.Add(new DuckDBParameter { Value = endUtc });
            if (await existsCommand.ExecuteScalarAsync() is null)
            {
                return null;
            }
        }

        using var command = connection.CreateCommand();
        command.CommandText = $@"
SELECT MIN(collection_time)
FROM {view}
WHERE server_id = $1
AND   collection_time <= $2";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });
        var result = await command.ExecuteScalarAsync();
        return result is DateTime dt ? dt : null;
    }
}
