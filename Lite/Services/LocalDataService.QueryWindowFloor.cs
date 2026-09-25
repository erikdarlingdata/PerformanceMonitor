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
/// The three RAW-ONLY relations #4231's window-floor disclosure covers: every Queries-tab grid and MCP tool
/// that reads one of these has no rollup underneath it to fall back on when the requested window reaches past
/// what the raw table actually retains.
/// </summary>
public enum QueryWindowRelation
{
    QueryStats,
    ProcedureStats,
    QueryStoreStats
}

public partial class LocalDataService
{
    private static string QueryWindowRelationView(QueryWindowRelation relation) => relation switch
    {
        QueryWindowRelation.QueryStats => "v_query_stats",
        QueryWindowRelation.ProcedureStats => "v_procedure_stats",
        QueryWindowRelation.QueryStoreStats => "v_query_store_stats",
        _ => throw new ArgumentOutOfRangeException(nameof(relation), relation, "unknown QueryWindowRelation")
    };

    /// <summary>
    /// #4231: the oldest <c>collection_time</c> this server actually has inside the requested window, for
    /// whichever of the three raw relations the caller names. ONE probe shared by every Queries-tab grid
    /// (<c>Lite/Controls/ServerTab.*</c>) and every MCP tool that reads that relation (<c>get_top_queries_by_cpu</c>,
    /// <c>get_top_procedures_by_cpu</c>, <c>get_query_store_top</c>), so there is one place that computes the
    /// floor rather than three near-identical copies that can drift apart. Twin of Darling's
    /// <c>DarlingDataReader.GetQueryStoreWindowFloorAsync</c> (#2364), generalized to Lite's three raw tables:
    /// Lite's default 30-day <c>retention_days</c> is per-collector and user-settable (lower it and the raw
    /// table is shorter than a "Last 7 days" ask), and a server added mid-window has less history than that
    /// regardless of the setting — the same silent cut #2364 fixed on Darling's <c>query_store_stats</c> can
    /// happen on any of Lite's three (#4231).
    ///
    /// <para>Reads the <c>v_</c> view, not the bare table: on a store with archived history that view is the
    /// hot table UNION ALL the parquet archive (<see cref="Database.DuckDbInitializer.CreateArchiveViewsAsync"/>),
    /// so the floor this returns is the true floor of everything the matching grid or tool read, archive
    /// included — never just the hot table's.</para>
    ///
    /// <para>Bounded on both sides of the window. An unbounded <c>MIN</c> would scan back to the start of
    /// retention to answer a question about the window's edge; the upper bound is what lets DuckDB's zone-map
    /// statistics prune row groups in the hot table AND skip whole files in the parquet glob, the same way the
    /// callers' own <c>collection_time</c> filters do (no separate index needed — see
    /// <c>DuckDbSchemaGenerator</c>'s <c>idx_query_store_time</c> comment neighbors for why the column stats
    /// alone are enough here). Null when the window holds nothing at all, which the caller reports as "nothing
    /// was read" rather than as a measured absence of activity.</para>
    /// </summary>
    public async Task<DateTime?> GetQueryWindowFloorAsync(QueryWindowRelation relation, int serverId, DateTime startUtc, DateTime endUtc)
    {
        var view = QueryWindowRelationView(relation);
        using var _q = TimeQuery("GetQueryWindowFloorAsync", $"{view} MIN(collection_time) window floor");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = $@"
SELECT MIN(collection_time)
FROM {view}
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });
        var result = await command.ExecuteScalarAsync();
        return result is DateTime dt ? dt : null;
    }
}
