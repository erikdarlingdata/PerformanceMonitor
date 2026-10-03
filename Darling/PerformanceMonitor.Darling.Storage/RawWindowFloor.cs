/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The raw-tier window floor probe (#4231), generalized from #2364's single-table
/// <c>DarlingDataReader.QueryStoreWindowFloorSql</c> to the three per-collection tables the Queries tab's
/// grids, and their MCP twins, read: <c>query_stats</c>, <c>procedure_stats</c>, <c>query_store_stats</c>.
/// All three are hypertables dropped at 4 days when TimescaleDB's rollups are armed, and a top-N-by-cost read
/// over a longer window silently serves whatever the raw tier still holds — #2364 fixed that for
/// <c>get_query_store_top</c> alone; this is the same probe, generalized so the other two raw tables get it
/// too, rather than two more hand-copied SQL constants.
///
/// <para><b>Why a separate probe rather than reading the returned rows.</b> Every read this floor serves ranks
/// its top N by COST, not by time, so the timestamps on the returned rows say nothing about how far back the
/// window actually reached — the most expensive row in a month may have happened this morning. The floor is a
/// property of the TIER, not of the result set, and has to be asked for separately.</para>
///
/// <para><b>Why the floor is not bounded below.</b> A floor found inside the window called a quiet start a cut: a
/// server with older rows, quiet for the first hours of the window, read as holding less than the window. The
/// floor is the oldest row at or before the window's end, the same question <see cref="DataWindowFloor"/> asks for
/// the Custom Views panels. It is one walk of the table's <c>(server_id, collection_time)</c> index in time order,
/// so TimescaleDB stops at the first row it meets instead of reading the retention window. It is still null when
/// the window itself holds nothing: a bounded existence check, cheap the same way, keeps that answer for the
/// callers that report an empty window. Unfiltered by database or module: the floor is the tier's, so a filter on
/// the caller's real read must not narrow it.</para>
///
/// <para><b>Why this lives in Storage.</b> The desktop viewer's Queries grids (<c>ViewerDataService</c>) and
/// the MCP tools (<c>DarlingDataReader</c>) both need the identical probe over the identical SQL, and the
/// viewer does not reference the service assembly (#1661 / #2530) — the same reason
/// <see cref="BaselineDiscontinuityReader"/> and <see cref="QueryStoreTrendRouting"/> live here rather than in
/// the MCP reader alone.</para>
/// </summary>
public static class RawWindowFloor
{
    /// <summary>The three raw, per-collection tables this probe answers for. A closed set: a fourth table
    /// means adding a case below, never a fourth hand-copied SQL constant.</summary>
    public enum Table
    {
        QueryStats,
        ProcedureStats,
        QueryStoreStats
    }

    private static string TableName(Table table) => table switch
    {
        Table.QueryStats => "query_stats",
        Table.ProcedureStats => "procedure_stats",
        Table.QueryStoreStats => "query_store_stats",
        _ => throw new ArgumentOutOfRangeException(nameof(table), table, "unknown raw window floor table")
    };

    /// <summary>
    /// The probe SQL for <paramref name="table"/>, the table name the only thing that varies. $1 server_id,
    /// $2/$3 window (naive UTC). The window's start bounds only the existence check, never the floor.
    /// </summary>
    public static string FloorSql(Table table) => $"""
        SELECT o.t
        FROM
        (
            SELECT f.collection_time AS t
            FROM {PgSchemaGenerator.CollectSchema}.{TableName(table)} AS f
            WHERE f.server_id = $1
            AND   f.collection_time <= $3
            ORDER BY f.collection_time
            LIMIT 1
        ) AS o
        WHERE EXISTS
        (
            SELECT 1
            FROM {PgSchemaGenerator.CollectSchema}.{TableName(table)} AS w
            WHERE w.server_id = $1
            AND   w.collection_time >= $2
            AND   w.collection_time <= $3
        )
        """;

    /// <summary>
    /// The oldest <c>collection_time</c> <paramref name="table"/> holds for <paramref name="serverId"/> at or before
    /// <paramref name="endUtc"/>, so it can sit before <paramref name="startUtc"/> when the rows reach back past the
    /// window. Null when the window [<paramref name="startUtc"/>, <paramref name="endUtc"/>] holds nothing at all,
    /// which the caller reports as "nothing was read" rather than as an absence of activity.
    /// </summary>
    /// <param name="commandTimeoutSeconds">The caller's deadline class — the MCP read deadline by default; the
    /// viewer passes its interactive one.</param>
    public static async Task<DateTime?> GetAsync(
        NpgsqlDataSource postgres, Table table, int serverId, DateTime startUtc, DateTime endUtc,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        await using var command = postgres.CreateCommand(FloorSql(table));
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified), not the bare value: Npgsql infers timestamptz from Kind=Utc and PostgreSQL
           then zone-shifts the window against the store's NAIVE timestamp columns — the convention every other
           PostgreSQL window read here follows (DarlingPgBlockingReader, DarlingPgXminReader,
           BaselineDiscontinuityReader). */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DateTime dt ? dt : null;
    }

    /// <summary>The window actually served: the floor when it sits after the requested start, the requested start
    /// otherwise. A floor before the start means the rows reach back past the window, so the window was served
    /// whole; the served start never precedes the one asked for.</summary>
    public static DateTime EffectiveStart(DateTime? floor, DateTime requestedStartUtc) =>
        floor is DateTime f && f > requestedStartUtc ? f : requestedStartUtc;

    /// <summary>
    /// Whether the floor sits far enough past the requested start to call the window <c>window_truncated</c> —
    /// <see cref="DurationTrendRouting.TruncationSlack"/>, the same 90-minute boundary get_query_store_top
    /// (#2364) and the duration-trend tiers (#2353) already treat as the point past which a served window
    /// counts as cut short, reused here rather than restated as a new 90-minute literal.
    /// </summary>
    public static bool IsTruncated(DateTime? floor, DateTime requestedStartUtc) =>
        floor is DateTime f && f > requestedStartUtc.Add(DurationTrendRouting.TruncationSlack);
}
