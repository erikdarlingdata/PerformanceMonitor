/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9055: what a page latch wait in get_active_queries is on, answered from the store alone. The request's own stored plan names the
/// indexes it touches, and the newest daily index snapshot (<c>v_index_object_stats</c>) ranks them by their page latch waits.
/// The page kind, the plan pick and the fallback are <see cref="PageLatchResource"/>, which Lite shares. The extra reads run only
/// for a page of rows that holds a page latch wait: one statement for the plans (read through <see cref="DarlingStoredPlanReader"/>, so the statement filter applies), one for the index snapshot rows, plus the
/// snapshot time probe <c>get_object_locking</c> uses.
/// </summary>
internal static class DarlingPageLatchReader
{
    /// <summary>The newest index snapshot's rows for the given databases: every row with a page latch or page I/O latch wait, plus the
    /// highest-waiting row of each database so its id can be named even when nothing in it waited. $1 server_id,
    /// $2 snapshot time (the instant <see cref="DarlingObjectStatsReader.GetLatestIndexObjectStatsSnapshotTimeAsync"/> resolved),
    /// $3 database ids.</summary>
    public const string IndexStatsSql = """
        SELECT database_id, database_name, schema_name, table_name, index_name,
               page_latch_wait_in_ms, page_latch_wait_count, page_io_latch_wait_in_ms, page_io_latch_wait_count
        FROM (
            SELECT
                database_id, database_name, schema_name, table_name, index_name,
                COALESCE(page_latch_wait_in_ms, 0) AS page_latch_wait_in_ms,
                COALESCE(page_latch_wait_count, 0) AS page_latch_wait_count,
                COALESCE(page_io_latch_wait_in_ms, 0) AS page_io_latch_wait_in_ms,
                COALESCE(page_io_latch_wait_count, 0) AS page_io_latch_wait_count,
                row_number() OVER (
                    PARTITION BY database_id
                    ORDER BY COALESCE(page_latch_wait_in_ms, 0) + COALESCE(page_io_latch_wait_in_ms, 0) DESC, schema_name, table_name, index_name
                ) AS rn
            FROM v_index_object_stats
            WHERE server_id = $1
            AND   collection_time = $2
            AND   database_id = ANY($3)
        ) AS ranked
        WHERE rn = 1
        OR    page_latch_wait_in_ms > 0
        OR    page_io_latch_wait_in_ms > 0
        """;

    /// <summary>The page latch rows among <paramref name="rows"/>: a PAGELATCH_ or PAGEIOLATCH_ wait whose wait_resource is a page.</summary>
    public static List<DarlingSessionReader.ActiveQueryRow> PageLatchRows(IEnumerable<DarlingSessionReader.ActiveQueryRow> rows) =>
        rows.Where(r => PageLatchResource.IsPageLatchWait(r.WaitType) && PageLatchResource.TryParse(r.WaitResource, out _, out _, out _)).ToList();

    /// <summary>
    /// The answer for each page latch row, keyed by (collection_time, session_id, request_id or 0). Empty, with no store read at all, when
    /// no row waits on a page latch.
    /// </summary>
    public static async Task<Dictionary<(DateTime CollectionTime, int SessionId, int RequestId), PageLatchAnswer>> DescribeAsync(
        NpgsqlDataSource postgres, int serverId, IReadOnlyList<DarlingSessionReader.ActiveQueryRow> rows, CancellationToken cancellationToken)
    {
        var answers = new Dictionary<(DateTime, int, int), PageLatchAnswer>();
        var pageRows = PageLatchRows(rows);
        if (pageRows.Count == 0) return answers;

        var keys = pageRows.Select(Key).Distinct().ToList();
        var plans = await DarlingStoredPlanReader.GetQuerySnapshotPlansAsync(postgres, serverId, keys, cancellationToken: cancellationToken);

        var databaseIds = pageRows
            .Select(r => PageLatchResource.TryParse(r.WaitResource, out var db, out _, out _) ? db : -1)
            .Where(db => db > 2)
            .Distinct()
            .ToArray();
        DateTime? snapshotTime = null;
        var stats = new List<PageLatchIndexStat>();
        if (databaseIds.Length > 0)
        {
            snapshotTime = await DarlingObjectStatsReader.GetLatestIndexObjectStatsSnapshotTimeAsync(postgres, serverId, cancellationToken);
            if (snapshotTime != null) stats = await ReadIndexStatsAsync(postgres, serverId, snapshotTime.Value, databaseIds, cancellationToken);
        }

        /* A plan shared by several rows is parsed once. */
        var parsed = new Dictionary<string, IReadOnlyList<PlanIndexReference>>(StringComparer.Ordinal);
        foreach (var row in pageRows)
        {
            var key = Key(row);
            if (answers.ContainsKey(key)) continue;
            var plan = plans.TryGetValue(key, out var text) ? text : null;
            var answer = PageLatchResource.Describe(
                row.WaitType, row.WaitResource, stats, snapshotTime,
                () =>
                {
                    if (plan == null) return [];
                    if (!parsed.TryGetValue(plan, out var references))
                    {
                        references = PageLatchResource.IndexesInPlan(plan, cancellationToken);
                        parsed[plan] = references;
                    }

                    return references;
                });
            if (answer != null) answers[key] = answer;
        }

        return answers;
    }

    /// <summary>The key a row's answer is stored under.</summary>
    public static (DateTime CollectionTime, int SessionId, int RequestId) Key(DarlingSessionReader.ActiveQueryRow row) =>
        (row.CollectionTime, row.SessionId, row.RequestId ?? 0);

    private static async Task<List<PageLatchIndexStat>> ReadIndexStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime snapshotTime, int[] databaseIds, CancellationToken cancellationToken)
    {
        var stats = new List<PageLatchIndexStat>();
        await using var command = postgres.CreateCommand(IndexStatsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, snapshotTime);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = databaseIds });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            stats.Add(new PageLatchIndexStat(
                reader.GetInt32(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetInt64(5),
                reader.GetInt64(6),
                reader.GetInt64(7),
                reader.GetInt64(8)));
        }

        return stats;
    }
}
