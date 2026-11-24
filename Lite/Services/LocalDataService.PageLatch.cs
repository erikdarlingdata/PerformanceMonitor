/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// #9055: what a page latch wait in get_active_queries is on, from the stored data alone: the row's own plan names the indexes it
    /// touches, and the newest daily index snapshot ranks them by page latch waits. The page kind, the plan pick and the fallback are
    /// <see cref="PageLatchResource"/>, which Darling shares (its twin is <c>DarlingPageLatchReader</c>). The reads run only when a row
    /// waits on a page latch: one statement for the plans, one for the snapshot time, one for the index rows.
    /// </summary>
    public async Task<Dictionary<(DateTime CollectionTime, int SessionId, int RequestId), PageLatchAnswer>> DescribePageLatchWaitsAsync(
        int serverId, IReadOnlyList<QuerySnapshotRow> rows, CancellationToken cancellationToken = default)
    {
        var answers = new Dictionary<(DateTime, int, int), PageLatchAnswer>();
        var pageRows = rows.Where(r => PageLatchResource.IsPageLatchWait(r.WaitType) && PageLatchResource.TryParse(r.WaitResource, out _, out _, out _)).ToList();
        if (pageRows.Count == 0) return answers;

        var keys = pageRows.Select(r => (r.CollectionTime, r.SessionId, r.RequestId)).Distinct().ToList();
        var plans = await ReadPageLatchPlansAsync(serverId, keys);

        var databaseIds = pageRows
            .Select(r => PageLatchResource.TryParse(r.WaitResource, out var db, out _, out _) ? db : -1)
            .Where(db => db > 2)
            .Distinct()
            .ToList();
        DateTime? snapshotTime = null;
        var stats = new List<PageLatchIndexStat>();
        if (databaseIds.Count > 0)
        {
            (snapshotTime, stats) = await ReadPageLatchIndexStatsAsync(serverId, databaseIds);
        }

        var parsed = new Dictionary<string, IReadOnlyList<PlanIndexReference>>(StringComparer.Ordinal);
        foreach (var row in pageRows)
        {
            var key = (row.CollectionTime, row.SessionId, row.RequestId);
            if (answers.ContainsKey(key)) continue;
            var plan = plans.TryGetValue(key, out var text) ? text : null;
            var answer = PageLatchResource.Describe(
                row.WaitType, row.WaitResource, stats, snapshotTime,
                () =>
                {
                    if (plan == null) return [];
                    if (!parsed.TryGetValue(plan, out var references))
                    {
                        /* #4348: the plan is judged before it is parsed, the same as every other Lite plan reader and as Darling's batch read. A plan
                           the judge withholds is not readable, so the row falls back to the inferred ranking like a row with no plan. */
                        references = PageLatchResource.IndexesInPlan(SensitiveStatements.Xml(plan), cancellationToken);
                        parsed[plan] = references;
                    }

                    return references;
                });
            if (answer != null) answers[key] = answer;
        }

        return answers;
    }

    private async Task<Dictionary<(DateTime, int, int), string>> ReadPageLatchPlansAsync(
        int serverId, List<(DateTime CollectionTime, int SessionId, int RequestId)> keys)
    {
        var windowStart = keys.Min(k => k.CollectionTime);
        return await ReadWindowSkippingEmptyArchiveAsync("query_snapshots", "collection_time", windowStart, ReadAsync, CancellationToken.None);

        async Task<Dictionary<(DateTime, int, int), string>> ReadAsync(string relation)
        {
            using var connection = await OpenConnectionAsync();
            using var command = connection.CreateCommand();
            var where = new StringBuilder();
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            var next = 2;
            foreach (var key in keys)
            {
                if (where.Length > 0) where.Append(" OR ");
                where.Append(System.Globalization.CultureInfo.InvariantCulture,
                    $"(collection_time = ${next} AND session_id = ${next + 1} AND COALESCE(request_id, 0) = ${next + 2})");
                command.Parameters.Add(new DuckDBParameter { Value = key.CollectionTime });
                command.Parameters.Add(new DuckDBParameter { Value = key.SessionId });
                command.Parameters.Add(new DuckDBParameter { Value = key.RequestId });
                next += 3;
            }

            command.CommandText = $@"
SELECT collection_time, session_id, COALESCE(request_id, 0), query_plan
FROM {relation}
WHERE server_id = $1
AND   query_plan IS NOT NULL
AND   ({where})";

            var plans = new Dictionary<(DateTime, int, int), string>();
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                plans[(reader.GetDateTime(0), reader.GetInt32(1), reader.GetInt32(2))] = reader.GetString(3);
            }

            return plans;
        }
    }

    /// <summary>The newest index snapshot's time, and its rows for the given databases: every row with a page latch or page I/O latch
    /// wait, plus the highest-waiting row of each database so its id can be named even when nothing in it waited (Darling's
    /// <c>DarlingPageLatchReader.IndexStatsSql</c>). The database ids are integers parsed from wait resources, so they are written
    /// into the statement.</summary>
    private async Task<(DateTime? SnapshotTime, List<PageLatchIndexStat> Stats)> ReadPageLatchIndexStatsAsync(int serverId, List<int> databaseIds)
    {
        using var connection = await OpenConnectionAsync();
        /* #7216: the hot table holds the newest snapshot of any server still collected; the view is read only for a server with no hot row. */
        var relation = await HotTableHasServerAsync(connection, "index_object_stats", serverId) ? "index_object_stats" : "v_index_object_stats";

        DateTime? snapshotTime;
        using (var probe = connection.CreateCommand())
        {
            probe.CommandText = $"SELECT MAX(collection_time) FROM {relation} WHERE server_id = $1";
            probe.Parameters.Add(new DuckDBParameter { Value = serverId });
            var result = await probe.ExecuteScalarAsync();
            snapshotTime = result is DateTime stamp ? stamp : null;
        }

        var stats = new List<PageLatchIndexStat>();
        if (snapshotTime == null) return (null, stats);

        using var command = connection.CreateCommand();
        var ids = string.Join(", ", databaseIds.Select(id => id.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        command.CommandText = $@"
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
    FROM {relation}
    WHERE server_id = $1
    AND   collection_time = $2
    AND   database_id IN ({ids})
) AS ranked
WHERE rn = 1
OR    page_latch_wait_in_ms > 0
OR    page_io_latch_wait_in_ms > 0";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = snapshotTime.Value });
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            stats.Add(new PageLatchIndexStat(
                Convert.ToInt32(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                Convert.ToInt64(reader.GetValue(5), System.Globalization.CultureInfo.InvariantCulture),
                Convert.ToInt64(reader.GetValue(6), System.Globalization.CultureInfo.InvariantCulture),
                Convert.ToInt64(reader.GetValue(7), System.Globalization.CultureInfo.InvariantCulture),
                Convert.ToInt64(reader.GetValue(8), System.Globalization.CultureInfo.InvariantCulture)));
        }

        return (snapshotTime, stats);
    }
}
