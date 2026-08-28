/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The collector-cost reads for the MCP/web surface (#2674) — over the hourly aggregate the worker's
/// <see cref="CollectorCostAccumulator"/> persists into <c>collect.collector_cost</c>. Answers the one
/// question the series exists for: which of OUR collectors is the most expensive on the monitored servers,
/// so a hog shows on a dashboard the day it regresses rather than in a log scrape.
///
/// <para>Two reads: the ranked fleet summary over a window (total and per-run cost, and the TAIL — the
/// worst single execution, which is how a collector "sticks out" on a target), and a per-collector daily
/// trend that both the panel charts and the self-alert's baseline consume. sql_ms is a DURATION on the
/// target, not pure CPU.</para>
/// </summary>
internal static class DarlingCollectorCostReader
{
    /// <summary>Ranked per collector over the window ($1 = since, naive UTC), most expensive first.</summary>
    public const string TopSql = @"
SELECT
    collector_name,
    sum(run_count)         AS run_count,
    sum(total_sql_ms)      AS total_sql_ms,
    max(max_sql_ms)        AS max_sql_ms,
    sum(total_storage_ms)  AS total_storage_ms,
    sum(total_rows)        AS total_rows,
    count(DISTINCT server_id) AS server_count
FROM collect.collector_cost
WHERE metric_time >= $1
GROUP BY collector_name
ORDER BY sum(total_sql_ms) DESC";

    /// <summary>One collector's daily series ($1 = collector_name, $2 = since): the summed cost and the
    /// day's worst single execution, so a regression is visible against the collector's own history.</summary>
    public const string TrendSql = @"
SELECT
    date_trunc('day', metric_time) AS day,
    sum(run_count)    AS run_count,
    sum(total_sql_ms) AS total_sql_ms,
    max(max_sql_ms)   AS max_sql_ms
FROM collect.collector_cost
WHERE collector_name = $1
AND   metric_time >= $2
GROUP BY date_trunc('day', metric_time)
ORDER BY day";

    public sealed record CollectorCostSummaryRow(
        string CollectorName,
        long RunCount,
        long TotalSqlMs,
        long MaxSqlMs,
        long TotalStorageMs,
        long TotalRows,
        int ServerCount)
    {
        /// <summary>Average target-side duration per run, over the window. Zero when nothing ran.</summary>
        public long AvgSqlMs => RunCount > 0 ? TotalSqlMs / RunCount : 0;
    }

    public sealed record CollectorCostDailyPoint(
        DateTime Day,
        long RunCount,
        long TotalSqlMs,
        long MaxSqlMs);

    public static async Task<List<CollectorCostSummaryRow>> GetTopAsync(
        NpgsqlDataSource postgres, DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<CollectorCostSummaryRow>();
        await using var command = postgres.CreateCommand(TopSql);
        command.Parameters.AddWithValue(sinceUtc);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new CollectorCostSummaryRow(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetInt64(3),
                reader.GetInt64(4),
                reader.GetInt64(5),
                reader.GetInt32(6)));
        }

        return rows;
    }

    public static async Task<List<CollectorCostDailyPoint>> GetTrendAsync(
        NpgsqlDataSource postgres, string collectorName, DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<CollectorCostDailyPoint>();
        await using var command = postgres.CreateCommand(TrendSql);
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(sinceUtc);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new CollectorCostDailyPoint(
                reader.GetDateTime(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetInt64(3)));
        }

        return rows;
    }
}
