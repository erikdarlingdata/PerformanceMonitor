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
    /// <summary>A collector whose most-recent day's target-side cost regressed against its own baseline
    /// (#2674) — the self-alert's detection query. Per (server, collector): latest day's summed sql_ms vs
    /// the AVERAGE of the prior days in the window, returned only when the baseline is meaningful (>= floor,
    /// and at least 3 prior days so a new collector cannot trip it) and the latest exceeds it by the factor.
    /// $1 = baseline window start (naive UTC), $2 = baseline floor ms, $3 = factor.</summary>
    public const string RegressionSql = @"
WITH daily AS
(
    SELECT cc.server_id, cc.collector_name, date_trunc('day', cc.metric_time) AS day, sum(cc.total_sql_ms) AS sql_ms
    FROM collect.collector_cost AS cc
    WHERE cc.metric_time >= $1
    GROUP BY cc.server_id, cc.collector_name, date_trunc('day', cc.metric_time)
),
ranked AS
(
    SELECT server_id, collector_name, day, sql_ms,
           max(day) OVER (PARTITION BY server_id, collector_name) AS latest_day
    FROM daily
),
agg AS
(
    SELECT server_id, collector_name,
           max(sql_ms) FILTER (WHERE day = latest_day)  AS latest_ms,
           avg(sql_ms) FILTER (WHERE day < latest_day)  AS baseline_ms,
           count(*)    FILTER (WHERE day < latest_day)  AS baseline_days
    FROM ranked
    GROUP BY server_id, collector_name
)
SELECT a.server_id, COALESCE(s.display_name, s.server_name) AS server_name, a.collector_name, a.latest_ms, a.baseline_ms
FROM agg AS a
JOIN collect.servers AS s ON s.server_id = a.server_id
WHERE a.baseline_days >= 3
AND   a.baseline_ms >= $2
AND   a.latest_ms > a.baseline_ms * $3
ORDER BY (a.latest_ms - a.baseline_ms) DESC";

    public sealed record CostRegression(
        int ServerId,
        string ServerName,
        string CollectorName,
        long LatestMs,
        double BaselineMs);

    public static async Task<List<CostRegression>> GetCostRegressionsAsync(
        NpgsqlDataSource postgres, DateTime baselineSinceUtc, long baselineFloorMs, double factor,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<CostRegression>();
        await using var command = postgres.CreateCommand(RegressionSql);
        command.Parameters.AddWithValue(baselineSinceUtc);
        command.Parameters.AddWithValue(baselineFloorMs);
        command.Parameters.AddWithValue(factor);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new CostRegression(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetInt64(3),
                reader.GetDouble(4)));
        }

        return rows;
    }
}
