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
    /// (#2674) — the self-alert's detection query. Per (server, collector): latest day's cost PER RUN vs the
    /// run-weighted cost per run of the prior days in the window, returned only when the baseline is
    /// meaningful (total >= floor, and at least 3 prior days so a new collector cannot trip it) and the
    /// latest exceeds it by the factor.
    /// $1 = baseline window start (naive UTC), $2 = baseline floor ms, $3 = factor.
    ///
    /// <para><b>Why PER RUN and not the day's total (#2846).</b> Total daily cost is
    /// <c>runs x cost-per-run</c>, so comparing totals cannot tell "each run got more expensive" from "the
    /// same work ran more often". When use1's collection cadence recovered — it had been starved by
    /// query_store occupying the shared collection body — run counts rose sharply and daily totals rose with
    /// them while cost per run FELL. That fired 3,259 times across 612 (server, collector) pairs in one day,
    /// and 53% of those pairs had per-run cost going DOWN: the alert reported an improvement as a
    /// regression. Normalising by run_count also removes a second defect for free — <c>latest_day</c> is a
    /// PARTIAL day compared against FULL days, so a total grows monotonically until midnight and the
    /// tripping population can only grow within a day, whereas a per-run average is scale-free and a
    /// half-finished day is directly comparable to a whole one.</para>
    ///
    /// <para>The baseline is run-weighted (<c>sum(sql_ms) / sum(runs)</c>) rather than an average of daily
    /// averages, so a day with very few runs cannot dominate the baseline it contributes to. The total-cost
    /// floor stays on <c>baseline_ms</c> deliberately: it is what stops a 3 ms/run collector alerting on a
    /// doubling to 6 ms. There is deliberately NO minimum run count on the latest day — <c>index_object_stats</c>
    /// legitimately runs once per server per day, so a min-runs guard would blind the alert to every
    /// daily-cadence collector.</para>
    ///
    /// <para><c>latest_metric_time</c> (#2707) is the newest raw hourly row folded into <c>latest_ms</c> —
    /// the freshness anchor the self-alert needs to tell "this regression got worse" from "the hourly flush
    /// hasn't landed a new row since I last looked", the same distinction #2704 draws with wait_stats'
    /// collection_time. Without it, re-asking this query on a cooldown that outpaces the flush hands back the
    /// exact same latest_ms twice, and the evaluator has no way to know the second answer isn't new.</para></summary>
    public const string RegressionSql = @"
WITH daily AS
(
    SELECT cc.server_id, cc.collector_name, date_trunc('day', cc.metric_time) AS day,
           sum(cc.total_sql_ms) AS sql_ms, sum(cc.run_count) AS runs,
           max(cc.metric_time) AS latest_metric_time_in_day
    FROM collect.collector_cost AS cc
    WHERE cc.metric_time >= $1
    GROUP BY cc.server_id, cc.collector_name, date_trunc('day', cc.metric_time)
),
ranked AS
(
    SELECT server_id, collector_name, day, sql_ms, runs, latest_metric_time_in_day,
           max(day) OVER (PARTITION BY server_id, collector_name) AS latest_day
    FROM daily
),
agg AS
(
    SELECT server_id, collector_name,
           max(sql_ms)                     FILTER (WHERE day = latest_day) AS latest_ms,
           max(runs)                       FILTER (WHERE day = latest_day) AS latest_runs,
           avg(sql_ms)                     FILTER (WHERE day < latest_day) AS baseline_ms,
           sum(sql_ms)                     FILTER (WHERE day < latest_day) AS baseline_total_ms,
           sum(runs)                       FILTER (WHERE day < latest_day) AS baseline_total_runs,
           count(*)                        FILTER (WHERE day < latest_day) AS baseline_days,
           max(latest_metric_time_in_day)  FILTER (WHERE day = latest_day) AS latest_metric_time
    FROM ranked
    GROUP BY server_id, collector_name
),
scored AS
(
    SELECT a.server_id, a.collector_name, a.latest_ms, a.latest_runs, a.baseline_ms,
           a.baseline_days, a.latest_metric_time,
           a.latest_ms::double precision
               / nullif(a.latest_runs, 0)          AS latest_ms_per_run,
           a.baseline_total_ms::double precision
               / nullif(a.baseline_total_runs, 0)  AS baseline_ms_per_run
    FROM agg AS a
)
SELECT sc.server_id, COALESCE(s.display_name, s.server_name) AS server_name, sc.collector_name,
       sc.latest_ms, sc.baseline_ms, sc.latest_metric_time,
       sc.latest_runs, sc.latest_ms_per_run, sc.baseline_ms_per_run
FROM scored AS sc
JOIN collect.servers AS s ON s.server_id = sc.server_id
WHERE sc.baseline_days >= 3
AND   sc.baseline_ms >= $2
AND   sc.latest_ms_per_run IS NOT NULL
AND   sc.baseline_ms_per_run IS NOT NULL
AND   sc.latest_ms_per_run > sc.baseline_ms_per_run * $3
ORDER BY (sc.latest_ms_per_run - sc.baseline_ms_per_run) DESC";

    /// <summary>#2846: <see cref="LatestMsPerRun"/> and <see cref="BaselineMsPerRun"/> are what the
    /// predicate actually compares; the daily totals and run count are carried so the alert text can still
    /// show the operator the volume behind the ratio — a per-run rise on a collector that also runs far more
    /// often is a different conversation from one that does not. Positional and REQUIRED rather than
    /// defaulted, so the compiler finds every construction site if this shape changes again.</summary>
    public sealed record CostRegression(
        int ServerId,
        string ServerName,
        string CollectorName,
        long LatestMs,
        double BaselineMs,
        DateTime LatestMetricTime,
        long LatestRuns,
        double LatestMsPerRun,
        double BaselineMsPerRun);

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
                reader.GetDouble(4),
                reader.GetDateTime(5),
                reader.GetInt64(6),
                reader.GetDouble(7),
                reader.GetDouble(8)));
        }

        return rows;
    }
}
