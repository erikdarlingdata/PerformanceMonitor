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
/// trend that both the panel charts and the self-alert's baseline consume. sql_ms is a DURATION, not pure
/// CPU.</para>
///
/// <para><b>And not reliably a TARGET-side duration either (#3192)</b>, which is why the word is absent
/// above and from the members below. It rolls up <c>CollectorRunResult.SqlMs</c>, and on the enumerated path
/// that is the driver's per-item stopwatch around the watermark refresh and the whole <c>readItem</c> closure
/// — so for <c>query_store</c> the store's plan/text probe and write-back are inside it, measured at 107,334
/// of 124,972 ms on one production run. This series carries no phase split and is flushed hourly from an
/// in-memory accumulator, so nothing here can subtract it;
/// <c>get_collection_log</c>'s <c>sql_store_ms</c> is where the attribution lives.</para>
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
        /// <summary>Average duration per run, over the window. Zero when nothing ran. NOT purely target-side
        /// on the plan/text-fetching collectors — see this class's remarks (#3192).</summary>
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
    /// <summary>A collector whose most-recent day's cost regressed against its own baseline
    /// (#2674) — the self-alert's detection query. Per (server, collector): latest day's cost PER RUN vs the
    /// run-weighted cost per run of the prior days in the window, returned only when the baseline is
    /// meaningful (total >= floor, and at least 3 prior days so a new collector cannot trip it) and the
    /// latest exceeds the factor on BOTH that mean and the prior days' own per-run p95.
    /// $1 = baseline window start (naive UTC), $2 = baseline floor ms, $3 = factor (applied to the mean AND
    /// to the p95 per-run baseline), $4 = minimum added ms per day.
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
    /// <para><b>The bound is the factor on the baseline's own UPPER EDGE, not on its mean (#3440).</b>
    /// <c>baseline_p95_ms_per_run</c> is the 95th percentile of the PRIOR days' per-run cost, and the latest
    /// day has to clear the factor on it as well as on the run-weighted mean. The mean conjunct stays, so
    /// the pair is an AND and this predicate can only ever select a SUBSET of what the mean alone selected:
    /// nothing that was silent can start firing. It is there because a heavy collector's own spread already
    /// exceeds the factor — measured on one production fleet, <c>p95/avg</c> per run ran 2.03x
    /// (<c>index_object_stats</c>), 2.95x, 4.74x (<c>procedure_stats</c>) and 5.04x (<c>query_store</c>) —
    /// so against a mean baseline a perfectly normal upper-mode day cleared a 2.0x ratio by construction,
    /// and the alert could not tell "this collector got slower" from "this collector had a normal slow day".
    /// The materiality floor cannot screen those: the expense that makes a collector bimodal also makes its
    /// upper mode's excess large. The measured firing was 17,548 ms/run against a 6,477 ms mean on a
    /// once-daily collector whose own worst run that week was 17,935 ms.</para>
    ///
    /// <para><b>The mean conjunct is retained even though it is entailed at this window size.</b> DISC's
    /// 1-based rank is <c>ceil(0.95 * N)</c>, which equals N for every N up to 19, so on a baseline of at
    /// most 13 days the p95 IS the maximum of the daily per-run costs; and <c>baseline_ms_per_run</c> is the
    /// run-weighted mean of that same population, which cannot exceed its maximum. So the mean conjunct can
    /// never independently exclude a row as shipped. Keeping it is what makes the narrowing property hold by
    /// CONSTRUCTION — a conjunct added, none removed — rather than by that entailment, whose only
    /// precondition is the window constant in <c>DarlingSelfAlertEvaluator</c>. Past 19 baseline days DISC
    /// stops returning the maximum and the entailment ends, at which point a predicate that had dropped this
    /// line as dead would loosen with nothing to say so.
    /// <c>CollectorCostRegressionDispersionTests</c> pins both conjuncts and that window precondition.</para>
    ///
    /// <para><b>The percentile is taken at the grain of the quantity under test, over DAYS.</b>
    /// <c>latest_ms_per_run</c> is one day's mean per-run cost, so the distribution it has to be unusual
    /// against is the distribution of DAILY per-run means, which is what <c>ranked.ms_per_run</c> is. A
    /// per-run percentile over individual runs is a different population and is far wider on a
    /// many-runs-per-day collector, where a daily mean is a much more precise figure — taking the bound from
    /// that would loosen this test exactly where it is most able to discriminate.</para>
    ///
    /// <para><b>DISC rather than CONT</b>, the same choice and the same reason as the per-run p95
    /// <c>get_collection_health</c> serves (#2460): the baseline window holds at most 13 prior days, and an
    /// interpolation between a bimodal series' two modes is a figure no day ever cost. At 14 days it
    /// resolves to the most expensive prior day, which is the intended reading — clear twice your own worst
    /// day — and it degrades correctly rather than by definition if that window ever lengthens, because
    /// over enough days DISC starts discarding genuine one-offs.</para>
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
           sql_ms::double precision / nullif(runs, 0) AS ms_per_run,
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
           percentile_disc(0.95) WITHIN GROUP (ORDER BY ms_per_run)
               FILTER (WHERE day < latest_day)                      AS baseline_p95_ms_per_run,
           max(latest_metric_time_in_day)  FILTER (WHERE day = latest_day) AS latest_metric_time
    FROM ranked
    GROUP BY server_id, collector_name
),
scored AS
(
    SELECT a.server_id, a.collector_name, a.latest_ms, a.latest_runs, a.baseline_ms,
           a.baseline_days, a.latest_metric_time, a.baseline_p95_ms_per_run,
           a.latest_ms::double precision
               / nullif(a.latest_runs, 0)          AS latest_ms_per_run,
           a.baseline_total_ms::double precision
               / nullif(a.baseline_total_runs, 0)  AS baseline_ms_per_run
    FROM agg AS a
)
SELECT sc.server_id, COALESCE(s.display_name, s.server_name) AS server_name, sc.collector_name,
       sc.latest_ms, sc.baseline_ms, sc.latest_metric_time,
       sc.latest_runs, sc.latest_ms_per_run, sc.baseline_ms_per_run, sc.baseline_p95_ms_per_run
FROM scored AS sc
JOIN collect.servers AS s ON s.server_id = sc.server_id
WHERE sc.baseline_days >= 3
AND   sc.baseline_ms >= $2
AND   sc.latest_ms_per_run IS NOT NULL
AND   sc.baseline_ms_per_run IS NOT NULL
AND   sc.baseline_p95_ms_per_run IS NOT NULL
AND   sc.latest_ms_per_run > sc.baseline_ms_per_run * $3
AND   sc.latest_ms_per_run > sc.baseline_p95_ms_per_run * $3
AND   (sc.latest_ms_per_run - sc.baseline_ms_per_run) * sc.latest_runs >= $4
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
        double BaselineMsPerRun,
        double BaselineP95MsPerRun)
    {
        /// <summary>#3440: the per-run figure the latest day has to clear — the factor applied to the
        /// HIGHER of the two baselines, which is the dispersion-aware bound the query gates on. Derived
        /// from the members rather than carried, for the same reason
        /// <see cref="AddedMsPerDay"/> is: this is the number the alert reports as its threshold, and a
        /// carried copy could disagree with the predicate that actually selected the row. The
        /// <see cref="Math.Max"/> is also why an incoherent construction cannot loosen the bound — a
        /// <see cref="BaselineP95MsPerRun"/> below <see cref="BaselineMsPerRun"/> selects the mean and
        /// yields the pre-#3440 bound, never a lower one.</summary>
        public double ThresholdMsPerRun(double factor) =>
            Math.Max(BaselineMsPerRun, BaselineP95MsPerRun) * factor;

        /// <summary>#3316: the per-run rise multiplied by the volume it is paid on, in ms per day -
        /// what this regression actually COSTS. Derived from the members rather than carried, so it
        /// cannot disagree with the parts that explain it, and the query gates on the same
        /// expression. A truthful per-run doubling on a collector costing 3 ms per run and 305 ms
        /// per day is 0.16 s here, which is why the ratio alone is not a reason to alert.</summary>
        public double AddedMsPerDay => (LatestMsPerRun - BaselineMsPerRun) * LatestRuns;
    }

    public static async Task<List<CostRegression>> GetCostRegressionsAsync(
        NpgsqlDataSource postgres, DateTime baselineSinceUtc, long baselineFloorMs, double factor,
        long addedMsFloor, CancellationToken cancellationToken = default)
    {
        var rows = new List<CostRegression>();
        await using var command = postgres.CreateCommand(RegressionSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(baselineSinceUtc);
        command.Parameters.AddWithValue(baselineFloorMs);
        command.Parameters.AddWithValue(factor);
        command.Parameters.AddWithValue(addedMsFloor);

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
                reader.GetDouble(8),
                reader.GetDouble(9)));
        }

        return rows;
    }
}
