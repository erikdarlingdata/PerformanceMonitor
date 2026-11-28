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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>Which per-execution metric the Query Heatmap buckets rows by — the viewer's
/// <c>HeatmapMetric</c>, which is itself Lite's. Copied rather than referenced because the headless
/// service does not (and should not) reference the WPF viewer.</summary>
public enum HeatmapMetric
{
    Duration,
    Cpu,
    LogicalReads,
    LogicalWrites,
    ExecutionCount,
}

/// <summary>
/// The service-side read behind get_query_heatmap (#2484) — the viewer's
/// <c>ViewerDataService.BuildQueryHeatmapSql</c>, which is itself the Postgres port of Lite's DuckDB
/// heatmap. Copied VERBATIM apart from the things a desktop chart does not need and an MCP read does:
/// the bin width is a bound parameter instead of the literal <c>INTERVAL '5 minutes'</c> (defaulting to
/// that same 5), the tail carries <c>ORDER BY time_bin DESC</c> + <c>LIMIT</c> so a capped call keeps
/// the most RECENT bins rather than the oldest ones, and the preview width is
/// a caller-side cut rather than a literal 120 (#4198: at 500 cells the FIXED per-cell fields alone —
/// before one byte of query text — already ran well past the shared response budget, so the cell cap had to
/// fall regardless of the text width chosen). Every other clause — the magnitude CASE, the
/// <c>delta_execution_count &gt; 0</c> and <c>metric IS NOT NULL</c> filters, the top-1 window that replaced
/// DuckDB's <c>ARG_MAX</c> — is the viewer's.
///
/// <para><b>The bucketing is the viewer's, deliberately.</b> The whole point of the web/MCP surface is that
/// it answers the same question the desktop does, so a browser and a desktop pointed at the same server over
/// the same window must not draw different pictures. The viewer's bucketing turns out to be a CONSTANT
/// (5 minutes) rather than something derived from the window length, so there is nothing to reproduce — just
/// a default to honor. It is exposed as <c>bucket_minutes</c> because an agent asking about a 7-day window
/// wants coarser columns, not 2,016 of them, and because widening the bin is the one lever that covers more
/// window inside the row cap.</para>
///
/// <para><b>The origin is load-bearing once the width is a parameter.</b> Postgres <c>date_bin</c> takes an
/// explicit origin and this read passes the Unix epoch, matching the viewer. DuckDB's <c>time_bucket</c>
/// defaults to a DIFFERENT origin (2000-01-03), which is invisible at 5 minutes — the two origins are
/// 15,780,960 minutes apart, and 5 divides that exactly — and visible at 7, where the two SKUs bin the same
/// row one minute apart. Lite's twin therefore passes the epoch explicitly too. Verified across
/// {1, 5, 7, 13, 60, 90, 360, 1440}-minute strides on PostgreSQL 17 and DuckDB 1.5.5: identical bins.</para>
///
/// <para>A STORED read — no live monitored-server hit. <c>query_stats</c> is a PERIODIC table, not an edge
/// table: the collector writes rows every cycle for whatever is in the plan cache, so "no rows" here means
/// nobody looked, and an existence probe on the data is the right denominator (see
/// <see cref="HeatmapCoverageSql"/>).</para>
///
/// <para>#4233: the preview is resolved for the rn = 1 row of each cell only, not every row in the
/// window. <c>base</c> reads <c>query_stats</c> directly rather than <c>v_query_stats</c> (#1767's
/// payload-resolving view), so no row pays for the <c>query_text_dim</c> join or a truncation it will
/// never be shown - the raw inline <c>query_text</c> (pre-#1767 rows only) and the digest ride the
/// window sorts in its place. The outer rn = 1 filter runs before the SELECT list, so
/// <c>COALESCE(query_text, (SELECT ... FROM query_text_dim ...))</c> - the same resolution
/// <c>v_query_stats</c> would have performed - executes only for the
/// one row per cell the caller sees.</para>
///
/// <para>The SQL is built by a public method so the tests can pin the dialect and the shape without a live
/// Postgres.</para>
/// </summary>
internal static class DarlingQueryHeatmapReader
{
    /// <summary>The desktop viewer's bin width, and therefore this read's default. Not derived from the
    /// window length — the viewer hardcodes <c>INTERVAL '5 minutes'</c> whatever range is on screen.</summary>
    public const int ViewerBucketMinutes = 5;

    /// <summary>The widest bin a caller may ask for: one day. Past this the "heatmap" is one column.</summary>
    public const int MaxBucketMinutes = 1440;

    /// <summary>The seven log-magnitude rows of the grid — the viewer's, not a new banding.</summary>
    public const int BucketCount = 7;

    /// <summary>One heatmap cell: the query count in a (time bin x magnitude bucket) plus the most-executed
    /// query in it, which is what the desktop shows on hover. <see cref="TopQueryTextTruncated"/> is true
    /// when <see cref="TopQueryText"/> is a preview shorter than the stored statement (#4198) — the caller's
    /// only way to tell "this is the whole thing" from "this is cut off" without re-asking with full text.
    /// </summary>
    public sealed record HeatmapCellRow(
        DateTime TimeBucket,
        int BucketIndex,
        long QueryCount,
        string TopQueryHash,
        string TopQueryText,
        bool TopQueryTextTruncated);

    /// <summary>The viewer's per-metric magnitude labels, verbatim. Duration and CPU are milliseconds per
    /// execution; the rest are plain counts, so the two families label the same seven buckets differently.
    /// Returned with every result — a bare <c>bucket_index</c> is unreadable without them.</summary>
    public static readonly IReadOnlyDictionary<HeatmapMetric, string[]> BucketLabels =
        new Dictionary<HeatmapMetric, string[]>
        {
            [HeatmapMetric.Duration] = new[] { "0-1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", ">100s" },
            [HeatmapMetric.Cpu] = new[] { "0-1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", ">100s" },
            [HeatmapMetric.LogicalReads] = new[] { "0-1", "1-10", "10-100", "100-1K", "1K-10K", "10K-100K", ">100K" },
            [HeatmapMetric.LogicalWrites] = new[] { "0-1", "1-10", "10-100", "100-1K", "1K-10K", "10K-100K", ">100K" },
            [HeatmapMetric.ExecutionCount] = new[] { "0-1", "1-10", "10-100", "100-1K", "1K-10K", "10K-100K", ">100K" },
        };

    /// <summary>
    /// The per-execution metric expression, byte-identical with the viewer's <c>HeatmapMetricExpr</c> and
    /// Lite's <c>GetMetricColumn</c> — the <c>/ 1000.0</c> microsecond-to-millisecond scaling, the
    /// <c>NULLIF(delta_execution_count, 0)</c> per-execution average, the <c>CAST(... AS DOUBLE PRECISION)</c>.
    /// Internal constants only (no caller text reaches this), so string-composing it into the SQL is
    /// injection-safe; the caller's <c>metric</c> string is mapped through <see cref="TryParseMetric"/> first
    /// and never appears in the query.
    /// </summary>
    public static string MetricExpression(HeatmapMetric metric) => metric switch
    {
        HeatmapMetric.Duration => "(delta_elapsed_time / 1000.0) / NULLIF(delta_execution_count, 0)",
        HeatmapMetric.Cpu => "(delta_worker_time / 1000.0) / NULLIF(delta_execution_count, 0)",
        HeatmapMetric.LogicalReads => "CAST(delta_logical_reads AS DOUBLE PRECISION) / NULLIF(delta_execution_count, 0)",
        HeatmapMetric.LogicalWrites => "CAST(delta_logical_writes AS DOUBLE PRECISION) / NULLIF(delta_execution_count, 0)",
        HeatmapMetric.ExecutionCount => "CAST(delta_execution_count AS DOUBLE PRECISION)",
        _ => "(delta_elapsed_time / 1000.0) / NULLIF(delta_execution_count, 0)",
    };

    /// <summary>The snake_case name a caller passes, and the one echoed back in the result.</summary>
    public static string MetricName(HeatmapMetric metric) => metric switch
    {
        HeatmapMetric.Duration => "duration",
        HeatmapMetric.Cpu => "cpu",
        HeatmapMetric.LogicalReads => "logical_reads",
        HeatmapMetric.LogicalWrites => "logical_writes",
        HeatmapMetric.ExecutionCount => "execution_count",
        _ => "duration",
    };

    /// <summary>What one cell's magnitude actually measures. Without it a caller reading "1-10" cannot tell
    /// a per-execution average from a per-interval total, and execution_count is the one metric that is a
    /// TOTAL rather than an average.</summary>
    public static string MetricUnit(HeatmapMetric metric) => metric switch
    {
        HeatmapMetric.Duration => "milliseconds of elapsed time per execution",
        HeatmapMetric.Cpu => "milliseconds of CPU per execution",
        HeatmapMetric.LogicalReads => "logical reads per execution",
        HeatmapMetric.LogicalWrites => "logical writes per execution",
        HeatmapMetric.ExecutionCount => "executions in the collection interval (a total, not a per-execution average)",
        _ => "milliseconds of elapsed time per execution",
    };

    /// <summary>Maps the caller's <c>metric</c> string onto the enum. Returns false rather than silently
    /// falling back to duration: a caller who asked for CPU and got duration would read the wrong grid with
    /// no sign anything went wrong. Lite's twin accepts exactly these five names.</summary>
    public static bool TryParseMetric(string? metric, out HeatmapMetric parsed)
    {
        parsed = HeatmapMetric.Duration;
        if (string.IsNullOrWhiteSpace(metric)) return true;

        switch (metric.Trim().ToLowerInvariant())
        {
            case "duration": parsed = HeatmapMetric.Duration; return true;
            case "cpu": parsed = HeatmapMetric.Cpu; return true;
            case "logical_reads": parsed = HeatmapMetric.LogicalReads; return true;
            case "logical_writes": parsed = HeatmapMetric.LogicalWrites; return true;
            case "execution_count": parsed = HeatmapMetric.ExecutionCount; return true;
            default: return false;
        }
    }

    /// <summary>
    /// The viewer's heatmap read for one metric. $1 server_id, $2 window start, $3 window end, $4 database
    /// filter (text[] or NULL), $5 bin width in minutes, $6 cell cap. The top statement comes back
    /// WHOLE (#5320): the statement filter must judge the whole text before the preview is cut, so no cut can
    /// end inside a value whose naming text lies past it. The truncation signal (#4198) is the judged text
    /// running past the caller's preview length.
    /// <para>Ordered newest bin first ONLY so the cap keeps the recent end of the window; the tool re-sorts
    /// chronologically before returning. The viewer needs no cap and orders ascending.</para>
    /// </summary>
    public static string BuildQueryHeatmapSql(HeatmapMetric metric)
    {
        var metricExpr = MetricExpression(metric);
        return $"""
            WITH base AS (
                SELECT
                    date_bin(($5::integer * INTERVAL '1 minute'), collection_time, TIMESTAMP '1970-01-01 00:00:00') AS time_bin,
                    {metricExpr} AS metric_value,
                    query_hash,
                    query_text,
                    query_text_digest,
                    delta_execution_count
                FROM query_stats
                WHERE server_id = $1
                AND   collection_time > $2
                AND   collection_time <= $3
                AND   ($4::text[] IS NULL OR database_name = ANY($4))
                AND   delta_execution_count > 0
                AND   {metricExpr} IS NOT NULL
            ),
            binned AS (
                SELECT
                    time_bin,
                    CASE
                        WHEN metric_value < 1 THEN 0
                        WHEN metric_value < 10 THEN 1
                        WHEN metric_value < 100 THEN 2
                        WHEN metric_value < 1000 THEN 3
                        WHEN metric_value < 10000 THEN 4
                        WHEN metric_value < 100000 THEN 5
                        ELSE 6
                    END AS bucket_index,
                    query_hash,
                    query_text,
                    query_text_digest,
                    delta_execution_count
                FROM base
            ),
            hashed AS (
                SELECT
                    time_bin,
                    bucket_index,
                    query_hash,
                    query_text,
                    query_text_digest,
                    delta_execution_count,
                    ROW_NUMBER() OVER (PARTITION BY time_bin, bucket_index, query_hash ORDER BY delta_execution_count DESC) AS hash_rn
                FROM binned
            ),
            ranked AS (
                /* #7881: query_count is DISTINCT query_hash in the cell, as the tool says, not the sample rows (one per collection x
                   plan x handle, so a steady query counted once per collection). PostgreSQL has no COUNT(DISTINCT) window, so the first
                   row of each hash in the cell counts 1. A NULL hash is one group, so a cell never reads as 0 queries. One scan of the
                   same base read, so it uses the same query_stats index as before. */
                SELECT
                    time_bin,
                    bucket_index,
                    query_hash,
                    query_text,
                    query_text_digest,
                    SUM(CASE WHEN hash_rn = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY time_bin, bucket_index) AS query_count,
                    ROW_NUMBER() OVER (PARTITION BY time_bin, bucket_index ORDER BY delta_execution_count DESC) AS rn
                FROM hashed
            )
            SELECT
                time_bin,
                bucket_index,
                query_count,
                query_hash AS top_query_hash,
                COALESCE(query_text, (SELECT d.query_text FROM query_text_dim d WHERE d.digest = ranked.query_text_digest)) AS top_query_text
            FROM ranked
            WHERE rn = 1
            ORDER BY time_bin DESC, bucket_index
            LIMIT $6
            """;
    }

    /// <summary>
    /// Whether this server has query stats AT ALL, and whether it has any inside the window.
    /// <para>One round trip for the two facts that decide what an empty grid means, run only on the empty
    /// path. This probes the DATA rather than SUCCESS rows in <c>collection_log</c>, and that is a judgement
    /// about which kind of table this is: <c>query_stats</c> is PERIODIC, not an edge table. The collector
    /// writes a row every cycle for whatever sits in the plan cache, so a server with zero rows in its whole
    /// history is a server nobody collected — unlike blocking or deadlocks, where zero rows is the healthy
    /// answer and a data probe would send someone to fix collection that works.</para>
    /// <para>Probes <c>query_stats</c> directly (#4233 moved the read itself off <c>v_query_stats</c>
    /// onto the fact table; the view's payload-dimension joins are LEFT JOINs, so they never drop a
    /// fact row, and existence here is identical either way - this keeps the probe's FROM matching the
    /// read's literally rather than by that argument). $1 server_id, $2 window start, $3 window end.</para>
    /// </summary>
    public const string HeatmapCoverageSql = """
        SELECT
            EXISTS (
                SELECT 1
                FROM query_stats
                WHERE server_id = $1
            ) AS has_any,
            EXISTS (
                SELECT 1
                FROM query_stats
                WHERE server_id = $1
                AND   collection_time > $2
                AND   collection_time <= $3
            ) AS has_in_window
        """;

    /// <summary>The bin width, in minutes, from which the heat map reads whole hours from the hourly rollup (#8570).</summary>
    public const int RollupMinBucketMinutes = 60;

    /// <summary>The source name a reply carries when the heat map read the raw <c>query_stats</c> rows only.</summary>
    public const string SourceRaw = "raw";

    /// <summary>The source name a reply carries when whole hours came from the hourly rollup (#8570). The hours the rollup does
    /// not cover yet, and the partial hours at either end of the window, are still read raw.</summary>
    public const string SourceHourlyRollup = "hourly_rollup";

    /// <summary>Whether the hourly rollup carries what <paramref name="metric"/> needs. It holds the elapsed time, the CPU time and the
    /// execution count of each hour, so the two per-execution time metrics can be rebuilt from it. It holds no logical reads or writes, and
    /// the execution count metric is a per-sample figure that an hour's sum cannot give back, so those three read raw at every bin width.</summary>
    public static bool RollupServes(HeatmapMetric metric) => metric is HeatmapMetric.Duration or HeatmapMetric.Cpu;

    /// <summary>
    /// #8570: where the rollup stands in for the raw rows. <see cref="HourlyRelation"/> is the stitched FROM item (aliased <c>f</c>),
    /// <see cref="FromUtc"/> the first whole hour of the window the rollup serves, <see cref="ToUtc"/> the bucket bound (exclusive): the
    /// earlier of the window's last whole hour and the rollup's own materialization ceiling. Hours before
    /// <see cref="FromUtc"/> and from <see cref="ToUtc"/> on are read raw.
    /// </summary>
    internal sealed record HeatmapRollupPlan(string HourlyRelation, DateTime FromUtc, DateTime ToUtc);

    /// <summary>What one heat map read answered: the cells, and the source that served them (<see cref="SourceRaw"/> or
    /// <see cref="SourceHourlyRollup"/>) with the span the rollup served.</summary>
    internal sealed record HeatmapRead(List<HeatmapCellRow> Rows, string Source, HeatmapRollupPlan? Rollup, DateTime ReadFromUtc = default, int Statements = 1);

    private static readonly DateTime Epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// #9965: the start of the newest <paramref name="cap"/> bins, on the same <c>date_bin</c> origin the read bins on, or null when the window
    /// starts inside them (then the read is the whole window). The cap keeps the newest cells and every cell is built from the rows of its own bin,
    /// so the cells of bins at or after this boundary are the same whether the read starts here or at the window start. The read's start is
    /// then <c>boundary - 1 microsecond</c>, which makes <c>collection_time &gt; start</c> mean <c>collection_time &gt;= boundary</c>.
    /// </summary>
    internal static DateTime? NewestBinsFloor(DateTime startUtc, DateTime endUtc, int bucketMinutes, int cap)
    {
        if (cap < 1 || bucketMinutes < 1 || endUtc < Epoch)
        {
            return null;
        }

        var width = TimeSpan.FromMinutes(bucketMinutes).Ticks;
        var newestBin = (endUtc - Epoch).Ticks / width;
        var firstBin = newestBin - (cap - 1);
        if (firstBin <= 0)
        {
            return null;
        }

        var floor = DateTime.SpecifyKind(Epoch.AddTicks(firstBin * width), DateTimeKind.Unspecified);
        return floor > startUtc ? floor : null;
    }

    /// <summary>One microsecond, the resolution of a PostgreSQL timestamp.</summary>
    private const long MicrosecondTicks = 10;

    /// <summary>
    /// #8570: the plan for a read, or null (read raw, as before) unless every condition holds. A bin of 60 minutes or more, a metric the rollup
    /// carries (<see cref="RollupServes"/>), a measured materialization ceiling (an unknown ceiling means the rollup may not be used), a measured
    /// floor, and at least one whole hour between the later of the window's start and the floor and the earlier of the window's last whole hour and
    /// the ceiling. The window is cut to whole hours: a partial hour at either end is read raw, so a row is read from the rollup or from raw and never from both.
    /// </summary>
    internal static HeatmapRollupPlan? PlanRollup(RollupCoverage coverage, HeatmapMetric metric, int bucketMinutes, DateTime startUtc, DateTime endUtc)
    {
        if (bucketMinutes < RollupMinBucketMinutes || !RollupServes(metric))
        {
            return null;
        }

        if (coverage.HourlyEndCeiling(TimescaleSupport.QueryStatsHourlyView, startUtc) is not DateTime ceiling
            || coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView).HourlyFloorUtc is not DateTime floor)
        {
            return null;
        }

        var from = CeilingHour(startUtc > floor ? startUtc : floor);
        var lastWholeHour = new DateTime(endUtc.Year, endUtc.Month, endUtc.Day, endUtc.Hour, 0, 0, DateTimeKind.Unspecified);
        var to = lastWholeHour < ceiling ? lastWholeHour : ceiling;
        if (to <= from)
        {
            return null;
        }

        return new HeatmapRollupPlan(
            coverage.StitchedRelationSql(TimescaleSupport.QueryStatsHourlyView, "f", startUtc, RollupCoverage.StitchTier.Hourly),
            DateTime.SpecifyKind(from, DateTimeKind.Unspecified),
            DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
    }

    private static DateTime CeilingHour(DateTime t)
    {
        var hour = new DateTime(t.Year, t.Month, t.Day, t.Hour, 0, 0, DateTimeKind.Unspecified);
        return hour == t ? hour : hour.AddHours(1);
    }

    /// <summary>
    /// The window-floor probe's sources for a rollup read: the raw table, plus the hourly relations the read's FROM item names. A successor
    /// a stitch takes only from the stitch floor up carries that floor, as the compose route's data-start probe does.
    /// </summary>
    internal static IReadOnlyList<DataWindowFloor.Source> NoticeSources(RollupCoverage coverage, HeatmapRollupPlan plan, DateTime startUtc)
    {
        var sources = new List<DataWindowFloor.Source> { DataWindowFloor.Source.ForCollectorTable("query_stats") };
        var successor = TimescaleSupport.SuccessorOf(TimescaleSupport.QueryStatsHourlyView);
        var stitchFloor = coverage.StitchFloor(TimescaleSupport.QueryStatsHourlyView, RollupCoverage.StitchTier.Hourly, startUtc);
        foreach (var relation in new[] { TimescaleSupport.QueryStatsHourlyView, successor })
        {
            if (relation is null || !plan.HourlyRelation.Contains("collect." + relation + " ", StringComparison.Ordinal))
            {
                continue;
            }

            DateTime? lowerBound = stitchFloor is not null && string.Equals(relation, successor, StringComparison.Ordinal) ? stitchFloor : null;
            if (DataWindowFloor.Source.TryForRollup(relation, out var rollup, lowerBound))
            {
                sources.Add(rollup);
            }
        }

        return sources;
    }

    /// <summary>
    /// #8570: the heat map over the hourly rollup, for bins of <see cref="RollupMinBucketMinutes"/> minutes or more. Same parameters as
    /// <see cref="BuildQueryHeatmapSql"/> ($1 to $6), plus $7 the first whole hour the rollup serves and $8 the bucket bound (exclusive).
    /// <para>Three arms feed one <c>base</c>: the raw rows before $7, the rollup's whole hours from $7 to $8, and the raw rows from $8. A rollup hour
    /// is one row per (database, query_hash, sql_handle) and counts as one sample: its metric is the hour's sum over the hour's executions, so a cell
    /// counts each query's hourly average where the raw read counts every sample. With one sample per query per hour the two reads agree cell for cell.
    /// An hour counts in the bin that holds its start: at a bin width that is not a multiple of 60 minutes a bin edge can fall inside an hour, and the hour
    /// is still counted once, in the bin it starts in.</para>
    /// <para>The rollup carries no statement text. The top query of each winning cell (and only those, after the cap) takes the text of its hash's
    /// newest raw row through <c>idx_query_stats_server_hash_time</c>, falling back to <c>query_text_dim</c>. A hash whose raw rows have all aged
    /// out reads an empty text.</para>
    /// </summary>
    public static string BuildQueryHeatmapRollupSql(HeatmapMetric metric, string hourlyRelation)
    {
        var rawMetric = MetricExpression(metric);
        var hourlyMetric = metric == HeatmapMetric.Cpu
            ? "(f.worker_time_sum / 1000.0) / NULLIF(f.execution_count_sum, 0)"
            : "(f.elapsed_time_sum / 1000.0) / NULLIF(f.execution_count_sum, 0)";
        return $"""
            WITH base AS (
                SELECT
                    date_bin(($5::integer * INTERVAL '1 minute'), collection_time, TIMESTAMP '1970-01-01 00:00:00') AS time_bin,
                    {rawMetric} AS metric_value,
                    query_hash,
                    delta_execution_count
                FROM query_stats
                WHERE server_id = $1
                AND   collection_time > $2
                AND   collection_time < $7
                AND   collection_time <= $3
                AND   ($4::text[] IS NULL OR database_name = ANY($4))
                AND   delta_execution_count > 0
                AND   {rawMetric} IS NOT NULL
                UNION ALL
                SELECT
                    date_bin(($5::integer * INTERVAL '1 minute'), f.bucket, TIMESTAMP '1970-01-01 00:00:00') AS time_bin,
                    {hourlyMetric} AS metric_value,
                    f.query_hash,
                    f.execution_count_sum AS delta_execution_count
                FROM {hourlyRelation}
                WHERE f.server_id = $1
                AND   f.bucket >= $7
                AND   f.bucket < $8
                AND   ($4::text[] IS NULL OR f.database_name = ANY($4))
                AND   f.execution_count_sum > 0
                AND   {hourlyMetric} IS NOT NULL
                UNION ALL
                SELECT
                    date_bin(($5::integer * INTERVAL '1 minute'), collection_time, TIMESTAMP '1970-01-01 00:00:00') AS time_bin,
                    {rawMetric} AS metric_value,
                    query_hash,
                    delta_execution_count
                FROM query_stats
                WHERE server_id = $1
                AND   collection_time >= $8
                AND   collection_time > $2
                AND   collection_time <= $3
                AND   ($4::text[] IS NULL OR database_name = ANY($4))
                AND   delta_execution_count > 0
                AND   {rawMetric} IS NOT NULL
            ),
            binned AS (
                SELECT
                    time_bin,
                    CASE
                        WHEN metric_value < 1 THEN 0
                        WHEN metric_value < 10 THEN 1
                        WHEN metric_value < 100 THEN 2
                        WHEN metric_value < 1000 THEN 3
                        WHEN metric_value < 10000 THEN 4
                        WHEN metric_value < 100000 THEN 5
                        ELSE 6
                    END AS bucket_index,
                    query_hash,
                    delta_execution_count
                FROM base
            ),
            hashed AS (
                SELECT
                    time_bin,
                    bucket_index,
                    query_hash,
                    delta_execution_count,
                    ROW_NUMBER() OVER (PARTITION BY time_bin, bucket_index, query_hash ORDER BY delta_execution_count DESC) AS hash_rn
                FROM binned
            ),
            ranked AS (
                SELECT
                    time_bin,
                    bucket_index,
                    query_hash,
                    SUM(CASE WHEN hash_rn = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY time_bin, bucket_index) AS query_count,
                    ROW_NUMBER() OVER (PARTITION BY time_bin, bucket_index ORDER BY delta_execution_count DESC) AS rn
                FROM hashed
            ),
            winners AS (
                SELECT time_bin, bucket_index, query_count, query_hash
                FROM ranked
                WHERE rn = 1
                ORDER BY time_bin DESC, bucket_index
                LIMIT $6
            )
            SELECT
                w.time_bin,
                w.bucket_index,
                w.query_count,
                w.query_hash AS top_query_hash,
                COALESCE(l.query_text, d.query_text) AS top_query_text
            FROM winners AS w
            LEFT JOIN LATERAL
            (
                (
                    SELECT q.query_text, q.query_text_digest, q.collection_time
                    FROM query_stats AS q
                    WHERE w.query_hash IS NOT NULL
                    AND   q.server_id = $1
                    AND   q.query_hash = w.query_hash
                    AND   (q.query_text IS NOT NULL OR EXISTS (SELECT 1 FROM query_text_dim AS d0 WHERE d0.digest = q.query_text_digest))
                    ORDER BY q.collection_time DESC
                    LIMIT 1
                )
                UNION ALL
                (
                    SELECT q.query_text, q.query_text_digest, q.collection_time
                    FROM query_stats AS q
                    WHERE w.query_hash IS NULL
                    AND   q.server_id = $1
                    AND   q.query_hash IS NULL
                    AND   (q.query_text IS NOT NULL OR EXISTS (SELECT 1 FROM query_text_dim AS d0 WHERE d0.digest = q.query_text_digest))
                    ORDER BY q.collection_time DESC
                    LIMIT 1
                )
                ORDER BY collection_time DESC
                LIMIT 1
            ) AS l ON TRUE
            LEFT JOIN query_text_dim AS d ON d.digest = l.query_text_digest
            ORDER BY w.time_bin DESC, w.bucket_index
            """;
    }

    /// <summary>
    /// #8570: <see cref="GetQueryHeatmapAsync"/> with the rollup route. Reads whole hours from the hourly rollup when <see cref="PlanRollup"/> allows
    /// it (a bin of 60 minutes or more, a metric the rollup carries, a store whose rollup has measured coverage) and raw otherwise, and says which.
    /// <para>#9965: the cap keeps the newest cells, so the read first covers only the newest <paramref name="limit"/> bins (<see cref="NewestBinsFloor"/>).
    /// A slice that answers <paramref name="limit"/> cells holds exactly the first <paramref name="limit"/> cells of the whole-window read. A shorter
    /// slice (empty bins, a database filter, a metric that is null on some rows, a quiet server) is read again over the whole window.
    /// <paramref name="boundTheRead"/> false reads the whole window at once.</para>
    /// </summary>
    internal static async Task<HeatmapRead> GetQueryHeatmapRoutedAsync(
        NpgsqlDataSource postgres, RollupCoverage coverage, int serverId, HeatmapMetric metric, DateTime startUtc, DateTime endUtc,
        DatabaseFilter databases, int bucketMinutes, int limit, int previewLength, CancellationToken cancellationToken = default, bool boundTheRead = true)
    {
        var plan = PlanRollup(coverage, metric, bucketMinutes, startUtc, endUtc);
        var source = plan is null ? SourceRaw : SourceHourlyRollup;
        var statements = 0;

        var floor = boundTheRead ? NewestBinsFloor(startUtc, endUtc, bucketMinutes, limit) : null;
        if (floor is DateTime boundary)
        {
            var sliceStart = boundary.AddTicks(-MicrosecondTicks);
            var slicePlan = plan is not null && plan.FromUtc < boundary ? plan with { FromUtc = boundary } : plan;
            var slice = await RunAsync(postgres, serverId, metric, sliceStart, endUtc, databases, bucketMinutes, limit, previewLength, slicePlan, cancellationToken);
            statements++;
            if (slice.Count >= limit)
            {
                return new HeatmapRead(slice, source, plan, boundary, statements);
            }
        }

        var whole = await RunAsync(postgres, serverId, metric, startUtc, endUtc, databases, bucketMinutes, limit, previewLength, plan, cancellationToken);
        return new HeatmapRead(whole, source, plan, startUtc, statements + 1);
    }

    /// <summary>Runs the raw statement, or the rollup statement when <paramref name="plan"/> is set, over (<paramref name="readFromUtc"/>, <paramref name="endUtc"/>].</summary>
    private static async Task<List<HeatmapCellRow>> RunAsync(
        NpgsqlDataSource postgres, int serverId, HeatmapMetric metric, DateTime readFromUtc, DateTime endUtc,
        DatabaseFilter databases, int bucketMinutes, int limit, int previewLength, HeatmapRollupPlan? plan, CancellationToken cancellationToken)
    {
        if (plan is null)
        {
            return await GetQueryHeatmapAsync(postgres, serverId, metric, readFromUtc, endUtc, databases, bucketMinutes, limit, previewLength, cancellationToken);
        }

        var rows = new List<HeatmapCellRow>();
        await using var command = postgres.CreateCommand(BuildQueryHeatmapRollupSql(metric, plan.HourlyRelation));
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, readFromUtc, endUtc);
        command.Parameters.Add(databases.Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.FromUtc, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.ToUtc, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadCell(reader, previewLength));
        }

        return rows;
    }

    /// <summary>One cell row, in the column order both statements return. The statement text comes back WHOLE and the statement filter judges
    /// it before the preview is cut (#5320); a named statement is the placeholder (no cut).</summary>
    private static HeatmapCellRow ReadCell(NpgsqlDataReader reader, int previewLength)
    {
        var fetchedText = SensitiveStatements.Text(reader.IsDBNull(4) ? "" : reader.GetString(4)) ?? "";
        var truncated = fetchedText.Length > previewLength;
        return new HeatmapCellRow(
            reader.GetDateTime(0),
            reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
            reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
            reader.IsDBNull(3) ? "" : reader.GetString(3),
            truncated ? fetchedText[..McpHelpers.TextElementCutLength(fetchedText, previewLength)] : fetchedText,
            truncated);
    }

    /// <summary>Runs <see cref="BuildQueryHeatmapSql"/>. Rows come back newest bin first. Judges
    /// each cell's whole query text (#5320), then cuts it to <paramref name="previewLength"/> before it reaches
    /// <see cref="HeatmapCellRow.TopQueryText"/>; the truncation signal (#4198) is the judged text running past it.</summary>
    public static async Task<List<HeatmapCellRow>> GetQueryHeatmapAsync(
        NpgsqlDataSource postgres, int serverId, HeatmapMetric metric, DateTime startUtc, DateTime endUtc,
        DatabaseFilter databases, int bucketMinutes, int limit, int previewLength, CancellationToken cancellationToken = default)
    {
        var rows = new List<HeatmapCellRow>();
        await using var command = postgres.CreateCommand(BuildQueryHeatmapSql(metric));
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        command.Parameters.Add(databases.Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadCell(reader, previewLength));
        }

        return rows;
    }

    /// <summary>Runs <see cref="HeatmapCoverageSql"/>.</summary>
    public static async Task<(bool HasAny, bool HasInWindow)> GetCoverageAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(HeatmapCoverageSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return (false, false);
        return (reader.GetBoolean(0), reader.GetBoolean(1));
    }
}
