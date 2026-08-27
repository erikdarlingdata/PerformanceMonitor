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
using NpgsqlTypes;

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
/// heatmap. Copied VERBATIM apart from the two things a desktop chart does not need and an MCP read does:
/// the bin width is a bound parameter instead of the literal <c>INTERVAL '5 minutes'</c> (defaulting to
/// that same 5), and the tail carries <c>ORDER BY time_bin DESC</c> + <c>LIMIT</c> so a capped call keeps
/// the most RECENT bins rather than the oldest ones. Every other clause — the magnitude CASE, the
/// <c>delta_execution_count &gt; 0</c> and <c>metric IS NOT NULL</c> filters, the <c>LEFT(query_text, 120)</c>
/// preview, the top-1 window that replaced DuckDB's <c>ARG_MAX</c> — is the viewer's.
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
    /// query in it, which is what the desktop shows on hover.</summary>
    public sealed record HeatmapCellRow(
        DateTime TimeBucket,
        int BucketIndex,
        long QueryCount,
        string TopQueryHash,
        string TopQueryText);

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
    /// filter (text[] or NULL), $5 bin width in minutes, $6 cell cap.
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
                    LEFT(query_text, 120) AS query_preview,
                    delta_execution_count
                FROM v_query_stats
                WHERE server_id = $1
                AND   collection_time >= $2
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
                    query_preview,
                    delta_execution_count
                FROM base
            ),
            ranked AS (
                SELECT
                    time_bin,
                    bucket_index,
                    query_hash,
                    query_preview,
                    COUNT(*) OVER (PARTITION BY time_bin, bucket_index) AS query_count,
                    ROW_NUMBER() OVER (PARTITION BY time_bin, bucket_index ORDER BY delta_execution_count DESC) AS rn
                FROM binned
            )
            SELECT
                time_bin,
                bucket_index,
                query_count,
                query_hash AS top_query_hash,
                query_preview AS top_query_text
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
    /// <para>Probes <c>v_query_stats</c>, the same relation the read itself uses, so the probe cannot
    /// disagree with the read about which rows exist. $1 server_id, $2 window start, $3 window end.</para>
    /// </summary>
    public const string HeatmapCoverageSql = """
        SELECT
            EXISTS (
                SELECT 1
                FROM v_query_stats
                WHERE server_id = $1
            ) AS has_any,
            EXISTS (
                SELECT 1
                FROM v_query_stats
                WHERE server_id = $1
                AND   collection_time >= $2
                AND   collection_time <= $3
            ) AS has_in_window
        """;

    /// <summary>Runs <see cref="BuildQueryHeatmapSql"/>. Rows come back newest bin first.</summary>
    public static async Task<List<HeatmapCellRow>> GetQueryHeatmapAsync(
        NpgsqlDataSource postgres, int serverId, HeatmapMetric metric, DateTime startUtc, DateTime endUtc,
        string? databaseName, int bucketMinutes, int limit, CancellationToken cancellationToken = default)
    {
        var rows = new List<HeatmapCellRow>();
        await using var command = postgres.CreateCommand(BuildQueryHeatmapSql(metric));
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = string.IsNullOrWhiteSpace(databaseName) ? DBNull.Value : new[] { databaseName },
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new HeatmapCellRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? "" : reader.GetString(4)));
        }

        return rows;
    }

    /// <summary>Runs <see cref="HeatmapCoverageSql"/>.</summary>
    public static async Task<(bool HasAny, bool HasInWindow)> GetCoverageAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(HeatmapCoverageSql);
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return (false, false);
        return (reader.GetBoolean(0), reader.GetBoolean(1));
    }
}
