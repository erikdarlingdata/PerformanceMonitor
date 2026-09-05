/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

/// <summary>One heatmap cell as the MCP read returns it — the flat twin of the chart's
/// <see cref="HeatmapCell"/>, which is a member of a 2-D grid. Darling twin:
/// <c>DarlingQueryHeatmapReader.HeatmapCellRow</c>.</summary>
public sealed class QueryHeatmapCellRow
{
    public DateTime TimeBucket { get; set; }
    public int BucketIndex { get; set; }
    public long QueryCount { get; set; }
    public string TopQueryHash { get; set; } = "";
    public string TopQueryText { get; set; } = "";
}

/*
 * The Query Heatmap read behind get_query_heatmap (#2484) — the same query GetQueryHeatmapAsync runs for
 * the WPF chart, returning FLAT cells instead of assembling the [bucket, timeBin] grid the chart plots.
 * Two deliberate differences from the chart's copy, both of them things an MCP read needs and a chart
 * does not:
 *
 *   (1) the bin width is a bound parameter rather than the literal INTERVAL '5 minutes'. It DEFAULTS to
 *       that same 5, which is the whole point: the desktop viewer hardcodes 5 minutes whatever range is on
 *       screen, so an agent and a desktop looking at the same server over the same window get the same
 *       picture unless the caller asks for something else.
 *
 *   (2) time_bucket is given an explicit ORIGIN of the Unix epoch. This is invisible at the default and
 *       load-bearing once the width is a parameter: DuckDB's time_bucket defaults to a 2000-01-03 origin
 *       while Postgres date_bin (Darling's twin, and the viewer's) is passed the epoch. The two origins are
 *       15,780,960 minutes apart, which 5 divides exactly — so 5-minute bins agree by luck — but 7 does
 *       not, and the two SKUs would bin the same row one minute apart. Verified identical across
 *       {1, 5, 7, 13, 60, 90, 360, 1440}-minute strides on DuckDB 1.5.5 and PostgreSQL 17.
 *
 * The chart's copy in LocalDataService.QueryStats.cs is left alone: it only ever asks for 5 minutes, where
 * the two origins agree.
 */
public partial class LocalDataService
{
    /// <summary>The desktop viewer's bin width, and therefore this read's default.</summary>
    public const int ViewerHeatmapBucketMinutes = 5;

    /// <summary>The widest bin a caller may ask for: one day.</summary>
    public const int MaxHeatmapBucketMinutes = 1440;

    /// <summary>
    /// The heatmap cells over [now - <paramref name="hoursBack"/>, now], newest bin first so a capped call
    /// keeps the RECENT end of the window rather than the oldest bins.
    /// </summary>
    public async Task<List<QueryHeatmapCellRow>> GetQueryHeatmapCellsAsync(
        int serverId, HeatmapMetric metric, int hoursBack = 24,
        int bucketMinutes = ViewerHeatmapBucketMinutes, int maxRows = 500,
        IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc, utcOffsetMinutes: 0);
        var metricExpr = GetMetricColumn(metric);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
        var bucketIndex = 4 + dbValues.Count;
        var limitIndex = 5 + dbValues.Count;

        command.CommandText = $@"
WITH per_query AS (
    SELECT
        time_bucket(to_minutes(CAST(${bucketIndex} AS INTEGER)), collection_time, TIMESTAMP '1970-01-01 00:00:00') AS time_bin,
        {metricExpr} AS metric_value,
        query_hash,
        LEFT(query_text, 120) AS query_preview,
        delta_execution_count
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   delta_execution_count > 0
    AND   {metricExpr} IS NOT NULL{dbClause}
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
    FROM per_query
)
SELECT
    time_bin,
    bucket_index,
    COUNT(*) AS query_count,
    ARG_MAX(query_hash, delta_execution_count) AS top_query_hash,
    ARG_MAX(query_preview, delta_execution_count) AS top_query_text
FROM binned
GROUP BY time_bin, bucket_index
ORDER BY time_bin DESC, bucket_index
LIMIT ${limitIndex}";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });
        command.Parameters.Add(new DuckDBParameter { Value = maxRows });

        var rows = new List<QueryHeatmapCellRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new QueryHeatmapCellRow
            {
                TimeBucket = reader.GetDateTime(0),
                BucketIndex = reader.IsDBNull(1) ? 0 : (int)ToDouble(reader.GetValue(1)),
                QueryCount = reader.IsDBNull(2) ? 0 : (long)ToDouble(reader.GetValue(2)),
                TopQueryHash = reader.IsDBNull(3) ? "" : reader.GetString(3),
                TopQueryText = reader.IsDBNull(4) ? "" : reader.GetString(4),
            });
        }

        return rows;
    }

    /// <summary>
    /// Whether this server has query stats AT ALL, and whether it has any inside the window.
    /// <para>One round trip for the two facts that decide what an empty grid means, run only on the empty
    /// path. It probes the DATA rather than SUCCESS rows in the collection log, and that is a judgement
    /// about which kind of table this is: <c>query_stats</c> is PERIODIC, not an edge table — the collector
    /// writes rows every cycle for whatever is in the plan cache, so a server with no rows at all is a
    /// server nobody collected. Reads <c>v_query_stats</c>, the same view the read itself uses. Darling
    /// twin: <c>DarlingQueryHeatmapReader.HeatmapCoverageSql</c>.</para>
    /// </summary>
    public async Task<(bool HasAny, bool HasInWindow)> GetQueryHeatmapCoverageAsync(
        int serverId, int hoursBack = 24, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc, utcOffsetMinutes: 0);

        command.CommandText = @"
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
    ) AS has_in_window";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return (false, false);
        return (reader.GetBoolean(0), reader.GetBoolean(1));
    }

    /// <summary>The viewer's per-metric magnitude labels, verbatim — the same seven rows the chart draws.
    /// Returned with every MCP result because a bare bucket_index is unreadable without them, and the two
    /// metric families label the same buckets differently (milliseconds vs plain counts).</summary>
    public static string[] HeatmapBucketLabelsFor(HeatmapMetric metric) => BucketLabelsMap[metric];

    /// <summary>The snake_case name a caller passes, and the one echoed back in the result.</summary>
    public static string HeatmapMetricName(HeatmapMetric metric) => metric switch
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
    public static string HeatmapMetricUnit(HeatmapMetric metric) => metric switch
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
    /// no sign anything went wrong. Darling's twin accepts exactly these five names.</summary>
    public static bool TryParseHeatmapMetric(string? metric, out HeatmapMetric parsed)
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
}
