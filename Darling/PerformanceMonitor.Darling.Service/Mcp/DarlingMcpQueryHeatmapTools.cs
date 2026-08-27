/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// get_query_heatmap (#2484) — the viewer's Queries &gt; Query Heatmap tab, the last of the ten viewer
/// surfaces that had no <c>/api/read</c> endpoint.
///
/// <para>The interactive plot is desktop-only by design (#2484 group (c)); the READ behind it is not, and a
/// bucketed table is the same answer in a shape a browser and an agent can both take. What it adds over
/// every other query read is the TIME axis: <c>get_top_queries_by_cpu</c> ranks a whole window and cannot
/// show that the window had a quiet half and a bad half, which is the question anyone asks first about an
/// incident that has already ended.</para>
///
/// <para>A STORED read over <see cref="DarlingQueryHeatmapReader"/>, no live monitored-server hit.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpQueryHeatmapTools
{
    /// <summary>The web panel's cap and this tool's default: 500 cells, which is a full day of 5-minute bins
    /// on a server whose queries land in two or three magnitude buckets per bin.</summary>
    public const int DefaultCellLimit = 500;

    [McpServerTool(Name = "get_query_heatmap"), Description("Draws the desktop viewer's Query Heatmap as a table: how many distinct queries fell into each (time bin x log-magnitude bucket) cell over a window, plus the most-executed query in each cell. It answers when a server was slow and how slow at the same time - get_top_queries_by_cpu ranks queries over a whole window and cannot show that the window had two very different halves. Bins are 5 minutes wide by default because that is exactly what the desktop viewer uses, so a browser, an agent and a desktop pointed at the same server draw the same picture; raise bucket_minutes for a longer window, which is also the lever that fits more of the window inside the cell cap. Magnitude buckets are the viewer's seven, in the metric's own unit: under 1, 1-10, 10-100, 100-1K, 1K-10K, 10K-100K and over 100K.")]
    public static async Task<string> GetQueryHeatmap(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("How far back to look, in hours. Default 24.")] int hours_back = 24,
        [Description("Which per-execution metric to bucket by: duration, cpu, logical_reads, logical_writes or execution_count. Default duration.")] string? metric = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        [Description("Width of each time bin, in minutes. Default 5 - the desktop viewer's own bin width, so the two surfaces agree. Raise it to cover a longer window in fewer cells.")] int bucket_minutes = DarlingQueryHeatmapReader.ViewerBucketMinutes,
        [Description("Maximum CELLS to return, most recent bins first. Default 500. A full day of 5-minute bins can reach 2,016 cells on a busy server; raise bucket_minutes rather than the cap to see the whole window.")] int limit = DefaultCellLimit,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd)
            ?? McpHelpers.ValidateTop(limit)
            ?? ValidateBucketMinutes(bucket_minutes);
        if (validation != null) return validation;

        /* A metric we do not know is REFUSED, not quietly turned into duration: a caller who asked for CPU
           and silently got elapsed time would read the wrong grid with nothing to tell them so. */
        if (!DarlingQueryHeatmapReader.TryParseMetric(metric, out var parsedMetric))
            return InvalidMetric(metric!);

        try
        {
            /*
                #2495: the anchor, and it earns its place here more than on most reads. A heatmap IS a time
                axis, so "the 4 hours ending Tuesday 03:00" is the shape of every question anyone brings to
                it — and widening hours_back until an old incident falls inside is not the same question,
                because the extra hours land as extra COLUMNS that push the incident's own columns past the
                cell cap.
            */
            var end = windowEnd;
            var start = end.AddHours(-hours_back);

            /*
                Over-fetch by one. Comparing the row count to the cap reports truncation for a server that
                happens to have exactly `limit` cells and nothing more, which is a false positive in the one
                field whose whole reason for existing is that the cap should not have to be inferred.
            */
            var rows = await DarlingQueryHeatmapReader.GetQueryHeatmapAsync(
                postgres, resolved.ServerId, parsedMetric, start, end, database_name, bucket_minutes, limit + 1);

            if (rows.Count == 0)
                return await EmptyAsync(postgres, resolved.ServerName, resolved.ServerId, start, end, hours_back);

            var truncated = rows.Count > limit;
            var cells = rows.Take(limit).ToList();

            if (truncated)
            {
                /*
                    The rows arrive newest-bin-first so the cap keeps the RECENT end of the window, which is
                    what anyone looking at an incident wants. The cost is that the cap can land in the middle
                    of the oldest bin it reached, handing back a column missing its low buckets — and a column
                    with holes reads as "nothing fast ran then" rather than "we stopped looking", which is the
                    kind of quiet wrong answer a grid makes very easy to believe. So the partial column goes.
                    Kept when it is the ONLY column (a cap below one bin's seven cells has nothing to fall
                    back to); `truncated` and last_time_bin still say what happened.
                */
                var oldestReached = cells[^1].TimeBucket;
                var wholeColumns = cells.Where(c => c.TimeBucket > oldestReached).ToList();
                if (wholeColumns.Count > 0) cells = wholeColumns;
            }

            /*
                Back into reading order: time ascending, and buckets ascending WITHIN each bin. A plain
                Reverse() would hand back the bins in the right order with each bin's buckets upside down,
                because the SQL sorts time DESC and bucket ASC. The DESC exists only so the cap cuts the
                right end of the window; it should not leak into the shape of the grid.
            */
            cells = cells.OrderBy(c => c.TimeBucket).ThenBy(c => c.BucketIndex).ToList();

            var labels = DarlingQueryHeatmapReader.BucketLabels[parsedMetric];

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                metric = DarlingQueryHeatmapReader.MetricName(parsedMetric),
                metric_unit = DarlingQueryHeatmapReader.MetricUnit(parsedMetric),
                database_name,
                /* The window that was QUERIED, anchored or not — the caller reads the bins against it. */
                window_start = start.ToString("o"),
                window_end = end.ToString("o"),
                bucket_minutes,
                /* The same bin width the desktop viewer hardcodes, so the two surfaces cannot disagree
                   about the same server over the same window. */
                bucket_minutes_matches_desktop_viewer = bucket_minutes == DarlingQueryHeatmapReader.ViewerBucketMinutes,
                /* A bare bucket_index is unreadable, and the labels differ by metric family: duration and
                   CPU are milliseconds, the other three are counts. */
                magnitude_buckets = labels.Select((label, index) => new { bucket_index = index, label }),
                time_bin_count = cells.Select(c => c.TimeBucket).Distinct().Count(),
                cell_count = cells.Count,
                /* Which slice of [window_start, window_end] actually came back. When truncated is true the
                   read dropped the OLDEST bins, not the least interesting cells, so these two are the only
                   way to see how much of the window is missing. */
                first_time_bin = cells[0].TimeBucket.ToString("o"),
                last_time_bin = cells[^1].TimeBucket.ToString("o"),
                truncated,
                cells = cells.Select(c => new
                {
                    time_bucket = c.TimeBucket.ToString("o"),
                    bucket_index = c.BucketIndex,
                    bucket_label = labels[Math.Clamp(c.BucketIndex, 0, DarlingQueryHeatmapReader.BucketCount - 1)],
                    /* Distinct queries in the cell, NOT executions — a cell of 40 is forty different queries
                       that ran at that speed, which is a different finding from one query running 40 times. */
                    query_count = c.QueryCount,
                    top_query_hash = c.TopQueryHash,
                    top_query_text = c.TopQueryText,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_heatmap", ex);
        }
    }

    /// <summary>
    /// What an empty grid means, which is three different things.
    /// <para>The probe reads the DATA rather than counting SUCCESS runs in <c>collection_log</c>, and that is
    /// a judgement about which kind of table this is. <c>query_stats</c> is PERIODIC: the collector writes
    /// rows every cycle for whatever is in the plan cache, so an empty history really does mean nobody
    /// looked. An edge table — blocking, deadlocks — would need the opposite treatment, because there zero
    /// rows is the healthy answer and a data probe sends someone to fix collection that works.</para>
    /// <para>The third branch is the one only this read has: collection ran, the window has captures, and
    /// every one of them recorded zero executions. That is an IDLE server, not a broken one, and telling a
    /// caller to widen the window there would be advice pointed at the wrong problem.</para>
    /// </summary>
    private static async Task<string> EmptyAsync(
        NpgsqlDataSource postgres, string serverName, int serverId, DateTime start, DateTime end, int hours_back)
    {
        var (hasAny, hasInWindow) = await DarlingQueryHeatmapReader.GetCoverageAsync(postgres, serverId, start, end);

        if (!hasAny)
        {
            return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, serverId, serverName, "query_stats")
                ?? McpHelpers.Status(
                    "unavailable",
                    $"No query stats have EVER been collected for {serverName}, so this is NOT a report of a quiet server — there is nothing to draw. query_stats is a PERIODIC table rather than an edge table: the collector writes rows every cycle for whatever is in the plan cache, so an empty history means nobody looked. Check get_collection_health for this server.");
        }

        if (!hasInWindow)
        {
            return McpHelpers.Status(
                "empty",
                $"{serverName} has query stats from outside this window but nothing collected IN the last {hours_back} hour(s), so the grid has no columns rather than no hot cells. Widen hours_back, or check get_collection_health — a collector that stopped looks exactly like this.");
        }

        return McpHelpers.Status(
            "empty",
            $"Query stats WERE collected for {serverName} in the last {hours_back} hour(s), but no capture recorded an execution: every row carried a zero execution delta, so nothing lands on the grid. A server that is up and idle looks exactly like this, and so does a database_name filter matching nothing collected. Delta-based collection also needs a SECOND cycle before the first non-zero row exists.");
    }

    /// <summary>The bin-width bound. Refuses out of range rather than clamping, for the same reason the row
    /// cap does: a silently rewritten bin width draws a different grid than the one that was asked for.</summary>
    private static string? ValidateBucketMinutes(int bucket_minutes) =>
        bucket_minutes >= 1 && bucket_minutes <= DarlingQueryHeatmapReader.MaxBucketMinutes
            ? null
            : $"Invalid bucket_minutes value '{bucket_minutes}'. Must be between 1 and 1440 (one day). The desktop viewer's Query Heatmap uses 5, which is this read's default.";

    private static string InvalidMetric(string metric) =>
        $"Invalid metric '{metric}'. Valid values: duration, cpu, logical_reads, logical_writes, execution_count.";
}
