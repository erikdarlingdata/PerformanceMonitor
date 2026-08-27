// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Time-series reads for PostgreSQL (#2663). The service had fourteen trend reads and none worked on a
/// PostgreSQL target, so every PostgreSQL answer described one window and none of them answered "is this
/// getting worse".
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgTrendTools
{
    [McpServerTool(Name = "get_pg_wait_trend"), Description("Gets a time series for ONE PostgreSQL wait event: how much the server waited on it in each collection interval, normalised per second. Use get_pg_wait_sampling first to find which events dominate, then this to see whether one is growing. Summed across the queries that waited, because this answers a question about the SERVER - per-query attribution for a single window is what get_pg_wait_sampling already gives. The figures are estimates from a sampling profiler (samples x profile period), not measured durations, so treat the SHAPE as the finding rather than the absolute number. An interval where pg_wait_sampling's profile was reset - by pg_wait_sampling_reset_profile() or a server restart - is flagged, and reports everything since the reset rather than a misleadingly quiet interval.")]
    public static async Task<string> GetPgWaitTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("The exact wait event name, e.g. DataFileRead, WALWrite. Omit to follow whichever event dominates the window.")] string? wait_event = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var start = windowEnd.AddHours(-hours_back);

            /* With no event named, follow the one that actually dominates this server rather than a name
               somebody guessed. Which one was chosen is reported, so the answer is never about a different
               event than the reader thinks. */
            var chosen = string.IsNullOrWhiteSpace(wait_event)
                ? await DarlingPgTrendReader.GetDominantWaitEventAsync(postgres, resolved.ServerId, start, windowEnd)
                : wait_event.Trim();

            if (string.IsNullOrWhiteSpace(chosen))
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_wait_sampling")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No wait event was sampled on {resolved.ServerName} in the last {hours_back} "
                        + "hour(s), so there is nothing to follow. pg_wait_sampling needs the extension "
                        + "loaded; get_pg_extensions reports whether it is.");
            }

            var points = await DarlingPgTrendReader.GetWaitTrendAsync(
                postgres, resolved.ServerId, chosen, start, windowEnd);

            if (points.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_wait_sampling")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No samples for wait event '{chosen}' on {resolved.ServerName} in the last "
                        + $"{hours_back} hour(s). A trend needs at least TWO snapshots to difference, so a "
                        + "window holding one collection is legitimately empty here even when the event is "
                        + "being sampled. get_pg_wait_sampling lists the events this server does record.");
            }

            var resets = points.Count(p => p.CounterReset);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                wait_event = chosen,
                /* Said plainly when it was not asked for, so a caller never reads this as an answer about
                   the event they had in mind. */
                wait_event_source = string.IsNullOrWhiteSpace(wait_event)
                    ? "chosen automatically: the most-sampled event in this window that is an actual WAIT. "
                      + "The CPU class - pg_wait_sampling's 'Running', meaning the backend was not waiting - "
                      + "is excluded from the choice because it dominates any healthy server's profile and "
                      + "would answer the opposite of the question. Name it explicitly to follow it."
                    : "as requested",
                hours_back,
                status = "wait_trend",
                point_count = points.Count,
                counter_reset_count = resets,
                note = "estimated_wait_ms_per_second is samples x the profiler's period, over the interval's "
                     + "length - an estimate from a sampling profiler, not a measured duration, so the shape "
                     + "over time is the finding rather than the absolute value. Per SECOND because "
                     + "collection intervals are not uniform: a restart or a slow cycle stretches one, and a "
                     + "per-interval total would render that as a spike."
                     + (resets > 0
                         ? $"  {resets} interval(s) span a profile RESET - pg_wait_sampling_reset_profile() "
                           + "or a server restart - and report everything since the reset rather than a "
                           + "quiet interval, which is what clamping the difference at zero would have shown."
                         : string.Empty),
                points = points.Select(p => new
                {
                    collection_time = p.CollectionTimeUtc,
                    sample_count = p.SampleCount,
                    estimated_wait_ms_per_second = Math.Round(p.EstimatedWaitMsPerSecond, 3),
                    backend_count = p.BackendCount,
                    counter_reset = p.CounterReset,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading the PostgreSQL wait trend failed: {ex.Message}");
        }
    }

    [McpServerTool(Name = "get_pg_query_duration_trend"), Description("Gets a time series for ONE PostgreSQL statement by queryid: what a single execution cost in each collection interval, how many times it ran, and its call rate. This is the regression read - a query whose mean execution time steps up and stays up has changed plan or lost an index, and the step is visible here where a single-window average hides it. Use get_pg_top_queries first to get a queryid. mean_exec_ms is null rather than zero for an interval where the statement did not run, because a mean over no calls is absent rather than fast.")]
    public static async Task<string> GetPgQueryDurationTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("The queryid from get_pg_top_queries; PostgreSQL query ids can be negative. Omit to follow the statement that spent the most time in the window.")] string? queryid = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        long parsedQueryId;
        var requested = !string.IsNullOrWhiteSpace(queryid);

        /* Taken as TEXT and parsed here: a PostgreSQL query id is a signed 64-bit hash that routinely
           exceeds what a JSON number survives intact, and a client that rounds one silently asks about a
           statement that does not exist. */
        if (requested && !long.TryParse(queryid!.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out parsedQueryId))
        {
            return McpHelpers.Status(
                "error",
                $"queryid '{queryid}' is not a 64-bit integer. PostgreSQL query ids are signed and often "
                + "negative; pass the value from get_pg_top_queries exactly as it appears.");
        }

        try
        {
            var start = windowEnd.AddHours(-hours_back);

            if (!requested)
            {
                var top = await DarlingPgTrendReader.GetTopQueryIdAsync(postgres, resolved.ServerId, start, windowEnd);

                if (top is null)
                {
                    return await DarlingEngineCapability.NotCollectedStatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_statement_stats")
                        ?? McpHelpers.Status(
                            "empty",
                            $"No statement recorded execution time on {resolved.ServerName} in the last "
                            + $"{hours_back} hour(s), so there is nothing to follow.");
                }

                parsedQueryId = top.Value;
            }
            else
            {
                parsedQueryId = long.Parse(queryid!.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture);
            }

            var points = await DarlingPgTrendReader.GetQueryDurationTrendAsync(
                postgres, resolved.ServerId, parsedQueryId, start, windowEnd);

            if (points.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_statement_stats")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No samples for queryid {parsedQueryId} on {resolved.ServerName} in the last "
                        + $"{hours_back} hour(s). pg_stat_statements evicts statements under memory "
                        + "pressure, so a queryid that was there yesterday can be gone rather than idle - "
                        + "get_pg_top_queries shows what the server currently tracks.");
            }

            var ran = points.Where(p => p.Calls > 0).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                queryid = parsedQueryId.ToString(CultureInfo.InvariantCulture),
                queryid_source = requested
                    ? "as requested"
                    : "chosen automatically: the statement with the most execution time in this window",
                hours_back,
                status = "query_duration_trend",
                point_count = points.Count,
                intervals_with_calls = ran.Count,
                /* The two ends of the series that actually ran, so a step is visible without reading every
                   point - and null when nothing ran, rather than a shape invented from no executions. */
                first_mean_exec_ms = ran.Count > 0 ? Math.Round(ran[0].MeanExecMs, 3) : (double?)null,
                last_mean_exec_ms = ran.Count > 0 ? Math.Round(ran[^1].MeanExecMs, 3) : (double?)null,
                note = "mean_exec_ms is the interval's total execution time over its calls - what ONE "
                     + "execution cost then. It is null for an interval with no calls, because a mean over "
                     + "no executions is absent rather than zero. A step that persists is a plan or index "
                     + "change; a spike that recovers is usually contention, which get_pg_wait_trend and "
                     + "get_pg_blocking speak to.",
                points = points.Select(p => new
                {
                    collection_time = p.CollectionTimeUtc,
                    calls = p.Calls,
                    total_exec_ms = Math.Round(p.TotalExecMs, 3),
                    mean_exec_ms = p.Calls > 0 ? Math.Round(p.MeanExecMs, 3) : (double?)null,
                    calls_per_second = Math.Round(p.CallsPerSecond, 4),
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading the PostgreSQL query duration trend failed: {ex.Message}");
        }
    }
}
