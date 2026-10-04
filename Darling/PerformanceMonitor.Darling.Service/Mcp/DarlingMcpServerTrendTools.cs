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
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>get_server_trend</c> (#4843): the per-server instance trends the desktop viewer charts on its CPU, Memory and
/// Overview tabs, behind one tool with a <c>metric</c> switch: <c>total_waits</c>, <c>cpu_scheduler</c>,
/// <c>memory_clerks</c> and <c>plan_cache</c>. Every metric reads the same SQL the viewer's chart reads
/// (<see cref="ServerTrendSql"/>), windowed on both sides, bucketed to the same width ladder as the other trend tools
/// (<see cref="TrendBuckets"/>), and ends with the shared <c>discontinuities[]</c> block.
///
/// <para>Points are stamped at the bucket's start. A field with no value is omitted rather than written as null,
/// except a cpu_scheduler count, which the shared SQL averages as 0 when a collection did not report it.
/// An empty window is a status envelope that tells a quiet window from a collector that never ran, and a
/// gated engine from both.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpServerTrendTools
{
    /// <summary>The metrics this tool serves, in the order its description lists them.</summary>
    internal static readonly string[] Metrics = ["total_waits", "cpu_scheduler", "memory_clerks", "plan_cache"];

    /// <summary>The most points an explicitly requested width may put on the wire, across every clerk series.</summary>
    internal const int MaxPoints = 1500;

    /// <summary>How many clerk types <c>memory_clerks</c> follows when the caller names none.</summary>
    internal const int DefaultClerkCount = 5;

    /// <summary>The most clerk types one call may name.</summary>
    internal const int MaxClerkCount = 10;

    private const string TrendGuide =
        " total_waits is every wait type summed into one wait_time_ms_per_second rate (summed wait time over the seconds it covered); a collection whose interval was unknowable is left out, not drawn as 0. cpu_scheduler points average the runnable, blocked and queued task counts; a count a collection did not report averages as 0, not as missing. memory_clerks gives one series per clerk type, in MB; clerk_types names them (comma-separated, up to 10, matched exactly), default the 5 heaviest in the window; named types with no samples in the window come back in missing_clerk_types. plan_cache gives single-use and multi-use plan cache MB. All but total_waits are levels, averaged per bucket.";

    [McpServerTool(Name = "get_server_trend"), Description(
        "Gets one instance trend over time in buckets, ending at as_of. metric is one of total_waits, cpu_scheduler, memory_clerks, plan_cache. A quiet window answers empty; unavailable means the collector has never run; not_collected covers a gated engine. <<GUIDE>>" + TrendGuide + BaselineDiscontinuities.DescriptionSentence)]
    public static Task<string> GetServerTrend(
        NpgsqlDataSource postgres,
        [Description("One of total_waits, cpu_scheduler, memory_clerks, plan_cache.")] string metric,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null,
        [Description("memory_clerks only: clerk types, comma-separated, exact case. Default the 5 heaviest.")] string? clerk_types = null,
        CancellationToken cancellationToken = default) =>
        GetServerTrend(postgres, metric, server_name, hours_back, as_of, bucket_minutes, clerk_types, TrendBudget.Mcp(MaxPoints), cancellationToken);

    /// <summary>get_server_trend under an explicit <paramref name="budget"/>: the MCP tool passes its own, the web
    /// viewer's <c>/api/read</c> mirror passes <see cref="TrendBudget.Chart"/>.</summary>
    internal static async Task<string> GetServerTrend(
        NpgsqlDataSource postgres, string? metric, string? server_name, int hours_back, string? as_of, int? bucket_minutes,
        string? clerk_types, TrendBudget budget, CancellationToken cancellationToken = default)
    {
        var name = (metric ?? string.Empty).Trim().ToLowerInvariant();
        if (!Metrics.Contains(name, StringComparer.Ordinal))
        {
            return McpHelpers.Refusal(
                "metric",
                $"Invalid metric '{metric}'. Must be one of: {string.Join(", ", Metrics)}.");
        }

        if (!string.IsNullOrWhiteSpace(clerk_types) && name != "memory_clerks")
        {
            return McpHelpers.Refusal("clerk_types", $"clerk_types applies to metric memory_clerks only, not {name}. Omit it.");
        }

        var clerks = ParseClerks(clerk_types);
        if (clerks.Count > MaxClerkCount)
        {
            return McpHelpers.Refusal("clerk_types", $"clerk_types names {clerks.Count} clerk types; at most {MaxClerkCount} per call.");
        }

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var widthError = TrendBuckets.ValidateWidth(bucket_minutes);
        if (widthError != null) return widthError;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-hours_back);

            if (name == "memory_clerks")
            {
                if (clerks.Count == 0)
                {
                    clerks = await ReadTopClerksAsync(postgres, resolved.ServerId, start, end, cancellationToken);
                }

                if (clerks.Count == 0)
                {
                    return await EmptyAsync(postgres, resolved, name, hours_back, cancellationToken);
                }
            }

            var seriesCount = name == "memory_clerks" ? clerks.Count : 1;
            var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, seriesCount, budget, out var bucketMinutes);
            if (bucketError != null) return bucketError;

            var envelope = new Dictionary<string, object?>
            {
                ["server"] = resolved.ServerName,
                ["metric"] = name,
                ["hours_back"] = hours_back,
                ["bucket"] = TrendBuckets.Word(bucketMinutes),
                ["bucket_minutes"] = bucketMinutes,
                ["aggregate_note"] = Note(name, bucketMinutes, bucket_minutes is not null, budget.AutoPoints),
            };

            if (name == "memory_clerks")
            {
                var series = await ReadClerkSeriesAsync(postgres, resolved.ServerId, clerks, start, end, bucketMinutes, cancellationToken);
                var named = !string.IsNullOrWhiteSpace(clerk_types);
                if (series.Count == 0)
                {
                    return named
                        ? await NoNamedClerksAsync(postgres, resolved, clerks, start, end, hours_back, cancellationToken)
                        : await EmptyAsync(postgres, resolved, name, hours_back, cancellationToken);
                }

                var missing = named ? clerks.Where(c => !series.ContainsKey(c)).ToList() : [];
                if (missing.Count > 0)
                {
                    envelope["missing_clerk_types"] = missing;
                }

                envelope["series"] = clerks
                    .Where(series.ContainsKey)
                    .Select(c => new Dictionary<string, object?> { ["clerk_type"] = c, ["trend"] = series[c] })
                    .ToList();
            }
            else
            {
                var points = await ReadPointsAsync(postgres, name, resolved.ServerId, start, end, bucketMinutes, cancellationToken);
                if (points.Count == 0)
                {
                    return await EmptyAsync(postgres, resolved, name, hours_back, cancellationToken);
                }

                envelope["trend"] = points;
            }

            var discontinuities = await DarlingTrendReader.GetBaselineDiscontinuitiesAsync(postgres, resolved.ServerId, start, end, cancellationToken);
            envelope[BaselineDiscontinuities.PayloadKey] = BaselineDiscontinuities.ToPayload(discontinuities);
            return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_server_trend", ex);
        }
    }

    /// <summary>The clerk types a caller named: trimmed, de-duplicated, in the order given.</summary>
    internal static List<string> ParseClerks(string? clerkTypes) =>
        string.IsNullOrWhiteSpace(clerkTypes)
            ? []
            : clerkTypes.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).Distinct(StringComparer.Ordinal).ToList();

    /// <summary>The fields each metric's points carry, in column order after the bucket stamp. Notes may name only these.</summary>
    internal static string[] Fields(string metric) => metric switch
    {
        "total_waits" => ["wait_time_ms_per_second"],
        "cpu_scheduler" => ["runnable_tasks", "blocked_tasks", "queued_requests"],
        "memory_clerks" => ["memory_mb"],
        _ => ["single_use_mb", "multi_use_mb"],
    };

    /// <summary>The note that tells a reader how each point was rolled up. It names only fields the metric emits.</summary>
    internal static string Note(string metric, int bucketMinutes, bool requested, int budgetPoints)
    {
        var stamp = $"is stamped at the bucket's start (the first point at the window's start). ";
        if (metric == "total_waits")
        {
            return $"Each point summarizes the collections in one {TrendBuckets.Adjective(bucketMinutes)} bucket and {stamp}"
                + "wait_time_ms_per_second is recomputed from the summed wait time over the seconds the collections covered, never averaged from per-collection rates, so a bucket the window cuts short still holds a true rate. "
                + TrendBuckets.Sizing(requested, budgetPoints);
        }

        var zero = metric == "cpu_scheduler" ? " A count a collection did not report averages as 0." : string.Empty;
        return $"Each point averages the samples in one {TrendBuckets.Adjective(bucketMinutes)} bucket and {stamp}"
            + $"A level is averaged, never summed, so a spike inside a bucket is smoothed into its average; narrow the bucket to see it.{zero} "
            + TrendBuckets.Sizing(requested, budgetPoints);
    }

    /// <summary>Clerk types were named and none has a sample in the window: say so, and list what the window did record.</summary>
    private static async Task<string> NoNamedClerksAsync(
        NpgsqlDataSource postgres, (int ServerId, string ServerName) resolved, List<string> named, DateTime start, DateTime end,
        int hoursBack, CancellationToken cancellationToken)
    {
        var heaviest = await ReadTopClerksAsync(postgres, resolved.ServerId, start, end, cancellationToken);
        if (heaviest.Count == 0)
        {
            return await EmptyAsync(postgres, resolved, "memory_clerks", hoursBack, cancellationToken);
        }

        return McpHelpers.Status(
            "empty",
            $"None of the named clerk types were recorded for {resolved.ServerName} in the last {hoursBack} hour(s). Clerk types are matched exactly, so check the spelling and case. The window does hold other clerk types: heaviest_clerk_types lists the heaviest of them.",
            new Dictionary<string, object?> { ["missing_clerk_types"] = named, ["heaviest_clerk_types"] = heaviest });
    }

    private static (string Sql, string Collector, string EverSql, string What) Describe(string metric) => metric switch
    {
        "total_waits" => (ServerTrendSql.TotalWaits, "wait_stats", ServerTrendSql.HasAnyWaits, "wait statistics"),
        "cpu_scheduler" => (ServerTrendSql.CpuScheduler, "cpu_scheduler_stats", ServerTrendSql.HasAnyCpuScheduler, "CPU scheduler samples"),
        "memory_clerks" => (string.Empty, "memory_clerks", ServerTrendSql.HasAnyMemoryClerks, "memory clerk samples"),
        _ => (ServerTrendSql.PlanCache, "plan_cache_stats", ServerTrendSql.HasAnyPlanCache, "plan cache samples"),
    };

    /// <summary>The two kinds of nothing, plus the gated engine: asked only on the path that already found no rows.</summary>
    private static async Task<string> EmptyAsync(
        NpgsqlDataSource postgres, (int ServerId, string ServerName) resolved, string metric, int hoursBack, CancellationToken cancellationToken)
    {
        var (_, collector, everSql, what) = Describe(metric);
        /* One literal per metric, so the engine-capability census can read which collectors this tool asks about. */
        var gated = metric switch
        {
            "total_waits" => await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "wait_stats", cancellationToken),
            "cpu_scheduler" => await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "cpu_scheduler_stats", cancellationToken),
            "memory_clerks" => await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_clerks", cancellationToken),
            _ => await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "plan_cache_stats", cancellationToken),
        };
        if (gated != null)
        {
            return gated;
        }

        await using var command = postgres.CreateCommand(everSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, resolved.ServerId);
        var everCollected = await command.ExecuteScalarAsync(cancellationToken) is not null;

        return everCollected
            ? McpHelpers.Status(
                "empty",
                $"No {what} recorded for {resolved.ServerName} in the last {hoursBack} hour(s). This server HAS collected them before, so this window is genuinely quiet rather than broken - widen hours_back to find the most recent samples.")
            : McpHelpers.Status(
                "unavailable",
                $"No {what} have EVER been recorded for {resolved.ServerName}. This is not an empty window - the {collector} collector has stored nothing at all for this server. Check that collection is running and that the server is enabled.");
    }

    private static async Task<List<string>> ReadTopClerksAsync(
        NpgsqlDataSource postgres, int serverId, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        var items = new List<string>();
        await using var command = postgres.CreateCommand(ServerTrendSql.TopMemoryClerks);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, start, end);
        DarlingMcpReadParameters.AddInt(command, DefaultClerkCount);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(reader.GetString(0));
        }

        return items;
    }

    private static async Task<Dictionary<string, List<Dictionary<string, object?>>>> ReadClerkSeriesAsync(
        NpgsqlDataSource postgres, int serverId, List<string> clerks, DateTime start, DateTime end, int bucketMinutes,
        CancellationToken cancellationToken)
    {
        var series = new Dictionary<string, List<Dictionary<string, object?>>>(StringComparer.Ordinal);
        await using var command = postgres.CreateCommand(ServerTrendSql.MemoryClerks(clerks.Count));
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, start, end);
        foreach (var clerk in clerks)
        {
            DarlingMcpReadParameters.AddText(command, clerk);
        }

        DarlingMcpReadParameters.AddInt(command, bucketMinutes);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var clerk = reader.GetString(0);
            if (!series.TryGetValue(clerk, out var list))
            {
                list = [];
                series[clerk] = list;
            }

            var point = new Dictionary<string, object?> { ["time"] = Stamp(reader.GetDateTime(1)) };
            AddNumber(point, Fields("memory_clerks")[0], reader, 2);
            list.Add(point);
        }

        return series;
    }

    private static async Task<List<Dictionary<string, object?>>> ReadPointsAsync(
        NpgsqlDataSource postgres, string metric, int serverId, DateTime start, DateTime end, int bucketMinutes,
        CancellationToken cancellationToken)
    {
        var fields = Fields(metric);

        var items = new List<Dictionary<string, object?>>();
        await using var command = postgres.CreateCommand(Describe(metric).Sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, start, end);
        DarlingMcpReadParameters.AddInt(command, bucketMinutes);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var point = new Dictionary<string, object?> { ["time"] = Stamp(reader.GetDateTime(0)) };
            for (var i = 0; i < fields.Length; i++)
            {
                AddNumber(point, fields[i], reader, i + 1);
            }

            items.Add(point);
        }

        return items;
    }

    /// <summary>Adds a number rounded to 2 places, or nothing when the column is NULL.</summary>
    private static void AddNumber(Dictionary<string, object?> point, string key, NpgsqlDataReader reader, int ordinal)
    {
        if (!reader.IsDBNull(ordinal))
        {
            point[key] = Math.Round(reader.GetDouble(ordinal), 2);
        }
    }

    private static string Stamp(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified).ToString("o", CultureInfo.InvariantCulture);
}
