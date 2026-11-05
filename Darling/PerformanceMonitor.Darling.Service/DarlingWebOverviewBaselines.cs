/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #6291: the web Overview's baseline read, <c>get_overview_baselines</c>. Web only, with no MCP tool behind it (see
/// <see cref="DarlingWebEndpoints.WebOnlyReadNames"/>; agents already get baselines through analyze_server): the strips
/// draw the Viewer's band and anomaly markers from it. It asks the SAME provider the Viewer's Overview lanes ask
/// (<see cref="PgBaselineProvider.GetBaselineAsync(int, string, DateTime, CancellationToken)"/>, as
/// <c>ViewerDataService.GetBaselineForLaneAsync</c> does) for the same four metrics at the window START, and adds no
/// statistics of its own: the page turns each bucket into mean +- 2 effective sigma, as the Viewer's lanes do.
/// </summary>
internal static class DarlingWebOverviewBaselines
{
    /// <summary>The four lane metrics, in the order the Viewer's Overview fetches them, with the name the page keys them by.</summary>
    private static readonly (string Key, string Metric)[] Lanes =
    [
        ("cpu", MetricNames.Cpu),
        ("waits", MetricNames.WaitMsPerSec),
        ("io", MetricNames.IoLatency),
        ("blocking", MetricNames.BlockingPerMinute),
    ];

    /* One provider per store connection pool, so the hourly cache the provider keeps (the I/O baseline is an expensive compute) survives
       between page loads. It borrows the process's shared tier, so a pass the worker already computed is not computed again. */
    private static readonly ConditionalWeakTable<NpgsqlDataSource, PgBaselineProvider> Providers = new();

    private static PgBaselineProvider ProviderFor(NpgsqlDataSource postgres, DarlingAnalysisService? analysis)
        => Providers.GetValue(postgres, p => new PgBaselineProvider(p, null, analysis?.SharedBaselineCache));

    public static async Task<string> GetOverviewBaselines(
        NpgsqlDataSource postgres, DarlingAnalysisService? analysis, string? serverName, string windowStart, CancellationToken cancellationToken)
    {
        /* The window start exactly as the page sends it: ISO 8601, UTC. The baseline buckets key on hour-of-day and day-of-week of the store's UTC stamps. */
        if (!DarlingMcpPlanTools.TryParseCollectionTime(windowStart, out var startUtc))
            return McpHelpers.Refusal("window_start", "Expected the window start as an ISO 8601 UTC time.");

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, serverName, cancellationToken);
        if (error != null) return error;

        try
        {
            var provider = ProviderFor(postgres, analysis);
            var buckets = await ReadLaneBucketsAsync(provider, resolved.ServerId, startUtc, DateTime.UtcNow, cancellationToken);

            var baselines = new JsonObject();
            var any = false;
            for (var i = 0; i < Lanes.Length; i++)
            {
                var bucket = buckets[i];
                var usable = Usable(bucket);
                any |= usable;
                baselines[Lanes[i].Key] = usable ? Bucket(bucket) : null;
            }

            /* Not enough history is its own answer, never a band at 0: the page draws no band and no markers and says why. */
            if (!any)
                return McpHelpers.Status("empty",
                    "Not enough history yet to draw a baseline: the store holds no samples of CPU, waits, I/O latency or blocking for this hour of the week.");

            return new JsonObject
            {
                ["status"] = "ok",
                ["server"] = resolved.DisplayName,
                ["window_start"] = startUtc.ToString("o", CultureInfo.InvariantCulture),
                ["baselines"] = baselines,
            }.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_overview_baselines", ex);
        }
    }

    /// <summary>
    /// #7644: the four lanes' buckets at the window start, in <see cref="Lanes"/> order. The series is computed and cached
    /// at <paramref name="nowUtc"/>, not at the window start: the answer holds all 168 hour-of-week buckets, and a
    /// daily-cache arm (I/O latency, CPU, blocking) keys on the UTC day of the instant it is asked at, so a window
    /// start of now minus 24 hours landed on yesterday's key, which the worker's pass never fills. Keyed on now it is the
    /// key the worker uses and the shared tier holds.
    /// </summary>
    internal static async Task<BaselineBucket[]> ReadLaneBucketsAsync(
        PgBaselineProvider provider, int serverId, DateTime windowStartUtc, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var tasks = new Task<BaselineBucket>[Lanes.Length];
        for (var i = 0; i < Lanes.Length; i++)
            tasks[i] = provider.GetBaselineSharedAsync(serverId, Lanes[i].Metric, windowStartUtc, nowUtc, cancellationToken);
        return await Task.WhenAll(tasks);
    }

    /// <summary>The Viewer's own guard: a bucket with samples and a dispersion above zero draws a band; anything else draws none.</summary>
    internal static bool Usable(BaselineBucket bucket) => bucket.SampleCount > 0 && bucket.EffectiveStdDev > 0;

    internal static JsonObject Bucket(BaselineBucket bucket) => new()
    {
        ["mean"] = bucket.Mean,
        ["effective_std_dev"] = bucket.EffectiveStdDev,
        ["sample_count"] = bucket.SampleCount,
        ["tier"] = bucket.Tier.ToString(),
    };
}
