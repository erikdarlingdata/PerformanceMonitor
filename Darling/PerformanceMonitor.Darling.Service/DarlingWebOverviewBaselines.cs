/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
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
/// (<see cref="PgBaselineProvider.GetBaselineStepsSharedAsync"/>, over the same four metrics: CPU, waits, the
/// <see cref="MetricNames.IoLatencyChart"/> arm the Viewer's I/O lane reads, and blocking) and adds no statistics of its own.
/// #7644: the answer is a stepped series, one step per local hour from the window start to now, so a window that crosses
/// hours of the week draws each hour's own band; the page turns each step into mean +- 2 effective sigma, as the Viewer's
/// lanes do. #7663: an hour that sat at exactly 0 is kept as a step marked <c>zero_activity</c> (no band, but not missing history); a lane with
/// no step at all is null, and the page says "not enough history" for it. Because the read costs one step per hour of the window, <c>window_start</c> is bounded:
/// <see cref="ValidateWindowStart"/> refuses one in the future (beyond <see cref="FutureSkew"/>) or older than
/// <see cref="MaxWindowHours"/> plus <see cref="WindowSlack"/> before now, never clamping it.
/// </summary>
internal static class DarlingWebOverviewBaselines
{
    /// <summary>
    /// The longest window the Overview page can ask for, in hours: the raw-table trends' reach (<see cref="WebReadReach.RawTrendHours"/>,
    /// 30 days), the largest <c>max_hours</c> any of the reads <c>overview-strips.js</c> sends <c>ctx.hours</c> to accepts
    /// (get_cpu_utilization, get_blocking_trend, get_deadlock_trend, get_memory_trend and get_file_io_trend; get_server_trend stops at 168).
    /// </summary>
    internal const int MaxWindowHours = WebReadReach.RawTrendHours;

    /// <summary>Room past <see cref="MaxWindowHours"/> for a custom range picked a moment ago: one day.</summary>
    internal static readonly TimeSpan WindowSlack = TimeSpan.FromDays(1);

    /// <summary>A browser clock a few minutes ahead of the service's still sends a window start that is "now" to the page: this much is not refused as future.</summary>
    internal static readonly TimeSpan FutureSkew = TimeSpan.FromMinutes(5);

    /// <summary>The four lane metrics, in the order the Viewer's Overview fetches them, with the name the page keys them by.</summary>
    private static readonly (string Key, string Metric)[] Lanes =
    [
        ("cpu", MetricNames.Cpu),
        ("waits", MetricNames.WaitMsPerSec),
        ("io", MetricNames.IoLatencyChart),
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

        /* #7644: the read costs one step per local hour from the window start to now, so an unbounded start (0001-01-01 is about 70 million steps)
           is refused before anything is read or allocated, and a future one is refused instead of answering "not enough history". */
        var windowError = ValidateWindowStart(startUtc, DateTime.UtcNow);
        if (windowError != null) return windowError;

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, serverName, cancellationToken);
        if (error != null) return error;

        try
        {
            var provider = ProviderFor(postgres, analysis);
            var lanes = await ReadLaneStepsAsync(provider, resolved.ServerId, startUtc, DateTime.UtcNow, cancellationToken);

            return BuildReply(resolved.DisplayName, startUtc, lanes);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_overview_baselines", ex);
        }
    }

    /// <summary>
    /// The reply for the four lanes' steps (in <see cref="Lanes"/> order). A lane with no step is null. A lane holding only zero-activity steps (#7663)
    /// still counts as an answer, so a strip that sat at 0 on every metric is <c>ok</c>, not <c>empty</c>: the page says each metric sat at 0.
    /// Not enough history in every lane is its own answer, never a band at 0: the page draws no band and no markers and says why.
    /// </summary>
    internal static string BuildReply(string displayName, DateTime startUtc, IReadOnlyList<PgBaselineProvider.BaselineStep>[] lanes)
    {
        var baselines = new JsonObject();
        var any = false;
        for (var i = 0; i < Lanes.Length; i++)
        {
            var usable = lanes[i].Count > 0;
            any |= usable;
            baselines[Lanes[i].Key] = usable ? new JsonObject { ["steps"] = new JsonArray(lanes[i].Select(Step).ToArray<JsonNode?>()) } : null;
        }

        if (!any)
            return McpHelpers.Status("empty",
                "Not enough history yet to draw a baseline: the store holds no samples of CPU, waits, I/O latency or blocking for this hour of the week.");

        return new JsonObject
        {
            ["status"] = "ok",
            ["server"] = displayName,
            ["window_start"] = startUtc.ToString("o", CultureInfo.InvariantCulture),
            ["baselines"] = baselines,
        }.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>
    /// #7644: null when <paramref name="windowStartUtc"/> is a window the page can ask for at <paramref name="nowUtc"/>, else the
    /// <see cref="McpHelpers.Refusal"/> envelope. Refused, never clamped: a shorter window than the one asked would draw a band
    /// for a different span than the strips beside it.
    /// </summary>
    internal static string? ValidateWindowStart(DateTime windowStartUtc, DateTime nowUtc)
    {
        if (windowStartUtc > nowUtc + FutureSkew)
            return McpHelpers.Refusal("window_start", "window_start is in the future. Use the start of the window the strips show, which is never later than now.");
        if (windowStartUtc < nowUtc - TimeSpan.FromHours(MaxWindowHours) - WindowSlack)
            return McpHelpers.Refusal("window_start",
                $"window_start is more than {MaxWindowHours} hours (plus {WindowSlack.TotalHours:0} hours of slack) before now, the longest window the Overview can show. Use a later start.");
        return null;
    }

    /// <summary>
    /// #7644: the four lanes' hourly steps over the window, in <see cref="Lanes"/> order. The series is computed and cached
    /// at <paramref name="nowUtc"/>, not at the window start: the answer holds all 168 hour-of-week buckets, and a
    /// daily-cache arm (I/O latency, CPU, blocking) keys on the UTC day of the instant it is asked at, so a window
    /// start of now minus 24 hours landed on yesterday's key, which the worker's pass never fills. Keyed on now it is the
    /// key the worker uses and the shared tier holds. #7663: an hour keeps its step when it has a band (<see cref="Usable"/>) or sat at exactly 0
    /// (<see cref="IsZeroActivity"/>, which <see cref="Step"/> marks); an hour with no samples has no step.
    /// </summary>
    internal static async Task<IReadOnlyList<PgBaselineProvider.BaselineStep>[]> ReadLaneStepsAsync(
        PgBaselineProvider provider, int serverId, DateTime windowStartUtc, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var tasks = new Task<IReadOnlyList<PgBaselineProvider.BaselineStep>>[Lanes.Length];
        for (var i = 0; i < Lanes.Length; i++)
            tasks[i] = provider.GetBaselineStepsSharedAsync(serverId, Lanes[i].Metric, windowStartUtc, nowUtc, nowUtc, cancellationToken);
        var all = await Task.WhenAll(tasks);
        return all.Select(steps => (IReadOnlyList<PgBaselineProvider.BaselineStep>)steps.Where(s => Usable(s.Bucket) || IsZeroActivity(s.Bucket)).ToList()).ToArray();
    }

    /// <summary>The Viewer's own guard: a bucket with samples and a dispersion above zero draws a band; anything else draws none.</summary>
    internal static bool Usable(BaselineBucket bucket) => bucket.SampleCount > 0 && bucket.EffectiveStdDev > 0;

    /// <summary>
    /// #7663: samples held, and every one read 0 (<see cref="BaselineBucket.EffectiveStdDev"/> is 0 only for mean 0 with no spread). Measured quiet, not
    /// missing history: it draws no band, but the page says so instead of "not enough history".
    /// </summary>
    internal static bool IsZeroActivity(BaselineBucket bucket) => bucket.SampleCount > 0 && bucket.EffectiveStdDev <= 0;

    internal static JsonObject Step(PgBaselineProvider.BaselineStep step)
    {
        var json = StepFields(step);
        if (IsZeroActivity(step.Bucket)) json["zero_activity"] = true;
        return json;
    }

    private static JsonObject StepFields(PgBaselineProvider.BaselineStep step) => new()
    {
        ["from"] = DateTime.SpecifyKind(step.FromUtc, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture),
        ["to"] = DateTime.SpecifyKind(step.ToUtc, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture),
        ["mean"] = step.Bucket.Mean,
        ["effective_std_dev"] = step.Bucket.EffectiveStdDev,
        ["sample_count"] = step.Bucket.SampleCount,
        ["tier"] = step.Bucket.Tier.ToString(),
    };
}
