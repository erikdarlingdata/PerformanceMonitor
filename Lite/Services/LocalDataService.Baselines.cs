/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    private BaselineProvider? _baselineProvider;

    private BaselineProvider GetBaselineProvider()
    {
        /* #3941: the store's shared tier, so the overview lanes and the analysis passes compute a series once per
           analysis hour between them. */
        return _baselineProvider ??= new BaselineProvider(_duckDb, sharedCache: BaselineCache.For(_duckDb));
    }

    /// <summary>
    /// Gets the baseline (mean ± stddev) for a metric at a specific time.
    /// Returns null if no baseline data is available.
    /// </summary>
    public async Task<BaselineBucket> GetBaselineForLaneAsync(
        int serverId, string metricName, DateTime referenceTime)
    {
        var baseline = await GetBaselineProvider().GetBaselineAsync(serverId, metricName, referenceTime);
        return baseline.SampleCount > 0 ? baseline : BaselineBucket.Empty;
    }

    /// <summary>
    /// #7644: the lane's band as one step per local hour of the window, the way the web Overview and the Viewer draw it. An hour with no
    /// usable bucket (no samples, or no dispersion) has no step. The series is computed at now, as the Viewer's and the web page's
    /// are, whatever window the page shows: the 30 days before now, so the lanes share one cache entry per metric and a window in the
    /// past reads the same band as a request at now.
    /// </summary>
    public async Task<IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep>> GetBaselineStepsForLaneAsync(
        int serverId, string metricName, DateTime windowStartUtc, DateTime windowEndUtc)
    {
        var steps = await GetBaselineProvider().GetBaselineStepsAsync(serverId, metricName, windowStartUtc, windowEndUtc, DateTime.UtcNow);
        return ToBandSteps(steps, windowStartUtc, windowEndUtc);
    }

    /// <summary>
    /// The usable hours (a bucket with samples and a dispersion above zero) as lane steps on the OADate axis, clipped to the window
    /// so the first hour does not start before the lane's left edge. The Viewer's <c>ToBandSteps</c>, twinned.
    /// </summary>
    internal static IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep> ToBandSteps(
        IReadOnlyList<BaselineProvider.BaselineStep> steps, DateTime windowStartUtc, DateTime windowEndUtc)
    {
        var result = new List<PerformanceMonitor.Ui.BaselineBand.HourStep>(steps.Count);
        foreach (var step in steps)
        {
            if (step.Bucket.SampleCount <= 0 || step.Bucket.EffectiveStdDev <= 0)
                continue;
            var from = step.FromUtc < windowStartUtc ? windowStartUtc : step.FromUtc;
            var to = step.ToUtc > windowEndUtc ? windowEndUtc : step.ToUtc;
            if (to <= from)
                continue;
            result.Add(PerformanceMonitor.Ui.BaselineBand.MakeStep(from.ToOADate(), to.ToOADate(), step.Bucket.Mean, step.Bucket.EffectiveStdDev));
        }

        return result;
    }
}
