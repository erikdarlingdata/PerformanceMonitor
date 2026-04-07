/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite.Analysis;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    private BaselineProvider? _baselineProvider;

    private BaselineProvider GetBaselineProvider()
    {
        return _baselineProvider ??= new BaselineProvider(_duckDb);
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
}
