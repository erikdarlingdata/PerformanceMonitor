/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// What the Blocking tab's trend charts draw as an event (#4966), so their "Showing since" note names the earlier of the coverage
/// start and the earliest point drawn (<see cref="ViewerEventDataStart.Of"/>). Pure, so it runs without the WPF tab.
/// </summary>
internal static class ViewerBlockingChartsDataStart
{
    /// <summary>The time of each point the Lock Wait Trend draws as data (a rate line; an empty read draws only a flat zero, which is no event).</summary>
    internal static IEnumerable<DateTime?> LockWaitTimesDrawn(IEnumerable<LockWaitTrendPoint> data) =>
        data.Select(p => (DateTime?)p.CollectionTime);

    /// <summary>The time of each bucket a count chart draws as an event: only buckets with a non-zero count (a zero bucket is the chart's baseline).</summary>
    internal static IEnumerable<DateTime?> BlockingTrendTimesDrawn(IEnumerable<BlockingTrendPoint> data) =>
        data.Where(p => p.Count > 0).Select(p => (DateTime?)p.Time);
}
