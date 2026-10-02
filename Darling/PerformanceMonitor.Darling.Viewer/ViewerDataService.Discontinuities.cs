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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's read of the identity-epoch markers a window holds (#3653 A5) — the Storage-side
/// <see cref="BaselineDiscontinuityReader"/> the service's trend tools also run, so the Performance Trends
/// charts mark exactly the instants <c>get_query_duration_trend</c> lists under <c>discontinuities[]</c>, and a
/// user reading the chart and an agent reading the payload are told the same thing. Lite's twin is
/// <c>LocalDataService.GetBaselineDiscontinuitiesAsync</c>.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// The baseline discontinuities for <paramref name="serverId"/> inside [<paramref name="startUtc"/>,
    /// <paramref name="endUtc"/>] (naive UTC, the chart's own window), oldest first; empty when none. One read
    /// per Performance Trends load, shared by the four charts, because the markers are a fact about the window
    /// and not about any one series on it.
    /// </summary>
    public Task<IReadOnlyList<BaselineDiscontinuity>> GetBaselineDiscontinuitiesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
        => BaselineDiscontinuityReader.ReadAsync(
            _dataSource, serverId, startUtc, endUtc, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
}
